use std::{collections::VecDeque, time::Duration};

use axum::extract::ws::{CloseFrame, Message, WebSocket};
use futures_util::SinkExt;
use tokio::task::JoinHandle;

use super::{OutboundMessage, SocketReceiver, SocketRegistry, socket_channel};
use crate::actor::{
    ActorSocketConnection, ActorSocketEffect, ActorSocketEvent, ActorSocketInvocation,
    ActorSocketMessage, validate_socket_effects,
};
use crate::control_plane::socket_ticket::SocketTicket;
use anyhow::{Result, ensure};
use async_trait::async_trait;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(crate) trait SocketDispatcher: Send + Sync {
    fn ensure_authority(&self) -> Result<()>;
    async fn dispatch(
        &self,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>>;
    fn notify(&self, _ticket: &SocketTicket, _event: &ActorSocketEvent) {}
}

#[derive(Clone)]
pub(crate) struct SocketServerState {
    pub registry: SocketRegistry,
    pub dispatcher: Arc<dyn SocketDispatcher>,
    pub stop: CancellationToken,
}

struct Closed {
    code: u16,
    reason: String,
    received: bool,
}

impl Closed {
    fn new(code: u16, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
            received: false,
        }
    }
}

pub(crate) async fn run(mut socket: WebSocket, state: SocketServerState, ticket: SocketTicket) {
    let connection = ActorSocketConnection {
        id: uuid::Uuid::new_v4().to_string(),
        metadata: ticket.metadata.clone(),
        tags: vec![],
    };
    let (sender, receiver) = socket_channel();
    if !state
        .registry
        .insert(&ticket.actor, connection.clone(), sender)
        .await
    {
        close(
            &mut socket,
            &Closed::new(1013, "actor connection limit reached"),
        )
        .await;
        return;
    }
    let mut session = Session {
        state,
        ticket,
        connection,
        outbound: receiver,
        pending: VecDeque::new(),
        handler: None,
    };
    session.start(ActorSocketEvent::Connect {
        connection: session.connection.clone(),
    });
    let closed = session
        .run(&mut socket)
        .await
        .unwrap_or_else(|closed| closed);
    let was_clean = close(&mut socket, &closed).await;
    session.disconnect(closed, was_clean).await;
}

struct Session {
    state: SocketServerState,
    ticket: SocketTicket,
    connection: ActorSocketConnection,
    outbound: SocketReceiver,
    pending: VecDeque<ActorSocketMessage>,
    handler: Option<JoinHandle<bool>>,
}

impl Session {
    async fn run(&mut self, socket: &mut WebSocket) -> Result<Closed, Closed> {
        let mut authority_checks = tokio::time::interval(Duration::from_secs(1));
        loop {
            let remaining = self.remaining()?;
            tokio::select! {
                biased;
                _ = self.state.stop.cancelled() => return Err(Closed::new(1012, "socket gateway stopping")),
                _ = authority_checks.tick() => {
                    self.state.dispatcher.ensure_authority().map_err(|_| Closed::new(1012, "socket gateway lease expired"))?;
                }
                _ = tokio::time::sleep(remaining) => return Err(Closed::new(4408, "socket authorization expired")),
                result = async { self.handler.as_mut().unwrap().await }, if self.handler.is_some() => {
                    self.handler.take();
                    if !result.unwrap_or(false) { return Err(Closed::new(4400, "actor socket handler failed")); }
                    if let Some(message) = self.pending.pop_front() {
                        self.start_message(message);
                    }
                }
                inbound = socket.recv() => self.receive(socket, inbound).await?,
                outbound = self.outbound.recv() => self.send_outbound(socket, outbound.ok_or_else(|| Closed::new(1006, "socket closed"))?).await?,
            }
        }
    }

    async fn receive(
        &mut self,
        socket: &mut WebSocket,
        inbound: Option<Result<Message, axum::Error>>,
    ) -> Result<(), Closed> {
        match inbound {
            Some(Ok(Message::Text(text))) => {
                if let Some(response) = self
                    .state
                    .registry
                    .auto_response(&self.ticket.actor, &text)
                    .await
                {
                    self.send_frame(socket, Message::Text(response.into()))
                        .await
                } else {
                    self.enqueue(ActorSocketMessage::Text {
                        data: text.to_string(),
                    })
                }
            }
            Some(Ok(Message::Ping(data))) => self.send_frame(socket, Message::Pong(data)).await,
            Some(Ok(Message::Pong(_))) => Ok(()),
            Some(Ok(Message::Close(frame))) => Err(Closed {
                code: frame.as_ref().map_or(1005, |frame| frame.code),
                reason: frame.map_or_else(String::new, |frame| frame.reason.to_string()),
                received: true,
            }),
            Some(Ok(Message::Binary(_))) => {
                Err(Closed::new(4400, "socket messages must be JSON text"))
            }
            Some(Err(error)) if super::message_too_large(&error) => {
                Err(Closed::new(1009, "message exceeds 32 MiB"))
            }
            Some(Err(_)) | None => Err(Closed::new(1006, "transport closed")),
        }
    }

    fn enqueue(&mut self, message: ActorSocketMessage) -> Result<(), Closed> {
        if self.handler.is_none() {
            self.start_message(message);
        } else {
            self.pending.push_back(message);
        }
        Ok(())
    }

    fn start_message(&mut self, message: ActorSocketMessage) {
        self.start(ActorSocketEvent::Message {
            connection_id: self.connection.id.clone(),
            message,
        });
    }

    fn start(&mut self, event: ActorSocketEvent) {
        let state = self.state.clone();
        let ticket = self.ticket.clone();
        self.handler = Some(tokio::spawn(async move {
            dispatch(&state, &ticket, event).await.is_ok()
        }));
    }

    async fn send_outbound(
        &self,
        socket: &mut WebSocket,
        outbound: OutboundMessage,
    ) -> Result<(), Closed> {
        match outbound {
            OutboundMessage::Control(value) => {
                self.send_frame(socket, Message::Text(value.to_string().into()))
                    .await
            }
            OutboundMessage::Message(ActorSocketMessage::Text { data }) => {
                self.send_frame(socket, Message::Text(data.into())).await
            }
            OutboundMessage::Message(_) => {
                Err(Closed::new(4400, "actor produced a binary message"))
            }
            OutboundMessage::Close { code, reason } => Err(Closed::new(code, reason)),
        }
    }

    async fn send_frame(&self, socket: &mut WebSocket, frame: Message) -> Result<(), Closed> {
        self.state
            .dispatcher
            .ensure_authority()
            .map_err(|_| Closed::new(1012, "socket gateway lease expired"))?;
        let deadline = tokio::time::Instant::now() + self.remaining()?;
        let mut authority_checks = tokio::time::interval(Duration::from_secs(1));
        let send = socket.send(frame);
        tokio::pin!(send);
        loop {
            tokio::select! {
                biased;
                _ = self.state.stop.cancelled() => return Err(Closed::new(1012, "socket gateway stopping")),
                _ = tokio::time::sleep_until(deadline) => return Err(Closed::new(4408, "socket authorization expired")),
                _ = authority_checks.tick() => self.state.dispatcher.ensure_authority().map_err(|_| Closed::new(1012, "socket gateway lease expired"))?,
                result = &mut send => return result.map_err(|_| Closed::new(1006, "transport closed")),
            }
        }
    }

    fn remaining(&self) -> Result<Duration, Closed> {
        let millis = self.ticket.authorized_until_ms - now_ms();
        if millis <= 0 {
            return Err(Closed::new(4408, "socket authorization expired"));
        }
        Ok(Duration::from_millis(millis as u64))
    }

    async fn disconnect(mut self, closed: Closed, was_clean: bool) {
        self.pending.clear();
        let connection = self
            .state
            .registry
            .remove(&self.ticket.actor, &self.connection.id)
            .await
            .unwrap_or(self.connection.clone());
        if let Some(handler) = self.handler.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handler).await;
        }
        let event = ActorSocketEvent::Disconnect {
            connection,
            code: closed.code,
            reason: closed.reason,
            was_clean,
        };
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            dispatch(&self.state, &self.ticket, event),
        )
        .await;
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

async fn close(socket: &mut WebSocket, closed: &Closed) -> bool {
    if closed.code == 1006 {
        return false;
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        if closed.received {
            return socket.flush().await.is_ok();
        }
        if socket
            .send(Message::Close(Some(CloseFrame {
                code: closed.code,
                reason: closed.reason.clone().into(),
            })))
            .await
            .is_err()
        {
            return false;
        }
        while let Some(Ok(message)) = socket.recv().await {
            if matches!(message, Message::Close(_)) {
                return socket.flush().await.is_ok();
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

async fn dispatch(
    state: &SocketServerState,
    ticket: &SocketTicket,
    event: ActorSocketEvent,
) -> Result<()> {
    ensure!(!state.stop.is_cancelled(), "socket gateway stopping");
    state.dispatcher.ensure_authority()?;
    let (event, connections) = state.registry.prepare_event(&ticket.actor, event).await;
    let invocation = ActorSocketInvocation {
        request_id: uuid::Uuid::new_v4().to_string(),
        actor: ticket.actor.clone(),
        event: event.clone(),
        connections,
    };
    let effects = state.dispatcher.dispatch(ticket, invocation).await?;
    state.dispatcher.ensure_authority()?;
    validate_socket_effects(&effects)?;
    if let ActorSocketEvent::Connect { connection } = &event {
        let rejected = effects.iter().any(|effect| {
            matches!(effect,
            ActorSocketEffect::Close { connection_id, .. }
                | ActorSocketEffect::Reject { connection_id, .. } if connection_id == &connection.id)
        });
        if !rejected {
            state.registry.activate(&ticket.actor, &connection.id).await;
        }
    }
    state.registry.apply(&ticket.actor, effects).await;
    state.dispatcher.notify(ticket, &event);
    Ok(())
}

#[cfg(test)]
#[path = "../../tests/unit/sockets/browser.rs"]
mod tests;
