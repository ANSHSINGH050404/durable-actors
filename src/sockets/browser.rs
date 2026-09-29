use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use axum::{
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::StatusCode,
    response::Response,
};
use serde::Deserialize;
use tokio::task::JoinHandle;

use super::{OutboundMessage, SocketReceiver, SocketRegistry, socket_channel};
use crate::actor::{
    ActorSocketConnection, ActorSocketEffect, ActorSocketEvent, ActorSocketInvocation,
    ActorSocketMessage, validate_socket_effects,
};
use crate::control_plane::socket_ticket::{SocketTicket, SocketTicketVerifier};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(crate) trait SocketDispatcher: Send + Sync {
    async fn authorize(&self, ticket: &SocketTicket) -> Result<()>;
    fn ensure_authority(&self) -> Result<()>;
    async fn dispatch(
        &self,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>>;
    fn notify(&self, _ticket: &SocketTicket, _event: &ActorSocketEvent) {}
    async fn dispatch_since(
        &self,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
        _received: Instant,
    ) -> Result<Vec<ActorSocketEffect>> {
        self.dispatch(ticket, invocation).await
    }
    fn discard(
        &self,
        _ticket: &SocketTicket,
        _event: ActorSocketEvent,
        _received: Instant,
        _outcome: crate::request_traces::RequestOutcome,
    ) {
    }
}

#[derive(Clone)]
pub(crate) struct SocketServerState {
    pub registry: SocketRegistry,
    pub verifier: SocketTicketVerifier,
    pub dispatcher: Arc<dyn SocketDispatcher>,
    pub stop: CancellationToken,
}

pub(crate) fn router(state: SocketServerState) -> axum::Router {
    axum::Router::new()
        .route("/v1/socket", axum::routing::get(connect))
        .layer(axum::extract::DefaultBodyLimit::disable())
        .with_state(state)
}

#[derive(Deserialize)]
pub(super) struct SocketQuery {
    key: Option<String>,
}

type Closed = (u16, &'static str);

pub(super) async fn connect(
    State(state): State<SocketServerState>,
    Query(query): Query<SocketQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let ticket = state
        .verifier
        .verify(query.key.as_deref().unwrap_or_default())
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    state
        .dispatcher
        .authorize(&ticket)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    if state.stop.is_cancelled() {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(upgrade
        .max_frame_size(usize::MAX)
        .max_message_size(usize::MAX)
        .on_upgrade(move |socket| run(socket, state, ticket)))
}

async fn run(mut socket: WebSocket, state: SocketServerState, ticket: SocketTicket) {
    let connection = ActorSocketConnection {
        id: uuid::Uuid::new_v4().to_string(),
        metadata: ticket.metadata.clone(),
        tags: vec![],
    };
    let (sender, receiver) = socket_channel();
    if !state
        .registry
        .insert(&ticket.actor, connection.clone(), sender, None)
        .await
    {
        close(&mut socket, (1013, "actor connection limit reached")).await;
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
    close(&mut socket, closed).await;
    session.disconnect(closed).await;
}

struct Session {
    state: SocketServerState,
    ticket: SocketTicket,
    connection: ActorSocketConnection,
    outbound: SocketReceiver,
    pending: VecDeque<(ActorSocketMessage, Instant)>,
    handler: Option<JoinHandle<bool>>,
}

impl Session {
    async fn run(&mut self, socket: &mut WebSocket) -> Result<Closed, Closed> {
        let mut authority_checks = tokio::time::interval(Duration::from_secs(1));
        loop {
            let remaining = self.remaining()?;
            tokio::select! {
                biased;
                _ = self.state.stop.cancelled() => return Err((1012, "actor host stopping")),
                _ = authority_checks.tick() => {
                    self.state.dispatcher.ensure_authority().map_err(|_| (1012, "actor host lease expired"))?;
                }
                _ = tokio::time::sleep(remaining) => return Err((4408, "socket authorization expired")),
                result = async { self.handler.as_mut().unwrap().await }, if self.handler.is_some() => {
                    self.handler.take();
                    if !result.unwrap_or(false) { return Err((4400, "actor socket handler failed")); }
                    if let Some((message, received)) = self.pending.pop_front() { self.start_message(message, received); }
                }
                inbound = socket.recv() => self.receive(socket, inbound).await?,
                outbound = self.outbound.recv() => self.send_outbound(socket, outbound.ok_or((1006, "socket closed"))?).await?,
            }
        }
    }

    async fn receive(
        &mut self,
        socket: &mut WebSocket,
        inbound: Option<Result<Message, axum::Error>>,
    ) -> Result<(), Closed> {
        match inbound {
            Some(Ok(Message::Text(text))) => self.enqueue(ActorSocketMessage::Text {
                data: text.to_string(),
            }),
            Some(Ok(Message::Ping(data))) => self.send_frame(socket, Message::Pong(data)).await,
            Some(Ok(Message::Pong(_))) => Ok(()),
            Some(Ok(Message::Close(_))) => Err((1000, "client closed")),
            Some(Ok(Message::Binary(_))) => Err((4400, "socket messages must be JSON text")),
            Some(Err(_)) | None => Err((1006, "transport closed")),
        }
    }

    fn enqueue(&mut self, message: ActorSocketMessage) -> Result<(), Closed> {
        let received = Instant::now();
        if self.handler.is_none() {
            self.start_message(message, received);
        } else if self.pending.len() < 32 {
            self.pending.push_back((message, received));
        } else {
            self.discard(
                message,
                received,
                crate::request_traces::RequestOutcome::Rejected,
            );
            return Err((1013, "socket operation queue is full"));
        }
        Ok(())
    }

    fn start_message(&mut self, message: ActorSocketMessage, received: Instant) {
        self.start_since(
            ActorSocketEvent::Message {
                connection_id: self.connection.id.clone(),
                message,
            },
            received,
        );
    }

    fn start(&mut self, event: ActorSocketEvent) {
        self.start_since(event, Instant::now());
    }

    fn start_since(&mut self, event: ActorSocketEvent, received: Instant) {
        let state = self.state.clone();
        let ticket = self.ticket.clone();
        self.handler = Some(tokio::spawn(async move {
            dispatch_since(&state, &ticket, event, received)
                .await
                .is_ok()
        }));
    }

    fn discard(
        &self,
        message: ActorSocketMessage,
        received: Instant,
        outcome: crate::request_traces::RequestOutcome,
    ) {
        self.state.dispatcher.discard(
            &self.ticket,
            ActorSocketEvent::Message {
                connection_id: self.connection.id.clone(),
                message,
            },
            received,
            outcome,
        );
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
            OutboundMessage::Message(_) => Err((4400, "actor produced a binary message")),
            OutboundMessage::Close { code, reason } => {
                let _ = self
                    .send_frame(
                        socket,
                        Message::Close(Some(CloseFrame {
                            code,
                            reason: reason.into(),
                        })),
                    )
                    .await;
                Err((code, "actor closed connection"))
            }
        }
    }

    async fn send_frame(&self, socket: &mut WebSocket, frame: Message) -> Result<(), Closed> {
        self.state
            .dispatcher
            .ensure_authority()
            .map_err(|_| (1012, "actor host lease expired"))?;
        tokio::time::timeout(
            self.remaining()?.min(Duration::from_secs(5)),
            socket.send(frame),
        )
        .await
        .map_err(|_| (4408, "socket delivery timed out"))?
        .map_err(|_| (1006, "transport closed"))
    }

    fn remaining(&self) -> Result<Duration, Closed> {
        let millis = self.ticket.authorized_until_ms - now_ms();
        if millis <= 0 {
            return Err((4408, "socket authorization expired"));
        }
        Ok(Duration::from_millis(millis as u64))
    }

    async fn disconnect(mut self, closed: Closed) {
        while let Some((message, received)) = self.pending.pop_front() {
            self.discard(
                message,
                received,
                crate::request_traces::RequestOutcome::Interrupted,
            );
        }
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
            code: closed.0,
            reason: closed.1.into(),
            was_clean: closed.0 == 1000,
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

async fn close(socket: &mut WebSocket, (code, reason): Closed) {
    if code == 1006 {
        return;
    }
    let _ = tokio::time::timeout(
        Duration::from_secs(1),
        socket.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}

async fn dispatch(
    state: &SocketServerState,
    ticket: &SocketTicket,
    event: ActorSocketEvent,
) -> Result<()> {
    dispatch_since(state, ticket, event, Instant::now()).await
}

async fn dispatch_since(
    state: &SocketServerState,
    ticket: &SocketTicket,
    event: ActorSocketEvent,
    received: Instant,
) -> Result<()> {
    ensure!(!state.stop.is_cancelled(), "actor host stopping");
    state.dispatcher.ensure_authority()?;
    let (event, connections) = state.registry.prepare_event(&ticket.actor, event).await;
    let invocation = ActorSocketInvocation {
        request_id: uuid::Uuid::new_v4().to_string(),
        actor: ticket.actor.clone(),
        event: event.clone(),
        connections,
    };
    let effects = state
        .dispatcher
        .dispatch_since(ticket, invocation, received)
        .await?;
    state.dispatcher.ensure_authority()?;
    validate_socket_effects(&effects)?;
    state.registry.apply(&ticket.actor, effects).await;
    if let ActorSocketEvent::Connect { connection } = &event {
        state.registry.activate(&ticket.actor, &connection.id).await;
    }
    state.dispatcher.notify(ticket, &event);
    Ok(())
}
