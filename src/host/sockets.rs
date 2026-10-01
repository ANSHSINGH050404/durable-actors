use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;

use crate::{
    actor::{
        ActorExecutionResult, ActorKey, ActorSocketConnection, ActorSocketEffect,
        ActorSocketInvocation, ActorSocketPublisher, ActorSocketSource, validate_socket_effects,
    },
    control_plane::socket_ticket::SocketTicket,
    sockets::{SocketRegistry, browser::SocketDispatcher},
};

use super::{ActorHost, actor_runtime::ActorStorage};

pub(crate) struct HostSockets {
    pub registry: SocketRegistry,
    storage: Arc<dyn ActorStorage>,
}

impl HostSockets {
    pub(crate) async fn publish_authorized(
        &self,
        actor: &ActorKey,
        host: &super::HostId,
        epoch: u64,
        effects: Vec<ActorSocketEffect>,
    ) -> Result<()> {
        self.storage.ensure_authority()?;
        self.storage
            .verify_actor_ownership(actor, host, epoch)
            .await?;
        self.publish(actor, effects).await
    }

    pub(crate) fn new(storage: Arc<dyn ActorStorage>) -> Self {
        Self {
            registry: SocketRegistry::default(),
            storage,
        }
    }
}

#[async_trait]
impl ActorSocketPublisher for HostSockets {
    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        self.storage.ensure_authority()?;
        validate_socket_effects(&effects)?;
        self.registry.apply(actor, effects).await;
        Ok(())
    }
}

#[async_trait]
impl ActorSocketSource for HostSockets {
    async fn connections(&self, actor: &ActorKey) -> Result<Vec<ActorSocketConnection>> {
        self.storage.ensure_authority()?;
        Ok(self.registry.connections(actor).await)
    }
}

pub(crate) struct HostSocketDispatcher {
    host: Arc<ActorHost>,
    sockets: Arc<HostSockets>,
    session: String,
    events: Option<tokio::sync::mpsc::Sender<(ActorKey, crate::actor::ActorSocketEvent)>>,
}

impl HostSocketDispatcher {
    pub(crate) fn with_events(
        mut self,
        client: Arc<crate::control_plane::ControlPlaneClient>,
        stop: tokio_util::sync::CancellationToken,
    ) -> Self {
        let (sender, mut events) = tokio::sync::mpsc::channel(128);
        self.events = Some(sender);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    item = events.recv() => {
                        let Some((actor, event)) = item else { break; };
                        if let Err(error) = client.notify_socket_message(actor, event).await {
                            tracing::warn!(error = %error, "socket event notification failed");
                        }
                    }
                }
            }
        });
        self
    }

    pub(crate) fn new(host: Arc<ActorHost>, sockets: Arc<HostSockets>, session: String) -> Self {
        Self {
            host,
            sockets,
            session,
            events: None,
        }
    }
}

#[async_trait]
impl SocketDispatcher for HostSocketDispatcher {
    async fn authorize(&self, ticket: &SocketTicket) -> Result<()> {
        self.ensure_authority()?;
        let target = ticket
            .target
            .as_ref()
            .context("socket ticket has no host binding")?;
        ensure!(
            target.host_id == *self.host.id() && target.session_id == self.session,
            "socket ticket belongs to another host session"
        );
        ensure!(
            target.owner_epoch > 0,
            "socket ticket has no ownership epoch"
        );
        // Checking the persisted fence avoids queueing the upgrade behind a long actor handler.
        self.sockets
            .storage
            .verify_actor_ownership(&ticket.actor, self.host.id(), target.owner_epoch)
            .await?;
        Ok(())
    }

    fn notify(&self, ticket: &SocketTicket, event: &crate::actor::ActorSocketEvent) {
        if matches!(event, crate::actor::ActorSocketEvent::Message { .. })
            && let Some(events) = &self.events
            && events
                .try_send((ticket.actor.clone(), event.clone()))
                .is_err()
        {
            tracing::warn!("socket event notification queue is full");
        }
    }

    fn ensure_authority(&self) -> Result<()> {
        self.sockets.storage.ensure_authority()
    }

    async fn dispatch(
        &self,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        self.dispatch_since(ticket, invocation, std::time::Instant::now())
            .await
    }

    fn discard(
        &self,
        ticket: &SocketTicket,
        event: crate::actor::ActorSocketEvent,
        received: std::time::Instant,
        outcome: crate::request_traces::RequestOutcome,
    ) {
        self.host.discard_socket_event(
            ActorSocketInvocation {
                request_id: uuid::Uuid::new_v4().to_string(),
                actor: ticket.actor.clone(),
                event,
                connections: vec![],
            },
            received,
            outcome,
        );
    }

    async fn dispatch_since(
        &self,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
        received: std::time::Instant,
    ) -> Result<Vec<ActorSocketEffect>> {
        let target = ticket
            .target
            .as_ref()
            .context("socket ticket has no host binding")?;
        match self
            .host
            .handle_socket_event_since(invocation, target.owner_epoch, received)
            .await?
        {
            ActorExecutionResult::Completed { effects, .. } => Ok(effects),
            result => anyhow::bail!("actor socket execution failed: {result:?}"),
        }
    }
}
