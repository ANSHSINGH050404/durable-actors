use super::actor_runtime::ActorStorage;
use crate::{
    actor::{
        ActorKey, ActorSocketEffect, ActorSocketPublisher, ActorSocketSource, SocketLookup,
        SocketQuery, validate_socket_effects,
    },
    sockets::operations::{SocketOperation, SocketOperationReply, SocketOperations},
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

pub(crate) struct HostSockets {
    storage: Arc<dyn ActorStorage>,
    gateway: Arc<dyn SocketOperations>,
}

impl HostSockets {
    pub(crate) fn new(storage: Arc<dyn ActorStorage>, gateway: Arc<dyn SocketOperations>) -> Self {
        Self { storage, gateway }
    }

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
}

#[async_trait]
impl ActorSocketPublisher for HostSockets {
    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
        self.storage.ensure_authority()?;
        validate_socket_effects(&effects)?;
        if !effects.is_empty() {
            self.gateway
                .execute(actor, SocketOperation::Publish { effects })
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ActorSocketSource for HostSockets {
    async fn query(&self, actor: &ActorKey, query: SocketQuery) -> Result<SocketLookup> {
        self.storage.ensure_authority()?;
        let operation = if query.count_only {
            SocketOperation::Count
        } else {
            SocketOperation::Connections { tag: query.tag }
        };
        match self.gateway.execute(actor, operation).await? {
            SocketOperationReply::Connections { connections } => {
                Ok(SocketLookup::Connections(connections))
            }
            SocketOperationReply::Count { count } => Ok(SocketLookup::Count(count)),
            _ => anyhow::bail!("unexpected connection lookup reply"),
        }
    }
}
