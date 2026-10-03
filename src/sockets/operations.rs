use crate::actor::{ActorKey, ActorSocketConnection, ActorSocketEffect};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum SocketOperation {
    Publish { effects: Vec<ActorSocketEffect> },
    Connections { tag: Option<String> },
    Count,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum SocketOperationReply {
    Published,
    Connections {
        connections: Vec<ActorSocketConnection>,
    },
    Count {
        count: usize,
    },
}

#[async_trait]
pub(crate) trait SocketOperations: Send + Sync {
    async fn execute(
        &self,
        actor: &ActorKey,
        operation: SocketOperation,
    ) -> Result<SocketOperationReply>;
}

#[async_trait]
impl SocketOperations for super::SocketRegistry {
    async fn execute(
        &self,
        actor: &ActorKey,
        operation: SocketOperation,
    ) -> Result<SocketOperationReply> {
        match operation {
            SocketOperation::Publish { effects } => {
                crate::actor::validate_socket_effects(&effects)?;
                self.apply(actor, effects).await;
                Ok(SocketOperationReply::Published)
            }
            SocketOperation::Connections { tag } => Ok(SocketOperationReply::Connections {
                connections: self.connections_with_tag(actor, tag.as_deref()).await,
            }),
            SocketOperation::Count => Ok(SocketOperationReply::Count {
                count: self.count(actor).await,
            }),
        }
    }
}
