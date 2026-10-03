use crate::actor::{
    ActorKey, ActorSocketConnection, ActorSocketEffect, ActorSocketEvent, ActorSocketMessage,
    ActorSocketTagMatch,
};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{RwLock, mpsc};

pub(crate) mod browser;
pub(crate) mod operations;
pub(crate) const DEFAULT_MAX_CONNECTIONS: usize = 32768;
pub(crate) const READ_BUFFER_BYTES: usize = 8 * 1024;
pub(crate) const WRITE_BUFFER_BYTES: usize = 8 * 1024;
pub(crate) const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

pub(crate) fn max_connections(
    get: &mut impl FnMut(&str) -> Option<String>,
) -> anyhow::Result<usize> {
    let value = get("DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS")
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);
    anyhow::ensure!(
        (1..=DEFAULT_MAX_CONNECTIONS).contains(&value),
        "DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS must be between 1 and 32768"
    );
    Ok(value)
}

pub(crate) fn message_too_large(error: &axum::Error) -> bool {
    use std::error::Error;
    matches!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<tokio_tungstenite::tungstenite::Error>()),
        Some(tokio_tungstenite::tungstenite::Error::Capacity(_))
    )
}

#[derive(Clone)]
pub(crate) struct SocketRegistry {
    max_connections: usize,
    entries: Arc<RwLock<HashMap<ActorKey, RoomSockets>>>,
}

#[derive(Default)]
struct RoomSockets {
    connections: HashMap<String, RegisteredSocket>,
    active: usize,
    auto_response: Option<(String, String)>,
}

impl std::ops::Deref for RoomSockets {
    type Target = HashMap<String, RegisteredSocket>;
    fn deref(&self) -> &Self::Target {
        &self.connections
    }
}

impl std::ops::DerefMut for RoomSockets {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connections
    }
}

#[derive(Clone)]
struct RegisteredSocket {
    connection: ActorSocketConnection,
    outbound: SocketSender,
    open: bool,
    state_ready: bool,
}

pub(crate) enum OutboundMessage {
    Message(ActorSocketMessage),
    Control(Value),
    Close { code: u16, reason: String },
}

impl Default for SocketRegistry {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            entries: Default::default(),
        }
    }
}

impl SocketRegistry {
    pub(crate) fn with_max_connections(max_connections: usize) -> Self {
        Self {
            max_connections,
            ..Self::default()
        }
    }

    pub(crate) async fn inventory(
        &self,
        project: &str,
    ) -> Vec<crate::host_leases::ActorSocketInventory> {
        let entries = self.entries.read().await;
        let mut inventory = Vec::new();
        for (actor, sockets) in entries
            .iter()
            .filter(|(actor, _)| actor.project_id == project)
        {
            let mut connections: Vec<_> = sockets
                .values()
                .filter(|entry| entry.open)
                .map(|entry| entry.connection.clone())
                .collect();
            connections.sort_by(|a, b| a.id.cmp(&b.id));
            if !connections.is_empty() {
                inventory.push(crate::host_leases::ActorSocketInventory {
                    actor: actor.clone(),
                    connections,
                });
            }
        }
        inventory.sort_by(|a, b| {
            (&a.actor.actor_name, &a.actor.actor_id).cmp(&(&b.actor.actor_name, &b.actor.actor_id))
        });
        inventory
    }

    pub(crate) async fn insert(
        &self,
        actor: &ActorKey,
        connection: ActorSocketConnection,
        outbound: SocketSender,
    ) -> bool {
        let mut entries = self.entries.write().await;
        let connections = entries.entry(actor.clone()).or_default();
        if connections.len() >= self.max_connections {
            return false;
        }
        connections.insert(
            connection.id.clone(),
            RegisteredSocket {
                connection,
                outbound,
                open: false,
                state_ready: false,
            },
        );
        true
    }

    pub(crate) async fn remove(
        &self,
        actor: &ActorKey,
        connection_id: &str,
    ) -> Option<ActorSocketConnection> {
        let mut entries = self.entries.write().await;
        let connections = entries.get_mut(actor)?;
        let removed = connections.remove(connection_id).map(|entry| {
            if entry.open {
                connections.active -= 1;
            }
            entry.connection
        });
        if connections.is_empty() && connections.auto_response.is_none() {
            entries.remove(actor);
        }
        removed
    }

    pub(crate) async fn connections_with_tag(
        &self,
        actor: &ActorKey,
        tag: Option<&str>,
    ) -> Vec<ActorSocketConnection> {
        self.entries
            .read()
            .await
            .get(actor)
            .map(|room| {
                room.values()
                    .filter(|entry| {
                        entry.open
                            && tag.is_none_or(|tag| {
                                entry.connection.tags.iter().any(|value| value == tag)
                            })
                    })
                    .map(|entry| entry.connection.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) async fn count(&self, actor: &ActorKey) -> usize {
        self.entries
            .read()
            .await
            .get(actor)
            .map_or(0, |room| room.active)
    }

    pub(crate) async fn auto_response(&self, actor: &ActorKey, message: &str) -> Option<String> {
        self.entries
            .read()
            .await
            .get(actor)
            .and_then(|room| room.auto_response.as_ref())
            .filter(|(request, _)| request == message)
            .map(|(_, response)| response.clone())
    }

    pub(crate) async fn prepare_event(
        &self,
        actor: &ActorKey,
        event: ActorSocketEvent,
    ) -> (ActorSocketEvent, Vec<ActorSocketConnection>) {
        match event {
            ActorSocketEvent::Connect { connection } => {
                let connections = vec![connection.clone()];
                (ActorSocketEvent::Connect { connection }, connections)
            }
            ActorSocketEvent::Message {
                connection_id,
                message,
            } => {
                let connections = self
                    .entries
                    .read()
                    .await
                    .get(actor)
                    .and_then(|entries| entries.get(&connection_id))
                    .map(|entry| entry.connection.clone())
                    .into_iter()
                    .collect();
                (
                    ActorSocketEvent::Message {
                        connection_id,
                        message,
                    },
                    connections,
                )
            }
            ActorSocketEvent::Disconnect {
                connection,
                code,
                reason,
                was_clean,
            } => {
                let connection = self
                    .remove(actor, &connection.id)
                    .await
                    .unwrap_or(connection);
                (
                    ActorSocketEvent::Disconnect {
                        connection,
                        code,
                        reason,
                        was_clean,
                    },
                    vec![],
                )
            }
        }
    }

    pub(crate) async fn apply(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) {
        for effect in effects {
            self.apply_one(actor, effect).await;
        }
    }

    async fn apply_one(&self, actor: &ActorKey, effect: ActorSocketEffect) {
        match effect {
            ActorSocketEffect::SetAutoResponse { request, response } => {
                self.entries
                    .write()
                    .await
                    .entry(actor.clone())
                    .or_default()
                    .auto_response = request.zip(response);
            }
            ActorSocketEffect::StateSnapshot {
                connection_id,
                state,
                version,
            } => {
                let Some(version) = version else {
                    return;
                };
                let mut entries = self.entries.write().await;
                if let Some(entry) = entries
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.state_ready = true;
                    let _ = entry.outbound.send(OutboundMessage::Control(
                        serde_json::json!({ "type":"state", "state":state, "version":version }),
                    ));
                }
            }
            ActorSocketEffect::StateUpdate {
                changes,
                removed,
                except_connection_ids,
                version,
            } => {
                let Some(version) = version else {
                    return;
                };
                let except_connection_ids: HashSet<_> = except_connection_ids.into_iter().collect();
                let entries = self.entries.read().await;
                if let Some(connections) = entries.get(actor) {
                    let value = serde_json::json!({ "type":"state_update", "changes":changes, "removed":removed, "version":version });
                    for entry in connections.values().filter(|entry| {
                        entry.state_ready && !except_connection_ids.contains(&entry.connection.id)
                    }) {
                        let _ = entry.outbound.send(OutboundMessage::Control(value.clone()));
                    }
                }
            }
            ActorSocketEffect::Broadcast {
                message,
                except_connection_ids,
                tags,
                tag_match,
            } => {
                let except_connection_ids: HashSet<_> = except_connection_ids.into_iter().collect();
                let recipients = self
                    .entries
                    .read()
                    .await
                    .get(actor)
                    .map(|connections| {
                        connections
                            .values()
                            .filter(|entry| {
                                entry.open
                                    && !except_connection_ids.contains(&entry.connection.id)
                                    && (tags.is_empty()
                                        || match tag_match {
                                            ActorSocketTagMatch::All => tags
                                                .iter()
                                                .all(|tag| entry.connection.tags.contains(tag)),
                                            ActorSocketTagMatch::Any => tags
                                                .iter()
                                                .any(|tag| entry.connection.tags.contains(tag)),
                                        })
                            })
                            .map(|entry| entry.outbound.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                for sender in recipients {
                    let _ = sender.send(OutboundMessage::Message(message.clone()));
                }
            }
            ActorSocketEffect::SetMetadata {
                connection_id,
                metadata,
            } => {
                if let Some(entry) = self
                    .entries
                    .write()
                    .await
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.connection.metadata = metadata;
                }
            }
            ActorSocketEffect::SetTags {
                connection_id,
                tags,
            } => {
                if let Some(entry) = self
                    .entries
                    .write()
                    .await
                    .get_mut(actor)
                    .and_then(|connections| connections.get_mut(&connection_id))
                {
                    entry.connection.tags = tags;
                }
            }
            ActorSocketEffect::Send {
                connection_id,
                message,
            } => {
                if let Some(sender) = self.sender(actor, &connection_id).await {
                    let _ = sender.send(OutboundMessage::Message(message));
                }
            }
            ActorSocketEffect::Close {
                connection_id,
                code,
                reason,
            }
            | ActorSocketEffect::Reject {
                connection_id,
                code,
                reason,
            } => {
                if let Some(sender) = self.sender(actor, &connection_id).await {
                    let _ = sender.send(OutboundMessage::Close { code, reason });
                }
            }
        }
    }

    async fn sender(&self, actor: &ActorKey, connection_id: &str) -> Option<SocketSender> {
        self.entries
            .read()
            .await
            .get(actor)
            .and_then(|connections| connections.get(connection_id))
            .map(|entry| entry.outbound.clone())
    }

    pub(crate) async fn activate(&self, actor: &ActorKey, connection_id: &str) {
        let mut entries = self.entries.write().await;
        if let Some(room) = entries.get_mut(actor) {
            if let Some(entry) = room.get_mut(connection_id) {
                if !entry.open {
                    entry.open = true;
                    room.active += 1;
                }
            }
        }
    }
}

pub(crate) type SocketSender = mpsc::UnboundedSender<OutboundMessage>;
pub(crate) type SocketReceiver = mpsc::UnboundedReceiver<OutboundMessage>;

pub(crate) fn socket_channel() -> (SocketSender, SocketReceiver) {
    mpsc::unbounded_channel()
}

#[cfg(test)]
#[path = "../../tests/unit/sockets/mod.rs"]
mod tests;
