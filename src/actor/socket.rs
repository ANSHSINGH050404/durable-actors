use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::executor_connection::{ActorSocketEffect, ActorSocketMessage};
use super::{ActorKey, ActorSocketConnection};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SocketQuery {
    pub tag: Option<String>,
    #[serde(default)]
    pub count_only: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum SocketLookup {
    Connections(Vec<ActorSocketConnection>),
    Count(usize),
}

#[async_trait]
pub(crate) trait ActorSocketSource: Send + Sync {
    async fn query(&self, actor: &ActorKey, query: SocketQuery) -> Result<SocketLookup>;
}

pub(crate) const MAX_SOCKET_METADATA_BYTES: usize = 16 * 1024;

const MAX_SOCKET_CONNECTION_ID_BYTES: usize = 128;
const MAX_SOCKET_TAGS: usize = 10;
const MAX_SOCKET_TAG_CHARACTERS: usize = 256;
const MAX_SOCKET_CLOSE_REASON_BYTES: usize = 123;

pub(crate) fn validate_socket_metadata(metadata: &Value) -> Result<()> {
    ensure!(
        serde_json::to_vec(metadata)?.len() <= MAX_SOCKET_METADATA_BYTES,
        "socket metadata exceeds {MAX_SOCKET_METADATA_BYTES} bytes"
    );
    Ok(())
}

pub(crate) fn validate_socket_effects(effects: &[ActorSocketEffect]) -> Result<()> {
    for effect in effects {
        validate_socket_effect(effect)?;
    }
    Ok(())
}

fn validate_socket_effect(effect: &ActorSocketEffect) -> Result<()> {
    match effect {
        ActorSocketEffect::SetAutoResponse { request, response } => {
            ensure!(
                request.is_some() == response.is_some(),
                "automatic response requires both request and response"
            );
            ensure!(
                request
                    .iter()
                    .chain(response.iter())
                    .all(|value| value.chars().count() <= 2048),
                "automatic response exceeds 2048 characters"
            );
            Ok(())
        }
        ActorSocketEffect::StateSnapshot {
            connection_id,
            state,
            ..
        } => {
            validate_connection_id(connection_id)?;
            validate_public_state(state)
        }
        ActorSocketEffect::StateUpdate {
            changes,
            removed,
            except_connection_ids,
            ..
        } => {
            validate_public_state(changes)?;
            ensure!(
                removed.iter().all(|field| changes.get(field).is_none()),
                "state field is both changed and removed"
            );
            for connection_id in except_connection_ids {
                validate_connection_id(connection_id)?;
            }
            Ok(())
        }
        ActorSocketEffect::Send {
            connection_id,
            message,
        } => {
            validate_connection_id(connection_id)?;
            validate_socket_message(message)
        }
        ActorSocketEffect::Broadcast {
            message,
            except_connection_ids,
            tags,
            ..
        } => {
            for connection_id in except_connection_ids {
                validate_connection_id(connection_id)?;
            }
            validate_socket_tags(tags)?;
            validate_socket_message(message)
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
            validate_connection_id(connection_id)?;
            validate_close(*code, reason)
        }
        ActorSocketEffect::SetMetadata {
            connection_id,
            metadata,
        } => {
            validate_connection_id(connection_id)?;
            validate_socket_metadata(metadata)
        }
        ActorSocketEffect::SetTags {
            connection_id,
            tags,
        } => {
            validate_connection_id(connection_id)?;
            validate_socket_tags(tags)
        }
    }
}

fn validate_public_state(state: &Value) -> Result<()> {
    ensure!(state.is_object(), "public state must be a JSON object");
    Ok(())
}

fn validate_socket_message(message: &ActorSocketMessage) -> Result<()> {
    if let ActorSocketMessage::Binary { data } = message {
        STANDARD
            .decode(data)
            .context("socket binary message is not valid base64")?;
    }
    Ok(())
}

fn validate_connection_id(connection_id: &str) -> Result<()> {
    ensure!(!connection_id.is_empty(), "socket connection ID is empty");
    ensure!(
        connection_id.len() <= MAX_SOCKET_CONNECTION_ID_BYTES,
        "socket connection ID exceeds {MAX_SOCKET_CONNECTION_ID_BYTES} bytes"
    );
    Ok(())
}

fn validate_socket_tags(tags: &[String]) -> Result<()> {
    ensure!(
        tags.len() <= MAX_SOCKET_TAGS,
        "socket tags exceed {MAX_SOCKET_TAGS} entries"
    );
    for tag in tags {
        ensure!(!tag.is_empty(), "socket tag is empty");
        ensure!(
            tag.chars().count() <= MAX_SOCKET_TAG_CHARACTERS,
            "socket tag exceeds {MAX_SOCKET_TAG_CHARACTERS} characters"
        );
    }
    Ok(())
}

fn validate_close(code: u16, reason: &str) -> Result<()> {
    ensure!(
        code == 1000 || (3000..=4999).contains(&code),
        "socket close code must be 1000 or between 3000 and 4999"
    );
    ensure!(
        reason.len() <= MAX_SOCKET_CLOSE_REASON_BYTES,
        "socket close reason exceeds {MAX_SOCKET_CLOSE_REASON_BYTES} bytes"
    );
    Ok(())
}
