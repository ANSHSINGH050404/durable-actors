use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::actor::ActorKey;

mod stream;
pub use stream::{SnapshotRef, StateStream};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WritePlan {
    pub stream: StateStream,
    pub state_version: u64,
    pub object_name: String,
}

#[async_trait]
pub trait SnapshotReader: Send + Sync {
    async fn read_snapshot(&self, region: &str, object: &str) -> Result<bytes::Bytes>;
}

pub fn snapshot_object_name(actor: &ActorKey, state_version: u64, nonce: &str) -> Result<String> {
    actor.validate()?;
    ensure!(state_version > 0, "actor state version must be positive");
    ensure!(
        nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "actor state object nonce is invalid"
    );
    Ok(format!(
        "{}{nonce}/{state_version}.json",
        snapshot_prefix(actor)?
    ))
}

pub(crate) fn snapshot_prefix(actor: &ActorKey) -> Result<String> {
    crate::storage_paths::snapshots(actor)
}

pub fn validate_snapshot_object_name(
    actor: &ActorKey,
    state_version: u64,
    object_name: &str,
) -> Result<()> {
    validate_object_name(object_name)?;
    let prefix = snapshot_prefix(actor)?;
    let mut parts = object_name.strip_prefix(&prefix).unwrap_or("").split('/');
    let valid = matches!(parts.next(), Some(nonce) if nonce.len() == 32 && nonce.bytes().all(|byte| byte.is_ascii_hexdigit()))
        && parts.next() == Some(format!("{state_version}.json").as_str())
        && parts.next().is_none();
    ensure!(valid, "actor state object name does not match its commit");
    Ok(())
}

pub fn validate_bucket(bucket: &str) -> Result<()> {
    ensure!(
        !bucket.is_empty()
            && bucket.trim() == bucket
            && bucket.len() <= 222
            && !bucket.contains('/'),
        "invalid storage bucket"
    );
    Ok(())
}

fn validate_object_name(object_name: &str) -> Result<()> {
    ensure!(
        object_name.starts_with(crate::storage_paths::ROOT)
            && object_name.len() <= 1024
            && !object_name.chars().any(char::is_control),
        "actor state object name is invalid"
    );
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/storage.rs"]
mod tests;
