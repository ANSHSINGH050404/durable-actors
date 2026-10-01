use crate::{litestream::storage::LtxFile, storage::SnapshotRef};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution: Option<StateAttribution>,
    pub state_version: u64,
    pub owner_epoch: u64,
    pub request_id: String,
    pub sqlite: SqliteSnapshot,
    pub result: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SqliteSnapshot {
    pub txid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<SnapshotRef>,
    pub files: Vec<LtxFile>,
}

impl SqliteSnapshot {
    fn validate(&self, state_version: u64) -> Result<()> {
        ensure!(self.txid > 0, "SQLite transaction must be positive");
        let first = self
            .files
            .first()
            .context("SQLite commit has no replication files")?;
        ensure!(
            (first.first == 1) == self.parent.is_none(),
            "SQLite dependency mismatch"
        );
        if let Some(parent) = &self.parent {
            ensure!(
                parent.state_version < state_version,
                "SQLite dependency must precede its commit"
            );
        }
        let mut next = first.first;
        for file in &self.files {
            file.validate()?;
            ensure!(file.first == next, "SQLite transaction gap");
            next = file
                .last
                .checked_add(1)
                .context("SQLite transaction overflow")?;
        }
        ensure!(
            next.checked_sub(1) == Some(self.txid),
            "SQLite transaction mismatch"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateAttribution {
    pub operation: String,
    pub connection_id: Option<String>,
    pub committed_at_ms: u64,
    pub interleaved: bool,
}

impl StateSnapshot {
    pub fn new(
        state_version: u64,
        owner_epoch: u64,
        request_id: String,
        sqlite: SqliteSnapshot,
        result: Value,
    ) -> Result<Self> {
        let snapshot = Self {
            attribution: None,
            state_version,
            owner_epoch,
            request_id,
            sqlite,
            result,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let snapshot: Self = serde_json::from_slice(bytes).context("decode actor SQLite commit")?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
    }

    pub(crate) fn validate_object(&self, object: &str) -> Result<()> {
        if let Some(parent) = &self.sqlite.parent {
            ensure!(
                crate::storage_paths::actor_from_snapshot(object)?
                    == crate::storage_paths::actor_from_snapshot(&parent.object)?,
                "SQLite dependency belongs to another actor"
            );
        }
        Ok(())
    }

    fn validate(&self) -> Result<()> {
        ensure!(
            self.state_version > 0,
            "actor state version must be positive"
        );
        ensure!(self.owner_epoch > 0, "owner epoch must be positive");
        ensure!(
            !self.request_id.is_empty() && self.request_id.len() <= 255,
            "actor state request ID is invalid"
        );
        self.sqlite.validate(self.state_version)
    }
}

#[cfg(test)]
#[path = "../tests/unit/state_log.rs"]
mod tests;
