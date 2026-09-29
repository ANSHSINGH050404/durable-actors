use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{
    Value,
    value::{RawValue, to_raw_value},
};
use std::borrow::Borrow;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attribution: Option<StateAttribution>,
    pub state_version: u64,
    pub owner_epoch: u64,
    pub request_id: String,
    pub state: Box<RawValue>,
    pub result: Value,
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
        state: impl Borrow<Value>,
        result: Value,
    ) -> Result<Self> {
        let snapshot = Self {
            attribution: None,
            state_version,
            owner_epoch,
            request_id,
            state: to_raw_value(state.borrow())?,
            result,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let snapshot: Self =
            serde_json::from_slice(bytes).context("decode actor state snapshot")?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(serde_json::to_vec(self)?)
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
        ensure!(
            self.state.get().starts_with('{'),
            "actor state must be a JSON object"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "../tests/unit/state_log.rs"]
mod tests;
