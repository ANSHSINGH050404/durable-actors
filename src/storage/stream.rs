use anyhow::{Result, ensure};
use aws_lc_rs::digest::{SHA256, digest};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::state_log::StateSnapshot;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateStream {
    pub session: String,
    pub prefix: String,
    pub owner_epoch: u64,
    pub base_version: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotRef {
    pub object: String,
    pub state_version: u64,
    pub request_id: String,
    pub digest: String,
}

impl StateStream {
    pub fn object(&self, version: u64) -> String {
        format!("{}{version}.json", self.prefix)
    }

    pub fn snapshot(&self, bytes: &[u8]) -> Result<SnapshotRef> {
        let snapshot = StateSnapshot::decode(bytes)?;
        ensure!(
            snapshot.owner_epoch == self.owner_epoch && snapshot.state_version > self.base_version,
            "snapshot belongs to another stream"
        );
        Ok(SnapshotRef::new(
            self.object(snapshot.state_version),
            &snapshot,
            bytes,
        ))
    }
}

impl SnapshotRef {
    pub fn new(object: String, snapshot: &StateSnapshot, bytes: &[u8]) -> Self {
        Self {
            object,
            state_version: snapshot.state_version,
            request_id: snapshot.request_id.clone(),
            digest: checksum(bytes),
        }
    }

    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        ensure!(checksum(bytes) == self.digest, "snapshot checksum mismatch");
        Ok(())
    }
}

fn checksum(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(digest(&SHA256, bytes).as_ref())
}
