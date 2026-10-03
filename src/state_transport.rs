use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StateWrite {
    Written,
    AlreadyExists,
}

#[async_trait]
pub trait SnapshotWriter: Send + Sync {
    async fn write_snapshot(
        &self,
        plan: &crate::storage::WritePlan,
        bytes: bytes::Bytes,
    ) -> Result<StateWrite>;
}
