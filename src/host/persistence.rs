use super::{actor_runtime::ActorStorage, storage::HostStorage};
use crate::{
    state_transport::{SnapshotWriter, StateWrite},
    storage::WritePlan,
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

pub(super) struct ActorPersistence {
    storage: Arc<HostStorage>,
    writing: Mutex<()>,
}
impl ActorPersistence {
    pub fn new(storage: Arc<HostStorage>) -> Arc<Self> {
        Arc::new(Self {
            storage,
            writing: Mutex::new(()),
        })
    }
}
#[async_trait]
impl SnapshotWriter for ActorPersistence {
    async fn write_snapshot(&self, plan: &WritePlan, bytes: bytes::Bytes) -> Result<StateWrite> {
        let _writing = self.writing.lock().await;
        self.storage.ensure_authority()?;
        let attempt = self.storage.stop.clone().drop_guard();
        let result = self.storage.runtime.write_snapshot(plan, bytes).await?;
        self.storage.ensure_authority()?;
        attempt.disarm();
        Ok(result)
    }
}
