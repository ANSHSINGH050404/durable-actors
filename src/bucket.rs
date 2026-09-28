pub(crate) mod access;
mod file;
mod gcs;
mod peers;
mod runtime;

use anyhow::Result;
use async_trait::async_trait;

pub use file::FileBucket;
pub use gcs::GcsBucket;
pub(crate) use gcs::WarmGcs;
pub use peers::{GrpcReplicaPeers, ReplicaPeers};
pub(crate) use runtime::ActorStateReader;
pub use runtime::{LoadedActor, OwnershipHint, ReplicaMembership, RuntimeStorage};

#[derive(Clone, Debug)]
pub struct BucketObject {
    pub generation: i64,
    pub bytes: Vec<u8>,
}

#[async_trait]
pub trait Bucket: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>>;
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
}

pub(crate) async fn replace(
    bucket: &dyn Bucket,
    key: &str,
    generation: Option<i64>,
    bytes: Vec<u8>,
) -> Result<bool> {
    match bucket
        .compare_and_swap(key, generation, bytes.clone())
        .await
    {
        Ok(true) => Ok(true),
        result => {
            // A retry can report a failed precondition after the first request succeeded.
            if bucket
                .get(key)
                .await?
                .is_some_and(|object| object.bytes == bytes)
            {
                return Ok(true);
            }
            result
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/bucket.rs"]
pub(crate) mod testing;
