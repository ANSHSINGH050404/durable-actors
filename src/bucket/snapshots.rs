use super::recovery;
use super::{Bucket, replace};
use anyhow::{Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PersistenceConfig {
    #[default]
    Local,
    Rapid {
        buckets: Vec<RapidBucket>,
        archive_bucket: String,
        #[serde(default)]
        archive_batch: ArchiveBatchConfig,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveBatchConfig {
    pub bytes: usize,
    pub interval_ms: u64,
}

impl Default for ArchiveBatchConfig {
    fn default() -> Self {
        Self {
            bytes: 16 * 1024 * 1024,
            interval_ms: 10_000,
        }
    }
}

impl ArchiveBatchConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.bytes > 0 && self.bytes <= usize::MAX - 8 * 1024 * 1024,
            "archive batch bytes must be positive and leave room for one record"
        );
        ensure!(
            self.interval_ms > 0
                && std::time::Instant::now()
                    .checked_add(std::time::Duration::from_millis(self.interval_ms))
                    .is_some(),
            "archive batch interval must be positive and representable"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RapidBucket {
    pub bucket: String,
    pub zone: String,
}

impl PersistenceConfig {
    pub(crate) fn rapid_settings(&self) -> Option<(&[RapidBucket], &str)> {
        match self {
            Self::Local => None,
            Self::Rapid {
                buckets,
                archive_bucket,
                ..
            } => Some((buckets, archive_bucket)),
        }
    }
    pub(crate) fn same_backend(&self, other: &Self) -> bool {
        self.rapid_settings() == other.rapid_settings()
    }

    pub(crate) fn validate(&self) -> Result<()> {
        use std::collections::HashSet;
        if let Self::Rapid { archive_batch, .. } = self {
            archive_batch.validate()?;
        }
        let Some((buckets, archive_bucket)) = self.rapid_settings() else {
            return Ok(());
        };
        ensure!(
            buckets.len() == 2,
            "append logs require exactly two Rapid zones"
        );
        crate::storage::validate_bucket(archive_bucket)?;
        let mut zones = HashSet::new();
        let mut names = HashSet::new();
        for placement in buckets {
            crate::storage::validate_bucket(&placement.bucket)?;
            let (region, suffix) = placement
                .zone
                .rsplit_once('-')
                .ok_or_else(|| anyhow::anyhow!("Rapid placement must name a Google Cloud zone"))?;
            ensure!(
                suffix.len() == 1
                    && suffix.as_bytes()[0].is_ascii_lowercase()
                    && region.chars().last().is_some_and(|c| c.is_ascii_digit()),
                "invalid Rapid zone"
            );
            ensure!(
                zones.insert(&placement.zone),
                "Rapid buckets must occupy distinct zones"
            );
            ensure!(
                names.insert(&placement.bucket) && placement.bucket != archive_bucket,
                "Rapid and Standard buckets must be distinct"
            );
        }
        Ok(())
    }
}

#[async_trait]
pub(crate) trait SnapshotStore: Send + Sync {
    async fn get(&self, object: &str) -> Result<Option<Bytes>>;
    async fn restore(
        &self,
        object: &str,
        bytes: Bytes,
    ) -> Result<crate::state_log::SqliteSnapshot> {
        Ok(
            recovery::resolve(&mut recovery::StoredHistory(self), object, bytes)
                .await?
                .sqlite,
        )
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>>;
    async fn put(&self, object: &str, bytes: Bytes) -> Result<()>;
    async fn start(&self, _stream: &crate::storage::StateStream) -> Result<()> {
        Ok(())
    }
    async fn recover(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let latest = self.latest(prefix).await?;
        if let Some((object, bytes)) = &latest {
            self.put(object, bytes.clone()).await?;
        }
        Ok(latest)
    }
    async fn finish(
        &self,
        _stream: &crate::storage::StateStream,
        _deadline: tokio::time::Instant,
    ) -> Result<()> {
        Ok(())
    }
}

pub(crate) struct BucketSnapshots(pub Arc<dyn Bucket>);

#[async_trait]
impl SnapshotStore for BucketSnapshots {
    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        Ok(self
            .0
            .get(object)
            .await?
            .map(|object| Bytes::from(object.bytes)))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.0.list(prefix).await
    }

    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let newest = self
            .list(prefix)
            .await?
            .into_iter()
            .filter_map(|key| version(&key).map(|v| (v, key)))
            .max_by_key(|(version, _)| *version)
            .map(|(_, key)| key);
        match newest {
            Some(key) => Ok(self.get(&key).await?.map(|bytes| (key, bytes))),
            None => Ok(None),
        }
    }

    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        ensure!(
            replace(self.0.as_ref(), object, None, bytes.to_vec()).await?,
            "conflicting immutable snapshot"
        );
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/persistence.rs"]
mod tests;

pub(super) fn version(object: &str) -> Option<u64> {
    object
        .rsplit('/')
        .next()?
        .strip_suffix(".json")?
        .parse()
        .ok()
}
