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
        #[serde(default = "default_ack_zones")]
        ack_zones: usize,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RapidBucket {
    pub bucket: String,
    pub zone: String,
}

fn default_ack_zones() -> usize {
    2
}

impl PersistenceConfig {
    pub(crate) fn same_backend(&self, other: &Self) -> bool {
        self == other
    }

    pub(crate) fn validate(&self) -> Result<()> {
        use std::collections::HashSet;
        let Self::Rapid {
            buckets,
            archive_bucket,
            ack_zones,
        } = self
        else {
            return Ok(());
        };
        crate::storage::validate_bucket(archive_bucket)?;
        ensure!(
            *ack_zones > 0 && *ack_zones <= buckets.len(),
            "acknowledgments must require 1..=configured Rapid zones"
        );
        ensure!(
            buckets.len() <= 7,
            "at most seven Rapid buckets fit in the credential access boundary"
        );
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
                names.insert(&placement.bucket) && placement.bucket != *archive_bucket,
                "Rapid and Standard buckets must be distinct"
            );
        }
        Ok(())
    }
}

#[async_trait]
pub(crate) trait SnapshotStore: Send + Sync {
    async fn get(&self, object: &str) -> Result<Option<Bytes>>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>>;
    async fn put(&self, object: &str, bytes: Bytes) -> Result<()>;
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
