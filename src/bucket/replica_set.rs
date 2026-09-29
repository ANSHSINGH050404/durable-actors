use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use tokio::sync::Mutex;

use super::SnapshotStore;

pub(crate) struct ReplicaSet {
    copies: Vec<Arc<dyn SnapshotStore>>,
    epochs: Mutex<BTreeMap<String, Arc<Mutex<bool>>>>,
}

impl ReplicaSet {
    pub fn new(copies: Vec<Arc<dyn SnapshotStore>>) -> Result<Self> {
        ensure!(!copies.is_empty(), "replica requires a persistence bucket");
        Ok(Self {
            copies,
            epochs: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn from_replicas(
        replicas: &[super::ReplicaPlacement],
        token: String,
        http: reqwest::Client,
    ) -> Result<Self> {
        Self::new(
            replicas
                .iter()
                .map(|replica| {
                    Arc::new(crate::replicas::client::ReplicaClient::with_http(
                        replica.clone(),
                        token.clone(),
                        http.clone(),
                    )) as Arc<dyn SnapshotStore>
                })
                .collect(),
        )
    }

    async fn epoch(&self, prefix: &str) -> Arc<Mutex<bool>> {
        self.epochs
            .lock()
            .await
            .entry(prefix.into())
            .or_insert_with(|| Arc::new(Mutex::new(true)))
            .clone()
    }
}

#[async_trait]
impl SnapshotStore for ReplicaSet {
    async fn prepare(&self, prefix: &str) -> Result<()> {
        let epoch = self.epoch(prefix).await;
        let mut healthy = epoch.lock().await;
        ensure!(
            *healthy,
            "replica epoch has an ambiguous write; recovery is required"
        );
        *healthy = false;
        for result in join_all(self.copies.iter().map(|copy| copy.prepare(prefix))).await {
            result?;
        }
        *healthy = true;
        Ok(())
    }

    async fn seal(&self, prefix: &str) -> Result<()> {
        let epoch = self.epoch(prefix).await;
        let mut healthy = epoch.lock().await;
        *healthy = false;
        let results = join_all(self.copies.iter().map(|copy| copy.seal(prefix))).await;
        ensure!(
            results.iter().any(Result::is_ok),
            "no replica copy could be sealed for recovery"
        );
        Ok(())
    }

    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        let (prefix, _) = object.rsplit_once('/').context("replica epoch missing")?;
        let epoch = self.epoch(&format!("{prefix}/")).await;
        let mut healthy = epoch.lock().await;
        ensure!(
            *healthy,
            "replica epoch has an ambiguous write; recovery is required"
        );
        // Cancellation must fence subsequent writes even when a remote copy persisted the record.
        *healthy = false;
        for result in join_all(
            self.copies
                .iter()
                .map(|copy| copy.put(object, bytes.clone())),
        )
        .await
        {
            result?;
        }
        *healthy = true;
        Ok(())
    }

    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        let mut available = false;
        let mut selected = None;
        for result in join_all(self.copies.iter().map(|copy| copy.get(object))).await {
            let Ok(bytes) = result else {
                continue;
            };
            available = true;
            if let Some(bytes) = bytes {
                ensure!(
                    selected.as_ref().is_none_or(|previous| previous == &bytes),
                    "conflicting replica copies"
                );
                selected = Some(bytes);
            }
        }
        ensure!(available, "no replica copy is available for recovery");
        Ok(selected)
    }

    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let mut available = false;
        let mut selected: Option<(String, Bytes)> = None;
        for result in join_all(self.copies.iter().map(|copy| copy.latest(prefix))).await {
            let Ok(candidate) = result else {
                continue;
            };
            available = true;
            if let Some(candidate) = candidate {
                if let Some(previous) = &selected {
                    ensure!(
                        previous.0 != candidate.0 || previous.1 == candidate.1,
                        "conflicting replica copies"
                    );
                    if super::snapshots::version(&previous.0)
                        >= super::snapshots::version(&candidate.0)
                    {
                        continue;
                    }
                }
                selected = Some(candidate);
            }
        }
        ensure!(available, "no replica copy is available for recovery");
        Ok(selected)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut available = false;
        let mut keys = BTreeSet::new();
        for result in join_all(self.copies.iter().map(|copy| copy.list(prefix))).await {
            if let Ok(copy) = result {
                available = true;
                keys.extend(copy);
            }
        }
        ensure!(available, "no replica copy is available for recovery");
        Ok(keys.into_iter().collect())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/replica_set.rs"]
mod tests;
