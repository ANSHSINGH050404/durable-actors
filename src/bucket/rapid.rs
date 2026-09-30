use super::{GcsBucket, PersistenceConfig, SnapshotStore, gcs::GcsClients};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

mod gcs;
use gcs::{GcsSnapshots, StorageClass};

pub(crate) struct RapidSnapshots {
    archive: Arc<dyn SnapshotStore>,
    zones: Vec<Arc<dyn SnapshotStore>>,
    ack_zones: usize,
    uploads: Arc<Semaphore>,
}

impl RapidSnapshots {
    pub(crate) fn gcs(config: &PersistenceConfig, clients: GcsClients) -> Result<Self> {
        config.validate()?;
        let PersistenceConfig::Rapid {
            buckets,
            archive_bucket,
            ack_zones,
        } = config
        else {
            anyhow::bail!("Rapid persistence configuration required");
        };
        let archive = Arc::new(GcsSnapshots::new(
            archive_bucket,
            clients.clone(),
            StorageClass::Standard,
        )?);
        let zones = buckets
            .iter()
            .map(|placement| {
                Ok(Arc::new(GcsSnapshots::new(
                    &placement.bucket,
                    clients.clone(),
                    StorageClass::Rapid,
                )?) as Arc<dyn SnapshotStore>)
            })
            .collect::<Result<_>>()?;
        Self::new(archive, zones, *ack_zones)
    }

    pub(crate) async fn validate_gcs(
        config: &PersistenceConfig,
        clients: GcsClients,
    ) -> Result<()> {
        let PersistenceConfig::Rapid {
            buckets,
            archive_bucket,
            ..
        } = config
        else {
            anyhow::bail!("Rapid persistence configuration required");
        };
        GcsBucket::with_clients(archive_bucket, clients.clone())?
            .require_standard()
            .await?;
        for placement in buckets {
            let bucket = clients
                .control
                .get_bucket()
                .set_name(format!("projects/_/buckets/{}", placement.bucket))
                .send()
                .await?;
            ensure!(
                bucket.storage_class == "RAPID",
                "Rapid bucket has the wrong storage class"
            );
            let locations = bucket
                .custom_placement_config
                .context("Rapid bucket zone missing")?
                .data_locations;
            ensure!(
                locations.len() == 1 && locations[0].eq_ignore_ascii_case(&placement.zone),
                "Rapid bucket does not occupy its configured zone"
            );
        }
        Ok(())
    }

    fn new(
        archive: Arc<dyn SnapshotStore>,
        zones: Vec<Arc<dyn SnapshotStore>>,
        ack_zones: usize,
    ) -> Result<Self> {
        ensure!(
            ack_zones > 0 && ack_zones <= zones.len(),
            "invalid Rapid acknowledgment count"
        );
        Ok(Self {
            archive,
            zones,
            ack_zones,
            uploads: Arc::new(Semaphore::new(8)),
        })
    }

    fn archive(&self, object: String, bytes: Bytes) -> tokio::task::JoinHandle<Result<()>> {
        let permit = self.uploads.clone().try_acquire_owned();
        let archive = self.archive.clone();
        tokio::spawn(async move {
            let result = match permit {
                Ok(_permit) => bounded(archive.put(&object, bytes)).await,
                Err(error) => Err(error.into()),
            };
            if let Err(error) = &result {
                tracing::warn!(event = "rapid_archive_deferred", %error, %object, "Standard archival deferred to managed transfer");
            }
            result
        })
    }

    async fn persist_zones(&self, object: &str, bytes: Bytes) -> Result<usize> {
        let mut pending: FuturesUnordered<_> = self
            .zones
            .iter()
            .map(|store| bounded(store.put(object, bytes.clone())))
            .collect();
        let mut persisted = 0;
        while let Some(result) = pending.next().await {
            match result {
                Ok(()) => persisted += 1,
                Err(error) => tracing::warn!(%error, %object, "Rapid zone write failed"),
            }
            if persisted == self.ack_zones {
                return Ok(persisted);
            }
            ensure!(
                persisted + pending.len() >= self.ack_zones,
                "insufficient Rapid zones persisted the snapshot"
            );
        }
        anyhow::bail!("insufficient Rapid zones persisted the snapshot")
    }
}

#[async_trait]
impl SnapshotStore for RapidSnapshots {
    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        let started = std::time::Instant::now();
        let upload = self.archive(object.to_owned(), bytes.clone());
        let archive = async { upload.await.context("Standard upload task failed")? };
        let zones = self.persist_zones(object, bytes.clone());
        tokio::pin!(archive, zones);
        let (winner, ack_zones) = tokio::select! {
            result = &mut archive => match result {
                Ok(()) => ("standard", 0),
                Err(_) => ("rapid", zones.await?),
            },
            result = &mut zones => match result {
                Ok(count) => ("rapid", count),
                Err(_) => {
                    archive.await.context("neither Standard nor the Rapid quorum persisted the snapshot")?;
                    ("standard", 0)
                }
            },
        };
        tracing::info!(event = "rapid_snapshot", %object, winner, ack_zones, bytes = bytes.len(), duration_ms = started.elapsed().as_secs_f64() * 1000.0);
        Ok(())
    }

    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        let reads = self
            .zones
            .iter()
            .chain(std::iter::once(&self.archive))
            .map(|store| bounded(store.get(object)));
        let mut selected = None;
        let mut available = 0;
        for result in futures_util::future::join_all(reads).await {
            if let Ok(copy) = result {
                available += 1;
                if let Some(bytes) = copy {
                    ensure!(
                        selected.as_ref().is_none_or(|previous| previous == &bytes),
                        "conflicting immutable snapshots"
                    );
                    selected = Some(bytes);
                }
            }
        }
        ensure!(
            selected.is_some() || available == self.zones.len() + 1,
            "snapshot unavailable"
        );
        Ok(selected)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let (archive, zones) = tokio::join!(
            bounded(self.archive.list(prefix)),
            futures_util::future::join_all(
                self.zones.iter().map(|store| bounded(store.list(prefix)))
            )
        );
        let mut keys: BTreeSet<_> = archive?.into_iter().collect();
        let mut available = 0;
        for result in zones {
            if let Ok(copy) = result {
                available += 1;
                keys.extend(copy);
            }
        }
        ensure!(
            available > self.zones.len() - self.ack_zones,
            "insufficient Rapid zones to recover acknowledged writes"
        );
        Ok(keys.into_iter().collect())
    }

    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        let key = self
            .list(prefix)
            .await?
            .into_iter()
            .filter_map(|key| super::snapshots::version(&key).map(|v| (v, key)))
            .max_by_key(|(version, _)| *version);
        match key {
            Some((_, key)) => {
                let bytes = self
                    .get(&key)
                    .await?
                    .context("latest snapshot disappeared before archival")?;
                Ok(Some((key, bytes)))
            }
            None => Ok(None),
        }
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .context("snapshot operation timed out")?
}

#[cfg(test)]
#[path = "../../tests/unit/bucket/rapid.rs"]
mod tests;

pub(super) fn object_name(logical: &str) -> Result<String> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let root = format!("{}snapshots/", crate::storage_paths::ROOT);
    let relative = logical
        .strip_prefix(&root)
        .context("invalid snapshot root")?;
    let parts: Vec<_> = relative.splitn(5, '/').collect();
    ensure!(
        parts.len() == 5 && parts[..4].iter().all(|p| !p.is_empty()),
        "invalid snapshot actor prefix"
    );
    let actor = parts[..4].join("/");
    let hash = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, actor.as_bytes());
    Ok(format!(
        "durable-actors-v3-snapshots-{}~{}",
        URL_SAFE_NO_PAD.encode(hash.as_ref()),
        parts[4].replace('/', "~")
    ))
}
