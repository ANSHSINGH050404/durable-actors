use super::{Bucket, SnapshotStore, replace};
use crate::storage::StateStream;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

mod archive;
mod frame;
mod gcs;
mod prepared;
mod reader;
mod segment;
mod standard;
mod writer;

use frame::Record;
use prepared::Prepared;
use segment::Segment;
use tokio_util::task::AbortOnDropHandle;
use writer::Session;

const ROTATION_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Replica {
    bucket: String,
    object: String,
    generation: i64,
}

#[async_trait]
trait LogWriter: Send {
    async fn append_and_flush(&mut self, bytes: Bytes) -> Result<u64>;
}

#[async_trait]
trait LogZone: Send + Sync {
    fn bucket(&self) -> &str;
    async fn open(&self, object: &str) -> Result<(Replica, Box<dyn LogWriter>)>;
    async fn read(&self, replica: &Replica, fence: bool) -> Result<Bytes>;
    async fn read_range(&self, replica: &Replica, start: u64, length: u64) -> Result<Bytes>;
    async fn delete(&self, replica: &Replica) -> Result<()>;
}

pub(crate) struct RapidSnapshots {
    storage: Arc<LogStorage>,
    session: Arc<Mutex<Option<Session>>>,
    stop: CancellationToken,
    cleanup: std::sync::Mutex<Option<AbortOnDropHandle<()>>>,
}

struct LogStorage {
    archive: Arc<dyn Bucket>,
    snapshots: Arc<dyn SnapshotStore>,
    zones: Vec<Arc<dyn LogZone>>,
    prepared: std::sync::Mutex<Option<Prepared>>,
    batch: super::ArchiveBatchConfig,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u32,
    id: String,
    stream: StateStream,
    first_version: u64,
    replicas: Vec<Replica>,
}

impl RapidSnapshots {
    fn start_cleanup(&self, prefix: String) {
        let storage = self.storage.clone();
        let stop = self.stop.clone();
        let session = Arc::downgrade(&self.session);
        let task = tokio::spawn(async move {
            loop {
                if session.upgrade().is_none() {
                    return;
                }
                tokio::select! {
                    _ = stop.cancelled() => return,
                    result = storage.sweep(&prefix) => {
                        if let Err(error) = result { tracing::warn!(%error, "Rapid cleanup deferred"); }
                    }
                }
                tokio::select! {
                    _ = stop.cancelled() => return,
                    _ = tokio::time::sleep(ROTATION_INTERVAL) => {}
                }
            }
        });
        *self.cleanup.lock().unwrap() = Some(AbortOnDropHandle::new(task));
    }
    fn prepare(&self, actor: &crate::actor::ActorKey) -> Result<()> {
        let prefix = object_name(&crate::storage_paths::snapshots(actor)?)?.replacen(
            "snapshots-",
            "logs-",
            1,
        );
        *self.storage.prepared.lock().unwrap() =
            Some(Prepared::new(prefix, self.storage.zones.clone()));
        Ok(())
    }

    fn new(
        archive: Arc<dyn Bucket>,
        snapshots: Arc<dyn SnapshotStore>,
        zones: Vec<Arc<dyn LogZone>>,
        batch: super::ArchiveBatchConfig,
        stop: CancellationToken,
    ) -> Result<Self> {
        ensure!(
            zones.len() == 2 && zones[0].bucket() != zones[1].bucket(),
            "two independent log replicas required"
        );
        batch.validate()?;
        let storage = Arc::new(LogStorage {
            archive,
            snapshots,
            zones,
            prepared: std::sync::Mutex::new(None),
            batch,
        });
        let session = Arc::new(Mutex::new(None));
        writer::start_rotation(storage.clone(), Arc::downgrade(&session), stop.clone());
        Ok(Self {
            storage,
            session,
            stop,
            cleanup: std::sync::Mutex::new(None),
        })
    }
}

#[async_trait]
impl SnapshotStore for RapidSnapshots {
    async fn get(&self, object: &str) -> Result<Option<Bytes>> {
        if let Some(bytes) = self
            .session
            .lock()
            .await
            .as_ref()
            .and_then(|session| session.cached(object))
        {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.storage.snapshots.get(object).await? {
            return Ok(Some(bytes));
        }
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        Ok(self
            .storage
            .records(&format!("{prefix}/"), false)
            .await?
            .remove(object))
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys: std::collections::BTreeSet<_> = self
            .storage
            .snapshots
            .list(prefix)
            .await?
            .into_iter()
            .filter(|key| super::snapshots::version(key).is_some())
            .collect();
        keys.extend(self.storage.records(prefix, false).await?.into_keys());
        Ok(keys.into_iter().collect())
    }
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        self.storage.latest(prefix, false).await
    }
    async fn put(&self, object: &str, bytes: Bytes) -> Result<()> {
        let mut session = self.session.lock().await;
        session
            .as_mut()
            .context("log stream is not activated")?
            .put(self.storage.clone(), object, bytes)
            .await
    }
    async fn start(&self, stream: &StateStream) -> Result<()> {
        let mut session = self.session.lock().await;
        ensure!(session.is_none(), "log stream is already activated");
        *session = Some(Session::open(self.storage.clone(), stream.clone())?);
        let actor = crate::storage_paths::actor_from_snapshot(&stream.object(stream.base_version))?;
        self.start_cleanup(object_name(&crate::storage_paths::snapshots(&actor)?)?);
        Ok(())
    }
    async fn recover(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        for key in self.storage.archive.list(&object_name(prefix)?).await? {
            if !key.ends_with(".manifest") {
                continue;
            }
            let bytes = self
                .storage
                .archive
                .get(&key)
                .await?
                .context("manifest disappeared")?;
            let manifest: Manifest = serde_json::from_slice(&bytes.bytes)?;
            manifest.validate(&key, prefix, &self.storage.zones)?;
            self.storage.recover_segment(&manifest).await?;
        }
        let latest = self.storage.latest(prefix, false).await?;
        if let Some((object, bytes)) = &latest {
            self.storage.snapshots.put(object, bytes.clone()).await?;
        }
        Ok(latest)
    }
    async fn finish(&self, stream: &StateStream) -> Result<()> {
        let mut session = self.session.lock().await;
        let active = session.as_mut().context("log stream is not activated")?;
        ensure!(active.stream == *stream, "cannot finish another log stream");
        active.finish(self.storage.clone()).await?;
        *session = None;
        Ok(())
    }
}

impl Manifest {
    fn key(&self) -> Result<String> {
        Ok(format!(
            "{}log~{}.manifest",
            super::rapid::object_name(&self.stream.prefix)?,
            self.id
        ))
    }
    fn marker(&self, kind: &str) -> Result<String> {
        Ok(self.key()?.replace(".manifest", &format!(".{kind}")))
    }
    fn object(&self) -> Result<String> {
        let actor =
            crate::storage_paths::actor_from_snapshot(&self.stream.object(self.first_version))?;
        Ok(format!(
            "{}{}.segment",
            object_name(&crate::storage_paths::snapshots(&actor)?)?.replacen(
                "snapshots-",
                "logs-",
                1
            ),
            self.id
        ))
    }
    fn validate(&self, key: &str, prefix: &str, zones: &[Arc<dyn LogZone>]) -> Result<()> {
        ensure!(
            self.format == 1 && self.first_version > self.stream.base_version,
            "invalid log manifest"
        );
        uuid::Uuid::parse_str(&self.id)?;
        ensure!(
            self.key()? == key && self.stream.prefix.starts_with(prefix),
            "manifest escaped its stream"
        );
        let object = self.stream.object(self.first_version);
        let actor = crate::storage_paths::actor_from_snapshot(&object)?;
        crate::storage::validate_snapshot_object_name(&actor, self.first_version, &object)?;
        ensure!(
            object.rsplit('/').nth(1) == Some(format!("{:032x}", self.stream.owner_epoch).as_str()),
            "manifest epoch mismatch"
        );
        ensure!(self.replicas.len() == zones.len(), "missing log replicas");
        for (replica, zone) in self.replicas.iter().zip(zones) {
            ensure!(
                replica.bucket == zone.bucket()
                    && replica.object == self.object()?
                    && replica.generation > 0,
                "invalid log replica descriptor"
            );
        }
        Ok(())
    }
}

async fn bounded<T>(operation: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(Duration::from_secs(10), operation)
        .await
        .context("log operation timed out")?
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
