use super::*;
use crate::bucket::recovery::{Checkpoint, ResolvedSqlite, SnapshotHistory, resolve};
use crate::state_log::{SqliteSnapshot, StateSnapshot};
use crate::storage::SnapshotRef;

pub(super) const CHECKPOINT_INTERVAL: u64 = 32;

impl LogStorage {
    pub async fn restore(&self, object: &str, bytes: Bytes) -> Result<ResolvedSqlite> {
        let started = std::time::Instant::now();
        tracing::info!(event = "sqlite_history_started", object);
        let mut history = LogHistory {
            storage: self,
            epochs: BTreeMap::new(),
            reads: 0,
            probes: 0,
        };
        let resolved = resolve(&mut history, object, bytes).await?;
        tracing::info!(
            event = "sqlite_history_restored",
            object,
            checkpoint = resolved.checkpoint,
            checkpoint_version = resolved.checkpoint_version,
            parents = resolved.parents,
            archive_epochs = history
                .epochs
                .values()
                .filter(|epoch| epoch.records.is_some())
                .count(),
            dependency_gets = history.reads,
            checkpoint_gets = history.probes,
            files = resolved.sqlite.files.len(),
            ltx_base64_bytes = resolved
                .sqlite
                .files
                .iter()
                .map(|file| file.data.len())
                .sum::<usize>(),
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(resolved)
    }

    pub async fn checkpoint(&self, object: &str, bytes: Bytes) -> Result<()> {
        let started = std::time::Instant::now();
        let snapshot = StateSnapshot::decode(&bytes)?;
        if snapshot.sqlite.parent.is_none() {
            tracing::info!(
                event = "sqlite_checkpoint",
                object,
                state_version = snapshot.state_version,
                outcome = "full_snapshot",
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            );
            return Ok(());
        }
        let source = SnapshotRef::new(object.into(), &snapshot, &bytes);
        if let Some(bytes) = self.snapshots.get(&format!("{object}.checkpoint")).await? {
            Checkpoint::decode(&bytes, &source, snapshot.sqlite.txid)?;
            tracing::info!(
                event = "sqlite_checkpoint",
                object,
                state_version = snapshot.state_version,
                outcome = "existing",
                duration_ms = started.elapsed().as_secs_f64() * 1000.0
            );
            return Ok(());
        }
        let sqlite = self
            .restore(object, bytes)
            .await
            .context("resolve checkpoint inputs")?
            .sqlite;
        let history_ms = started.elapsed().as_secs_f64() * 1000.0;
        let input_files = sqlite.files.len();
        let compaction_started = std::time::Instant::now();
        let file = self
            .compactor
            .compact(&sqlite.files)
            .await
            .context("compact SQLite checkpoint")?;
        let compaction_ms = compaction_started.elapsed().as_secs_f64() * 1000.0;
        let checkpoint = Checkpoint {
            source,
            sqlite: SqliteSnapshot {
                txid: sqlite.txid,
                parent: None,
                files: vec![file],
            },
        };
        let bytes =
            tokio::task::spawn_blocking(move || crate::payload::encode(&checkpoint)).await??;
        let size = bytes.len();
        let upload_started = std::time::Instant::now();
        bounded(
            self.snapshots
                .put(&format!("{object}.checkpoint"), bytes.into()),
        )
        .await
        .context("upload SQLite checkpoint")?;
        tracing::info!(
            event = "sqlite_checkpoint",
            object,
            outcome = "published",
            history_ms,
            compaction_ms,
            upload_ms = upload_started.elapsed().as_secs_f64() * 1000.0,
            state_version = snapshot.state_version,
            input_files,
            bytes = size,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }
}

struct LogHistory<'a> {
    storage: &'a LogStorage,
    epochs: BTreeMap<String, EpochHistory>,
    reads: usize,
    probes: usize,
}

struct EpochHistory {
    records: Option<BTreeMap<String, Bytes>>,
    indexes: Vec<String>,
    indexed: BTreeMap<String, Bytes>,
    checkpoints: std::collections::BTreeSet<String>,
}

#[async_trait]
impl SnapshotHistory for LogHistory<'_> {
    async fn read(&mut self, object: &str) -> Result<Bytes> {
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        if !self.epochs.contains_key(prefix) {
            let prefix_key = format!("{prefix}/");
            let (keys, indexes) = tokio::try_join!(
                self.storage.snapshots.list(&prefix_key),
                self.storage.index_keys(&prefix_key)
            )?;
            self.epochs.insert(
                prefix.into(),
                EpochHistory {
                    records: None,
                    indexes,
                    indexed: BTreeMap::new(),
                    checkpoints: keys
                        .into_iter()
                        .filter(|key| key.ends_with(".checkpoint"))
                        .collect(),
                },
            );
        }
        let epoch = self.epochs.get_mut(prefix).unwrap();
        if let Some(bytes) = epoch
            .records
            .as_ref()
            .and_then(|records| records.get(object))
        {
            return Ok(bytes.clone());
        }
        if let Some(bytes) = epoch.indexed.remove(object) {
            return Ok(bytes);
        }
        let version =
            super::super::snapshots::version(object).context("invalid snapshot version")?;
        let first = epoch
            .checkpoints
            .iter()
            .filter_map(|key| key.strip_suffix(".checkpoint"))
            .filter_map(super::super::snapshots::version)
            .filter(|v| *v <= version)
            .max()
            .unwrap_or(0);
        if let Some(records) = self.storage.live_records(object, first).await? {
            self.reads += 1;
            epoch.indexed = records;
            return epoch
                .indexed
                .remove(object)
                .context("live SQLite dependency missing");
        }
        self.reads += 1;
        if let Some(bytes) = self.storage.snapshots.get(object).await? {
            return Ok(bytes);
        }
        if let Some(records) = self
            .storage
            .indexed_records(object, first, &epoch.indexes)
            .await
        {
            epoch.indexed = records;
            return epoch
                .indexed
                .remove(object)
                .context("indexed SQLite dependency missing");
        }
        if epoch.records.is_none() {
            epoch.records = Some(self.storage.records(&format!("{prefix}/"), false).await?);
        }
        epoch
            .records
            .as_ref()
            .and_then(|records| records.get(object))
            .cloned()
            .context("SQLite dependency missing")
    }

    async fn checkpoint(&mut self, object: &str) -> Result<Option<Bytes>> {
        let key = format!("{object}.checkpoint");
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot name")?;
        if self
            .epochs
            .get(prefix)
            .is_some_and(|epoch| !epoch.checkpoints.contains(&key))
        {
            return Ok(None);
        }
        self.probes += 1;
        self.storage.snapshots.get(&key).await
    }
}

#[derive(Default)]
pub(super) struct Progress {
    completed: u64,
    task: Option<(u64, AbortOnDropHandle<Result<()>>)>,
    retry_at: Option<tokio::time::Instant>,
}

impl Progress {
    pub fn restored(&mut self, version: u64) {
        self.completed = self.completed.max(version);
    }

    pub async fn schedule(
        &mut self,
        storage: Arc<LogStorage>,
        latest: Option<&(String, Bytes)>,
        version: u64,
    ) {
        self.collect().await;
        if self.task.is_some()
            || version.saturating_sub(self.completed) < CHECKPOINT_INTERVAL
            || self
                .retry_at
                .is_some_and(|retry| tokio::time::Instant::now() < retry)
        {
            return;
        }
        if let Some((object, bytes)) = latest {
            self.start(
                storage,
                object.clone(),
                bytes.clone(),
                version,
                "background",
            );
        }
    }

    pub async fn finish(
        &mut self,
        storage: Arc<LogStorage>,
        object: &str,
        bytes: Bytes,
        version: u64,
        deadline: tokio::time::Instant,
    ) {
        self.collect().await;
        if self.completed >= version {
            return;
        }
        if self
            .task
            .as_ref()
            .is_some_and(|(running, _)| *running != version)
        {
            let (superseded, task) = self.task.take().unwrap();
            tracing::info!(
                event = "sqlite_checkpoint_cancelled",
                state_version = superseded,
                reason = "superseded_on_shutdown"
            );
            task.abort();
            let _ = task.await;
        }
        if self.task.is_none() {
            self.start(storage, object.into(), bytes, version, "shutdown");
        }
        let (version, task) = self.task.take().unwrap();
        match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(Ok(()))) => self.completed = version,
            result => {
                tracing::warn!(event = "sqlite_checkpoint_deferred", object, state_version = version,
                error = ?result, "checkpoint incomplete; archived history remains recoverable")
            }
        }
    }

    fn start(
        &mut self,
        storage: Arc<LogStorage>,
        object: String,
        bytes: Bytes,
        version: u64,
        trigger: &'static str,
    ) {
        tracing::info!(
            event = "sqlite_checkpoint_started",
            object,
            state_version = version,
            trigger
        );
        self.task = Some((
            version,
            AbortOnDropHandle::new(tokio::spawn(async move {
                let started = std::time::Instant::now();
                let result = tokio::time::timeout(
                    Duration::from_secs(120),
                    storage.checkpoint(&object, bytes),
                )
                .await
                .context("checkpoint timed out")
                .and_then(|result| result);
                if let Err(error) = &result {
                    tracing::warn!(event = "sqlite_checkpoint_failed", object, state_version = version,
                        error = %format!("{error:#}"), duration_ms = started.elapsed().as_secs_f64() * 1000.0);
                }
                result
            })),
        ));
    }

    async fn collect(&mut self) {
        if !self
            .task
            .as_ref()
            .is_some_and(|(_, task)| task.is_finished())
        {
            return;
        }
        let (version, task) = self.task.take().unwrap();
        match task.await {
            Ok(Ok(())) => {
                self.completed = self.completed.max(version);
                self.retry_at = None;
            }
            error => {
                self.retry_at = Some(tokio::time::Instant::now() + Duration::from_secs(5));
                tracing::warn!(event = "sqlite_checkpoint_deferred", state_version = version,
                    error = ?error, "SQLite checkpoint will retry");
            }
        }
    }
}
