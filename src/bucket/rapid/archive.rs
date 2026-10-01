use super::*;
use std::sync::Mutex as StdMutex;
use tokio::{sync::Notify, time::Instant};

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct ClosedSegment {
    pub last_version: u64,
    pub end_offset: u64,
}

impl ClosedSegment {
    pub fn validate(&self, manifest: &Manifest, end: u64, records: &[Record]) -> Result<()> {
        ensure!(
            self.end_offset == end
                && self.last_version >= manifest.first_version - 1
                && records
                    .last()
                    .is_none_or(|record| record.version == self.last_version),
            "incomplete replicated segment"
        );
        Ok(())
    }
}

struct Pending {
    end: u64,
    version: u64,
    since: Option<Instant>,
    cache_start: u64,
    cache: Vec<Bytes>,
    cache_bytes: usize,
    closed: bool,
}

pub(super) struct Archiver {
    pending: Arc<StdMutex<Pending>>,
    wake: Arc<Notify>,
    task: AbortOnDropHandle<Result<()>>,
    batch_bytes: usize,
}

impl Archiver {
    pub fn new(storage: Arc<LogStorage>, manifest: Manifest, version: u64) -> Self {
        let pending = Arc::new(StdMutex::new(Pending {
            end: 0,
            version,
            since: None,
            cache_start: 0,
            cache: Vec::new(),
            cache_bytes: 0,
            closed: false,
        }));
        let wake = Arc::new(Notify::new());
        let batch_bytes = storage.batch.bytes;
        let task = AbortOnDropHandle::new(tokio::spawn(run(
            storage,
            manifest,
            pending.clone(),
            wake.clone(),
        )));
        Self {
            pending,
            wake,
            task,
            batch_bytes,
        }
    }

    pub fn append(&self, frame: Bytes, version: u64) {
        let mut pending = self.pending.lock().unwrap();
        if pending.cache_start + pending.cache_bytes as u64 == pending.end
            && pending.cache_bytes < self.batch_bytes
        {
            pending.cache_bytes += frame.len();
            pending.cache.push(frame.clone());
        }
        pending.end += frame.len() as u64;
        pending.version = version;
        pending.since.get_or_insert_with(Instant::now);
        self.wake.notify_one();
    }

    pub fn close(&self) {
        self.pending.lock().unwrap().closed = true;
        self.wake.notify_one();
    }

    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }

    pub async fn finish(self) -> Result<()> {
        self.close();
        bounded(async { self.task.await.context("archive worker failed")? }).await
    }
}

async fn run(
    storage: Arc<LogStorage>,
    manifest: Manifest,
    pending: Arc<StdMutex<Pending>>,
    wake: Arc<Notify>,
) -> Result<()> {
    let mut through = 0;
    let mut closed_published = false;
    let mut retry = false;
    let interval = Duration::from_millis(storage.batch.interval_ms);
    loop {
        let (end, closed, due, last_version) = {
            let p = pending.lock().unwrap();
            (p.end, p.closed, p.since.map(|at| at + interval), p.version)
        };
        if closed && !closed_published {
            storage
                .close_segment(
                    &manifest,
                    &ClosedSegment {
                        last_version,
                        end_offset: end,
                    },
                )
                .await?;
            closed_published = true;
        }
        if through == end && closed {
            storage
                .mark_replicated(
                    &manifest,
                    &ClosedSegment {
                        last_version,
                        end_offset: end,
                    },
                )
                .await?;
            tokio::spawn(async move {
                if let Err(error) = storage.cleanup_segment(&manifest).await {
                    tracing::warn!(%error, "Rapid cleanup deferred");
                }
            });
            return Ok(());
        }
        if through < end
            && (closed
                || retry
                || end - through >= storage.batch.bytes as u64
                || due.is_some_and(|at| at <= Instant::now()))
        {
            let cached = take_cache(&pending, through);
            match upload(&storage, &manifest, through, end, cached).await {
                Ok(next) => {
                    retry = false;
                    through = next;
                    let mut p = pending.lock().unwrap();
                    if p.cache.is_empty() {
                        p.cache_start = through;
                    }
                    if through == p.end {
                        p.since = None;
                    } else {
                        p.since.get_or_insert_with(Instant::now);
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "Standard batch upload deferred");
                    retry = true;
                    // Rapid holds the backlog; a failed upload must not grow the memory queue.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            continue;
        }
        match due {
            Some(at) => {
                tokio::select! { _ = wake.notified() => {}, _ = tokio::time::sleep_until(at) => {} }
            }
            None => wake.notified().await,
        }
    }
}

fn take_cache(pending: &StdMutex<Pending>, through: u64) -> Option<Bytes> {
    let frames = {
        let mut p = pending.lock().unwrap();
        if p.cache_start != through || p.cache_bytes == 0 {
            return None;
        }
        p.cache_start += p.cache_bytes as u64;
        p.cache_bytes = 0;
        if p.cache_start == p.end {
            p.since = None;
        }
        std::mem::take(&mut p.cache)
    };
    Some(frames.concat().into())
}

async fn upload(
    storage: &LogStorage,
    manifest: &Manifest,
    start: u64,
    end: u64,
    cached: Option<Bytes>,
) -> Result<u64> {
    let bytes = match cached {
        Some(bytes) => bytes,
        None => {
            let length =
                (end - start).min((storage.batch.bytes + frame::MAX_STATE + frame::HEADER) as u64);
            match storage.read_range(manifest, start, length).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    // Another worker may have archived and deleted the Rapid copies.
                    let (through, _) = storage.archived(manifest).await?;
                    if through > start {
                        return Ok(through.min(end));
                    }
                    return Err(error);
                }
            }
        }
    };
    let records = frame::decode(&bytes)?;
    let mut length = 0;
    for record in records {
        ensure!(
            manifest.stream.snapshot(&record.state)?.object
                == manifest.stream.object(record.version)
                && record.version >= manifest.first_version,
            "archive record identity mismatch"
        );
        length += frame::HEADER + record.state.len();
        if length >= storage.batch.bytes {
            break;
        }
    }
    ensure!(length > 0, "archive range has no complete record");
    storage
        .put_batch(manifest, start, bytes.slice(..length))
        .await?;
    Ok(start + length as u64)
}

impl LogStorage {
    pub async fn put_batch(&self, manifest: &Manifest, start: u64, bytes: Bytes) -> Result<()> {
        let end = start
            .checked_add(bytes.len() as u64)
            .context("batch offset overflow")?;
        let key = format!(
            "{}~{start:020}~{end:020}.batch",
            manifest.key()?.trim_end_matches(".manifest")
        );
        let began = Instant::now();
        ensure!(
            bounded(replace(self.archive.as_ref(), &key, None, bytes.to_vec())).await?,
            "conflicting archive batch"
        );
        tracing::info!(
            event = "rapid_log_batch",
            start_offset = start,
            end_offset = end,
            bytes = bytes.len(),
            duration_ms = began.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }

    pub async fn read_range(&self, manifest: &Manifest, start: u64, length: u64) -> Result<Bytes> {
        for (zone, replica) in self.zones.iter().zip(&manifest.replicas) {
            match bounded(zone.read_range(replica, start, length)).await {
                Ok(bytes) => return Ok(bytes),
                Err(error) => tracing::warn!(%error, "Rapid archive source unavailable"),
            }
        }
        anyhow::bail!("both Rapid archive sources unavailable")
    }

    pub async fn close_segment(&self, manifest: &Manifest, closed: &ClosedSegment) -> Result<()> {
        ensure!(
            bounded(replace(
                self.archive.as_ref(),
                &manifest.marker("closed")?,
                None,
                serde_json::to_vec(closed)?
            ))
            .await?,
            "conflicting closed segment"
        );
        Ok(())
    }

    pub async fn mark_replicated(&self, manifest: &Manifest, closed: &ClosedSegment) -> Result<()> {
        ensure!(
            bounded(replace(
                self.archive.as_ref(),
                &manifest.marker("replicated")?,
                None,
                serde_json::to_vec(closed)?
            ))
            .await?,
            "conflicting replication coverage"
        );
        Ok(())
    }

    pub async fn complete_segment(
        &self,
        manifest: &Manifest,
        closed: &ClosedSegment,
    ) -> Result<()> {
        let (mut through, _) = self.archived(manifest).await?;
        ensure!(
            through <= closed.end_offset,
            "archive exceeds closed segment"
        );
        while through < closed.end_offset {
            through = upload(self, manifest, through, closed.end_offset, None).await?;
        }
        let (end, records) = self.archived(manifest).await?;
        closed.validate(manifest, end, &records)?;
        self.mark_replicated(manifest, closed).await
    }

    pub async fn cleanup_segment(&self, manifest: &Manifest) -> Result<()> {
        ensure!(
            self.archive
                .get(&manifest.marker("replicated")?)
                .await?
                .is_some(),
            "cannot delete unreplicated segment"
        );
        for (zone, replica) in self.zones.iter().zip(&manifest.replicas) {
            bounded(zone.delete(replica)).await?;
        }
        ensure!(
            bounded(replace(
                self.archive.as_ref(),
                &manifest.marker("cleaned")?,
                None,
                b"{}".to_vec()
            ))
            .await?,
            "conflicting cleanup marker"
        );
        Ok(())
    }

    pub async fn sweep(&self, prefix: &str) -> Result<()> {
        let keys: std::collections::BTreeSet<_> =
            self.archive.list(prefix).await?.into_iter().collect();
        for key in keys
            .iter()
            .filter(|k| k.ends_with(".closed") && !keys.contains(&k.replace(".closed", ".cleaned")))
        {
            if let Err(error) = self.sweep_segment(key).await {
                tracing::warn!(%error, key, "Rapid segment cleanup deferred");
            }
        }
        Ok(())
    }

    async fn sweep_segment(&self, key: &str) -> Result<()> {
        if self
            .archive
            .get(&key.replace(".closed", ".cleaned"))
            .await?
            .is_some()
        {
            return Ok(());
        }
        let manifest_key = key.replace(".closed", ".manifest");
        let bytes = self
            .archive
            .get(&manifest_key)
            .await?
            .context("closed segment manifest missing")?;
        let manifest: Manifest = serde_json::from_slice(&bytes.bytes)?;
        manifest.validate(&manifest_key, &manifest.stream.prefix, &self.zones)?;
        let bytes = self
            .archive
            .get(key)
            .await?
            .context("closed segment marker missing")?;
        let closed: ClosedSegment = serde_json::from_slice(&bytes.bytes)?;
        if self
            .archive
            .get(&manifest.marker("replicated")?)
            .await?
            .is_none()
        {
            self.complete_segment(&manifest, &closed).await?;
        }
        self.cleanup_segment(&manifest).await
    }
}
