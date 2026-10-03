use super::archive::Archiver;
use super::*;
use futures_util::FutureExt;
use std::sync::Weak;
use tokio::time::Instant;

type Preparing = AbortOnDropHandle<Result<Option<Segment>>>;

pub(super) struct Session {
    pub stream: StateStream,
    segment: Option<Segment>,
    preparing: Option<Preparing>,
    next: Option<Preparing>,
    archive: Option<Archiver>,
    retired: Option<Archiver>,
    retirement_error: Option<String>,
    latest: Option<(String, Bytes)>,
    version: u64,
    standard: bool,
    healthy: bool,
    rotated: Instant,
    checkpoint: checkpoint::Progress,
}

impl Session {
    pub fn open(storage: Arc<LogStorage>, stream: StateStream) -> Result<Self> {
        let version = stream.base_version;
        let preparing = prepare(
            storage,
            stream.clone(),
            version.checked_add(1).context("state version overflow")?,
        );
        Ok(Self {
            stream,
            preparing: Some(preparing),
            segment: None,
            next: None,
            archive: None,
            retired: None,
            retirement_error: None,
            latest: None,
            version,
            standard: false,
            healthy: true,
            rotated: Instant::now(),
            checkpoint: checkpoint::Progress::default(),
        })
    }

    pub fn cached(&self, object: &str) -> Option<Bytes> {
        self.latest
            .as_ref()
            .filter(|(key, _)| key == object)
            .map(|(_, bytes)| bytes.clone())
    }

    pub async fn put(
        &mut self,
        storage: Arc<LogStorage>,
        object: &str,
        bytes: Bytes,
    ) -> Result<()> {
        let started = Instant::now();
        ensure!(self.healthy, "uncertain write requires activation recovery");
        let reference = self.stream.snapshot(&bytes)?;
        ensure!(
            reference.object == object,
            "snapshot belongs to another stream"
        );
        if self.cached(object).as_ref() == Some(&bytes) {
            return Ok(());
        }
        ensure!(
            self.version.checked_add(1) == Some(reference.state_version),
            "nonconsecutive state version"
        );
        self.rotate(storage.clone()).await;
        self.healthy = false;
        if let Some(preparing) = self.preparing.take() {
            self.install(
                storage.clone(),
                preparing.await.context("Rapid preparation failed")??,
            );
        }
        if self.standard {
            bounded(storage.snapshots.put(object, bytes.clone())).await?;
        } else {
            let frame = self
                .segment
                .as_mut()
                .context("Rapid stream missing")?
                .append(reference.state_version, bytes.clone())
                .await?;
            storage.remember(
                self.segment.as_ref().unwrap(),
                reference.state_version,
                frame.len(),
            );
            self.archive
                .as_ref()
                .context("archive worker missing")?
                .append(frame, reference.state_version);
        }
        self.version = reference.state_version;
        self.latest = Some((object.into(), bytes));
        self.healthy = true;
        self.checkpoint
            .schedule(storage, self.latest.as_ref(), self.version)
            .await;
        tracing::info!(
            event = "rapid_log_snapshot",
            state_version = self.version,
            owner_epoch = self.stream.owner_epoch,
            ack_zones = if self.standard { 0 } else { 2 },
            bytes = self.latest.as_ref().unwrap().1.len(),
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(())
    }

    pub async fn restored(
        &mut self,
        storage: Arc<LogStorage>,
        object: &str,
        bytes: Bytes,
        checkpoint_version: u64,
    ) -> Result<()> {
        let snapshot = crate::state_log::StateSnapshot::decode(&bytes)?;
        if self.latest.is_some()
            || snapshot.state_version != self.stream.base_version
            || crate::storage_paths::actor_from_snapshot(object)?
                != crate::storage_paths::actor_from_snapshot(&self.stream.object(self.version))?
        {
            return Ok(());
        }
        self.latest = Some((object.into(), bytes));
        self.checkpoint.restored(checkpoint_version);
        self.checkpoint
            .schedule(storage, self.latest.as_ref(), self.version)
            .await;
        Ok(())
    }

    fn install(&mut self, storage: Arc<LogStorage>, segment: Option<Segment>) {
        self.standard = segment.is_none();
        self.archive = segment
            .as_ref()
            .map(|s| Archiver::new(storage, s.manifest.clone(), self.version));
        self.segment = segment;
    }

    pub async fn rotate(&mut self, storage: Arc<LogStorage>) {
        if self.retired.as_ref().is_some_and(Archiver::is_finished)
            && let Err(error) = self.retired.take().unwrap().finish().await
        {
            self.retirement_error = Some(format!("{error:#}"));
            tracing::warn!(%error, "Rapid retirement deferred");
        }
        if !self.healthy
            || self.standard
            || self.retired.is_some()
            || self.rotated.elapsed() < ROTATION_INTERVAL
            || self.latest.is_none()
        {
            return;
        }
        let Some(first_version) = self.version.checked_add(1) else {
            return;
        };
        let next = self
            .next
            .get_or_insert_with(|| prepare(storage.clone(), self.stream.clone(), first_version));
        let Some(result) = next.now_or_never() else {
            return;
        };
        self.next = None;
        match result {
            Ok(Ok(Some(segment))) => {
                if let Some(archive) = self.archive.take() {
                    archive.close();
                    self.retired = Some(archive);
                }
                self.install(storage, Some(segment));
            }
            Ok(Ok(None)) => {}
            Ok(Err(error)) => tracing::warn!(%error, "Rapid rotation preparation deferred"),
            Err(error) => tracing::warn!(%error, "Rapid rotation preparation failed"),
        }
        self.rotated = Instant::now();
    }

    pub async fn finish(&mut self, storage: Arc<LogStorage>, deadline: Instant) -> Result<()> {
        ensure!(self.healthy, "uncertain write cannot be sealed");
        self.healthy = false;
        let started = Instant::now();
        tokio::time::timeout_at(deadline, self.finish_archive(storage.clone()))
            .await
            .context("archive drain timed out")??;
        tracing::info!(
            event = "rapid_archive_finished",
            state_version = self.version,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        if let Some((object, bytes)) = &self.latest {
            self.checkpoint
                .finish(storage, object, bytes.clone(), self.version, deadline)
                .await;
        }
        self.segment = None;
        self.healthy = true;
        Ok(())
    }

    async fn finish_archive(&mut self, storage: Arc<LogStorage>) -> Result<()> {
        if let Some(preparing) = self.preparing.take() {
            self.install(
                storage.clone(),
                preparing.await.context("Rapid preparation failed")??,
            );
        }
        // Prepared replacement streams already have a manifest and must also be retired.
        if let Some(next) = self.next.take()
            && let Some(segment) = next.await.context("Rapid preparation failed")??
        {
            Archiver::new(storage.clone(), segment.manifest.clone(), self.version)
                .finish()
                .await?;
        }
        if let Some(archive) = self.archive.take() {
            archive.finish().await?;
        }
        if let Some(retired) = self.retired.take() {
            retired.finish().await?;
        }
        ensure!(
            self.retirement_error.is_none(),
            "previous archive retirement failed: {:?}",
            self.retirement_error
        );
        if let Some((object, bytes)) = &self.latest {
            bounded(storage.snapshots.put(object, bytes.clone())).await?;
        }
        Ok(())
    }
}

fn prepare(storage: Arc<LogStorage>, stream: StateStream, first: u64) -> Preparing {
    AbortOnDropHandle::new(tokio::spawn(async move {
        Segment::open(&storage, stream, first).await
    }))
}

pub(super) fn start_rotation(
    storage: Arc<LogStorage>,
    session: Weak<Mutex<Option<Session>>>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            let Some(session) = session.upgrade() else {
                return;
            };
            if let Some(session) = session.lock().await.as_mut() {
                session.rotate(storage.clone()).await;
                if session.healthy {
                    session
                        .checkpoint
                        .schedule(storage.clone(), session.latest.as_ref(), session.version)
                        .await;
                }
            }
        }
    });
}
