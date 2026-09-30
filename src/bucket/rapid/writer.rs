use super::*;
use std::{sync::Weak, time::Instant};

pub(super) struct Session {
    pub stream: StateStream,
    segment: Option<Segment>,
    preparing: Option<AbortOnDropHandle<Result<Option<Segment>>>>,
    latest: Option<(String, Bytes)>,
    version: u64,
    standard: bool,
    healthy: bool,
    checkpointed: Instant,
}

impl Session {
    pub fn open(storage: Arc<LogStorage>, stream: StateStream) -> Result<Self> {
        let version = stream.base_version;
        let first_version = version.checked_add(1).context("state version overflow")?;
        let preparing_stream = stream.clone();
        let preparing = AbortOnDropHandle::new(tokio::spawn(async move {
            Segment::open(&storage, preparing_stream, first_version).await
        }));
        Ok(Self {
            stream,
            preparing: Some(preparing),
            segment: None,
            standard: false,
            latest: None,
            version,
            healthy: true,
            checkpointed: Instant::now(),
        })
    }

    pub async fn put(&mut self, storage: &LogStorage, object: &str, bytes: Bytes) -> Result<()> {
        let started = Instant::now();
        ensure!(self.healthy, "uncertain write requires activation recovery");
        let reference = self.stream.snapshot(&bytes)?;
        ensure!(
            reference.object == object,
            "snapshot belongs to another stream"
        );
        if self
            .latest
            .as_ref()
            .is_some_and(|(key, old)| key == object && *old == bytes)
        {
            return Ok(());
        }
        ensure!(
            self.version.checked_add(1) == Some(reference.state_version),
            "nonconsecutive state version"
        );
        self.healthy = false;
        self.prepared().await?;
        if self.standard {
            storage.snapshots.put(object, bytes.clone()).await?;
        } else {
            self.append(storage, reference.state_version, bytes.clone())
                .await?;
        }
        self.version = reference.state_version;
        tracing::info!(
            event = "rapid_log_snapshot",
            state_version = self.version,
            owner_epoch = self.stream.owner_epoch,
            bytes = bytes.len(),
            ack_zones = if self.standard { 0 } else { 2 },
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        self.latest = Some((object.into(), bytes));
        self.healthy = true;
        Ok(())
    }

    pub async fn finish(&mut self, storage: &LogStorage) -> Result<()> {
        ensure!(self.healthy, "uncertain write cannot be sealed");
        self.healthy = false;
        self.prepared().await?;
        self.checkpoint(storage).await?;
        self.healthy = true;
        Ok(())
    }

    async fn prepared(&mut self) -> Result<()> {
        if let Some(preparing) = self.preparing.take() {
            self.segment = preparing.await.context("log preparation task failed")??;
            self.standard = self.segment.is_none();
        }
        Ok(())
    }

    async fn append(&mut self, storage: &LogStorage, version: u64, bytes: Bytes) -> Result<()> {
        if bytes.len() > frame::MAX_STATE {
            self.checkpoint(storage).await?;
            self.standard = true;
            return storage
                .snapshots
                .put(&self.stream.object(version), bytes)
                .await;
        }
        let frame_size = Record {
            version,
            state: bytes.clone(),
        }
        .encode()?
        .len();
        if self
            .segment
            .as_ref()
            .is_some_and(|segment| segment.bytes.len() + frame_size > MAX_SEGMENT_BYTES as usize)
        {
            self.checkpoint(storage).await?;
        }
        if self.segment.is_none() {
            self.segment = Segment::open(storage, self.stream.clone(), version).await?;
        }
        if let Some(segment) = &mut self.segment {
            segment.append(version, bytes).await
        } else {
            self.standard = true;
            storage
                .snapshots
                .put(&self.stream.object(version), bytes)
                .await
        }
    }

    async fn checkpoint(&mut self, storage: &LogStorage) -> Result<()> {
        if let Some(segment) = self.segment.take() {
            segment.archive(storage).await?;
        }
        if let Some((object, bytes)) = &self.latest {
            storage.snapshots.put(object, bytes.clone()).await?;
        }
        self.checkpointed = Instant::now();
        Ok(())
    }
}

pub(super) fn start_checkpoints(
    storage: Arc<LogStorage>,
    session: Weak<Mutex<Option<Session>>>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop.cancelled() => return,
                _ = tokio::time::sleep(CHECKPOINT_INTERVAL) => {}
            }
            let Some(session) = session.upgrade() else {
                return;
            };
            let mut session = session.lock().await;
            if let Some(session) = session.as_mut()
                && session.latest.is_some()
                && session.segment.is_some()
                && session.checkpointed.elapsed() >= CHECKPOINT_INTERVAL
                && let Err(error) = session.finish(&storage).await
            {
                tracing::error!(%error, "log checkpoint failed; fencing activation");
                stop.cancel();
                return;
            }
        }
    });
}
