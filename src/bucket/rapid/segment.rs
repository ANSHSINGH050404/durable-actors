use super::*;

pub(super) struct Segment {
    pub manifest: Manifest,
    writers: Vec<Box<dyn LogWriter>>,
    pub bytes: Vec<u8>,
    healthy: bool,
}

impl Segment {
    pub async fn open(
        storage: &LogStorage,
        stream: StateStream,
        first_version: u64,
    ) -> Result<Option<Self>> {
        let started = std::time::Instant::now();
        let actor = crate::storage_paths::actor_from_snapshot(&stream.object(first_version))?;
        let prefix = object_name(&crate::storage_paths::snapshots(&actor)?)?.replacen(
            "snapshots-",
            "logs-",
            1,
        );
        let prepared = storage.prepared.lock().unwrap().take();
        let prepared = match prepared {
            Some(prepared) => {
                ensure!(
                    prepared.prefix == prefix,
                    "prepared log belongs to another actor"
                );
                prepared
            }
            None => Prepared::new(prefix, storage.zones.clone()),
        };
        let mut manifest = Manifest {
            format: 1,
            id: prepared.id.clone(),
            stream,
            first_version,
            replicas: Vec::new(),
        };
        let Some(opened) = prepared.take().await? else {
            return Ok(None);
        };
        let mut writers = Vec::new();
        for (replica, writer) in opened {
            manifest.replicas.push(replica);
            writers.push(writer);
        }
        let key = manifest.key()?;
        let open_ms = started.elapsed().as_secs_f64() * 1000.0;
        ensure!(
            bounded(replace(
                storage.archive.as_ref(),
                &key,
                None,
                serde_json::to_vec(&manifest)?
            ))
            .await?,
            "conflicting log manifest"
        );
        tracing::info!(
            event = "rapid_log_open",
            owner_epoch = manifest.stream.owner_epoch,
            open_ms,
            manifest_ms = started.elapsed().as_secs_f64() * 1000.0 - open_ms,
            duration_ms = started.elapsed().as_secs_f64() * 1000.0
        );
        Ok(Some(Self {
            manifest,
            writers,
            bytes: Vec::new(),
            healthy: true,
        }))
    }

    pub async fn append(&mut self, version: u64, state: Bytes) -> Result<()> {
        ensure!(
            self.healthy,
            "uncertain log append requires activation recovery"
        );
        let frame = Record { version, state }.encode()?;
        let expected = self.bytes.len() as u64 + frame.len() as u64;
        ensure!(expected <= MAX_SEGMENT_BYTES, "log segment is full");
        // An interrupted flush may have persisted bytes; this pair must never be reused.
        self.healthy = false;
        let flushed = bounded(async {
            Ok(futures_util::future::join_all(
                self.writers
                    .iter_mut()
                    .map(|writer| writer.append_and_flush(frame.clone())),
            )
            .await)
        })
        .await?;
        for result in flushed {
            ensure!(result? == expected, "unexpected persisted log offset");
        }
        self.bytes.extend_from_slice(&frame);
        self.healthy = true;
        Ok(())
    }

    pub fn ensure_healthy(&self) -> Result<()> {
        ensure!(
            self.healthy,
            "uncertain log append requires activation recovery"
        );
        Ok(())
    }

    pub async fn archive(self, storage: &LogStorage) -> Result<()> {
        self.ensure_healthy()?;
        let key = self.manifest.archive_key()?;
        ensure!(
            bounded(replace(storage.archive.as_ref(), &key, None, self.bytes)).await?,
            "conflicting archived log segment"
        );
        drop(self.writers);
        for (zone, replica) in storage.zones.iter().zip(&self.manifest.replicas) {
            if let Err(error) = bounded(zone.delete(replica)).await {
                tracing::warn!(%error, object=%replica.object, "archived Rapid log cleanup deferred");
            }
        }
        Ok(())
    }
}
