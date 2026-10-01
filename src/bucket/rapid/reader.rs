use super::*;

impl LogStorage {
    pub async fn latest(&self, prefix: &str, fence: bool) -> Result<Option<(String, Bytes)>> {
        let (checkpoint, records) =
            tokio::try_join!(self.snapshots.latest(prefix), self.records(prefix, fence))?;
        let mut records = records;
        if let Some((key, bytes)) = checkpoint {
            insert(&mut records, key, bytes)?;
        }
        Ok(records
            .into_iter()
            .max_by_key(|(key, _)| super::super::snapshots::version(key)))
    }

    pub async fn records(&self, prefix: &str, fence: bool) -> Result<BTreeMap<String, Bytes>> {
        let physical = super::super::rapid::object_name(prefix)?;
        let mut records = BTreeMap::new();
        for key in self.archive.list(&physical).await? {
            if !key.ends_with(".manifest") {
                continue;
            }
            let bytes = self
                .archive
                .get(&key)
                .await?
                .context("log manifest disappeared")?;
            let manifest: Manifest = serde_json::from_slice(&bytes.bytes)?;
            manifest.validate(&key, prefix, &self.zones)?;
            for record in self.read_segment(&manifest, fence).await? {
                let object = manifest.stream.object(record.version);
                let reference = manifest.stream.snapshot(&record.state)?;
                ensure!(
                    reference.object == object && record.version >= manifest.first_version,
                    "log record identity mismatch"
                );
                insert(&mut records, object, record.state)?;
            }
        }
        Ok(records)
    }

    async fn read_segment(&self, manifest: &Manifest, fence: bool) -> Result<Vec<Record>> {
        let marker = self.archive.get(&manifest.marker("replicated")?).await?;
        let (end, archived) = self.archived(manifest).await?;
        if let Some(marker) = marker {
            let closed: archive::ClosedSegment = serde_json::from_slice(&marker.bytes)?;
            closed.validate(manifest, end, &archived)?;
            return Ok(archived);
        }
        let rapid = match self.read_replicas(manifest, fence).await {
            Ok(records) => records,
            Err(error) => {
                if let Some(marker) = self.archive.get(&manifest.marker("replicated")?).await? {
                    let closed: archive::ClosedSegment = serde_json::from_slice(&marker.bytes)?;
                    let (end, records) = self.archived(manifest).await?;
                    closed.validate(manifest, end, &records)?;
                    return Ok(records);
                }
                return Err(error);
            }
        };
        let mut merged = BTreeMap::new();
        for record in archived.into_iter().chain(rapid) {
            ensure!(
                merged
                    .get(&record.version)
                    .is_none_or(|prior| prior == &record),
                "conflicting archived record"
            );
            merged.insert(record.version, record);
        }
        Ok(merged.into_values().collect())
    }

    pub async fn archived(&self, manifest: &Manifest) -> Result<(u64, Vec<Record>)> {
        let prefix = format!("{}~", manifest.key()?.trim_end_matches(".manifest"));
        let mut offsets = BTreeMap::new();
        for key in self.archive.list(&prefix).await? {
            let range = key
                .strip_prefix(&prefix)
                .and_then(|v| v.strip_suffix(".batch"))
                .context("invalid archive batch key")?;
            let (start, end) = range
                .split_once('~')
                .context("invalid archive batch range")?;
            let (mut offset, end) = (start.parse::<u64>()?, end.parse::<u64>()?);
            ensure!(end > offset, "invalid archive batch boundary");
            let bytes: Bytes = self
                .archive
                .get(&key)
                .await?
                .context("archive batch missing")?
                .bytes
                .into();
            ensure!(
                bytes.len() as u64 == end - offset,
                "archive batch length mismatch"
            );
            for record in frame::decode(&bytes)? {
                let reference = manifest.stream.snapshot(&record.state)?;
                ensure!(
                    reference.object == manifest.stream.object(record.version)
                        && record.version >= manifest.first_version,
                    "archive record identity mismatch"
                );
                let next = offset + (frame::HEADER + record.state.len()) as u64;
                ensure!(
                    offsets.get(&offset).is_none_or(|prior| prior == &record),
                    "conflicting archive overlap"
                );
                offsets.insert(offset, record);
                offset = next;
            }
            ensure!(offset == end, "incomplete archive batch");
        }
        let mut through = 0;
        let mut records: Vec<Record> = Vec::new();
        for (offset, record) in offsets {
            ensure!(offset == through, "gap in archived segment");
            ensure!(
                records
                    .last()
                    .is_none_or(|r| r.version.checked_add(1) == Some(record.version)),
                "nonconsecutive archived versions"
            );
            through += (frame::HEADER + record.state.len()) as u64;
            records.push(record);
        }
        Ok((through, records))
    }

    pub async fn recover_segment(&self, manifest: &Manifest) -> Result<()> {
        let records = self.read_segment(manifest, true).await?;
        if self
            .archive
            .get(&manifest.marker("replicated")?)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let (mut through, _) = self.archived(manifest).await?;
        let end_offset = records
            .iter()
            .map(|r| (frame::HEADER + r.state.len()) as u64)
            .sum();
        let mut closed = archive::ClosedSegment {
            last_version: records
                .last()
                .map_or(manifest.first_version - 1, |r| r.version),
            end_offset,
        };
        if let Some(marker) = self.archive.get(&manifest.marker("closed")?).await? {
            closed = serde_json::from_slice(&marker.bytes)?;
            closed.validate(manifest, end_offset, &records)?;
        }
        let mut offset = 0;
        let mut batch = Vec::new();
        for record in records {
            let frame = record.encode()?;
            offset += frame.len() as u64;
            if offset <= through {
                continue;
            }
            batch.extend_from_slice(&frame);
            if batch.len() >= self.batch.bytes {
                let length = batch.len() as u64;
                self.put_batch(manifest, through, std::mem::take(&mut batch).into())
                    .await?;
                through += length;
            }
        }
        if !batch.is_empty() {
            self.put_batch(manifest, through, batch.into()).await?;
        }
        if self
            .archive
            .get(&manifest.marker("closed")?)
            .await?
            .is_none()
        {
            self.close_segment(manifest, &closed).await?;
        }
        self.mark_replicated(manifest, &closed).await
    }

    pub(super) async fn read_replicas(
        &self,
        manifest: &Manifest,
        fence: bool,
    ) -> Result<Vec<Record>> {
        let reads = futures_util::future::join_all(
            self.zones
                .iter()
                .zip(&manifest.replicas)
                .map(|(zone, replica)| bounded(zone.read(replica, fence))),
        )
        .await;
        let mut copies = Vec::new();
        for read in reads {
            match read {
                Ok(bytes) => copies.push(frame::decode(&bytes)?),
                Err(error) => tracing::warn!(%error, "Rapid log replica unavailable during read"),
            }
        }
        ensure!(
            !copies.is_empty(),
            "acknowledged log is unavailable in both zones"
        );
        let mut selected = Vec::new();
        for records in copies {
            for (left, right) in selected.iter().zip(&records) {
                ensure!(left == right, "divergent log replicas");
            }
            if records.len() > selected.len() {
                selected = records;
            }
        }
        // A complete uncertain tail is made durable in Standard before ownership advances.
        Ok(selected)
    }
}

fn insert(records: &mut BTreeMap<String, Bytes>, key: String, bytes: Bytes) -> Result<()> {
    ensure!(
        records.get(&key).is_none_or(|prior| prior == &bytes),
        "conflicting state records"
    );
    records.insert(key, bytes);
    Ok(())
}
