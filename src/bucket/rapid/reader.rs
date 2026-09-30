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
        if let Some(archived) = self.archive.get(&manifest.archive_key()?).await? {
            return frame::decode(&Bytes::from(archived.bytes));
        }
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
