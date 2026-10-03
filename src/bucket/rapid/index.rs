use super::*;

#[derive(Clone)]
pub(super) struct LiveRecord {
    manifest: Manifest,
    start: u64,
    length: u64,
}

#[derive(Serialize, Deserialize)]
struct Index {
    batch: String,
    records: Vec<Entry>,
}
#[derive(Serialize, Deserialize)]
struct Entry {
    version: u64,
    start: u64,
    length: u64,
}

impl LogStorage {
    pub fn remember(&self, segment: &Segment, version: u64, length: usize) {
        let mut records = self.live.lock().unwrap();
        records.insert(
            version,
            LiveRecord {
                manifest: segment.manifest.clone(),
                start: segment.offset - length as u64,
                length: length as u64,
            },
        );
        while records.len() > 128 {
            records.pop_first();
        }
    }

    pub async fn live_records(
        &self,
        object: &str,
        first: u64,
    ) -> Result<Option<BTreeMap<String, Bytes>>> {
        let Some(record) = self.live_range(object, first) else {
            return Ok(None);
        };
        match self
            .read_range(&record.manifest, record.start, record.length)
            .await
        {
            Ok(bytes) => {
                let (prefix, _) = object.rsplit_once('/').context("invalid snapshot object")?;
                let records = decode_records(prefix, bytes)?;
                ensure!(records.contains_key(object), "live record missing");
                Ok(Some(records))
            }
            Err(_) => Ok(None),
        }
    }

    fn live_range(&self, object: &str, first: u64) -> Option<LiveRecord> {
        let version = super::super::snapshots::version(object)?;
        let records = self.live.lock().unwrap();
        let mut record = records
            .get(&version)
            .filter(|r| r.manifest.stream.object(version) == object)?
            .clone();
        for (_, prior) in records.range(first..version).rev() {
            if prior.manifest.id != record.manifest.id
                || prior.start + prior.length != record.start
                || record.length + prior.length > self.batch.bytes as u64
            {
                break;
            }
            record.start = prior.start;
            record.length += prior.length;
        }
        Some(record)
    }

    pub async fn index_keys(&self, prefix: &str) -> Result<Vec<String>> {
        match self
            .archive
            .list(&format!("{}index~", object_name(prefix)?))
            .await
        {
            Ok(keys) => Ok(keys),
            Err(error) => {
                tracing::warn!(event = "rapid_index_fallback", prefix, %error);
                Ok(Vec::new())
            }
        }
    }

    pub async fn indexed_records(
        &self,
        object: &str,
        first: u64,
        keys: &[String],
    ) -> Option<BTreeMap<String, Bytes>> {
        match self.read_indexed(object, first, keys).await {
            Ok(records) => records,
            Err(error) => {
                tracing::warn!(event = "rapid_index_fallback", object, %error);
                None
            }
        }
    }

    async fn read_indexed(
        &self,
        object: &str,
        first: u64,
        keys: &[String],
    ) -> Result<Option<BTreeMap<String, Bytes>>> {
        let version =
            super::super::snapshots::version(object).context("invalid snapshot version")?;
        let (prefix, _) = object.rsplit_once('/').context("invalid snapshot object")?;
        let physical = object_name(&format!("{prefix}/"))?;
        for key in keys {
            let Some(segment) = index_segment(key, &physical, version)? else {
                continue;
            };
            let stored = self
                .archive
                .get(key)
                .await?
                .context("archive index missing")?;
            let index: Index = serde_json::from_slice(&stored.bytes)?;
            index.validate(&format!("{physical}log~{segment}~"))?;
            let (start, length) = index.range(first, version, self.batch.bytes as u64)?;
            let bytes = self
                .archive
                .range(&index.batch, start, length)
                .await?
                .context("indexed archive batch missing")?
                .bytes;
            let records = decode_records(prefix, bytes)?;
            ensure!(records.contains_key(object), "indexed record missing");
            tracing::info!(
                event = "rapid_index_read",
                state_version = version,
                downloaded_bytes = length,
                records = records.len()
            );
            return Ok(Some(records));
        }
        Ok(None)
    }

    pub async fn index_batch(
        &self,
        manifest: &Manifest,
        batch: String,
        bytes: &Bytes,
    ) -> Result<()> {
        let mut start = 0;
        let mut entries = Vec::new();
        for record in frame::decode(bytes)? {
            let length = (frame::HEADER + record.state.len()) as u64;
            entries.push(Entry {
                version: record.version,
                start,
                length,
            });
            start += length;
        }
        ensure!(start == bytes.len() as u64, "incomplete indexed batch");
        let first = entries.first().context("empty indexed batch")?.version;
        let last = entries.last().unwrap().version;
        let key = format!(
            "{}index~{first:020}~{last:020}~{}.idx",
            object_name(&manifest.stream.prefix)?,
            manifest.id
        );
        ensure!(
            replace(
                self.archive.as_ref(),
                &key,
                None,
                crate::payload::encode(&Index {
                    batch,
                    records: entries
                })?
            )
            .await?,
            "conflicting archive index"
        );
        Ok(())
    }
}

fn validate(object: &str, record: &Record) -> Result<()> {
    let snapshot = crate::state_log::StateSnapshot::decode(&record.state)?;
    snapshot.validate_object(object)?;
    ensure!(
        super::super::snapshots::version(object) == Some(record.version)
            && snapshot.state_version == record.version,
        "indexed record identity mismatch"
    );
    ensure!(
        object.rsplit('/').nth(1) == Some(format!("{:032x}", snapshot.owner_epoch).as_str()),
        "indexed record epoch mismatch"
    );
    Ok(())
}

fn index_segment<'a>(key: &'a str, prefix: &str, version: u64) -> Result<Option<&'a str>> {
    let Some(name) = key
        .strip_prefix(&format!("{prefix}index~"))
        .and_then(|key| key.strip_suffix(".idx"))
    else {
        return Ok(None);
    };
    let parts = name.split('~').collect::<Vec<_>>();
    ensure!(parts.len() == 3, "invalid archive index name");
    let first = parts[0].parse::<u64>()?;
    let last = parts[1].parse::<u64>()?;
    ensure!(first <= last, "invalid archive index range");
    Ok((first..=last).contains(&version).then_some(parts[2]))
}

impl Index {
    fn validate(&self, prefix: &str) -> Result<()> {
        let name = self
            .batch
            .strip_prefix(prefix)
            .and_then(|key| key.strip_suffix(".batch"))
            .context("archive index escaped its segment")?;
        let (start, end) = name
            .split_once('~')
            .context("invalid indexed batch range")?;
        let length = end
            .parse::<u64>()?
            .checked_sub(start.parse::<u64>()?)
            .context("invalid indexed batch boundary")?;
        let mut offset = 0;
        let mut previous: Option<u64> = None;
        for entry in &self.records {
            ensure!(
                entry.start == offset
                    && entry.length >= frame::HEADER as u64
                    && entry.length <= frame::HEADER as u64 + u64::from(u32::MAX)
                    && previous.is_none_or(|v| v.checked_add(1) == Some(entry.version)),
                "invalid archive index entry"
            );
            offset = offset
                .checked_add(entry.length)
                .context("indexed offset overflow")?;
            previous = Some(entry.version);
        }
        ensure!(
            !self.records.is_empty() && offset == length,
            "incomplete archive index"
        );
        Ok(())
    }

    fn range(&self, first: u64, version: u64, limit: u64) -> Result<(u64, u64)> {
        let last = self
            .records
            .binary_search_by_key(&version, |r| r.version)
            .ok()
            .context("archive index record missing")?;
        let end = self.records[last].start + self.records[last].length;
        let mut start = self.records[last].start;
        for record in self.records[..last].iter().rev() {
            if record.version < first || end - record.start > limit {
                break;
            }
            start = record.start;
        }
        Ok((start, end - start))
    }
}

fn decode_records(prefix: &str, bytes: Bytes) -> Result<BTreeMap<String, Bytes>> {
    let mut length = 0;
    let mut records = BTreeMap::new();
    for record in frame::decode(&bytes)? {
        length += frame::HEADER + record.state.len();
        let object = format!("{prefix}/{}.json", record.version);
        validate(&object, &record)?;
        ensure!(
            records.insert(object, record.state).is_none(),
            "duplicate indexed record"
        );
    }
    ensure!(length == bytes.len(), "incomplete indexed range");
    Ok(records)
}
