use anyhow::{Context, Result, ensure};
use aws_lc_rs::digest::{SHA256, digest};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Record {
    pub version: u64,
    pub digest: String,
    pub base: Option<String>,
    pub length: usize,
    pub data: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Batch {
    pub prefix: String,
    pub records: Vec<Record>,
}

impl Record {
    pub fn encode(version: u64, bytes: &[u8], previous: Option<&[u8]>) -> Result<Self> {
        let full = zstd::bulk::compress(bytes, 1)?;
        let delta = previous
            .map(|previous| zstd::bulk::Compressor::with_dictionary(1, previous)?.compress(bytes))
            .transpose()?;
        let (data, base) = match (previous, delta) {
            (Some(previous), Some(delta)) if delta.len() < full.len() => {
                (delta, Some(checksum(previous)))
            }
            _ => (full, None),
        };
        Ok(Self {
            version,
            digest: checksum(bytes),
            base,
            length: bytes.len(),
            data: STANDARD.encode(data),
        })
    }

    pub fn decode(&self, previous: Option<&[u8]>) -> Result<Vec<u8>> {
        let dictionary = if let Some(base) = &self.base {
            let previous = previous.context("change log base is missing")?;
            ensure!(
                checksum(previous) == *base,
                "change log base checksum mismatch"
            );
            previous
        } else {
            &[]
        };
        let bytes = zstd::bulk::Decompressor::with_dictionary(dictionary)?
            .decompress(&STANDARD.decode(&self.data)?, self.length)?;
        ensure!(
            bytes.len() == self.length && checksum(&bytes) == self.digest,
            "change log checksum mismatch"
        );
        let snapshot = crate::state_log::StateSnapshot::decode(&bytes)?;
        ensure!(
            snapshot.state_version == self.version,
            "change log version mismatch"
        );
        Ok(bytes)
    }
}

impl Batch {
    pub fn decode(&self) -> Result<Vec<(u64, Vec<u8>)>> {
        ensure!(!self.records.is_empty(), "empty archive batch");
        let mut decoded: Vec<(u64, Vec<u8>)> = Vec::new();
        for record in &self.records {
            if let Some((version, _)) = decoded.last() {
                ensure!(
                    version.checked_add(1) == Some(record.version),
                    "archive log has a gap"
                );
            }
            let bytes = record.decode(decoded.last().map(|(_, bytes)| bytes.as_slice()))?;
            decoded.push((record.version, bytes));
        }
        Ok(decoded)
    }

    pub fn key(&self) -> Result<String> {
        let first = self.records.first().context("empty archive batch")?.version;
        let last = self.records.last().unwrap().version;
        Ok(format!(
            "{}{first:020}-{last:020}-{}.json",
            archive_prefix(&self.prefix)?,
            checksum(&serde_json::to_vec(self)?)
        ))
    }
}

pub(super) fn archive_prefix(prefix: &str) -> Result<String> {
    let root = crate::storage_paths::ROOT;
    Ok(format!(
        "{root}archive/{}",
        prefix
            .strip_prefix(&format!("{root}snapshots/"))
            .context("invalid snapshot prefix")?
    ))
}

pub(super) fn checksum(bytes: &[u8]) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    URL_SAFE_NO_PAD.encode(digest(&SHA256, bytes).as_ref())
}
