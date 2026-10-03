use super::*;
use aws_lc_rs::digest::{Context as Digest, SHA256};
use std::io::Write;

pub(super) const HEADER: usize = 48;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub version: u64,
    pub state: Bytes,
}

impl Record {
    pub fn encode(&self) -> Result<Bytes> {
        ensure!(self.version > 0, "invalid state version");
        let length = u32::try_from(self.state.len()).context("record length overflow")?;
        let mut bytes = Vec::with_capacity(HEADER);
        bytes.extend_from_slice(b"RLG1");
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(checksum(&bytes, &self.state).as_ref());
        let mut spool = crate::payload::Spool::new();
        spool.write_all(&bytes)?;
        spool.write_all(&self.state)?;
        spool.finish()
    }
}

pub(super) fn decode(bytes: &Bytes) -> Result<Vec<Record>> {
    let mut records = Vec::new();
    let mut offset = 0;
    while bytes.len() - offset >= HEADER {
        let header = &bytes[offset..offset + HEADER];
        let end = offset
            .checked_add(length(header)?)
            .context("record offset overflow")?;
        if end > bytes.len() {
            break;
        }
        let version = u64::from_le_bytes(header[8..16].try_into()?);
        ensure!(version > 0, "invalid state version");
        if let Some(previous) = records.last() {
            let previous: &Record = previous;
            ensure!(
                previous.version.checked_add(1) == Some(version),
                "nonconsecutive state versions"
            );
        }
        let state = bytes.slice(offset + HEADER..end);
        ensure!(
            checksum(&header[..16], &state).as_ref() == &header[16..48],
            "corrupt log record"
        );
        records.push(Record { version, state });
        offset = end;
    }
    Ok(records)
}

pub(super) fn length(header: &[u8]) -> Result<usize> {
    ensure!(
        header.len() >= HEADER && &header[..4] == b"RLG1",
        "invalid record header"
    );
    HEADER
        .checked_add(u32::from_le_bytes(header[4..8].try_into()?) as usize)
        .context("record length overflow")
}

fn checksum(header: &[u8], state: &[u8]) -> aws_lc_rs::digest::Digest {
    let mut hash = Digest::new(&SHA256);
    hash.update(header);
    hash.update(state);
    hash.finish()
}
