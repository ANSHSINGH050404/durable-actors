use super::*;
use aws_lc_rs::digest::{Context as Digest, SHA256};

const HEADER: usize = 48;
pub(super) const MAX_STATE: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub version: u64,
    pub state: Bytes,
}

impl Record {
    pub fn encode(&self) -> Result<Bytes> {
        ensure!(self.version > 0, "invalid state version");
        ensure!(
            self.state.len() <= MAX_STATE,
            "state exceeds append-record limit"
        );
        let mut bytes = Vec::with_capacity(HEADER + self.state.len());
        bytes.extend_from_slice(b"RLG1");
        bytes.extend_from_slice(&(self.state.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(checksum(&bytes, &self.state).as_ref());
        bytes.extend_from_slice(&self.state);
        Ok(bytes.into())
    }
}

pub(super) fn decode(bytes: &Bytes) -> Result<Vec<Record>> {
    ensure!(
        bytes.len() as u64 <= MAX_SEGMENT_BYTES,
        "segment exceeds size limit"
    );
    let mut records = Vec::new();
    let mut offset = 0;
    while bytes.len() - offset >= HEADER {
        let header = &bytes[offset..offset + HEADER];
        ensure!(&header[..4] == b"RLG1", "invalid record header");
        let length = u32::from_le_bytes(header[4..8].try_into()?) as usize;
        ensure!(length <= MAX_STATE, "invalid record length");
        let end = offset + HEADER + length;
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

fn checksum(header: &[u8], state: &[u8]) -> aws_lc_rs::digest::Digest {
    let mut hash = Digest::new(&SHA256);
    hash.update(header);
    hash.update(state);
    hash.finish()
}
