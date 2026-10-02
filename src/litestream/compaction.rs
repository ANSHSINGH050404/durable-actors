use super::storage::LtxFile;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    io::{self, Cursor, Read},
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub(crate) trait LtxCompactor: Send + Sync {
    async fn compact(&self, files: &[LtxFile]) -> Result<LtxFile>;
}

pub(crate) struct RustCompactor;

#[async_trait]
impl LtxCompactor for RustCompactor {
    async fn compact(&self, files: &[LtxFile]) -> Result<LtxFile> {
        let files = files.to_vec();
        let cancel = CancellationToken::new();
        let _guard = cancel.clone().drop_guard();
        tokio::task::spawn_blocking(move || compact(files, cancel)).await?
    }
}

fn compact(files: Vec<LtxFile>, cancel: CancellationToken) -> Result<LtxFile> {
    let last = files.last().context("empty checkpoint")?.last;
    let deadline = Instant::now() + Duration::from_secs(120);
    let readers = files
        .into_iter()
        .map(|file| {
            Ok(CompactionInput {
                reader: Cursor::new(STANDARD.decode(file.data)?),
                cancel: cancel.clone(),
                deadline,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut data = Vec::new();
    let header = terse_ltx::compact(readers, &mut data).context("compact LTX checkpoint")?;
    ensure!(
        header.min_txid == 1 && header.max_txid == last,
        "compacted checkpoint transaction mismatch"
    );
    Ok(LtxFile {
        level: 9,
        first: header.min_txid,
        last: header.max_txid,
        data: STANDARD.encode(data),
    })
}

struct CompactionInput {
    reader: Cursor<Vec<u8>>,
    cancel: CancellationToken,
    deadline: Instant,
}

impl Read for CompactionInput {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.cancel.is_cancelled() {
            return Err(io::Error::other("LTX compaction cancelled"));
        }
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "LTX compaction timed out",
            ));
        }
        self.reader.read(bytes)
    }
}
