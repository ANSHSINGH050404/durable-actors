use super::storage::LtxFile;
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use std::{
    io::{self, Read},
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
                reader: base64::read::DecoderReader::new(
                    std::io::Cursor::new(file.data),
                    &STANDARD,
                ),
                cancel: cancel.clone(),
                deadline,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut data = crate::payload::Spool::new();
    let mut encoder = base64::write::EncoderWriter::new(&mut data, &STANDARD);
    let header = terse_ltx::compact(readers, &mut encoder).context("compact LTX checkpoint")?;
    ensure!(
        header.min_txid == 1 && header.max_txid == last,
        "compacted checkpoint transaction mismatch"
    );
    encoder.finish()?;
    drop(encoder);
    Ok(LtxFile {
        level: 9,
        first: header.min_txid,
        last: header.max_txid,
        data: crate::payload::Text::from_bytes(data.finish()?)?,
    })
}

struct CompactionInput {
    reader: base64::read::DecoderReader<
        'static,
        base64::engine::general_purpose::GeneralPurpose,
        std::io::Cursor<crate::payload::Text>,
    >,
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
