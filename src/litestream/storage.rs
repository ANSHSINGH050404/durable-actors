use super::{DatabaseRestore, Replicator};
use anyhow::{Context, Result, ensure};
use base64::engine::general_purpose::STANDARD;
use serde::{Deserialize, Serialize};
use std::{
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqliteState {
    pub txid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

impl PartialEq for SqliteState {
    fn eq(&self, other: &Self) -> bool {
        self.txid == other.txid
    }
}

impl SqliteState {
    #[cfg(test)]
    pub(crate) fn position(txid: u64) -> Self {
        Self { txid, path: None }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LtxFile {
    pub level: u8,
    pub first: u64,
    pub last: u64,
    pub data: crate::payload::Text,
}

impl LtxFile {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(matches!(self.level, 0 | 9), "invalid LTX level");
        ensure!(
            self.first > 0 && self.first <= self.last,
            "invalid LTX transaction range"
        );
        ensure!(self.level != 9 || self.first == 1, "invalid LTX snapshot");
        let count = std::io::copy(&mut self.reader(), &mut std::io::sink())?;
        ensure!(count > 0, "empty LTX file");
        Ok(())
    }

    pub(crate) fn reader(&self) -> impl std::io::Read {
        base64::read::DecoderReader::new(self.data.as_ref(), &STANDARD)
    }

    fn path(&self, replica: &Path) -> PathBuf {
        replica
            .join("ltx")
            .join(self.level.to_string())
            .join(format!("{:016x}-{:016x}.ltx", self.first, self.last))
    }
}

pub(crate) struct SqliteCapture {
    replication: Arc<dyn Replicator>,
    directory: Option<tempfile::TempDir>,
    txid: u64,
    published: u64,
}

impl SqliteCapture {
    pub(crate) async fn new(replication: Arc<dyn Replicator>) -> Result<Self> {
        Self::open(replication, tempfile::tempdir()?).await
    }

    pub(crate) async fn restore(
        replication: Arc<dyn Replicator>,
        files: &[LtxFile],
        txid: u64,
    ) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        restore_database(replication.as_ref(), directory.path(), files, txid).await?;
        Self::open(replication, directory).await
    }

    pub(crate) fn path(&self) -> PathBuf {
        self.directory
            .as_ref()
            .expect("live SQLite directory")
            .path()
            .join("actor.sqlite")
    }
    fn replica(&self) -> PathBuf {
        self.path().with_file_name("replica")
    }
    pub(crate) fn txid(&self) -> u64 {
        self.txid
    }

    pub(crate) fn state(&self) -> SqliteState {
        SqliteState {
            txid: self.txid,
            path: Some(self.path().to_string_lossy().into_owned()),
        }
    }

    pub(crate) async fn capture(&mut self, state: &SqliteState) -> Result<Vec<LtxFile>> {
        ensure!(state.txid >= self.txid, "SQLite transaction went backwards");
        let checkpoint = self
            .files(9, state.txid)
            .await?
            .into_iter()
            .filter(|file| file.last > self.published)
            .max_by_key(|file| file.last);
        let snapshot = checkpoint.as_ref().map(|file| file.last);
        let mut next = checkpoint
            .as_ref()
            .map_or(self.published + 1, |file| file.last + 1);
        let mut files = checkpoint.into_iter().collect::<Vec<_>>();
        for file in self.files(0, state.txid).await? {
            if file.last < next {
                continue;
            }
            ensure!(
                file.first == next,
                "Litestream capture has a transaction gap"
            );
            next = file
                .last
                .checked_add(1)
                .context("Litestream transaction overflow")?;
            files.push(file);
        }
        ensure!(
            next == state
                .txid
                .checked_add(1)
                .context("Litestream transaction overflow")?,
            "Litestream has not replicated the actor commit"
        );
        if let Some(snapshot) = snapshot {
            self.prune(snapshot).await?;
        }
        self.txid = state.txid;
        self.published = state.txid;
        Ok(files)
    }

    async fn prune(&self, snapshot: u64) -> Result<()> {
        let replica = self.replica();
        tokio::task::spawn_blocking(move || {
            use terse_litestream::ReplicaStore;
            let store = terse_litestream::FileStore::new(replica);
            for level in [0, 9] {
                for segment in store.list(level)? {
                    if segment.max_txid < snapshot || (level == 0 && segment.max_txid == snapshot) {
                        store.remove(&segment)?;
                    }
                }
            }
            anyhow::Ok(())
        })
        .await?
    }

    async fn open(replication: Arc<dyn Replicator>, directory: tempfile::TempDir) -> Result<Self> {
        let mut capture = Self {
            replication,
            directory: Some(directory),
            txid: 0,
            published: 0,
        };
        let path = capture.path();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let db = rusqlite::Connection::open(path)?;
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
                CREATE TABLE IF NOT EXISTS __terse_fields (name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))")?;
            let check: String = db.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
            ensure!(check == "ok", "invalid actor SQLite database");
            Ok(())
        }).await??;
        capture
            .replication
            .register(&capture.path(), &capture.replica())
            .await?;
        capture.txid = capture.replication.sync(&capture.path()).await?;
        ensure!(
            capture.txid > 0,
            "Litestream did not capture the actor database"
        );
        Ok(capture)
    }

    async fn files(&self, level: u8, through: u64) -> Result<Vec<LtxFile>> {
        let directory = self.replica().join("ltx").join(level.to_string());
        let mut entries = match tokio::fs::read_dir(directory).await {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            Err(error) => return Err(error.into()),
        };
        let mut files = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let Some((first, last)) = range(&name.to_string_lossy()) else {
                continue;
            };
            if last > through || last <= self.published {
                continue;
            }
            files.push((first, last, entry.path()));
        }
        files.sort_by_key(|(first, last, _)| (*first, *last));
        if level == 9 && files.len() > 1 {
            files.drain(..files.len() - 1);
        }
        let mut captured = Vec::with_capacity(files.len());
        for (first, last, path) in files {
            captured.push(read_ltx(level, first, last, path).await?);
        }
        Ok(captured)
    }
}

impl Drop for SqliteCapture {
    fn drop(&mut self) {
        let replication = self.replication.clone();
        let path = self.path();
        let directory = self.directory.take();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = replication.unregister(&path).await;
                drop(directory);
            });
        }
    }
}

async fn read_ltx(level: u8, first: u64, last: u64, path: PathBuf) -> Result<LtxFile> {
    tokio::task::spawn_blocking(move || {
        let mut input = std::fs::File::open(path)?;
        let mut spool = crate::payload::Spool::new();
        let mut encoder = base64::write::EncoderWriter::new(&mut spool, &STANDARD);
        std::io::copy(&mut input, &mut encoder)?;
        encoder.finish()?;
        drop(encoder);
        Ok(LtxFile {
            level,
            first,
            last,
            data: crate::payload::Text::from_bytes(spool.finish()?)?,
        })
    })
    .await?
}

fn range(name: &str) -> Option<(u64, u64)> {
    let (first, last) = name.strip_suffix(".ltx")?.split_once('-')?;
    if first.len() != 16 || last.len() != 16 {
        return None;
    }
    let first = u64::from_str_radix(first, 16).ok()?;
    let last = u64::from_str_radix(last, 16).ok()?;
    (first > 0 && first <= last).then_some((first, last))
}

pub(crate) async fn restored_fields(
    restore: &dyn DatabaseRestore,
    files: &[LtxFile],
    txid: u64,
) -> Result<serde_json::Value> {
    let directory = tempfile::tempdir()?;
    restore_database(restore, directory.path(), files, txid).await?;
    tokio::task::spawn_blocking(move || {
        let db = rusqlite::Connection::open(directory.path().join("actor.sqlite"))?;
        read_fields(&db)
    })
    .await?
}

async fn restore_database(
    restore: &dyn DatabaseRestore,
    directory: &Path,
    files: &[LtxFile],
    txid: u64,
) -> Result<()> {
    let recovery = directory.join("recovery");
    for file in files {
        file.validate()?;
        let path = file.path(&recovery);
        tokio::fs::create_dir_all(path.parent().context("LTX directory missing")?).await?;
        let file = file.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut output = BufWriter::with_capacity(
                crate::payload::IO_BUFFER_BYTES,
                std::fs::File::create(path)?,
            );
            std::io::copy(&mut file.reader(), &mut output)?;
            output.flush()?;
            Ok(())
        })
        .await??;
    }
    restore
        .restore(&recovery, &directory.join("actor.sqlite"), txid)
        .await?;
    tokio::fs::remove_dir_all(recovery).await?;
    Ok(())
}

pub(crate) fn read_fields(db: &rusqlite::Connection) -> Result<serde_json::Value> {
    let mut statement = db.prepare("SELECT name, value FROM __terse_fields ORDER BY name")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut fields = serde_json::Map::new();
    for row in rows {
        let (name, value) = row?;
        fields.insert(name, serde_json::from_str(&value)?);
    }
    Ok(serde_json::Value::Object(fields))
}

#[cfg(test)]
#[path = "../../tests/unit/sqlite_storage.rs"]
mod tests;
