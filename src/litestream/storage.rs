use super::{DatabaseRestore, Replicator};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SqliteState {
    pub txid: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub socket: Option<String>,
}

impl PartialEq for SqliteState {
    fn eq(&self, other: &Self) -> bool {
        self.txid == other.txid
    }
}

impl SqliteState {
    #[cfg(test)]
    pub(crate) fn position(txid: u64) -> Self {
        Self {
            txid,
            path: None,
            socket: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LtxFile {
    pub level: u8,
    pub first: u64,
    pub last: u64,
    pub data: String,
}

impl LtxFile {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(matches!(self.level, 0 | 9), "invalid LTX level");
        ensure!(
            self.first > 0 && self.first <= self.last,
            "invalid LTX transaction range"
        );
        ensure!(self.level != 9 || self.first == 1, "invalid LTX snapshot");
        ensure!(!STANDARD.decode(&self.data)?.is_empty(), "empty LTX file");
        Ok(())
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
            socket: Some(self.replication.socket().to_string_lossy().into_owned()),
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
        self.txid = state.txid;
        self.published = state.txid;
        Ok(files)
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
            if last > through || (level == 0 && last <= self.published) {
                continue;
            }
            files.push(LtxFile {
                level,
                first,
                last,
                data: STANDARD.encode(tokio::fs::read(entry.path()).await?),
            });
        }
        files.sort_by_key(|file| (file.first, file.last));
        Ok(files)
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
        tokio::fs::write(path, STANDARD.decode(&file.data)?).await?;
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
