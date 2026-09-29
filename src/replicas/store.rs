use super::record::{Batch, Record, checksum};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub(super) struct ReplicaDisk(Arc<Mutex<Disk>>);
struct Disk {
    db: Connection,
    _lock: std::fs::File,
}

impl ReplicaDisk {
    pub async fn open(path: PathBuf) -> Result<Self> {
        tokio::task::spawn_blocking(move || {
            let parent = path.parent().context("replica data parent missing")?;
            std::fs::create_dir_all(parent)?;
            let lock = std::fs::File::options().create(true).truncate(false).write(true).open(path.with_extension("lock"))?;
            lock.try_lock().context("replica disk is already in use")?;
            let db = Connection::open(&path)?;
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;
                CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS streams (prefix TEXT PRIMARY KEY, initialized INTEGER NOT NULL, sealed INTEGER NOT NULL DEFAULT 0,
                    version INTEGER, latest BLOB, checkpoint_version INTEGER, checkpoint BLOB);
                CREATE TABLE IF NOT EXISTS records (prefix TEXT NOT NULL REFERENCES streams(prefix), version INTEGER NOT NULL,
                    created_at INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY(prefix, version));
                CREATE TABLE IF NOT EXISTS archives (prefix TEXT NOT NULL, first INTEGER NOT NULL, last INTEGER NOT NULL,
                    key TEXT PRIMARY KEY);")?;
            ensure!(db.query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0))? == "ok", "replica disk integrity check failed");
            db.execute("INSERT OR IGNORE INTO metadata VALUES ('identity', ?1)", [uuid::Uuid::new_v4().to_string()])?;
            std::fs::File::open(parent)?.sync_all()?;
            Ok(Self(Arc::new(Mutex::new(Disk { db, _lock: lock }))))
        }).await?
    }

    pub async fn bind_assignment(&self, assignment: &super::Assignment) -> Result<()> {
        let config = serde_json::to_string(assignment)?;
        let prefix = assignment.prefix.clone();
        self.run(move |db| {
            let tx = db.transaction()?;
            tx.execute(
                "INSERT OR IGNORE INTO metadata VALUES ('assignment_config', ?1)",
                [&config],
            )?;
            let previous: String = tx.query_row(
                "SELECT value FROM metadata WHERE key='assignment_config'",
                [],
                |row| row.get(0),
            )?;
            ensure!(previous == config, "replica membership cannot change");
            tx.execute(
                "INSERT OR IGNORE INTO streams(prefix, initialized) VALUES (?1, 1)",
                [&prefix],
            )?;
            let sealed: bool = tx.query_row(
                "SELECT sealed FROM streams WHERE prefix=?1",
                [&prefix],
                |row| row.get(0),
            )?;
            ensure!(!sealed, "replica stream is permanently sealed");
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn assignment(&self) -> Result<Option<super::Assignment>> {
        self.run(|db| {
            let config: Option<String> = db
                .query_row(
                    "SELECT value FROM metadata WHERE key='assignment_config'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(config
                .map(|value| serde_json::from_str(&value))
                .transpose()?)
        })
        .await
    }

    pub async fn require_sealed(&self, prefix: &str) -> Result<()> {
        let prefix = prefix.to_owned();
        self.run(move |db| {
            let sealed: bool = db.query_row(
                "SELECT sealed FROM streams WHERE prefix=?1 AND initialized=1",
                [prefix],
                |row| row.get(0),
            )?;
            ensure!(sealed, "replica must be sealed before final archival");
            Ok(())
        })
        .await
    }

    pub async fn identity(&self) -> Result<String> {
        self.run(|db| {
            Ok(db.query_row(
                "SELECT value FROM metadata WHERE key='identity'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
    }

    pub async fn prepare(&self, prefix: &str) -> Result<()> {
        let prefix = prefix.to_owned();
        self.run(move |db| {
            db.execute(
                "INSERT OR IGNORE INTO streams(prefix, initialized) VALUES (?1, 1)",
                [&prefix],
            )?;
            let (initialized, sealed): (bool, bool) = db.query_row(
                "SELECT initialized, sealed FROM streams WHERE prefix=?1",
                [&prefix],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            ensure!(
                initialized && !sealed,
                "replica stream is permanently sealed"
            );
            Ok(())
        })
        .await
    }

    pub async fn seal(&self, prefix: &str) -> Result<()> {
        let prefix = prefix.to_owned();
        self.run(move |db| {
            ensure!(
                db.execute(
                    "UPDATE streams SET sealed=1 WHERE prefix=?1 AND initialized=1",
                    [&prefix]
                )? == 1,
                "uninitialized replica is not a recovery witness"
            );
            release_archived_payloads(db, &prefix)?;

            Ok(())
        })
        .await
    }

    pub async fn append(&self, prefix: &str, record: Record, now: u64) -> Result<()> {
        let prefix = prefix.to_owned();
        self.run(move |db| {
            let tx = db.transaction()?;
            let (initialized, sealed, version, previous): (
                bool,
                bool,
                Option<i64>,
                Option<Vec<u8>>,
            ) = tx.query_row(
                "SELECT initialized, sealed, version, latest FROM streams WHERE prefix=?1",
                [&prefix],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            ensure!(initialized && !sealed, "replica stream is absent or sealed");
            if version == Some(i64::try_from(record.version)?) {
                ensure!(
                    previous.as_deref().map(checksum).as_ref() == Some(&record.digest),
                    "conflicting retry"
                );
                return Ok(());
            }
            ensure!(
                version.is_none_or(|v| v.checked_add(1) == i64::try_from(record.version).ok()),
                "replica log has a gap or regressed"
            );
            let bytes = record.decode(previous.as_deref())?;
            let snapshot = crate::state_log::StateSnapshot::decode(&bytes)?;
            let epoch = prefix
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .context("epoch missing")?;
            ensure!(
                u64::from_str_radix(epoch, 16)? == snapshot.owner_epoch,
                "replica epoch mismatch"
            );
            tx.execute(
                "INSERT INTO records VALUES (?1, ?2, ?3, ?4)",
                params![
                    prefix,
                    i64::try_from(record.version)?,
                    i64::try_from(now)?,
                    serde_json::to_vec(&record)?
                ],
            )?;
            tx.execute(
                "UPDATE streams SET version=?2, latest=?3 WHERE prefix=?1",
                params![prefix, i64::try_from(record.version)?, bytes],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    pub async fn latest(&self, prefix: &str) -> Result<Option<(String, Vec<u8>)>> {
        let prefix = prefix.to_owned();
        self.run(move |db| {
            let (initialized, version, bytes): (bool, Option<i64>, Option<Vec<u8>>) = db
                .query_row(
                    "SELECT initialized, version, latest FROM streams WHERE prefix=?1",
                    [&prefix],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
            ensure!(initialized, "replica is not a recovery witness");
            Ok(version
                .zip(bytes)
                .map(|(version, bytes)| (format!("{prefix}{version}.json"), bytes)))
        })
        .await
    }

    pub async fn latest_object(&self, prefix: &str) -> Result<Option<String>> {
        let prefix = prefix.to_owned();
        self.run(move |db| {
            let (initialized, version): (bool, Option<i64>) = db.query_row(
                "SELECT initialized, version FROM streams WHERE prefix=?1",
                [&prefix],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            ensure!(initialized, "replica is not a recovery witness");
            Ok(version.map(|v| format!("{prefix}{v}.json")))
        })
        .await
    }

    pub async fn get(&self, prefix: &str, version: u64) -> Result<Option<Vec<u8>>> {
        let prefix = prefix.to_owned();
        self.run(move |db| materialize(db, &prefix, version)).await
    }

    pub async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix = prefix.to_owned();
        self.run(move |db| {
            let mut query = db.prepare("SELECT prefix, version FROM records WHERE substr(prefix,1,length(?1))=?1 UNION SELECT prefix, version FROM streams WHERE substr(prefix,1,length(?1))=?1 AND version IS NOT NULL")?;
            Ok(query.query_map([prefix], |r| Ok(format!("{}{}.json", r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?.collect::<rusqlite::Result<_>>()?)
        }).await
    }

    pub async fn due(&self, now: u64, age: u64, size: usize) -> Result<Vec<String>> {
        self.run(move |db| {
            let mut query = db.prepare("SELECT prefix FROM records GROUP BY prefix HAVING min(created_at)<=?1 OR sum(length(data))>=?2")?;
            Ok(query.query_map(params![i64::try_from(now.saturating_sub(age))?, i64::try_from(size)?], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?)
        }).await
    }

    pub async fn batch(&self, prefix: &str, size: usize) -> Result<Option<Batch>> {
        let prefix = prefix.to_owned();
        self.run(move |db| {
            let mut previous: Option<Vec<u8>> = db.query_row(
                "SELECT checkpoint FROM streams WHERE prefix=?1",
                [&prefix],
                |r| r.get(0),
            )?;
            let records = records(db, &prefix)?;
            let mut batch = Batch {
                prefix,
                records: vec![],
            };
            let mut length = 0;
            for record in records {
                let bytes = record.decode(previous.as_deref())?;
                let archived = if batch.records.is_empty() {
                    Record::encode(record.version, &bytes, None)?
                } else {
                    record
                };
                length += serde_json::to_vec(&archived)?.len();
                batch.records.push(archived);
                previous = Some(bytes);
                if length >= size {
                    break;
                }
            }
            Ok((!batch.records.is_empty()).then_some(batch))
        })
        .await
    }

    pub async fn archived(&self, key: &str, batch: &Batch) -> Result<()> {
        ensure!(key == batch.key()?, "archive identity mismatch");
        let decoded = batch.decode()?;
        let prefix = batch.prefix.clone();
        let key = key.to_owned();
        self.run(move |db| {
            let tx = db.transaction()?;
            let first = decoded.first().unwrap().0;
            let (last, bytes) = decoded.last().unwrap();
            let checkpoint: Option<i64> = tx.query_row(
                "SELECT checkpoint_version FROM streams WHERE prefix=?1",
                [&prefix],
                |r| r.get(0),
            )?;
            if checkpoint.is_some_and(|version| version >= i64::try_from(*last).unwrap_or(i64::MAX))
            {
                return Ok(());
            }
            verify_archived_records(&tx, &prefix, &decoded)?;
            let oldest: Option<i64> = tx.query_row(
                "SELECT min(version) FROM records WHERE prefix=?1",
                [&prefix],
                |r| r.get(0),
            )?;
            ensure!(
                oldest
                    .is_none_or(|version| i64::try_from(first).is_ok_and(|first| first <= version)),
                "archive would skip pending records"
            );
            let latest: Option<i64> = tx.query_row(
                "SELECT version FROM streams WHERE prefix=?1",
                [&prefix],
                |r| r.get(0),
            )?;
            ensure!(
                latest.is_some_and(|version| version >= i64::try_from(*last).unwrap_or(i64::MAX)),
                "archive is ahead of replica"
            );
            tx.execute(
                "INSERT OR IGNORE INTO archives VALUES (?1, ?2, ?3, ?4)",
                params![prefix, i64::try_from(first)?, i64::try_from(*last)?, key],
            )?;
            tx.execute(
                "UPDATE streams SET checkpoint_version=?2, checkpoint=?3 WHERE prefix=?1",
                params![prefix, i64::try_from(*last)?, bytes],
            )?;
            tx.execute(
                "DELETE FROM records WHERE prefix=?1 AND version<=?2",
                params![prefix, i64::try_from(*last)?],
            )?;
            release_archived_payloads(&tx, &prefix)?;
            tx.commit()?;
            Ok(())
        })
        .await
    }

    async fn run<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let disk = self.0.clone();
        tokio::task::spawn_blocking(move || {
            action(
                &mut disk
                    .lock()
                    .map_err(|_| anyhow::anyhow!("replica disk poisoned"))?
                    .db,
            )
        })
        .await?
    }
}

fn release_archived_payloads(db: &Connection, prefix: &str) -> Result<()> {
    db.execute("UPDATE streams SET latest=NULL, checkpoint=NULL WHERE prefix=?1 AND sealed=1 AND version=checkpoint_version", [prefix])?;
    Ok(())
}

fn verify_archived_records(
    db: &Connection,
    prefix: &str,
    decoded: &[(u64, Vec<u8>)],
) -> Result<()> {
    let archived: std::collections::BTreeMap<_, _> = decoded.iter().map(|(v, b)| (*v, b)).collect();
    let mut previous: Option<Vec<u8>> = db.query_row(
        "SELECT checkpoint FROM streams WHERE prefix=?1",
        [prefix],
        |r| r.get(0),
    )?;
    let last = decoded.last().context("empty archive")?.0;
    for record in records(db, prefix)? {
        if record.version > last {
            break;
        }
        let bytes = record.decode(previous.as_deref())?;
        if let Some(expected) = archived.get(&record.version) {
            ensure!(bytes == **expected, "archive conflicts with replica data");
        }
        previous = Some(bytes);
    }
    Ok(())
}

fn records(db: &Connection, prefix: &str) -> Result<Vec<Record>> {
    let mut query = db.prepare("SELECT data FROM records WHERE prefix=?1 ORDER BY version")?;
    query
        .query_map([prefix], |r| r.get::<_, Vec<u8>>(0))?
        .map(|row| Ok(serde_json::from_slice(&row?)?))
        .collect()
}

fn materialize(db: &Connection, prefix: &str, version: u64) -> Result<Option<Vec<u8>>> {
    let row: Option<(Option<i64>, Option<Vec<u8>>, Option<i64>, Option<Vec<u8>>)> = db.query_row(
        "SELECT version, latest, checkpoint_version, checkpoint FROM streams WHERE prefix=?1 AND initialized=1", [prefix],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).optional()?;
    let Some((latest_version, latest, checkpoint_version, mut previous)) = row else {
        return Ok(None);
    };
    if latest_version == Some(i64::try_from(version)?) {
        return Ok(latest);
    }
    if checkpoint_version == Some(i64::try_from(version)?) {
        return Ok(previous);
    }
    if checkpoint_version.is_some_and(|v| u64::try_from(v).is_ok_and(|v| v > version)) {
        return Ok(None);
    }
    for record in records(db, prefix)? {
        let bytes = record.decode(previous.as_deref())?;
        if record.version == version {
            return Ok(Some(bytes));
        }
        if record.version > version {
            break;
        }
        previous = Some(bytes);
    }
    Ok(None)
}

#[cfg(test)]
#[path = "../../tests/unit/replicas/store.rs"]
mod tests;
