use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use terse_litestream::{Database, FileStore, Options};

pub(crate) mod compaction;
pub(crate) mod storage;

#[async_trait]
pub(crate) trait Replicator: DatabaseRestore {
    async fn register(&self, path: &Path, replica: &Path) -> Result<()>;
    async fn sync(&self, path: &Path) -> Result<u64>;
    async fn unregister(&self, path: &Path) -> Result<()>;
}

#[async_trait]
pub(crate) trait DatabaseRestore: Send + Sync {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()>;
}

pub(crate) struct EmbeddedRestore;

#[derive(Default)]
pub(crate) struct Litestream {
    registry: Arc<Registry>,
}

#[async_trait]
impl Replicator for Litestream {
    async fn register(&self, path: &Path, replica: &Path) -> Result<()> {
        let (registry, path, replica) =
            (self.registry.clone(), path.to_owned(), replica.to_owned());
        tokio::task::spawn_blocking(move || registry.register(path, replica)).await?
    }

    async fn sync(&self, path: &Path) -> Result<u64> {
        let (registry, path) = (self.registry.clone(), path.to_owned());
        tokio::task::spawn_blocking(move || registry.sync(&path)).await?
    }

    async fn unregister(&self, path: &Path) -> Result<()> {
        let (registry, path) = (self.registry.clone(), path.to_owned());
        tokio::task::spawn_blocking(move || registry.unregister(&path)).await?
    }
}

#[async_trait]
impl DatabaseRestore for Litestream {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()> {
        EmbeddedRestore.restore(replica, path, txid).await
    }
}

#[async_trait]
impl DatabaseRestore for EmbeddedRestore {
    async fn restore(&self, replica: &Path, path: &Path, txid: u64) -> Result<()> {
        ensure!(txid > 0, "cannot restore an empty SQLite position");
        let (replica, path) = (replica.to_owned(), path.to_owned());
        tokio::task::spawn_blocking(move || {
            terse_litestream::restore(&FileStore::new(replica), path, Some(txid))?;
            anyhow::Ok(())
        })
        .await?
    }
}

#[derive(Default)]
struct Registry {
    databases: Mutex<HashMap<PathBuf, Arc<Mutex<Option<Capture>>>>>,
}

impl Registry {
    fn register(&self, path: PathBuf, replica: PathBuf) -> Result<()> {
        let mut databases = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?;
        ensure!(
            !databases.contains_key(&path),
            "SQLite database already registered"
        );
        let database =
            Database::with_store(&path, Options::default(), Arc::new(FileStore::new(replica)))?;
        databases.insert(
            path,
            Arc::new(Mutex::new(Some(Capture {
                database,
                snapshot_at: Instant::now(),
            }))),
        );
        Ok(())
    }

    fn sync(&self, path: &Path) -> Result<u64> {
        let database = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?
            .get(path)
            .cloned()
            .context("SQLite database is not registered")?;
        let mut capture = database
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite capture poisoned"))?;
        capture
            .as_mut()
            .context("SQLite database was unregistered")?
            .sync()
    }

    fn unregister(&self, path: &Path) -> Result<()> {
        let database = self
            .databases
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite registry poisoned"))?
            .remove(path)
            .context("SQLite database is not registered")?;
        let capture = database
            .lock()
            .map_err(|_| anyhow::anyhow!("SQLite capture poisoned"))?
            .take()
            .context("SQLite database was unregistered")?;
        capture.database.close()?;
        Ok(())
    }
}

struct Capture {
    database: Database,
    snapshot_at: Instant,
}

impl Capture {
    fn sync(&mut self) -> Result<u64> {
        let captured = self.database.sync()?;
        if captured.changed && self.snapshot_at.elapsed() >= Duration::from_secs(60) {
            self.database.snapshot()?;
            self.snapshot_at = Instant::now();
        }
        Ok(self.database.position())
    }
}

#[cfg(test)]
#[path = "../tests/unit/litestream.rs"]
mod tests;
