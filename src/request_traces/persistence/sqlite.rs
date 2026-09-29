use std::{
    fs::File,
    io::ErrorKind,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::Deserialize;

use super::{TracePersistence, TraceStatus};
use crate::request_traces::{
    TraceEvent, TracePage,
    history::HistoryQuery,
    metrics::{OverviewMetrics, QueueWaitQuery, QueueWaitRow, SocketSession, TimeRange},
    replay::ReplayQuery,
};

mod history;
mod metrics;
mod replay;

const LOCAL_RETENTION: usize = 10_000;

pub(crate) struct SqliteTracePersistence {
    path: PathBuf,
    retention: usize,
    connection: Arc<Mutex<Option<Connection>>>,
}

impl SqliteTracePersistence {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            retention: LOCAL_RETENTION,
            connection: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn in_memory() -> Self {
        Self::new(":memory:".into())
    }

    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let path = self.path.clone();
        let shared = self.connection.clone();
        tokio::task::spawn_blocking(move || {
            let mut shared = shared.lock().unwrap();
            if shared.is_none() {
                let mut connection =
                    Connection::open(&path).context("open request trace database")?;
                connection.busy_timeout(Duration::from_secs(5))?;
                initialize(&mut connection, &path)?;
                connection.execute_batch("CREATE TABLE IF NOT EXISTS trace_status(project_id TEXT PRIMARY KEY,dropped INTEGER NOT NULL DEFAULT 0,persistence_failed INTEGER NOT NULL DEFAULT 0)")?;
                *shared = Some(connection);
            }
            operation(shared.as_mut().unwrap())
        })
        .await?
    }
}

#[async_trait]
impl TracePersistence for SqliteTracePersistence {
    async fn initialize(&self) -> Result<()> {
        self.run(|_| Ok(())).await
    }

    async fn record_status(&self, project: &str, dropped: u64, failed: bool) -> Result<()> {
        let project = project.to_owned();
        self.run(move |db| {
            db.execute("INSERT INTO trace_status(project_id,dropped,persistence_failed) VALUES(?1,?2,?3) ON CONFLICT(project_id) DO UPDATE SET dropped=CAST(MIN(9223372036854775807,dropped+excluded.dropped) AS INTEGER),persistence_failed=persistence_failed OR excluded.persistence_failed", params![project, dropped.min(i64::MAX as u64) as i64, failed])?;
            Ok(())
        }).await
    }

    async fn status(&self, project: &str) -> Result<TraceStatus> {
        let project = project.to_owned();
        self.run(move |db| {
            Ok(db
                .query_row(
                    "SELECT dropped,persistence_failed FROM trace_status WHERE project_id=?1",
                    [project],
                    |row| {
                        Ok(TraceStatus {
                            dropped: row.get::<_, i64>(0)? as u64,
                            persistence_failed: row.get(1)?,
                        })
                    },
                )
                .optional()?
                .unwrap_or_default())
        })
        .await
    }

    async fn append(&self, events: &[TraceEvent]) -> Result<()> {
        let events = events.to_vec();
        let retention = self.retention;
        self.run(move |connection| {
            let transaction = connection.transaction()?;
            let inserted = insert_events(&transaction, &events)?;
            transaction.execute("UPDATE trace_meta SET total = total + ?1", [inserted as i64])?;
            transaction.execute(
                "UPDATE trace_meta SET pruned = MAX(pruned, COALESCE((SELECT position FROM traces ORDER BY position DESC LIMIT 1 OFFSET ?1), 0))",
                [retention as i64],
            )?;
            transaction.execute("DELETE FROM traces WHERE position <= (SELECT pruned FROM trace_meta)", [])?;
            transaction.commit()?;
            Ok(())
        }).await
    }

    async fn metrics(&self, project_id: &str, query: &TimeRange) -> Result<OverviewMetrics> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| metrics::overview(connection, &project_id, &query))
            .await
    }

    async fn queue_waits(
        &self,
        project_id: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| metrics::queue_waits(connection, &project_id, &query))
            .await
    }

    async fn websockets(&self, project_id: &str, query: &TimeRange) -> Result<Vec<SocketSession>> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| metrics::websockets(connection, &project_id, &query))
            .await
    }

    async fn history(&self, project_id: &str, query: &HistoryQuery) -> Result<TracePage> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| history::query(connection, &project_id, &query))
            .await
    }

    async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage> {
        let project_id = project_id.to_owned();
        let query = query.clone();
        self.run(move |connection| replay::query(connection, &project_id, &query))
            .await
    }
}

fn initialize(connection: &mut Connection, path: &Path) -> Result<()> {
    let version: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    ensure!(version <= 5, "unsupported request trace database version");
    if version == 5 {
        return Ok(());
    }
    let transaction = connection.transaction()?;
    if version == 0 {
        transaction.execute_batch("CREATE TABLE traces (position INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, event TEXT NOT NULL);")?;
        if path != Path::new(":memory:") {
            import_snapshot(&transaction, &path.with_extension("json"))?;
        }
    }
    if version < 2 {
        transaction.execute_batch("
        ALTER TABLE traces ADD COLUMN started_at_ms INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE traces ADD COLUMN actor_name TEXT NOT NULL DEFAULT '';
        ALTER TABLE traces ADD COLUMN actor_id TEXT NOT NULL DEFAULT '';
        ALTER TABLE traces ADD COLUMN outcome TEXT NOT NULL DEFAULT '';
        UPDATE traces SET started_at_ms = json_extract(event, '$.startedAtMs'), actor_name = json_extract(event, '$.actorName'), actor_id = json_extract(event, '$.actorId'), outcome = json_extract(event, '$.outcome');
        CREATE INDEX traces_time ON traces(started_at_ms DESC, position DESC);
        CREATE INDEX traces_actor ON traces(actor_name, actor_id, started_at_ms DESC, position DESC);
        CREATE INDEX traces_outcome ON traces(outcome, started_at_ms DESC, position DESC);
        CREATE TABLE trace_meta (generation TEXT NOT NULL, pruned INTEGER NOT NULL, total INTEGER NOT NULL);
    ")?;
        transaction.execute(
            "INSERT INTO trace_meta VALUES (?1, 0, (SELECT COUNT(*) FROM traces))",
            [uuid::Uuid::new_v4().to_string()],
        )?;
    }
    transaction.execute_batch(
        "DROP VIEW IF EXISTS request_events; DROP VIEW IF EXISTS request_history;",
    )?;
    transaction.execute_batch(
        "ALTER TABLE traces ADD COLUMN project_id TEXT NOT NULL DEFAULT '';
        UPDATE traces SET project_id = COALESCE(json_extract(event, '$.projectId'), '');
        CREATE INDEX traces_project_time ON traces(project_id, started_at_ms DESC, position DESC);
        CREATE INDEX traces_project_position ON traces(project_id, position);
        CREATE TABLE trace_projects (project_id TEXT PRIMARY KEY, total INTEGER NOT NULL, head INTEGER NOT NULL, pruned INTEGER NOT NULL);
        INSERT INTO trace_projects SELECT project_id, COUNT(*), MAX(position), 0 FROM traces WHERE project_id <> '' GROUP BY project_id;
        CREATE TRIGGER traces_project_insert AFTER INSERT ON traces WHEN NEW.project_id <> '' BEGIN
            INSERT INTO trace_projects VALUES (NEW.project_id, 1, NEW.position, 0)
            ON CONFLICT(project_id) DO UPDATE SET total = total + 1, head = NEW.position;
        END;
        CREATE TRIGGER traces_project_delete AFTER DELETE ON traces BEGIN
            UPDATE trace_projects SET pruned = MAX(pruned, OLD.position) WHERE project_id = OLD.project_id;
        END;",
    )?;
    transaction.pragma_update(None, "user_version", 5)?;
    transaction.commit()?;
    Ok(())
}

fn insert_events(transaction: &Transaction<'_>, events: &[TraceEvent]) -> Result<usize> {
    let mut statement = transaction.prepare("INSERT INTO traces (event_id, event, started_at_ms, actor_name, actor_id, outcome, project_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT (event_id) DO NOTHING")?;
    let mut inserted = 0;
    for event in events {
        inserted += statement.execute(params![
            event.event_id,
            serde_json::to_string(event)?,
            i64::try_from(event.trace.started_at_ms)?,
            event.trace.actor_name,
            event.trace.actor_id,
            event.trace.outcome.as_str(),
            event.trace.project_id
        ])?;
    }
    Ok(inserted)
}

fn import_snapshot(transaction: &Transaction<'_>, path: &Path) -> Result<()> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("open legacy request history"),
    };
    let saved: LegacySnapshot =
        serde_json::from_reader(file).context("parse legacy request history")?;
    ensure!(
        saved.version == 1,
        "unsupported legacy request history version"
    );
    for mut event in saved.history.records {
        let id = event
            .get("eventId")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        event
            .as_object_mut()
            .context("invalid legacy trace record")?
            .insert("eventId".into(), id.clone().into());
        transaction.execute(
            "INSERT INTO traces (event_id, event) VALUES (?1, ?2)",
            params![id, serde_json::to_string(&event)?],
        )?;
    }
    Ok(())
}

#[derive(Deserialize)]
struct LegacySnapshot {
    version: u32,
    history: LegacyHistory,
}

#[derive(Deserialize)]
struct LegacyHistory {
    records: Vec<serde_json::Value>,
}

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/persistence/sqlite/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/persistence/sqlite/history_tests.rs"]
mod history_tests;

#[cfg(test)]
#[path = "../../../tests/unit/request_traces/persistence/sqlite/metrics_tests.rs"]
mod metrics_tests;
