use std::{collections::BTreeMap, time::Duration};

use anyhow::Result;
use async_trait::async_trait;
use tokio_postgres::IsolationLevel;

use super::{
    TracePersistence, TraceStatus,
    cursor::{HistoryPage, Metadata, ReplayPage},
};
use crate::{
    postgres::PostgresDatabase,
    request_traces::{
        TraceEvent, TracePage, TraceRecord,
        history::HistoryQuery,
        metrics::{
            ClassMetrics, OverviewMetrics, QueueWaitQuery, QueueWaitRow, SocketSession, TimeRange,
        },
        replay::ReplayQuery,
    },
};

mod retention;

#[derive(Clone)]
pub(crate) struct PostgresTracePersistence {
    database: PostgresDatabase,
    retention: Duration,
}

impl PostgresTracePersistence {
    pub(crate) fn new(database: PostgresDatabase, retention: Duration) -> Self {
        Self {
            database,
            retention,
        }
    }
}

#[async_trait]
impl TracePersistence for PostgresTracePersistence {
    async fn initialize(&self) -> Result<()> {
        // Connecting applies the database migrations.
        drop(self.database.connection().await?);
        Ok(())
    }

    async fn record_status(&self, project: &str, dropped: u64, failed: bool) -> Result<()> {
        self.database.execute(
            "INSERT INTO durable_actors_trace_projects(project_id,dropped,persistence_failed)
             VALUES($1,$2,$3) ON CONFLICT(project_id) DO UPDATE SET
             dropped=LEAST(9223372036854775807::numeric,durable_actors_trace_projects.dropped::numeric+EXCLUDED.dropped)::bigint,
             persistence_failed=durable_actors_trace_projects.persistence_failed OR EXCLUDED.persistence_failed",
            &[&project, &(dropped.min(i64::MAX as u64) as i64), &failed],
        ).await?;
        Ok(())
    }

    async fn status(&self, project: &str) -> Result<TraceStatus> {
        let row = self.database.query_opt(
            "SELECT dropped,persistence_failed FROM durable_actors_trace_projects WHERE project_id=$1",
            &[&project],
        ).await?;
        Ok(row
            .map(|row| TraceStatus {
                dropped: row.get::<_, i64>(0) as u64,
                persistence_failed: row.get(1),
            })
            .unwrap_or_default())
    }

    async fn append(&self, events: &[TraceEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let mut projects: BTreeMap<&str, Vec<&TraceEvent>> = BTreeMap::new();
        for event in events {
            projects
                .entry(&event.trace.project_id)
                .or_default()
                .push(event);
        }
        let mut client = self.database.connection().await?;
        let transaction = write_transaction(&mut client).await?;
        // Lock projects in one order; positions become visible in commit order.
        for (project, events) in projects {
            let head = lock_project(&transaction, project).await?;
            insert_events(&transaction, project, head, &events).await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn history(&self, project: &str, query: &HistoryQuery) -> Result<TracePage> {
        let mut client = self.database.connection().await?;
        let transaction = read_snapshot(&mut client).await?;
        let metadata = metadata(&transaction, project).await?;
        let page = HistoryPage::new(project, query, metadata)?;
        let outcome = query.outcome.map(|outcome| outcome.as_str());
        let rows = transaction
            .query(
                include_str!("postgres/history.sql"),
                &[
                    &project,
                    &(page.watermark as i64),
                    &query.actor_name,
                    &query.actor_id,
                    &outcome,
                    &query.from_ms.map(|ms| ms as i64),
                    &query.to_ms.map(|ms| ms as i64),
                    &page.after.as_ref().map(|c| c.time as i64),
                    &page.after.as_ref().map(|c| c.sequence as i64),
                    &((query.limit + 1) as i64),
                    &query.request_id.as_deref().map(str::as_bytes),
                    &query.connection_id,
                ],
            )
            .await?;
        page.finish(query.limit, records(rows)?)
    }

    async fn replay(&self, project: &str, query: &ReplayQuery) -> Result<TracePage> {
        let mut client = self.database.connection().await?;
        let transaction = read_snapshot(&mut client).await?;
        let metadata = metadata(&transaction, project).await?;
        let page = ReplayPage::new(project, query, metadata)?;
        let sql = if page.after().is_some() {
            "SELECT position, event FROM durable_actors_traces
             WHERE project_id = $1 AND position > $2 AND position <= $3
             ORDER BY position ASC
             LIMIT $4"
        } else {
            "SELECT position, event FROM durable_actors_traces
             WHERE project_id = $1 AND position > $2 AND position <= $3
             ORDER BY started_at_ms DESC, position DESC
             LIMIT $4"
        };
        let rows = transaction
            .query(
                sql,
                &[
                    &project,
                    &(page.after().unwrap_or(0) as i64),
                    &(page.head() as i64),
                    &((query.limit + 1) as i64),
                ],
            )
            .await?;
        page.finish(query.limit, records(rows)?)
    }

    async fn metrics(&self, project: &str, range: &TimeRange) -> Result<OverviewMetrics> {
        let mut client = self.database.connection().await?;
        let transaction = read_snapshot(&mut client).await?;
        let rows = transaction
            .query(
                include_str!("postgres/overview.sql"),
                &[
                    &project,
                    &range.from_ms.map(|ms| ms as i64),
                    &range.to_ms.map(|ms| ms as i64),
                ],
            )
            .await?;
        let mut metrics = OverviewMetrics::default();
        for row in rows {
            let attempts: i64 = row.get(2);
            let completed: i64 = row.get(3);
            let metric = ClassMetrics {
                actor_name: row.get::<_, Option<String>>(0).unwrap_or_default(),
                count: row.get::<_, i64>(1) as u64,
                success: (attempts > 0).then(|| 100.0 * completed as f64 / attempts as f64),
                p95: row.get(4),
                queue_p95: row.get(5),
            };
            if row.get::<_, bool>(6) {
                metrics.total = metric;
            } else {
                metrics.classes.push(metric);
            }
        }
        Ok(metrics)
    }

    async fn queue_waits(
        &self,
        project: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>> {
        let mut client = self.database.connection().await?;
        let transaction = read_snapshot(&mut client).await?;
        let rows = transaction
            .query(
                include_str!("postgres/queue_waits.sql"),
                &[
                    &project,
                    &query.from_ms.map(|ms| ms as i64),
                    &query.to_ms.map(|ms| ms as i64),
                    &query.actor_name,
                ],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| QueueWaitRow {
                actor_name: row.get(0),
                actor_id: row.get(1),
                admitted: row.get::<_, i64>(2) as u64,
                average_ms: row.get(3),
                max_ms: row.get(4),
            })
            .collect())
    }

    async fn websockets(&self, project: &str, range: &TimeRange) -> Result<Vec<SocketSession>> {
        let mut client = self.database.connection().await?;
        let transaction = read_snapshot(&mut client).await?;
        let rows = transaction
            .query(
                include_str!("postgres/websockets.sql"),
                &[
                    &project,
                    &range.from_ms.map(|ms| ms as i64),
                    &range.to_ms.map(|ms| ms as i64),
                ],
            )
            .await?;
        rows.into_iter()
            .map(|row| {
                let connect: Option<String> = row.get(7);
                let metadata = connect
                    .map(|event| serde_json::from_str::<serde_json::Value>(&event))
                    .transpose()?
                    .and_then(|mut event| event.as_object_mut()?.remove("metadata"));
                Ok(SocketSession {
                    connection_id: row.get(0),
                    actor_name: row.get(1),
                    actor_id: row.get(2),
                    host_id: row.get(3),
                    opened_at_ms: row.get::<_, Option<i64>>(4).map(|ms| ms as u64),
                    closed_at_ms: row.get::<_, Option<i64>>(5).map(|ms| ms as u64),
                    last_seen_ms: row.get::<_, Option<i64>>(6).map(|ms| ms as u64),
                    messages: row.get::<_, i64>(8) as u64,
                    failures: row.get::<_, i64>(9) as u64,
                    metadata,
                })
            })
            .collect()
    }
}

async fn read_snapshot(
    client: &mut deadpool_postgres::Object,
) -> Result<deadpool_postgres::Transaction<'_>> {
    let transaction = client
        .build_transaction()
        .read_only(true)
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    transaction
        .batch_execute(
            "SET LOCAL statement_timeout = '10s';
             SET LOCAL lock_timeout = '2s'",
        )
        .await?;
    Ok(transaction)
}

async fn write_transaction(
    client: &mut deadpool_postgres::Object,
) -> Result<deadpool_postgres::Transaction<'_>> {
    let transaction = client
        .build_transaction()
        .read_only(false)
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()
        .await?;
    transaction
        .batch_execute(
            "SET LOCAL statement_timeout = '10s';
             SET LOCAL lock_timeout = '2s'",
        )
        .await?;
    Ok(transaction)
}

async fn lock_project(transaction: &tokio_postgres::Transaction<'_>, project: &str) -> Result<i64> {
    Ok(transaction
        .query_one(
            "INSERT INTO durable_actors_trace_projects (project_id)
             VALUES ($1)
             ON CONFLICT (project_id)
             DO UPDATE SET head = durable_actors_trace_projects.head
             RETURNING head",
            &[&project],
        )
        .await?
        .get(0))
}

async fn insert_events(
    transaction: &tokio_postgres::Transaction<'_>,
    project: &str,
    head: i64,
    events: &[&TraceEvent],
) -> Result<()> {
    let payloads = events
        .iter()
        .map(serde_json::to_string)
        .collect::<Result<Vec<_>, _>>()?;
    let timestamps = events
        .iter()
        .map(|e| i64::try_from(e.trace.started_at_ms))
        .collect::<Result<Vec<_>, _>>()?;
    let ids: Vec<_> = events.iter().map(|e| e.event_id.as_str()).collect();
    let actors: Vec<_> = events.iter().map(|e| e.trace.actor_name.as_str()).collect();
    let actor_ids: Vec<_> = events.iter().map(|e| e.trace.actor_id.as_str()).collect();
    let outcomes: Vec<_> = events.iter().map(|e| e.trace.outcome.as_str()).collect();
    let durations: Vec<_> = events.iter().map(|e| e.trace.duration_ms).collect();
    let waits: Vec<_> = events.iter().map(|e| e.trace.queue_wait_ms).collect();
    let kinds: Vec<_> = events.iter().map(|e| e.trace.kind.as_str()).collect();
    let operations: Vec<_> = events.iter().map(|e| e.trace.operation.as_str()).collect();
    let connections: Vec<_> = events
        .iter()
        .map(|e| e.trace.connection_id.as_deref())
        .collect();
    let hosts: Vec<_> = events.iter().map(|e| e.host_id.as_str()).collect();
    let requests: Vec<_> = events
        .iter()
        .map(|e| e.trace.request_id.as_bytes())
        .collect();
    // Reports contain up to 64 events; one insert avoids a round trip per event.
    // Parameter order must match the unnest types and aliases in append.sql.
    transaction
        .execute(
            include_str!("postgres/append.sql"),
            &[
                &project,
                &head,
                &ids,
                &payloads,
                &timestamps,
                &actors,
                &actor_ids,
                &outcomes,
                &durations,
                &waits,
                &kinds,
                &operations,
                &connections,
                &hosts,
                &requests,
            ],
        )
        .await?;
    Ok(())
}

async fn metadata(
    transaction: &tokio_postgres::Transaction<'_>,
    project: &str,
) -> Result<Metadata> {
    let row = transaction
        .query_opt(
            "SELECT head, pruned, evicted
             FROM durable_actors_trace_projects
             WHERE project_id = $1",
            &[&project],
        )
        .await?;
    Ok(Metadata {
        // Cursors assume PostgreSQL project history is not manually recreated.
        generation: "postgres".into(),
        head: row.as_ref().map_or(0, |row| row.get::<_, i64>(0) as u64),
        pruned: row.as_ref().map_or(0, |row| row.get::<_, i64>(1) as u64),
        evicted: row.as_ref().map_or(0, |row| row.get::<_, i64>(2) as u64),
    })
}

fn records(rows: Vec<tokio_postgres::Row>) -> Result<Vec<TraceRecord>> {
    rows.into_iter()
        .map(|row| {
            Ok(TraceRecord {
                sequence: row.get::<_, i64>(0) as u64,
                event: serde_json::from_str(row.get::<_, &str>(1))?,
            })
        })
        .collect()
}
