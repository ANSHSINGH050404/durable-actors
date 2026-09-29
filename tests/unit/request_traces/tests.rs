use super::persistence::{TracePersistence, sqlite::SqliteTracePersistence};
use super::*;
use std::sync::Mutex;

#[tokio::test]
async fn trace_reports_require_the_authenticated_project() -> Result<()> {
    let store = TraceStore::default();
    assert!(
        store
            .record("other", "host", "session", vec![trace(1)], 0)
            .await
            .is_err()
    );
    assert!(
        store
            .record("", "host", "session", vec![], 0)
            .await
            .is_err()
    );
    let mut payload = serde_json::to_value(trace(1))?;
    payload.as_object_mut().unwrap().remove("projectId");
    assert!(serde_json::from_value::<RequestTrace>(payload).is_err());
    assert!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .records
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn persistence_receives_only_new_events_with_distinct_ids() -> Result<()> {
    struct RecordingPersistence(Mutex<Vec<Vec<TraceEvent>>>, SqliteTracePersistence);
    impl Default for RecordingPersistence {
        fn default() -> Self {
            Self(Mutex::new(vec![]), SqliteTracePersistence::in_memory())
        }
    }
    #[async_trait::async_trait]
    impl TracePersistence for RecordingPersistence {
        async fn record_status(&self, project: &str, dropped: u64, failed: bool) -> Result<()> {
            self.1.record_status(project, dropped, failed).await
        }
        async fn status(&self, project: &str) -> Result<persistence::TraceStatus> {
            self.1.status(project).await
        }

        async fn initialize(&self) -> Result<()> {
            Ok(())
        }
        async fn metrics(
            &self,
            project_id: &str,
            query: &metrics::TimeRange,
        ) -> Result<metrics::OverviewMetrics> {
            SqliteTracePersistence::in_memory()
                .metrics(project_id, query)
                .await
        }
        async fn queue_waits(
            &self,
            project_id: &str,
            query: &metrics::QueueWaitQuery,
        ) -> Result<Vec<metrics::QueueWaitRow>> {
            SqliteTracePersistence::in_memory()
                .queue_waits(project_id, query)
                .await
        }
        async fn websockets(
            &self,
            project_id: &str,
            query: &metrics::TimeRange,
        ) -> Result<Vec<metrics::SocketSession>> {
            SqliteTracePersistence::in_memory()
                .websockets(project_id, query)
                .await
        }

        async fn history(
            &self,
            project_id: &str,
            query: &history::HistoryQuery,
        ) -> Result<TracePage> {
            SqliteTracePersistence::in_memory()
                .history(project_id, query)
                .await
        }
        async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage> {
            SqliteTracePersistence::in_memory()
                .replay(project_id, query)
                .await
        }
        async fn append(&self, events: &[TraceEvent]) -> Result<()> {
            self.0.lock().unwrap().push(events.to_vec());
            Ok(())
        }
    }
    let persistence = Arc::new(RecordingPersistence::default());
    let store = TraceStore::open(persistence.clone()).await?;
    for _ in 0..2 {
        store
            .record("default", "host", "session", vec![trace(1)], 0)
            .await?;
    }
    let batches = persistence.0.lock().unwrap();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].len(), 1);
    assert_eq!(batches[1].len(), 1);
    assert_ne!(batches[0][0].event_id, batches[1][0].event_id);
    Ok(())
}

#[tokio::test]
async fn stalled_persistence_has_a_bounded_backlog() -> Result<()> {
    struct BlockedPersistence(tokio::sync::Semaphore, SqliteTracePersistence);
    #[async_trait::async_trait]
    impl TracePersistence for BlockedPersistence {
        async fn record_status(&self, project: &str, dropped: u64, failed: bool) -> Result<()> {
            self.1.record_status(project, dropped, failed).await
        }
        async fn status(&self, project: &str) -> Result<persistence::TraceStatus> {
            self.1.status(project).await
        }

        async fn initialize(&self) -> Result<()> {
            Ok(())
        }
        async fn metrics(
            &self,
            project_id: &str,
            query: &metrics::TimeRange,
        ) -> Result<metrics::OverviewMetrics> {
            SqliteTracePersistence::in_memory()
                .metrics(project_id, query)
                .await
        }
        async fn queue_waits(
            &self,
            project_id: &str,
            query: &metrics::QueueWaitQuery,
        ) -> Result<Vec<metrics::QueueWaitRow>> {
            SqliteTracePersistence::in_memory()
                .queue_waits(project_id, query)
                .await
        }
        async fn websockets(
            &self,
            project_id: &str,
            query: &metrics::TimeRange,
        ) -> Result<Vec<metrics::SocketSession>> {
            SqliteTracePersistence::in_memory()
                .websockets(project_id, query)
                .await
        }

        async fn history(
            &self,
            project_id: &str,
            query: &history::HistoryQuery,
        ) -> Result<TracePage> {
            SqliteTracePersistence::in_memory()
                .history(project_id, query)
                .await
        }
        async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage> {
            SqliteTracePersistence::in_memory()
                .replay(project_id, query)
                .await
        }
        async fn append(&self, _: &[TraceEvent]) -> Result<()> {
            self.0.acquire().await?.forget();
            Ok(())
        }
    }
    let persistence = Arc::new(BlockedPersistence(
        tokio::sync::Semaphore::new(0),
        SqliteTracePersistence::in_memory(),
    ));
    let store = TraceStore::open(persistence.clone()).await?;
    let mut writes = tokio::task::JoinSet::new();
    for id in 0..64 {
        let store = store.clone();
        writes.spawn(async move {
            store
                .record("default", "host", "session", vec![trace(id)], 0)
                .await
        });
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.pending.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            store.record("default", "host", "session", vec![trace(64)], 0),
        )
        .await?
        .is_err()
    );
    assert_eq!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .cursor,
        0
    );
    assert!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .persistence_failed
    );
    persistence.0.add_permits(64);
    while let Some(result) = writes.join_next().await {
        result??;
    }
    Ok(())
}

#[tokio::test]
async fn persisted_events_replay_after_restart_with_a_durable_cursor() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("requests.sqlite3");
    let open = || TraceStore::open(Arc::new(SqliteTracePersistence::new(path.clone())));
    let store = open().await?;
    store
        .record(
            "default",
            "host",
            "session",
            (0..TRACE_CAPACITY + 2).map(trace).collect(),
            3,
        )
        .await?;
    let before: Vec<_> = store
        .replay("default", &ReplayQuery::default())
        .await?
        .records
        .into_iter()
        .map(|record| record.event.event_id)
        .collect();
    drop(store);

    let restored = open().await?;
    assert_eq!(
        restored
            .replay("default", &ReplayQuery::default())
            .await?
            .records
            .iter()
            .map(|record| record.event.event_id.clone())
            .collect::<Vec<_>>(),
        before
    );
    assert_eq!(
        restored
            .replay("default", &ReplayQuery::default())
            .await?
            .records
            .len(),
        TRACE_CAPACITY
    );
    assert_eq!(
        restored
            .replay("default", &ReplayQuery::default())
            .await?
            .evicted,
        0
    );
    assert_eq!(
        restored
            .replay("default", &ReplayQuery::default())
            .await?
            .dropped,
        3
    );
    let cursor = restored
        .replay("default", &ReplayQuery::default())
        .await?
        .resume_cursor;
    restored
        .record("default", "host", "session", vec![trace(999)], 0)
        .await?;
    let delta = restored
        .replay(
            "default",
            &ReplayQuery {
                cursor: Some(cursor),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(delta.records.len(), 1);
    assert_eq!(delta.records[0].sequence, (TRACE_CAPACITY + 3) as u64);
    assert_eq!(delta.records[0].event.trace.request_id, "999");
    Ok(())
}

#[tokio::test]
async fn failed_persistence_is_not_published_and_reports_the_failure() -> Result<()> {
    struct FailingPersistence(SqliteTracePersistence);
    #[async_trait::async_trait]
    impl TracePersistence for FailingPersistence {
        async fn record_status(&self, project: &str, dropped: u64, failed: bool) -> Result<()> {
            self.0.record_status(project, dropped, failed).await
        }
        async fn status(&self, project: &str) -> Result<persistence::TraceStatus> {
            self.0.status(project).await
        }

        async fn initialize(&self) -> Result<()> {
            Ok(())
        }
        async fn metrics(
            &self,
            project_id: &str,
            query: &metrics::TimeRange,
        ) -> Result<metrics::OverviewMetrics> {
            SqliteTracePersistence::in_memory()
                .metrics(project_id, query)
                .await
        }
        async fn queue_waits(
            &self,
            project_id: &str,
            query: &metrics::QueueWaitQuery,
        ) -> Result<Vec<metrics::QueueWaitRow>> {
            SqliteTracePersistence::in_memory()
                .queue_waits(project_id, query)
                .await
        }
        async fn websockets(
            &self,
            project_id: &str,
            query: &metrics::TimeRange,
        ) -> Result<Vec<metrics::SocketSession>> {
            SqliteTracePersistence::in_memory()
                .websockets(project_id, query)
                .await
        }

        async fn history(
            &self,
            project_id: &str,
            query: &history::HistoryQuery,
        ) -> Result<TracePage> {
            SqliteTracePersistence::in_memory()
                .history(project_id, query)
                .await
        }
        async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage> {
            SqliteTracePersistence::in_memory()
                .replay(project_id, query)
                .await
        }
        async fn append(&self, _: &[TraceEvent]) -> Result<()> {
            anyhow::bail!("disk full")
        }
    }
    let store = TraceStore::open(Arc::new(FailingPersistence(
        SqliteTracePersistence::in_memory(),
    )))
    .await?;
    let changes = store.changes.subscribe();
    assert!(
        store
            .record("default", "host", "session", vec![trace(1)], 2)
            .await
            .is_err()
    );
    assert_eq!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .cursor,
        0
    );
    assert_eq!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .dropped,
        0
    );
    store
        .record("default", "host", "session", vec![], 3)
        .await?;
    assert_eq!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .dropped,
        3
    );
    assert_eq!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .records
            .len(),
        0
    );
    assert!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .persistence_failed
    );
    assert!(changes.has_changed()?);
    Ok(())
}

#[tokio::test]
async fn unreadable_history_is_not_silently_replaced() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("requests.sqlite3");
    std::fs::write(&path, "broken history")?;
    assert!(
        TraceStore::open(Arc::new(SqliteTracePersistence::new(path.clone())))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read_to_string(path)?, "broken history");
    Ok(())
}

#[tokio::test]
async fn concurrent_batches_survive_reload_without_lost_updates() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("requests.sqlite3");
    let store = TraceStore::open(Arc::new(SqliteTracePersistence::new(path.clone()))).await?;
    let mut writes = tokio::task::JoinSet::new();
    for id in 0..20 {
        let store = store.clone();
        writes.spawn(async move {
            store
                .record("default", "host", "session", vec![trace(id)], 1)
                .await
        });
    }
    while let Some(result) = writes.join_next().await {
        result??;
    }
    let restored = TraceStore::open(Arc::new(SqliteTracePersistence::new(path))).await?;
    let page = restored.replay("default", &ReplayQuery::default()).await?;
    assert_eq!(page.cursor, 20);
    assert_eq!(page.dropped, 20);
    let ids: std::collections::HashSet<_> = page
        .records
        .iter()
        .map(|record| &record.event.trace.request_id)
        .collect();
    assert_eq!(ids.len(), 20);
    let persisted_ids: std::collections::HashSet<_> = page
        .records
        .iter()
        .map(|record| &record.event.event_id)
        .collect();
    let live = store.replay("default", &ReplayQuery::default()).await?;
    assert_eq!(
        persisted_ids,
        live.records
            .iter()
            .map(|record| &record.event.event_id)
            .collect()
    );
    Ok(())
}

#[tokio::test]
async fn cancelling_a_report_does_not_cancel_its_commit() -> Result<()> {
    struct PausedPersistence {
        file: SqliteTracePersistence,
        started: tokio::sync::Notify,
        resume: tokio::sync::Notify,
        finished: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl TracePersistence for PausedPersistence {
        async fn record_status(&self, project: &str, dropped: u64, failed: bool) -> Result<()> {
            self.file.record_status(project, dropped, failed).await
        }
        async fn status(&self, project: &str) -> Result<persistence::TraceStatus> {
            self.file.status(project).await
        }

        async fn initialize(&self) -> Result<()> {
            Ok(())
        }
        async fn metrics(
            &self,
            project_id: &str,
            query: &metrics::TimeRange,
        ) -> Result<metrics::OverviewMetrics> {
            SqliteTracePersistence::in_memory()
                .metrics(project_id, query)
                .await
        }
        async fn queue_waits(
            &self,
            project_id: &str,
            query: &metrics::QueueWaitQuery,
        ) -> Result<Vec<metrics::QueueWaitRow>> {
            SqliteTracePersistence::in_memory()
                .queue_waits(project_id, query)
                .await
        }
        async fn websockets(
            &self,
            project_id: &str,
            query: &metrics::TimeRange,
        ) -> Result<Vec<metrics::SocketSession>> {
            SqliteTracePersistence::in_memory()
                .websockets(project_id, query)
                .await
        }

        async fn history(
            &self,
            project_id: &str,
            query: &history::HistoryQuery,
        ) -> Result<TracePage> {
            SqliteTracePersistence::in_memory()
                .history(project_id, query)
                .await
        }
        async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage> {
            self.file.replay(project_id, query).await
        }
        async fn append(&self, events: &[TraceEvent]) -> Result<()> {
            self.started.notify_one();
            self.resume.notified().await;
            self.file.append(events).await?;
            self.finished.notify_one();
            Ok(())
        }
    }
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("requests.sqlite3");
    let persistence = Arc::new(PausedPersistence {
        file: SqliteTracePersistence::new(path.clone()),
        started: tokio::sync::Notify::new(),
        resume: tokio::sync::Notify::new(),
        finished: tokio::sync::Notify::new(),
    });
    let store = TraceStore::open(persistence.clone()).await?;
    let mut changes = store.changes.subscribe();
    let writer = store.clone();
    let report = tokio::spawn(async move {
        writer
            .record("default", "host", "session", vec![trace(1)], 0)
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), persistence.started.notified()).await?;
    assert_eq!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .records
            .len(),
        0,
        "live delivery must wait for the commit"
    );
    assert!(!changes.has_changed()?);
    report.abort();
    assert!(report.await.unwrap_err().is_cancelled());
    persistence.resume.notify_one();
    tokio::time::timeout(Duration::from_secs(5), persistence.finished.notified()).await?;
    tokio::time::timeout(Duration::from_secs(5), changes.changed()).await??;
    let restored = TraceStore::open(Arc::new(SqliteTracePersistence::new(path))).await?;
    assert_eq!(
        restored
            .replay("default", &ReplayQuery::default())
            .await?
            .cursor,
        1
    );
    assert_eq!(
        serde_json::to_value(
            restored
                .replay("default", &ReplayQuery::default())
                .await?
                .records
        )?,
        serde_json::to_value(
            store
                .replay("default", &ReplayQuery::default())
                .await?
                .records
        )?
    );
    Ok(())
}

fn trace(id: usize) -> RequestTrace {
    RequestTrace {
        state_version: None,
        project_id: "default".into(),
        request_id: id.to_string(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
        kind: RequestKind::Method,
        operation: "increment".into(),
        connection_id: None,
        started_at_ms: 1000,
        duration_ms: 12.0,
        queue_wait_ms: Some(5.0),
        outcome: RequestOutcome::Completed,
        metadata: None,
    }
}

#[tokio::test]
async fn replay_limits_do_not_count_retained_requests_as_evicted() -> Result<()> {
    let store = TraceStore::default();
    for id in 0..TRACE_CAPACITY + 2 {
        store
            .record("default", "host", "session", vec![trace(id)], 0)
            .await?;
    }
    let page = store.replay("default", &ReplayQuery::default()).await?;
    assert_eq!(page.records.len(), TRACE_CAPACITY);
    assert_eq!(page.evicted, 0);
    assert_eq!(
        page.records
            .iter()
            .map(|r| r.event.trace.request_id.clone())
            .collect::<Vec<_>>(),
        (2..TRACE_CAPACITY + 2)
            .rev()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(page.cursor, (TRACE_CAPACITY + 2) as u64);
    assert!(
        store
            .replay(
                "default",
                &ReplayQuery {
                    cursor: Some(page.resume_cursor),
                    ..Default::default()
                }
            )
            .await?
            .records
            .is_empty()
    );
    Ok(())
}

#[test]
fn trace_validation_rejects_invalid_timings() {
    let mut value = trace(1);
    assert!(value.validate().is_ok());
    value.queue_wait_ms = Some(13.0);
    assert!(value.validate().is_err());
    value.queue_wait_ms = None;
    value.duration_ms = -1.0;
    assert!(value.validate().is_err());
}

#[tokio::test]
async fn delivery_loss_is_visible_even_without_new_records() -> Result<()> {
    let store = TraceStore::default();
    store
        .record("default", "host", "session", vec![], 3)
        .await?;
    assert_eq!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .dropped,
        3
    );
    assert_eq!(
        store
            .replay("other", &ReplayQuery::default())
            .await?
            .dropped,
        0
    );
    Ok(())
}

#[test]
fn backpressure_drops_telemetry_without_blocking_requests() {
    let (sender, _receiver) = TraceSender::channel(1);
    sender.send(trace(1));
    sender.send(trace(2));
    assert_eq!(sender.dropped.load(Ordering::Relaxed), 1);
}

#[test]
fn connect_spans_keep_bounded_metadata_and_validation_rejects_oversized_metadata() {
    let (sender, mut receiver) = TraceSender::channel(4);
    let invocation = crate::actor::ActorInvocation {
        request_id: "request".into(),
        actor: crate::actor::ActorKey {
            project_id: "project".into(),
            actor_name: "Room".into(),
            actor_id: "one".into(),
        },
        method: "onConnect".into(),
        args: vec![],
    };
    let metadata = serde_json::json!({ "name": "Ada" });
    let oversized = serde_json::json!({ "name": "x".repeat(TRACE_METADATA_LIMIT) });
    for value in [Some(metadata.clone()), Some(oversized.clone()), None] {
        RequestSpan::new(
            sender.clone(),
            &invocation,
            RequestKind::Websocket,
            Some("connection".into()),
            value,
            Instant::now(),
        )
        .finish(RequestOutcome::Completed);
    }
    let recorded: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok())
        .map(|trace| trace.metadata)
        .collect();
    assert_eq!(recorded, vec![Some(metadata.clone()), None, None]);

    assert!(
        !serde_json::to_value(trace(2))
            .unwrap()
            .as_object()
            .unwrap()
            .contains_key("metadata")
    );
    let mut labelled = trace(1);
    labelled.metadata = Some(metadata);
    labelled.validate().unwrap();
    assert_eq!(
        serde_json::to_value(&labelled).unwrap()["metadata"]["name"],
        "Ada"
    );
    labelled.metadata = Some(oversized);
    assert!(labelled.validate().is_err());
}

#[tokio::test]
async fn trace_status_is_shared_across_control_planes_and_survives_restart() -> Result<()> {
    crate::postgres::testing::with_postgres(async |fixture| {
        let open = async || -> Result<TraceStore> {
            TraceStore::open(Arc::new(
                persistence::postgres::PostgresTracePersistence::new(
                    crate::postgres::PostgresDatabase::connect(&fixture.url).await?,
                    Duration::from_secs(86400),
                ),
            ))
            .await
        };
        let first = open().await?;
        let second = open().await?;
        first
            .record("default", "host", "session", vec![trace(1)], 3)
            .await?;
        assert_eq!(
            second
                .replay("default", &ReplayQuery::default())
                .await?
                .dropped,
            3
        );
        second
            .record("default", "host", "session", vec![], 4)
            .await?;
        drop(first);
        drop(second);
        assert_eq!(
            open()
                .await?
                .replay("default", &ReplayQuery::default())
                .await?
                .dropped,
            7
        );
        Ok(())
    })
    .await
}
