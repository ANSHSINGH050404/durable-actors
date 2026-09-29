use super::super::contract_tests::event;
use super::*;

#[tokio::test]
async fn live_snapshot_only_offers_a_resume_cursor() -> Result<()> {
    let store = SqliteTracePersistence::in_memory();
    store.append(&[event("a"), event("b"), event("c")]).await?;
    let page = store
        .replay(
            "default",
            &ReplayQuery {
                limit: 2,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(page.records.len(), 2);
    assert_eq!(page.cursor, 3);
    assert!(page.next_cursor.is_none());
    assert!(!page.resume_cursor.is_empty());
    Ok(())
}

#[tokio::test]
async fn live_snapshot_resumes_with_late_arrivals() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SqliteTracePersistence::new(directory.path().join("traces.sqlite3"));
    store.append(&[event("a"), event("b"), event("c")]).await?;
    let first = store
        .replay(
            "default",
            &ReplayQuery {
                limit: 2,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        first
            .records
            .iter()
            .map(|r| r.event.event_id.as_str())
            .collect::<Vec<_>>(),
        ["c", "b"]
    );
    store.append(&[event("late")]).await?;
    let replay = store
        .replay(
            "default",
            &ReplayQuery {
                cursor: Some(first.resume_cursor),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.records[0].event.event_id, "late");
    Ok(())
}

#[tokio::test]
async fn appends_are_idempotent_and_reads_are_bounded_and_ordered() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SqliteTracePersistence::new(directory.path().join("traces.sqlite3"));
    let first = vec![event("a"), event("b")];
    store.append(&first).await?;
    store.append(&first).await?;
    store.append(&[event("c")]).await?;
    assert_eq!(ids(store.load_recent(100).await?), ["a", "b", "c"]);
    assert_eq!(ids(store.load_recent(2).await?), ["b", "c"]);
    assert!(store.load_recent(0).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn local_retention_is_independent_of_the_read_limit() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SqliteTracePersistence {
        path: directory.path().join("traces.sqlite3"),
        retention: 3,
        connection: Arc::new(Mutex::new(None)),
    };
    store
        .append(&[event("a"), event("b"), event("c"), event("d")])
        .await?;
    assert_eq!(ids(store.load_recent(1).await?), ["d"]);
    assert_eq!(ids(store.load_recent(10).await?), ["b", "c", "d"]);
    Ok(())
}

#[tokio::test]
async fn failed_batches_do_not_partially_commit() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = SqliteTracePersistence::new(directory.path().join("traces.sqlite3"));
    store.run(|connection| {
        connection.execute_batch("CREATE TRIGGER reject_bad BEFORE INSERT ON traces WHEN NEW.event_id = 'bad' BEGIN SELECT RAISE(ABORT, 'write failed'); END;")?;
        Ok(())
    }).await?;
    assert!(store.append(&[event("good"), event("bad")]).await.is_err());
    assert!(store.load_recent(10).await?.is_empty());
    Ok(())
}

fn ids(events: Vec<TraceEvent>) -> Vec<String> {
    events.into_iter().map(|event| event.event_id).collect()
}

#[tokio::test]
async fn replay_pages_do_not_skip_late_events_and_expired_cursors_reset() -> Result<()> {
    let store = SqliteTracePersistence {
        retention: 3,
        ..SqliteTracePersistence::in_memory()
    };
    let initial = store.replay("default", &ReplayQuery::default()).await?;
    store.append(&[event("a"), event("b"), event("c")]).await?;
    let first = store
        .replay(
            "default",
            &ReplayQuery {
                cursor: Some(initial.resume_cursor.clone()),
                limit: 2,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        first
            .records
            .iter()
            .map(|r| r.event.event_id.as_str())
            .collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(first.next_cursor.is_some());
    let last = store
        .replay(
            "default",
            &ReplayQuery {
                cursor: Some(first.resume_cursor),
                limit: 2,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(last.records[0].event.event_id, "c");
    assert!(last.next_cursor.is_none());
    store.append(&[event("d")]).await?;
    let reset = store
        .replay(
            "default",
            &ReplayQuery {
                cursor: Some(initial.resume_cursor),
                ..Default::default()
            },
        )
        .await?;
    assert!(reset.reset);
    assert_eq!(reset.records.len(), 3);
    assert_eq!(reset.records[0].event.event_id, "d");
    Ok(())
}

#[tokio::test]
async fn cursor_survives_reopening() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("traces.sqlite3");
    let store = SqliteTracePersistence::new(path.clone());
    store.append(&[event("a"), event("b")]).await?;
    let first = store
        .replay(
            "default",
            &ReplayQuery {
                limit: 1,
                ..Default::default()
            },
        )
        .await?;
    drop(store);
    let store = SqliteTracePersistence::new(path);
    store.append(&[event("c")]).await?;
    let query = ReplayQuery {
        cursor: Some(first.resume_cursor),
        ..Default::default()
    };
    let replay = store.replay("default", &query).await?;
    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.records[0].event.event_id, "c");
    Ok(())
}

#[tokio::test]
async fn replay_resets_after_database_recreation_even_from_position_zero() -> Result<()> {
    let old = SqliteTracePersistence::in_memory();
    let empty = old.replay("default", &ReplayQuery::default()).await?;
    old.append(&[event("old")]).await?;
    let populated = old.replay("default", &ReplayQuery::default()).await?;
    let fresh = SqliteTracePersistence::in_memory();
    fresh.append(&[event("a"), event("b")]).await?;
    for cursor in [empty.resume_cursor, populated.resume_cursor] {
        let page = fresh
            .replay(
                "default",
                &ReplayQuery {
                    cursor: Some(cursor),
                    limit: 1,
                },
            )
            .await?;
        assert!(page.reset);
        assert_eq!(page.records[0].event.event_id, "b");
        assert!(page.next_cursor.is_none());
    }
    Ok(())
}

impl SqliteTracePersistence {
    async fn load_recent(&self, limit: usize) -> Result<Vec<TraceEvent>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut events: Vec<_> = self
            .replay(
                "default",
                &ReplayQuery {
                    limit: limit.min(500),
                    ..Default::default()
                },
            )
            .await?
            .records
            .into_iter()
            .map(|r| r.event)
            .collect();
        events.reverse();
        Ok(events)
    }
}

#[tokio::test]
async fn existing_sqlite_events_survive_the_query_schema_migration() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("traces.sqlite3");
    {
        let connection = Connection::open(&path)?;
        connection.execute_batch("CREATE TABLE traces (position INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, event TEXT NOT NULL); PRAGMA user_version = 1;")?;
        let mut unscoped = serde_json::to_value(event("unscoped"))?;
        unscoped.as_object_mut().unwrap().remove("projectId");
        connection.execute(
            "INSERT INTO traces (position, event_id, event) VALUES (41, 'unscoped', ?1)",
            [serde_json::to_string(&unscoped)?],
        )?;
        connection.execute(
            "INSERT INTO traces (position, event_id, event) VALUES (42, ?1, ?2)",
            params!["saved", serde_json::to_string(&event("saved"))?],
        )?;
    }
    let store = SqliteTracePersistence::new(path);
    let page = store.replay("default", &ReplayQuery::default()).await?;
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].sequence, 42);
    assert_eq!(page.records[0].event.event_id, "saved");
    store.append(&[event("new")]).await?;
    assert_eq!(
        store
            .replay("default", &ReplayQuery::default())
            .await?
            .records[0]
            .sequence,
        43
    );
    Ok(())
}
