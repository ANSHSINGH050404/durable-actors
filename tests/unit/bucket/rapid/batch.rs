use super::*;
use crate::bucket::{ArchiveBatchConfig, BucketObject};

struct ArchiveBucket {
    inner: Arc<FileBucket>,
    stalled: AtomicBool,
    fail_batch: AtomicBool,
    batches: AtomicU64,
    cleaned_reads: AtomicU64,
    read_requests: AtomicU64,
    read_batches: AtomicU64,
    range_bytes: AtomicU64,
    index_failure: AtomicU64,
    fail_checkpoint: AtomicBool,
    stalled_checkpoint: AtomicBool,
    checkpoint_attempts: AtomicU64,
}

#[async_trait]
impl Bucket for ArchiveBucket {
    async fn range(
        &self,
        key: &str,
        start: u64,
        length: u64,
    ) -> Result<Option<crate::bucket::BucketObject>> {
        self.read_batches.fetch_add(1, Ordering::SeqCst);
        self.range_bytes.fetch_add(length, Ordering::SeqCst);
        self.inner.range(key, start, length).await
    }

    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        if key.ends_with(".idx") {
            match self.index_failure.load(Ordering::SeqCst) {
                1 => return Ok(None),
                2 => anyhow::bail!("injected index read failure"),
                _ => {}
            }
        }
        self.read_requests.fetch_add(1, Ordering::SeqCst);
        if key.ends_with(".batch") {
            self.read_batches.fetch_add(1, Ordering::SeqCst);
        }
        if key.ends_with(".cleaned") {
            self.cleaned_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        ensure!(
            !prefix.ends_with("index~") || self.index_failure.load(Ordering::SeqCst) != 3,
            "injected index list failure"
        );
        self.read_requests.fetch_add(1, Ordering::SeqCst);
        self.inner.list(prefix).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: bytes::Bytes,
    ) -> Result<bool> {
        if key.ends_with(".checkpoint") {
            self.checkpoint_attempts.fetch_add(1, Ordering::SeqCst);
            while self.stalled_checkpoint.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        ensure!(
            !key.ends_with(".checkpoint") || !self.fail_checkpoint.load(Ordering::SeqCst),
            "injected checkpoint failure"
        );
        if key.ends_with(".batch") {
            self.batches.fetch_add(1, Ordering::SeqCst);
            ensure!(
                !self.fail_batch.swap(false, Ordering::SeqCst),
                "injected archive failure"
            );
            while self.stalled.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        self.inner.compare_and_swap(key, generation, bytes).await
    }
}

fn controlled(
    f: &Fixture,
    bytes: usize,
    interval_ms: u64,
) -> Result<(RapidSnapshots, Arc<ArchiveBucket>)> {
    let archive = Arc::new(ArchiveBucket {
        inner: f.archive.clone(),
        stalled: AtomicBool::new(false),
        fail_batch: AtomicBool::new(false),
        batches: AtomicU64::new(0),
        cleaned_reads: AtomicU64::new(0),
        read_requests: AtomicU64::new(0),
        read_batches: AtomicU64::new(0),
        range_bytes: AtomicU64::new(0),
        index_failure: AtomicU64::new(0),
        fail_checkpoint: AtomicBool::new(false),
        stalled_checkpoint: AtomicBool::new(false),
        checkpoint_attempts: AtomicU64::new(0),
    });
    let store = RapidSnapshots::new(
        archive.clone(),
        Arc::new(BucketSnapshots(archive.clone())),
        f.zones
            .iter()
            .map(|z| z.clone() as Arc<dyn LogZone>)
            .collect(),
        ArchiveBatchConfig { bytes, interval_ms },
        Arc::new(crate::litestream::compaction::RustCompactor),
        CancellationToken::new(),
    )?;
    Ok((store, archive))
}

async fn batches(f: &Fixture, count: usize) -> Result<Vec<String>> {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let keys = f
                .archive
                .list("")
                .await?
                .into_iter()
                .filter(|k| k.ends_with(".batch"))
                .collect::<Vec<_>>();
            if keys.len() >= count {
                return Ok(keys);
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?
}

#[tokio::test]
async fn live_checkpoint_inputs_are_read_as_one_bounded_range() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 16 * 1024 * 1024, 60_000)?;
    let (replication, history) = write_history(&f, &writer, 31).await?;
    let sqlite = writer
        .restore(&f.stream.object(31), history[30].clone())
        .await?;
    assert_eq!(
        crate::litestream::storage::restored_fields(
            replication.as_ref(),
            &sqlite.files,
            sqlite.txid
        )
        .await?,
        serde_json::json!({"count":31})
    );
    assert_eq!(
        f.zones
            .iter()
            .map(|zone| zone.range_reads.load(Ordering::SeqCst))
            .sum::<u64>(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn checkpoints_compact_sqlite_history_for_cold_recovery() -> Result<()> {
    use crate::litestream::storage::{SqliteCapture, restored_fields};
    let f = Fixture::new()?;
    let writer = f.store()?;
    let (replication, history) = write_history(&f, &writer, 35).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while f
            .archive
            .get(&format!("{}.checkpoint", f.stream.object(32)))
            .await?
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    f.cleaned().await?;

    let (reader, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    let sqlite = reader
        .restore(&f.stream.object(35), history[34].clone())
        .await?;
    assert_eq!(archive.read_requests.load(Ordering::SeqCst), 1);
    assert!(sqlite.parent.is_none());
    assert_eq!(sqlite.files.len(), 1);
    let original_size: usize = history
        .iter()
        .map(|bytes| {
            StateSnapshot::decode(bytes)
                .unwrap()
                .sqlite
                .files
                .iter()
                .map(|file| file.data.len())
                .sum::<usize>()
        })
        .sum();
    assert!(sqlite.files[0].data.len() < original_size);
    let restored = SqliteCapture::restore(replication.clone(), &sqlite.files, sqlite.txid).await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(
        crate::litestream::storage::read_fields(&db)?,
        serde_json::json!({"count":35})
    );
    assert_eq!(
        db.query_row("SELECT count(*) FROM entries", [], |r| r.get::<_, i64>(0))?,
        35
    );

    archive.read_batches.store(0, Ordering::SeqCst);
    archive.range_bytes.store(0, Ordering::SeqCst);
    let tail = reader
        .restore(&f.stream.object(34), history[33].clone())
        .await?;
    assert_eq!(tail.files.len(), 3);
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 1);
    assert_eq!(
        archive.range_bytes.load(Ordering::SeqCst),
        (history[31].len() + history[32].len() + 2 * frame::HEADER) as u64
    );
    assert_eq!(
        restored_fields(replication.as_ref(), &tail.files, tail.txid).await?,
        serde_json::json!({"count":34})
    );
    archive.read_batches.store(0, Ordering::SeqCst);
    let older = reader
        .restore(&f.stream.object(31), history[30].clone())
        .await?;
    assert_eq!(
        restored_fields(replication.as_ref(), &older.files, older.txid).await?,
        serde_json::json!({"count":31})
    );
    let batch_count = f
        .archive
        .list("")
        .await?
        .iter()
        .filter(|key| key.ends_with(".batch"))
        .count();
    assert_eq!(
        archive.read_batches.load(Ordering::SeqCst),
        batch_count as u64
    );
    assert_eq!(
        reader.get(&f.stream.object(35)).await?,
        Some(history[34].clone())
    );
    let key = format!("{}.checkpoint", f.stream.object(35));
    let stored = f.archive.get(&key).await?.unwrap();
    let mut checkpoint: crate::bucket::recovery::Checkpoint =
        serde_json::from_slice(&stored.bytes)?;
    checkpoint.source.digest = "wrong-source".into();
    assert!(
        f.archive
            .compare_and_swap(
                &key,
                Some(stored.generation),
                crate::payload::encode(&checkpoint)?
            )
            .await?
    );
    assert!(
        reader
            .restore(&f.stream.object(35), history[34].clone())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn failed_checkpoint_publication_keeps_committed_state_recoverable() -> Result<()> {
    for stalled in [false, true] {
        let f = Fixture::new()?;
        let (writer, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
        archive.fail_checkpoint.store(!stalled, Ordering::SeqCst);
        archive.stalled_checkpoint.store(stalled, Ordering::SeqCst);
        let (replication, history) = write_history(&f, &writer, 2).await?;
        writer
            .finish(
                &f.stream,
                tokio::time::Instant::now() + Duration::from_secs(1),
            )
            .await?;
        f.cleaned().await?;
        let reader = f.store()?;
        assert_eq!(
            reader.get(&f.stream.object(2)).await?,
            Some(history[1].clone())
        );
        let sqlite = reader
            .restore(&f.stream.object(2), history[1].clone())
            .await?;
        assert_eq!(
            crate::litestream::storage::restored_fields(
                replication.as_ref(),
                &sqlite.files,
                sqlite.txid
            )
            .await?,
            serde_json::json!({"count":2})
        );
    }
    Ok(())
}

#[tokio::test]
async fn resumed_checkpoint_retries_without_new_writes() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    archive.fail_checkpoint.store(true, Ordering::SeqCst);
    let (_replication, history) = write_history(&f, &writer, 33).await?;
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await?;
    let (resumed, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    archive.fail_checkpoint.store(true, Ordering::SeqCst);
    let actor_prefix = f
        .stream
        .prefix
        .trim_end_matches('/')
        .rsplit_once('/')
        .unwrap()
        .0;
    let stream = StateStream {
        prefix: format!("{actor_prefix}/{:032x}/", 2),
        owner_epoch: 2,
        base_version: 33,
        ..f.stream.clone()
    };
    resumed.start(&stream).await?;
    resumed
        .restore(&f.stream.object(33), history[32].clone())
        .await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        while archive.checkpoint_attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    archive.fail_checkpoint.store(false, Ordering::SeqCst);
    let key = format!("{}.checkpoint", f.stream.object(33));
    tokio::time::timeout(Duration::from_secs(8), async {
        while f.archive.get(&key).await?.is_none() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    assert!(archive.checkpoint_attempts.load(Ordering::SeqCst) >= 2);
    resumed
        .finish(
            &stream,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await?;
    Ok(())
}

async fn write_history(
    f: &Fixture,
    writer: &RapidSnapshots,
    count: u64,
) -> Result<(Arc<crate::litestream::Litestream>, Vec<Bytes>)> {
    use crate::litestream::{
        Litestream, Replicator,
        storage::{SqliteCapture, SqliteState},
    };
    writer.start(&f.stream).await?;
    let replication = Arc::new(Litestream::default());
    let mut capture = SqliteCapture::new(replication.clone()).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute_batch(
        "CREATE TABLE entries(value INTEGER); INSERT INTO __terse_fields VALUES ('count', '0');",
    )?;
    let mut parent = None;
    let mut history = Vec::new();
    for version in 1..=count {
        db.execute("INSERT INTO entries VALUES (?)", [version as i64])?;
        db.execute("UPDATE __terse_fields SET value=?", [version.to_string()])?;
        let txid = replication.sync(&capture.path()).await?;
        let files = capture.capture(&SqliteState::position(txid)).await?;
        let snapshot = StateSnapshot::new(
            version,
            1,
            format!("request-{version}"),
            crate::state_log::SqliteSnapshot {
                txid,
                parent,
                files,
            },
            serde_json::json!(version),
        )?;
        let bytes: Bytes = snapshot.encode()?.into();
        writer.put(&f.stream.object(version), bytes.clone()).await?;
        parent = Some(f.stream.snapshot(&bytes)?);
        history.push(bytes);
    }
    Ok((replication, history))
}

#[tokio::test]
async fn byte_threshold_combines_commits_without_rotating_rapid_streams() -> Result<()> {
    let f = Fixture::new()?;
    let threshold = [state(1, 1)?, state(2, 1)?]
        .iter()
        .map(|b| b.len() + frame::HEADER)
        .sum();
    let (store, archive) = controlled(&f, threshold, 60_000)?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(archive.batches.load(Ordering::SeqCst), 0);
    store.put(&f.stream.object(2), state(2, 1)?).await?;
    let keys = batches(&f, 1).await?;
    let bytes = f.archive.get(&keys[0]).await?.unwrap().bytes.into();
    assert_eq!(frame::decode(&bytes)?.len(), 2);
    assert_eq!(archive.batches.load(Ordering::SeqCst), 1);
    assert!(f.zones.iter().all(|z| z.opens.load(Ordering::SeqCst) == 1));
    assert!(f.archive.get(&f.stream.object(1)).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn timer_flushes_partial_batches_and_never_uploads_empty_ones() -> Result<()> {
    let f = Fixture::new()?;
    let (store, archive) = controlled(&f, 16 * 1024 * 1024, 40)?;
    store.start(&f.stream).await?;
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(archive.batches.load(Ordering::SeqCst), 0);
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    batches(&f, 1).await?;
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(archive.batches.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn failed_partial_batch_retries_without_another_write() -> Result<()> {
    let f = Fixture::new()?;
    let (store, archive) = controlled(&f, 16 * 1024 * 1024, 40)?;
    archive.fail_batch.store(true, Ordering::SeqCst);
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    let keys = batches(&f, 1).await?;
    let bytes = f.archive.get(&keys[0]).await?.unwrap().bytes.into();
    let records = frame::decode(&bytes)?;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].state, state(1, 1)?);
    assert_eq!(archive.batches.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn stalled_archive_does_not_block_writes_and_backlog_is_recovered_from_rapid() -> Result<()> {
    let f = Fixture::new()?;
    let threshold = state(1, 1)?.len() + frame::HEADER;
    let (store, archive) = controlled(&f, threshold, 60_000)?;
    archive.stalled.store(true, Ordering::SeqCst);
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while archive.batches.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    for version in 2..=20 {
        tokio::time::timeout(
            Duration::from_secs(1),
            store.put(&f.stream.object(version), state(version, 1)?),
        )
        .await??;
    }
    assert!(archive.batches.load(Ordering::SeqCst) > 0);
    assert!(
        f.zones
            .iter()
            .all(|z| !z.objects.lock().unwrap().is_empty())
    );
    archive.stalled.store(false, Ordering::SeqCst);
    store
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    f.cleaned().await?;
    let reader = f.store()?;
    for version in 1..=20 {
        assert_eq!(
            reader.get(&f.stream.object(version)).await?,
            Some(state(version, 1)?)
        );
    }
    Ok(())
}

#[tokio::test]
async fn crash_recovery_joins_standard_batches_and_unflushed_rapid_tail() -> Result<()> {
    let f = Fixture::new()?;
    let threshold = state(1, 1)?.len() + frame::HEADER;
    let (store, archive) = controlled(&f, threshold, 60_000)?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    batches(&f, 1).await?;
    archive.stalled.store(true, Ordering::SeqCst);
    store.put(&f.stream.object(2), state(2, 1)?).await?;
    drop(store);
    f.zones[1].offline.store(true, Ordering::SeqCst);
    let reader = f.store()?;
    assert_eq!(
        reader.recover(&f.stream.prefix).await?,
        Some((f.stream.object(2), state(2, 1)?))
    );
    f.zones[0].offline.store(true, Ordering::SeqCst);
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    Ok(())
}

#[tokio::test]
async fn partial_flush_cannot_acknowledge_a_write_and_remains_recoverable() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    f.zones[1].stalled.store(true, Ordering::SeqCst);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(30),
            store.put(&f.stream.object(2), state(2, 1)?)
        )
        .await
        .is_err()
    );
    f.zones[1].stalled.store(false, Ordering::SeqCst);
    assert!(store.put(&f.stream.object(2), state(2, 1)?).await.is_err());
    assert!(
        store
            .finish(
                &f.stream,
                tokio::time::Instant::now() + Duration::from_secs(30)
            )
            .await
            .is_err()
    );
    assert!(f.archive.get(&f.stream.object(2)).await?.is_none());
    assert_eq!(
        f.store()?.recover(&f.stream.prefix).await?,
        Some((f.stream.object(2), state(2, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn archived_sqlite_parent_and_rapid_tail_restore_together() -> Result<()> {
    use crate::{
        litestream::{
            Litestream, Replicator,
            storage::{SqliteCapture, SqliteState},
        },
        state_log::SqliteSnapshot,
        storage::SnapshotRef,
    };
    let f = Fixture::new()?;
    let replication = Arc::new(Litestream::default());
    let mut capture = SqliteCapture::new(replication.clone()).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute_batch("INSERT INTO __terse_fields VALUES ('count','1'); CREATE TABLE entries(value INTEGER); INSERT INTO entries VALUES (1);")?;
    let txid = replication.sync(&capture.path()).await?;
    let first = StateSnapshot::new(
        1,
        1,
        "first".into(),
        SqliteSnapshot {
            txid,
            parent: None,
            files: capture.capture(&SqliteState::position(txid)).await?,
        },
        serde_json::json!(1),
    )?;
    let first_bytes: Bytes = first.encode()?.into();
    db.execute_batch("UPDATE __terse_fields SET value='2'; INSERT INTO entries VALUES (2);")?;
    let txid = replication.sync(&capture.path()).await?;
    let second = StateSnapshot::new(
        2,
        1,
        "second".into(),
        SqliteSnapshot {
            txid,
            parent: Some(SnapshotRef::new(f.stream.object(1), &first, &first_bytes)),
            files: capture.capture(&SqliteState::position(txid)).await?,
        },
        serde_json::json!(2),
    )?;
    assert!(second.sqlite.files[0].first > 1);
    let second_bytes: Bytes = second.encode()?.into();
    let (store, archive) = controlled(&f, first_bytes.len() + frame::HEADER, 60_000)?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), first_bytes).await?;
    batches(&f, 1).await?;
    archive.stalled.store(true, Ordering::SeqCst);
    store.put(&f.stream.object(2), second_bytes.clone()).await?;
    drop(store);
    let reader = f.store()?;
    assert_eq!(
        reader.recover(&f.stream.prefix).await?,
        Some((f.stream.object(2), second_bytes))
    );
    let parent = second.sqlite.parent.as_ref().unwrap();
    let parent_bytes = reader.get(&parent.object).await?.unwrap();
    parent.verify(&parent_bytes)?;
    let mut files = StateSnapshot::decode(&parent_bytes)?.sqlite.files;
    files.extend(second.sqlite.files);
    let restored = SqliteCapture::restore(replication, &files, txid).await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(
        db.query_row(
            "SELECT value FROM __terse_fields WHERE name='count'",
            [],
            |r| r.get::<_, String>(0)
        )?,
        "2"
    );
    assert_eq!(
        db.query_row("SELECT sum(value) FROM entries", [], |r| r.get::<_, i64>(0))?,
        3
    );
    Ok(())
}

#[tokio::test]
async fn rapid_rotation_continues_writing_while_old_archive_is_stalled() -> Result<()> {
    let f = Fixture::new()?;
    let (store, archive) = controlled(&f, 16 * 1024 * 1024, 60_000)?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    archive.stalled.store(true, Ordering::SeqCst);
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            store
                .session
                .lock()
                .await
                .as_mut()
                .unwrap()
                .rotate(store.storage.clone())
                .await;
            if f.archive
                .list("")
                .await
                .unwrap()
                .iter()
                .filter(|key| key.ends_with(".manifest"))
                .count()
                == 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let mut version = 1;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            version += 1;
            store
                .put(&f.stream.object(version), state(version, 1)?)
                .await?;
            if f.zones.iter().all(|z| {
                z.objects
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|o| !o.bytes.is_empty())
                    .count()
                    == 2
            }) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await??;
    assert!(f.zones.iter().all(|z| z.opens.load(Ordering::SeqCst) == 2));
    archive.stalled.store(false, Ordering::SeqCst);
    store
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    f.cleaned().await?;
    assert_eq!(
        f.store()?.get(&f.stream.object(1)).await?,
        Some(state(1, 1)?)
    );
    Ok(())
}

#[tokio::test]
async fn cleanup_retries_partial_deletion_and_skips_completed_segments() -> Result<()> {
    let f = Fixture::new()?;
    let (store, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    f.zones[1].offline.store(true, Ordering::SeqCst);
    store
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(30),
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        while !f.zones[0].objects.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(!f.zones[1].objects.lock().unwrap().is_empty());
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    f.zones[1].offline.store(false, Ordering::SeqCst);
    let prefix = object_name(&f.stream.prefix)?;
    store.storage.sweep(&prefix).await?;
    f.cleaned().await?;
    archive.cleaned_reads.store(0, Ordering::SeqCst);
    store.storage.sweep(&prefix).await?;
    assert_eq!(archive.cleaned_reads.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn conflicting_batches_never_allow_recovery_to_delete_rapid_data() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    let original = state(1, 1)?;
    store.put(&f.stream.object(1), original.clone()).await?;
    let key = f
        .archive
        .list("")
        .await?
        .into_iter()
        .find(|k| k.ends_with(".manifest"))
        .unwrap();
    let manifest: Manifest = serde_json::from_slice(&f.archive.get(&key).await?.unwrap().bytes)?;
    let mut snapshot = StateSnapshot::decode(&original)?;
    snapshot.result = serde_json::json!(9);
    let frame = Record {
        version: 1,
        state: snapshot.encode()?.into(),
    }
    .encode()?;
    store.storage.put_batch(&manifest, 0, frame).await?;
    assert!(f.store()?.recover(&f.stream.prefix).await.is_err());
    assert!(
        f.zones
            .iter()
            .all(|z| !z.objects.lock().unwrap().is_empty())
    );
    Ok(())
}

#[tokio::test]
async fn archiver_finishes_when_another_worker_archived_and_cleaned_its_backlog() -> Result<()> {
    let f = Fixture::new()?;
    let threshold = state(1, 1)?.len() + frame::HEADER;
    let (store, archive) = controlled(&f, threshold, 60_000)?;
    let mut segment = Segment::open(&store.storage, f.stream.clone(), 1)
        .await?
        .unwrap();
    let manifest = segment.manifest.clone();
    let worker = archive::Archiver::new(store.storage.clone(), manifest.clone(), 0);
    archive.stalled.store(true, Ordering::SeqCst);
    let first = segment.append(1, state(1, 1)?).await?;
    let mut end_offset = first.len() as u64;
    worker.append(first, 1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while archive.batches.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    for version in 2..=4 {
        let frame = segment.append(version, state(version, 1)?).await?;
        end_offset += frame.len() as u64;
        worker.append(frame, version);
    }
    worker.close();
    let rescue = f.store()?;
    rescue
        .storage
        .close_segment(
            &manifest,
            &archive::ClosedSegment {
                last_version: 4,
                end_offset,
            },
        )
        .await?;
    rescue
        .storage
        .sweep(&object_name(&f.stream.prefix)?)
        .await?;
    f.cleaned().await?;
    archive.stalled.store(false, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), worker.finish()).await??;
    assert_eq!(
        rescue.latest(&f.stream.prefix).await?,
        Some((f.stream.object(4), state(4, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn large_records_archive_after_retry_and_remain_indexed() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, archive) = controlled(&f, 64 * 1024, 60_000)?;
    archive.fail_batch.store(true, Ordering::SeqCst);
    writer.start(&f.stream).await?;
    let mut snapshot = StateSnapshot::decode(&state(1, 1)?)?;
    snapshot.result = serde_json::json!({"data": "x".repeat(5 * 1024 * 1024)});
    let bytes = snapshot.encode()?;
    writer.put(&f.stream.object(1), bytes.clone()).await?;
    writer.put(&f.stream.object(2), state(2, 1)?).await?;
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await?;
    f.cleaned().await?;
    assert!(
        f.zones
            .iter()
            .any(|z| z.range_reads.load(Ordering::SeqCst) > 0)
    );
    let (reader, archive) = controlled(&f, 64 * 1024, 60_000)?;
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(bytes.clone()));
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 1);
    assert_eq!(
        archive.range_bytes.load(Ordering::SeqCst),
        (bytes.len() + frame::HEADER) as u64
    );
    assert_eq!(reader.get(&f.stream.object(2)).await?, Some(state(2, 1)?));
    Ok(())
}

#[tokio::test]
async fn indexed_reads_fetch_only_the_requested_archived_record() -> Result<()> {
    let f = Fixture::new()?;
    let (writer, _) = controlled(&f, 1, 10_000)?;
    writer.start(&f.stream).await?;
    for version in 1..=5 {
        writer
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await?;
    f.cleaned().await?;
    let (reader, archive) = controlled(&f, 1, 10_000)?;
    let result = reader.get(&f.stream.object(3)).await?;
    assert_eq!(result, Some(state(3, 1)?));
    assert_eq!(archive.read_batches.load(Ordering::SeqCst), 1);
    assert_eq!(
        archive.range_bytes.load(Ordering::SeqCst),
        (state(3, 1)?.len() + frame::HEADER) as u64
    );
    Ok(())
}

#[tokio::test]
async fn unavailable_or_invalid_indexes_preserve_archive_recovery() -> Result<()> {
    let f = Fixture::new()?;
    let writer = f.store()?;
    writer.start(&f.stream).await?;
    for version in 1..=3 {
        writer
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    writer
        .finish(
            &f.stream,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await?;
    f.cleaned().await?;
    let (reader, archive) = controlled(&f, 16 * 1024 * 1024, 10_000)?;
    for failure in 1..=3 {
        archive.index_failure.store(failure, Ordering::SeqCst);
        assert_eq!(reader.get(&f.stream.object(2)).await?, Some(state(2, 1)?));
    }
    archive.index_failure.store(0, Ordering::SeqCst);
    let key = f
        .archive
        .list("")
        .await?
        .into_iter()
        .find(|key| key.ends_with(".idx"))
        .unwrap();
    let original = f.archive.get(&key).await?.unwrap();
    let mut wrong: serde_json::Value = serde_json::from_slice(&original.bytes)?;
    wrong["records"][1]["start"] = 1.into();
    for bytes in [
        Bytes::from_static(b"invalid json"),
        serde_json::to_vec(&wrong)?.into(),
    ] {
        let stored = f.archive.get(&key).await?.unwrap();
        assert!(
            f.archive
                .compare_and_swap(&key, Some(stored.generation), bytes)
                .await?
        );
        assert_eq!(reader.get(&f.stream.object(2)).await?, Some(state(2, 1)?));
    }
    Ok(())
}
