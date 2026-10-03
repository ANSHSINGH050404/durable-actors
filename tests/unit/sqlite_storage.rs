use super::*;
use crate::litestream::Litestream;

#[tokio::test]
#[ignore = "requires the pinned Litestream binary on PATH"]
async fn capture_restores_fields_and_user_tables_at_the_requested_commit() -> Result<()> {
    let replication = Arc::new(Litestream::start("litestream".into()).await?);
    let mut capture = SqliteCapture::new(replication.clone()).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute_batch(
        "BEGIN;
        INSERT INTO __terse_fields VALUES ('count','1');
        CREATE TABLE entries(value TEXT);
        INSERT INTO entries VALUES ('first');
        COMMIT;",
    )?;
    let first = replication.sync(&capture.path()).await?;
    db.execute_batch("BEGIN; UPDATE __terse_fields SET value='2'; INSERT INTO entries VALUES ('second'); COMMIT;")?;
    let second = replication.sync(&capture.path()).await?;
    assert!(second > first);
    let files = capture.capture(&SqliteState::position(first)).await?;
    let restored = SqliteCapture::restore(replication.clone(), &files, first).await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(read_fields(&db)?, serde_json::json!({"count":1}));
    assert_eq!(
        db.query_row("SELECT count(*) FROM entries", [], |r| r.get::<_, i64>(0))?,
        1
    );
    let obsolete = capture
        .replica()
        .join("ltx/9")
        .join(format!("{:016x}-{first:016x}.ltx", 1));
    tokio::fs::create_dir_all(&obsolete).await?;
    let delta = capture.capture(&SqliteState::position(second)).await?;
    assert!(delta.iter().all(|file| file.first > first));
    let mut combined = files;
    combined.extend(delta);
    let restored = SqliteCapture::restore(replication, &combined, second).await?;
    let db = rusqlite::Connection::open(restored.path())?;
    assert_eq!(read_fields(&db)?, serde_json::json!({"count":2}));
    assert_eq!(
        db.query_row("SELECT count(*) FROM entries", [], |r| r.get::<_, i64>(0))?,
        2
    );
    Ok(())
}

#[tokio::test]
#[ignore = "requires the pinned Litestream binary on PATH"]
async fn missing_replication_files_cannot_be_acknowledged() -> Result<()> {
    let replication = Arc::new(Litestream::start("litestream".into()).await?);
    let mut capture = SqliteCapture::new(replication.clone()).await?;
    let db = rusqlite::Connection::open(capture.path())?;
    db.execute("INSERT INTO __terse_fields VALUES ('count','1')", [])?;
    let position = replication.sync(&capture.path()).await?;
    let files = capture.files(0, position).await?;
    tokio::fs::remove_file(files.last().unwrap().path(&capture.replica())).await?;
    assert!(
        capture
            .capture(&SqliteState::position(position))
            .await
            .is_err()
    );
    Ok(())
}
