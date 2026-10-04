use super::*;

#[tokio::test]
async fn registered_databases_sync_restore_and_unregister() -> Result<()> {
    let replication = Litestream::default();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("actor.sqlite");
    let replica = directory.path().join("replica");
    let sql = rusqlite::Connection::open(&path)?;
    sql.execute_batch(
        "PRAGMA journal_mode=WAL; CREATE TABLE data(value); INSERT INTO data VALUES(1)",
    )?;
    replication.register(&path, &replica).await?;
    assert!(replication.register(&path, &replica).await.is_err());
    let first = replication.sync(&path).await?;
    sql.execute("INSERT INTO data VALUES(2)", [])?;
    let second = replication.sync(&path).await?;
    assert!(second > first);
    let restored = directory.path().join("restored.sqlite");
    replication.restore(&replica, &restored, first).await?;
    assert_eq!(
        rusqlite::Connection::open(restored)?
            .query_row("SELECT count(*) FROM data", [], |r| r.get::<_, i64>(0))?,
        1
    );
    replication.unregister(&path).await?;
    assert!(replication.sync(&path).await.is_err());
    Ok(())
}

#[tokio::test]
async fn failed_publication_cannot_acknowledge_a_write() -> Result<()> {
    let replication = Litestream::default();
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("actor.sqlite");
    let replica = directory.path().join("replica");
    let sql = rusqlite::Connection::open(&path)?;
    sql.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE data(value)")?;
    replication.register(&path, &replica).await?;
    replication.sync(&path).await?;
    std::fs::rename(replica.join("ltx/0"), replica.join("retained"))?;
    std::fs::write(replica.join("ltx/0"), b"not a directory")?;
    sql.execute("INSERT INTO data VALUES(1)", [])?;
    assert!(replication.sync(&path).await.is_err());
    Ok(())
}
