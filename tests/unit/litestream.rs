use super::*;
use serde_json::json;

#[test]
fn sync_requires_the_requested_database_and_replicated_position() {
    let valid = json!({"path": "/actor.sqlite", "txid": 8, "replicated_txid": 8});
    assert_eq!(
        SyncPosition::decode(&valid, "/actor.sqlite").unwrap().txid,
        8
    );
    for (field, value) in [
        ("path", json!("/other.sqlite")),
        ("replicated_txid", json!(7)),
        ("txid", json!(0)),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(SyncPosition::decode(&invalid, "/actor.sqlite").is_err());
    }
}

#[tokio::test]
#[ignore = "requires the pinned Litestream binary on PATH"]
async fn daemon_failure_cannot_return_a_successful_sync() -> Result<()> {
    let mut daemon = Litestream::start("litestream".into()).await?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("actor.sqlite");
    let db = rusqlite::Connection::open(&path)?;
    db.execute_batch(
        "PRAGMA journal_mode=WAL; CREATE TABLE data(value INTEGER); INSERT INTO data VALUES (1)",
    )?;
    daemon
        .register(&path, &directory.path().join("replica"))
        .await?;
    daemon.sync(&path).await?;
    daemon._process.kill().await?;
    db.execute("INSERT INTO data VALUES (2)", [])?;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), daemon.sync(&path))
            .await?
            .is_err()
    );
    Ok(())
}
