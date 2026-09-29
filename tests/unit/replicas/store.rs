use super::*;
use crate::replicas::record::Record;
fn data(v: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({"stateVersion":v,"ownerEpoch":1,"requestId":format!("r{v}"),"state":{"count":v},"result":v})).unwrap()
}
fn prefix() -> &'static str {
    "durable-actors/v3/snapshots/aa/cA/YQ/aQ/00000000000000000000000000000001/"
}
#[tokio::test]
async fn acknowledged_records_and_seals_survive_restart() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.prepare(prefix()).await?;
    let first = Record::encode(1, &data(1), None)?;
    store.append(prefix(), first.clone(), 100).await?;
    store.append(prefix(), first, 100).await?;
    drop(store);
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    assert_eq!(store.latest(prefix()).await?.unwrap().1, data(1));
    store.seal(prefix()).await?;
    drop(store);
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    assert!(store.prepare(prefix()).await.is_err());
    assert!(
        store
            .append(prefix(), Record::encode(2, &data(2), Some(&data(1)))?, 101)
            .await
            .is_err()
    );
    assert_eq!(store.latest(prefix()).await?.unwrap().1, data(1));
    Ok(())
}
#[tokio::test]
async fn an_empty_disk_cannot_become_a_recovery_witness_by_sealing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let disk = ReplicaDisk::open(dir.path().join("disk.sqlite")).await?;
    assert!(disk.seal(prefix()).await.is_err());
    assert!(disk.latest(prefix()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn batching_retains_records_until_archive_confirmation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.prepare(prefix()).await?;
    store
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    store
        .append(prefix(), Record::encode(2, &data(2), Some(&data(1)))?, 101)
        .await?;
    assert!(
        store
            .due(10_099, 10_000, 16 * 1024 * 1024)
            .await?
            .is_empty()
    );
    assert_eq!(
        store.due(10_100, 10_000, 16 * 1024 * 1024).await?,
        vec![prefix().to_owned()]
    );
    assert_eq!(store.due(101, 10_000, 1).await?, vec![prefix().to_owned()]);
    let batch = store.batch(prefix(), 16 * 1024 * 1024).await?.unwrap();
    assert_eq!(batch.records.len(), 2);
    assert_eq!(batch.decode()?.last().unwrap().1, data(2));
    drop(store);
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    assert!(store.batch(prefix(), 16 * 1024 * 1024).await?.is_some());
    store.archived(&batch.key()?, &batch).await?;
    assert!(store.batch(prefix(), 16 * 1024 * 1024).await?.is_none());
    assert_eq!(store.latest(prefix()).await?.unwrap().1, data(2));
    Ok(())
}
#[tokio::test]
async fn concurrent_appends_survive_archival_of_an_earlier_batch() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.prepare(prefix()).await?;
    store
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    let first = store.batch(prefix(), 1).await?.unwrap();
    store
        .append(prefix(), Record::encode(2, &data(2), Some(&data(1)))?, 101)
        .await?;
    store.archived(&first.key()?, &first).await?;
    let second = store.batch(prefix(), 1).await?.unwrap();
    assert_eq!(second.decode()?, vec![(2, data(2))]);
    Ok(())
}
#[tokio::test]
async fn corrupt_records_cannot_replace_durable_state() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store = ReplicaDisk::open(dir.path().join("data.sqlite")).await?;
    store.prepare(prefix()).await?;
    store
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    let mut wrong = Record::encode(2, &data(2), Some(&data(1)))?;
    wrong.digest = "invalid".into();
    assert!(store.append(prefix(), wrong, 101).await.is_err());
    assert!(
        store
            .append(prefix(), Record::encode(1, &data(2), None)?, 101)
            .await
            .is_err()
    );
    assert_eq!(store.latest(prefix()).await?.unwrap().1, data(1));
    Ok(())
}

#[tokio::test]
async fn replica_assignment_survives_restart_and_cannot_be_rebound() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("replica.sqlite");
    let disk = ReplicaDisk::open(path.clone()).await?;
    let assignment = crate::replicas::Assignment {
        prefix: prefix().into(),
        replicas: vec![crate::bucket::ReplicaPlacement {
            id: disk.identity().await?,
            address: "http://replica:7200".into(),
            zone: "us-west4-a".into(),
        }],
    };
    disk.bind_assignment(&assignment).await?;
    disk.append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    drop(disk);
    let reopened = ReplicaDisk::open(path).await?;
    assert_eq!(reopened.assignment().await?, Some(assignment.clone()));
    assert_eq!(reopened.latest(prefix()).await?.unwrap().1, data(1));
    reopened.bind_assignment(&assignment).await?;
    let mut changed = assignment.clone();
    changed.replicas[0].id = "new-disk".into();
    assert!(reopened.bind_assignment(&changed).await.is_err());
    changed = assignment;
    changed.prefix = "another/epoch/".into();
    assert!(reopened.bind_assignment(&changed).await.is_err());
    Ok(())
}
