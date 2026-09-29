use super::*;
use crate::bucket::{
    Bucket, BucketObject, FileBucket, ReplicaPlacement, ReplicaSet, SnapshotStore,
};
use crate::replicas::record::Record;
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct Time(AtomicU64);
impl Clock for Time {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}
struct FailingBucket {
    inner: FileBucket,
    unavailable: AtomicBool,
}
#[async_trait]
impl Bucket for FailingBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.inner.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        ensure!(!self.unavailable.load(Ordering::SeqCst), "GCS unavailable");
        self.inner.compare_and_swap(key, generation, bytes).await
    }
}
fn prefix() -> &'static str {
    static PREFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    PREFIX.get_or_init(|| {
        format!(
            "{}{:032x}/",
            crate::storage_paths::snapshots(&crate::actor::ActorKey {
                project_id: "p".into(),
                actor_name: "a".into(),
                actor_id: "i".into()
            })
            .unwrap(),
            1
        )
    })
}
fn data(version: u64) -> Vec<u8> {
    crate::state_log::StateSnapshot::new(
        version,
        1,
        format!("r{version}"),
        serde_json::json!({"count":version}),
        serde_json::json!(version),
    )
    .unwrap()
    .encode()
    .unwrap()
}
fn access() -> Access {
    Access::new("0123456789abcdef0123456789abcdef".into()).unwrap()
}

#[tokio::test]
async fn failed_archival_retains_state_and_a_follower_archives_after_primary_failure() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let bucket = Arc::new(FailingBucket {
        inner: FileBucket::new(dir.path().join("gcs"))?,
        unavailable: AtomicBool::new(true),
    });
    let time = Arc::new(Time(AtomicU64::new(100)));
    let server = ReplicaServer {
        id: "follower".into(),
        disk: ReplicaDisk::open(dir.path().join("disk.sqlite")).await?,
        archive: Archive(bucket.clone()),
        access: access(),
        assignment: assignment("follower", false),
        clock: time.clone(),
    };
    server.disk.prepare(prefix()).await?;
    server
        .disk
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    time.0.store(10_100, Ordering::SeqCst);
    server.upload_due().await?;
    assert!(
        bucket
            .list(&super::super::record::archive_prefix(prefix())?)
            .await?
            .is_empty()
    );
    time.0.store(30_100, Ordering::SeqCst);
    assert!(server.upload_due().await.is_err());
    assert!(server.disk.batch(prefix(), BATCH_BYTES).await?.is_some());
    bucket.unavailable.store(false, Ordering::SeqCst);
    server.upload_due().await?;
    assert!(server.disk.batch(prefix(), BATCH_BYTES).await?.is_none());
    assert_eq!(server.archive.get(prefix(), 1).await?, Some(data(1)));
    Ok(())
}

#[tokio::test]
async fn primary_archives_after_ten_seconds_and_repeated_uploads_are_idempotent() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let time = Arc::new(Time(AtomicU64::new(100)));
    let server = ReplicaServer {
        id: "primary".into(),
        disk: ReplicaDisk::open(dir.path().join("disk.sqlite")).await?,
        archive: Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?)),
        access: access(),
        assignment: assignment("primary", true),
        clock: time.clone(),
    };
    server.disk.prepare(prefix()).await?;
    server
        .disk
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    server.upload_due().await?;
    assert!(server.archive.list(prefix()).await?.is_empty());
    let batch = server.disk.batch(prefix(), BATCH_BYTES).await?.unwrap();
    let key = server.archive.write(&batch).await?;
    assert_eq!(key, server.archive.write(&batch).await?);
    time.0.store(10_100, Ordering::SeqCst);
    server.upload_due().await?;
    assert_eq!(
        server.archive.list(prefix()).await?,
        vec![format!("{}1.json", prefix())]
    );
    assert!(server.disk.batch(prefix(), BATCH_BYTES).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn network_replication_recovers_with_a_missing_node_and_fences_delayed_writes() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let archive = Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?));
    let mut servers = Vec::new();
    let mut tasks = Vec::new();
    let mut placements = Vec::new();
    for i in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let id = format!("replica-{i}");
        placements.push(ReplicaPlacement {
            id: id.clone(),
            address: format!("http://{}", listener.local_addr()?),
            zone: "us-west4-a".into(),
        });
        let server = Arc::new(ReplicaServer {
            id,
            disk: ReplicaDisk::open(dir.path().join(format!("{i}.sqlite"))).await?,
            archive: archive.clone(),
            access: access(),
            assignment: tokio::sync::OnceCell::new(),
            clock: Arc::new(SystemClock),
        });
        let app = routes(server.clone());
        tasks.push(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        servers.push(server);
    }
    for server in &servers {
        server
            .assign(Assignment {
                prefix: prefix().into(),
                replicas: placements.clone(),
            })
            .await?;
    }
    let store =
        ReplicaSet::from_replicas(&placements, access().admin().into(), ReplicaClient::http()?)?;
    store.prepare(prefix()).await?;
    store
        .put(&format!("{}1.json", prefix()), data(1).into())
        .await?;
    for server in &servers {
        assert_eq!(server.disk.latest(prefix()).await?.unwrap().1, data(1));
    }
    tasks[0].abort();
    let _ = (&mut tasks[0]).await;
    assert!(
        store
            .put(&format!("{}2.json", prefix()), data(2).into())
            .await
            .is_err()
    );
    let recovered =
        ReplicaSet::from_replicas(&placements, access().admin().into(), ReplicaClient::http()?)?;
    recovered.seal(prefix()).await?;
    assert!(recovered.latest(prefix()).await?.is_some());
    for server in &servers[1..] {
        assert!(
            server
                .disk
                .append(prefix(), Record::encode(3, &data(3), None)?, 200)
                .await
                .is_err()
        );
    }
    for task in tasks {
        task.abort();
    }
    Ok(())
}

#[test]
fn capabilities_are_actor_scoped_and_cannot_administer_replicas() -> Result<()> {
    let actor = crate::actor::ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let access = access();
    let token = access.scoped(&actor)?;
    let own = crate::storage_paths::snapshots(&actor)?;
    access.authorize(&token, Some(&own))?;
    assert!(access.authorize(&token, None).is_err());
    assert!(access.authorize(&token, Some(prefix())).is_err());
    assert!(access.authorize(&format!("{token}x"), Some(&own)).is_err());
    Ok(())
}

#[tokio::test]
async fn sealed_streams_release_local_payloads_only_after_verified_archival() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let server = ReplicaServer {
        id: "primary".into(),
        disk: ReplicaDisk::open(dir.path().join("disk.sqlite")).await?,
        archive: Archive(Arc::new(FileBucket::new(dir.path().join("gcs"))?)),
        access: access(),
        assignment: assignment("primary", true),
        clock: Arc::new(Time(AtomicU64::new(20_000))),
    };
    server.disk.prepare(prefix()).await?;
    server
        .disk
        .append(prefix(), Record::encode(1, &data(1), None)?, 100)
        .await?;
    server.disk.seal(prefix()).await?;
    assert_eq!(server.disk.latest(prefix()).await?.unwrap().1, data(1));
    server.upload_due().await?;
    assert!(server.disk.latest(prefix()).await?.is_none());
    let recovered = server
        .execute(Command::Latest {
            prefix: prefix().into(),
        })
        .await?;
    assert_eq!(STANDARD.decode(recovered.data.unwrap())?, data(1));
    let history = server
        .execute(Command::Get {
            object: format!("{}1.json", prefix()),
        })
        .await?;
    assert_eq!(STANDARD.decode(history.data.unwrap())?, data(1));
    Ok(())
}

fn assignment(id: &str, primary: bool) -> tokio::sync::OnceCell<Assignment> {
    let mut replicas = vec![];
    if !primary {
        replicas.push(ReplicaPlacement {
            id: "offline".into(),
            address: "http://127.0.0.1:1".into(),
            zone: "us-west4-a".into(),
        });
    }
    replicas.push(ReplicaPlacement {
        id: id.into(),
        address: "http://127.0.0.1:1".into(),
        zone: "us-west4-a".into(),
    });
    tokio::sync::OnceCell::new_with(Some(Assignment {
        prefix: prefix().into(),
        replicas,
    }))
}

#[tokio::test]
async fn assignment_accepts_a_reopened_disk_and_rejects_a_replacement_disk() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("original.sqlite");
    let disk = ReplicaDisk::open(path.clone()).await?;
    let cached = Assignment {
        prefix: prefix().into(),
        replicas: vec![ReplicaPlacement {
            id: disk.identity().await?,
            address: "http://replica:7200".into(),
            zone: "zone".into(),
        }],
    };
    disk.bind_assignment(&cached).await?;
    drop(disk);
    for (path, expected) in [
        (path, StatusCode::NO_CONTENT),
        (dir.path().join("replacement.sqlite"), StatusCode::CONFLICT),
    ] {
        let disk = ReplicaDisk::open(path).await?;
        let server = Arc::new(ReplicaServer {
            id: disk.identity().await?,
            assignment: tokio::sync::OnceCell::new_with(disk.assignment().await?),
            disk,
            archive: Archive(Arc::new(FileBucket::new(dir.path().join("archive"))?)),
            access: access(),
            clock: Arc::new(SystemClock),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}/assign", listener.local_addr()?);
        let task = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, routes(server)).await
        }));
        let response = reqwest::Client::new()
            .post(url)
            .bearer_auth(access().admin())
            .json(&cached)
            .send()
            .await?;
        assert_eq!(response.status(), expected);
        drop(task);
    }
    Ok(())
}
