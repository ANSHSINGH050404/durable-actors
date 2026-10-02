use super::*;
use crate::{bucket::BucketObject, state_log::StateSnapshot, state_transport::SnapshotWriter};
use std::collections::HashMap;

#[derive(Default)]
struct MemoryBucket {
    objects: Mutex<HashMap<String, BucketObject>>,
    owner_reads: std::sync::atomic::AtomicUsize,
    reject_snapshot_reads: std::sync::atomic::AtomicBool,
    reject_snapshots: std::sync::atomic::AtomicBool,
    delay_snapshot: Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
    delay_ownership: Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
}
#[async_trait]
impl Bucket for MemoryBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        ensure!(
            !key.contains("/snapshots/")
                || !self
                    .reject_snapshot_reads
                    .load(std::sync::atomic::Ordering::SeqCst),
            "snapshot read unavailable"
        );
        if key.contains("/owners/") {
            self.owner_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        if key.contains("/owners/") {
            let delayed = self.delay_ownership.lock().unwrap().take();
            if let Some((entered, resume)) = delayed {
                entered.add_permits(1);
                resume.acquire().await?.forget();
            }
        }
        if key.contains("/snapshots/") {
            let delayed = self.delay_snapshot.lock().unwrap().take();
            if let Some((entered, resume)) = delayed {
                entered.add_permits(1);
                resume.acquire().await?.forget();
            }
        }
        ensure!(
            !key.contains("/snapshots/")
                || !self
                    .reject_snapshots
                    .load(std::sync::atomic::Ordering::SeqCst),
            "snapshot write unavailable"
        );
        let mut objects = self.objects.lock().unwrap();
        let current = objects.get(key).map(|object| object.generation);
        if current != generation {
            return Ok(false);
        }
        objects.insert(
            key.into(),
            BucketObject {
                generation: current.unwrap_or(0) + 1,
                bytes,
            },
        );
        Ok(true)
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        ensure!(
            !prefix.contains("/snapshots/")
                || !self
                    .reject_snapshot_reads
                    .load(std::sync::atomic::Ordering::SeqCst),
            "snapshot read unavailable"
        );
        Ok(self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect())
    }
}

#[tokio::test]
async fn host_registers_claims_reads_and_writes_without_a_control_plane() -> Result<()> {
    let bucket = Arc::new(MemoryBucket::default());
    let storage = host_storage(bucket.clone()).await?;
    let host = storage.host.clone();
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    assert!(storage.acquire_actor(&actor, &host).await.is_err());
    storage
        .register(&HostLeaseRequest {
            id: host.clone(),
            session_id: "session".into(),
            route: "http://host".into(),
            duration_ms: 30_000,
        })
        .await?;
    let activation = storage.acquire_actor(&actor, &host).await?;
    assert_eq!(activation.owner_epoch, 1);
    assert_eq!(activation.state_version, 0);
    assert_eq!(
        bucket.owner_reads.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    let snapshot = StateSnapshot::new(
        1,
        1,
        "write".into(),
        crate::test_sqlite::snapshot(serde_json::json!({"count":1}))?,
        serde_json::json!(1),
    )?
    .encode()?;
    let ticket = storage.prepare_state_write(&actor, &host, 1, 0).await?;
    assert_eq!(
        bucket.owner_reads.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the first write uses locally established ownership"
    );
    storage
        .runtime
        .write_snapshot(&ticket, snapshot.clone())
        .await?;
    let (version, loaded) = storage.load_actor_state(&actor, &host, 1).await?;
    assert_eq!(version, 1);
    assert_eq!(loaded.as_ref(), snapshot.as_slice());
    assert!(
        storage
            .prepare_state_write(&actor, &host, 2, 1)
            .await
            .is_err()
    );
    assert!(
        storage
            .acquire_actor(&actor, &HostId::new("another-host"))
            .await
            .is_err()
    );
    storage.unregister(&host, "session").await?;
    assert!(storage.ensure_authority().is_err());
    assert!(storage.acquire_actor(&actor, &host).await.is_err());
    Ok(())
}

async fn host_storage(bucket: Arc<MemoryBucket>) -> Result<HostStorage> {
    let runtime = Arc::new(RuntimeStorage::new(bucket.clone(), Arc::new(SystemClock))?);
    let host = HostId::new("host.v3.revision.host");
    Ok(HostStorage {
        objects: None,
        observer: Arc::new(ControlPlaneClient::connect("http://127.0.0.1:1", "unavailable").await?),
        stop: CancellationToken::new(),
        runtime,
        host,
        session: "session".into(),
        region: "us-east".into(),
        actor: Some(ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        }),
        new_actor: true,
        owner_hint: None,
        activation: Mutex::new(None),
        fence: Mutex::new(LeaseFence::default()),
        lease: Mutex::new(None),
        renewal: tokio::sync::Mutex::new(()),
    })
}

#[tokio::test]
async fn persistence_failure_fences_the_host_and_requests_replacement() -> Result<()> {
    let bucket = Arc::new(MemoryBucket::default());
    let storage = Arc::new(host_storage(bucket.clone()).await?);
    storage
        .register(&HostLeaseRequest {
            id: storage.host.clone(),
            session_id: storage.session.clone(),
            route: "http://host".into(),
            duration_ms: 30_000,
        })
        .await?;
    let actor = storage.actor.as_ref().unwrap();
    let plan = storage
        .prepare_state_write(actor, &storage.host, 1, 0)
        .await?;
    let bytes = StateSnapshot::new(
        1,
        1,
        "write".into(),
        crate::test_sqlite::snapshot(serde_json::json!({"count":1}))?,
        serde_json::json!(1),
    )?
    .encode()?;
    bucket
        .reject_snapshots
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let writer = crate::host::persistence::ActorPersistence::new(storage.clone());
    assert!(writer.write_snapshot(&plan, bytes).await.is_err());
    assert!(storage.stop.is_cancelled());
    assert!(storage.ensure_authority().is_err());
    Ok(())
}

#[tokio::test]
async fn a_late_persisted_write_cannot_acknowledge_after_the_lease_expires() -> Result<()> {
    let (bucket, storage, plan, bytes) = pending_write().await?;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    *bucket.delay_snapshot.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let writer = crate::host::persistence::ActorPersistence::new(storage.clone());
    let key = plan.object_name.clone();
    let writing = tokio::spawn(async move { writer.write_snapshot(&plan, bytes).await });
    entered.acquire().await?.forget();
    assert!(
        storage
            .fence
            .lock()
            .unwrap()
            .check(Instant::now() + Duration::from_secs(31))
            .is_err()
    );
    resume.add_permits(1);
    assert!(writing.await?.is_err());
    assert!(bucket.get(&key).await?.is_some());
    assert!(storage.stop.is_cancelled());
    Ok(())
}

#[tokio::test]
async fn canceling_an_ambiguous_write_permanently_fences_the_host() -> Result<()> {
    let (bucket, storage, plan, bytes) = pending_write().await?;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    *bucket.delay_snapshot.lock().unwrap() =
        Some((entered.clone(), Arc::new(tokio::sync::Semaphore::new(0))));
    let writer = crate::host::persistence::ActorPersistence::new(storage.clone());
    let writing = tokio::spawn(async move { writer.write_snapshot(&plan, bytes).await });
    entered.acquire().await?.forget();
    writing.abort();
    assert!(writing.await.unwrap_err().is_cancelled());
    assert!(storage.ensure_authority().is_err());
    assert!(storage.stop.is_cancelled());
    Ok(())
}

async fn pending_write() -> Result<(Arc<MemoryBucket>, Arc<HostStorage>, WritePlan, Vec<u8>)> {
    let bucket = Arc::new(MemoryBucket::default());
    let storage = Arc::new(host_storage(bucket.clone()).await?);
    storage
        .register(&HostLeaseRequest {
            id: storage.host.clone(),
            session_id: storage.session.clone(),
            route: "http://host".into(),
            duration_ms: 30_000,
        })
        .await?;
    let plan = storage
        .prepare_state_write(storage.actor.as_ref().unwrap(), &storage.host, 1, 0)
        .await?;
    let bytes = StateSnapshot::new(
        1,
        1,
        "write".into(),
        crate::test_sqlite::snapshot(serde_json::json!({"count":1}))?,
        serde_json::json!(1),
    )?
    .encode()?;
    Ok((bucket, storage, plan, bytes))
}

#[tokio::test]
async fn socket_authorization_checks_persisted_ownership_without_reading_snapshots() -> Result<()> {
    let (bucket, storage, _, _) = pending_write().await?;
    let actor = storage.actor.as_ref().unwrap();
    let sockets = crate::host::sockets::HostSockets::new(storage.clone());
    bucket
        .reject_snapshot_reads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    sockets
        .publish_authorized(actor, &storage.host, 1, vec![])
        .await?;
    assert!(
        sockets
            .publish_authorized(actor, &storage.host, 2, vec![])
            .await
            .is_err()
    );
    assert!(
        sockets
            .publish_authorized(actor, &HostId::new("other"), 1, vec![])
            .await
            .is_err()
    );
    let other_actor = ActorKey {
        actor_id: "other".into(),
        ..actor.clone()
    };
    assert!(
        sockets
            .publish_authorized(&other_actor, &storage.host, 1, vec![])
            .await
            .is_err()
    );
    {
        let mut objects = bucket.objects.lock().unwrap();
        let owner = objects
            .iter_mut()
            .find(|(key, _)| key.contains("/owners/"))
            .unwrap()
            .1;
        let mut record: serde_json::Value = serde_json::from_slice(&owner.bytes)?;
        record["lease"]["session_id"] = "replacement".into();
        owner.bytes = serde_json::to_vec(&record)?;
        owner.generation += 1;
    }
    assert!(
        sockets
            .publish_authorized(actor, &storage.host, 1, vec![])
            .await
            .is_err()
    );
    storage.stop.cancel();
    assert!(
        sockets
            .publish_authorized(actor, &storage.host, 1, vec![])
            .await
            .is_err()
    );
    Ok(())
}

struct DelayedFinish {
    snapshots: crate::bucket::BucketSnapshots,
    entered: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    fail: bool,
}

#[async_trait]
impl crate::bucket::SnapshotStore for DelayedFinish {
    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>> {
        self.snapshots.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.snapshots.list(prefix).await
    }
    async fn latest(&self, prefix: &str) -> Result<Option<(String, bytes::Bytes)>> {
        self.snapshots.latest(prefix).await
    }
    async fn put(&self, key: &str, bytes: bytes::Bytes) -> Result<()> {
        self.snapshots.put(key, bytes).await
    }
    async fn finish(&self, _: &crate::storage::StateStream, _: tokio::time::Instant) -> Result<()> {
        self.entered.notify_one();
        self.resume.notified().await;
        ensure!(!self.fail, "archive failure");
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn storage_drain_renews_ownership_and_only_seals_archived_state() -> Result<()> {
    use crate::{bucket::PersistenceConfig, placement::ObjectPlacementStore};
    for (fail, stall) in [(false, false), (true, false), (false, true)] {
        let bucket = Arc::new(MemoryBucket::default());
        let delayed = Arc::new(DelayedFinish {
            snapshots: crate::bucket::BucketSnapshots(bucket.clone()),
            entered: tokio::sync::Notify::new(),
            resume: tokio::sync::Notify::new(),
            fail,
        });
        let mut storage = host_storage(bucket.clone()).await?;
        storage.runtime = Arc::new(
            RuntimeStorage::new(bucket, Arc::new(SystemClock))?
                .with_persistence(PersistenceConfig::Local, delayed.clone())?,
        );
        let storage = Arc::new(storage);
        let request = HostLeaseRequest {
            id: storage.host.clone(),
            session_id: storage.session.clone(),
            route: "http://host".into(),
            duration_ms: 30_000,
        };
        storage.register(&request).await?;
        let finishing = {
            let storage = storage.clone();
            tokio::spawn(async move { storage.unregister(&storage.host, &storage.session).await })
        };
        delayed.entered.notified().await;
        tokio::time::advance(Duration::from_secs(6)).await;
        storage.register(&request).await?;
        storage.ensure_authority()?;
        if stall {
            tokio::time::advance(Duration::from_secs(16)).await;
        } else {
            delayed.resume.notify_one();
        }
        finishing.await??;
        assert!(storage.register(&request).await.is_err());
        let (_, hint) = storage
            .runtime
            .get_owner_with_hint(&storage.actor.as_ref().unwrap().storage_key())
            .await?;
        let record = serde_json::to_value(hint.unwrap())?;
        assert_eq!(record["record"]["sealed"], !fail && !stall);
        assert_eq!(record["record"]["lease"]["expires_at_ms"], 0);
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_releases_ownership_while_a_renewal_is_stuck() -> Result<()> {
    use crate::placement::ObjectPlacementStore;
    let bucket = Arc::new(MemoryBucket::default());
    let storage = Arc::new(host_storage(bucket.clone()).await?);
    let request = HostLeaseRequest {
        id: storage.host.clone(),
        session_id: storage.session.clone(),
        route: "http://host".into(),
        duration_ms: 30_000,
    };
    storage.register(&request).await?;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    *bucket.delay_ownership.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let renewing = {
        let storage = storage.clone();
        tokio::spawn(async move { storage.register(&request).await })
    };
    entered.acquire().await?.forget();
    tokio::time::timeout(
        Duration::from_secs(5),
        storage.finish(tokio::time::Instant::now() + Duration::from_secs(5)),
    )
    .await??;
    assert!(storage.ensure_authority().is_err());
    resume.add_permits(1);
    assert!(renewing.await?.is_err());
    let (_, hint) = storage
        .runtime
        .get_owner_with_hint(&storage.actor.as_ref().unwrap().storage_key())
        .await?;
    let record = serde_json::to_value(hint.unwrap())?;
    assert_eq!(record["record"]["lease"]["expires_at_ms"], 0);
    assert_eq!(record["record"]["sealed"], true);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn shutdown_stays_fenced_when_ownership_release_times_out() -> Result<()> {
    let bucket = Arc::new(MemoryBucket::default());
    let storage = Arc::new(host_storage(bucket.clone()).await?);
    storage
        .register(&HostLeaseRequest {
            id: storage.host.clone(),
            session_id: storage.session.clone(),
            route: "http://host".into(),
            duration_ms: 30_000,
        })
        .await?;
    *bucket.delay_ownership.lock().unwrap() = Some((
        Arc::new(tokio::sync::Semaphore::new(0)),
        Arc::new(tokio::sync::Semaphore::new(0)),
    ));
    let error = tokio::time::timeout(
        Duration::from_secs(6),
        storage.finish(tokio::time::Instant::now() + Duration::from_secs(5)),
    )
    .await?
    .unwrap_err();
    assert!(error.to_string().contains("ownership release timed out"));
    assert!(storage.ensure_authority().is_err());
    Ok(())
}
