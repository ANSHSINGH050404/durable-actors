use super::*;

#[tokio::test]
async fn first_write_commits_without_waiting_for_replica_provisioning() -> Result<()> {
    use crate::{
        state_log::StateSnapshot,
        state_transport::{SnapshotWriter, StateWrite},
    };
    struct PendingFleet;
    #[async_trait]
    impl crate::replication::ReplicaProvisioner for PendingFleet {
        fn replica_regions(&self) -> Vec<String> {
            vec!["us-east".into()]
        }
        async fn ensure(&self, _: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
            std::future::pending().await
        }
    }
    let mut f = Fixture::new()?;
    f.runtime.fleet = Arc::new(PendingFleet);
    let activation = f
        .runtime
        .register_activation(&f.actor, &request("first"), "us-east", true, None)
        .await?
        .placement;
    let plan = tokio::time::timeout(
        Duration::from_millis(100),
        f.runtime
            .prepare_actor_write(&f.actor, &activation.lease, 1, 1),
    )
    .await??;
    assert!(plan.replication.is_none());
    let bytes = StateSnapshot::new(
        1,
        1,
        "first".into(),
        serde_json::json!({"count": 1}),
        serde_json::json!(1),
    )?
    .encode()?;
    assert_eq!(
        f.runtime.write_snapshot(&plan, bytes.clone()).await?,
        StateWrite::Written
    );
    f.clock.0.store(11_000, Ordering::SeqCst);
    let resumed = f
        .runtime
        .register_activation(&f.actor, &request("next"), "us-east", false, None)
        .await?;
    assert_eq!(resumed.state.unwrap().as_ref(), bytes);
    Ok(())
}
use crate::{
    bucket::FileBucket, clock::Clock, host_leases::HostLeaseRequest, replication::ReplicaSet,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

struct CountedBucket {
    inner: FileBucket,
    reads: AtomicU64,
    session_reads: AtomicU64,
    session_read_wait: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    paused_list: Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
    writes: AtomicU64,
    lists: AtomicU64,
    lose_reply: AtomicBool,
    delay_write: Mutex<Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>>,
}

#[async_trait]
impl Bucket for CountedBucket {
    async fn get(&self, key: &str) -> Result<Option<super::super::BucketObject>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let wait = if key.contains("/sessions/") {
            self.session_reads.fetch_add(1, Ordering::SeqCst);
            self.session_read_wait.lock().unwrap().take()
        } else {
            None
        };
        if let Some(wait) = wait {
            wait.acquire().await?.forget();
        }
        self.inner.get(key).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let delayed = self.delay_write.lock().unwrap().take();
        if let Some((entered, resume)) = delayed {
            entered.add_permits(1);
            resume.acquire().await?.forget();
        }
        let written = self.inner.compare_and_swap(key, generation, bytes).await?;
        ensure!(
            !self.lose_reply.swap(false, Ordering::SeqCst),
            "CAS response lost"
        );
        Ok(written)
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        let keys = self.inner.list(prefix).await?;
        let paused = self.paused_list.lock().unwrap().take();
        if let Some((entered, resume)) = paused {
            entered.add_permits(1);
            resume.acquire().await?.forget();
        }
        Ok(keys)
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    bucket: Arc<CountedBucket>,
    clock: Arc<TestClock>,
    runtime: RuntimeStorage,
    actor: ActorKey,
}

impl Fixture {
    fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(CountedBucket {
            inner: FileBucket::new(directory.path().into())?,
            reads: AtomicU64::new(0),
            session_reads: AtomicU64::new(0),
            session_read_wait: Mutex::new(None),
            paused_list: Mutex::new(None),
            writes: AtomicU64::new(0),
            lists: AtomicU64::new(0),
            lose_reply: AtomicBool::new(false),
            delay_write: Mutex::new(None),
        });
        let clock = Arc::new(TestClock(AtomicU64::new(1000)));
        let access = ReplicaAccess::new("secret", clock.clone());
        let runtime = RuntimeStorage::new(
            bucket.clone(),
            Arc::new(ReplicaSet::default()),
            Arc::new(crate::bucket::GrpcReplicaPeers::new(access.clone())?),
            access,
            "http://control".into(),
            clock.clone(),
        )?;
        Ok(Self {
            _directory: directory,
            bucket,
            clock,
            runtime,
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "one".into(),
            },
        })
    }

    async fn replicate(&mut self, lease: &HostLease) -> Result<Arc<DiskPeers>> {
        use crate::replication::FileReplicaStore;
        let mut stores = HashMap::new();
        let mut targets = Vec::new();
        for region in ["us-east", "us-central"] {
            stores.insert(
                region.into(),
                Arc::new(FileReplicaStore::open(self._directory.path().join(region), 4096).await?),
            );
            targets.push(ReplicaTarget {
                host_id: region.into(),
                url: format!("http://{region}"),
                region: region.into(),
            });
        }
        let peers = Arc::new(DiskPeers {
            stores,
            available: std::sync::atomic::AtomicBool::new(true),
        });
        self.runtime.fleet = Arc::new(ReplicaSet(targets));
        self.runtime.peers = peers.clone();
        let scope = ReplicaScope {
            actor: self.actor.clone(),
            host: lease.id.clone(),
            session: lease.session_id.clone(),
            region: "us-east".into(),
        };
        let targets = self.runtime.fleet.ensure(&scope).await?;
        let membership = self
            .runtime
            .replace_replicas(
                &scope,
                targets,
                None,
                &crate::state_transport::GrpcStateTransport::new(),
            )
            .await?;
        self.runtime.enable_replication(membership)?;
        Ok(peers)
    }
}

fn request(session: &str) -> HostLeaseRequest {
    HostLeaseRequest {
        id: HostId::new(format!("host-{session}")),
        session_id: session.into(),
        route: format!("http://{session}"),
        duration_ms: 10_000,
    }
}

#[tokio::test]
async fn waking_an_actor_requires_existing_ownership_and_never_claims_it() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    let lease = HostLease {
        id: first.id.clone(),
        session_id: first.session_id.clone(),
        route: first.route.clone(),
        expires_at_ms: 11_000,
    };
    assert!(
        f.runtime
            .activate_actor(&f.actor, &lease, "us-east")
            .await
            .is_err()
    );
    let activation = f
        .runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    let writes = f.bucket.writes.load(Ordering::SeqCst);
    let restored = f
        .runtime
        .activate_actor(&f.actor, &lease, "us-east")
        .await?;
    assert_eq!(restored.placement, activation.placement);
    assert_eq!(f.bucket.writes.load(Ordering::SeqCst), writes);
    f.clock.0.store(11_000, Ordering::SeqCst);
    let replacement = HostLease {
        id: request("replacement").id,
        session_id: "replacement".into(),
        route: "http://replacement".into(),
        expires_at_ms: 21_000,
    };
    assert!(
        f.runtime
            .activate_actor(&f.actor, &replacement, "us-east")
            .await
            .is_err()
    );
    assert_eq!(f.bucket.writes.load(Ordering::SeqCst), writes);
    Ok(())
}

#[tokio::test]
async fn new_actor_resolution_and_activation_use_one_read_and_one_write() -> Result<()> {
    let f = Fixture::new()?;
    assert!(f.runtime.get_owner(&f.actor.storage_key()).await?.is_none());
    let activation = f
        .runtime
        .register_activation(&f.actor, &request("first"), "us-east", true, None)
        .await?;
    assert_eq!(activation.placement.owner_epoch, 1);
    assert_eq!(activation.placement.lease.expires_at_ms, 11_000);
    assert!(activation.state.is_none());
    assert_eq!(f.bucket.reads.load(Ordering::SeqCst), 1);
    assert_eq!(f.bucket.writes.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.bucket.inner.list(crate::storage_paths::ROOT).await?.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn snapshot_writes_need_one_bucket_write_and_no_ownership_read() -> Result<()> {
    use crate::{
        state_log::StateSnapshot,
        state_transport::{SnapshotWriter, StateWrite},
    };
    let f = Fixture::new()?;
    let activation = f
        .runtime
        .register_activation(&f.actor, &request("first"), "us-east", true, None)
        .await?
        .placement;
    for version in 1..=3 {
        let plan = f
            .runtime
            .prepare_actor_write(&f.actor, &activation.lease, activation.owner_epoch, version)
            .await?;
        let bytes = StateSnapshot::new(
            version,
            activation.owner_epoch,
            format!("write-{version}"),
            serde_json::json!({"count": version}),
            serde_json::json!(version),
        )?
        .encode()?;
        let reads = f.bucket.reads.load(Ordering::SeqCst);
        let writes = f.bucket.writes.load(Ordering::SeqCst);
        assert_eq!(
            f.runtime.write_snapshot(&plan, bytes.clone()).await?,
            StateWrite::Written
        );
        assert_eq!(f.bucket.reads.load(Ordering::SeqCst), reads);
        assert_eq!(f.bucket.writes.load(Ordering::SeqCst), writes + 1);
        assert_eq!(
            f.bucket.inner.get(&plan.object_name).await?.unwrap().bytes,
            bytes
        );
    }
    Ok(())
}

#[tokio::test]
async fn stale_absence_and_concurrent_claims_cannot_replace_a_winner() -> Result<()> {
    let f = Fixture::new()?;
    let left = request("left");
    let right = request("right");
    let (a, b) = tokio::join!(
        f.runtime
            .register_activation(&f.actor, &left, "us-east", true, None),
        f.runtime
            .register_activation(&f.actor, &right, "us-east", true, None),
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let winner = a.or(b)?.placement;
    assert!(
        f.runtime
            .register_activation(&f.actor, &request("late"), "us-east", true, None)
            .await
            .is_err()
    );
    assert_eq!(
        f.runtime.get_owner(&f.actor.storage_key()).await?.unwrap(),
        winner
    );
    Ok(())
}

#[tokio::test]
async fn renewal_release_and_takeover_share_the_actor_record() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    assert!(
        f.runtime
            .register_activation(&f.actor, &request("second"), "us-east", false, None)
            .await
            .is_err()
    );
    f.clock.0.store(2000, Ordering::SeqCst);
    let renewed = f
        .runtime
        .renew_activation(&f.actor, &first, Default::default())
        .await?;
    assert_eq!(renewed.expires_at_ms, 12_000);
    assert_eq!(
        f.runtime
            .get_owner(&f.actor.storage_key())
            .await?
            .unwrap()
            .lease,
        renewed
    );
    f.runtime
        .release_activation(&f.actor, &first.id, &first.session_id)
        .await?;
    assert!(
        f.runtime
            .renew_activation(&f.actor, &first, Default::default())
            .await
            .is_err()
    );
    let second = f
        .runtime
        .register_activation(&f.actor, &request("second"), "us-east", false, None)
        .await?;
    assert_eq!(second.placement.owner_epoch, 2);
    f.runtime
        .release_activation(&f.actor, &first.id, &first.session_id)
        .await?;
    assert_eq!(
        f.runtime.get_owner(&f.actor.storage_key()).await?.unwrap(),
        second.placement
    );
    assert!(
        f.runtime
            .renew_activation(&f.actor, &first, Default::default())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn expired_activation_cannot_renew_or_reacquire_with_the_same_session() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    f.clock.0.store(11_000, Ordering::SeqCst);
    assert!(
        f.runtime
            .renew_activation(&f.actor, &first, Default::default())
            .await
            .is_err()
    );
    assert!(
        f.runtime
            .register_activation(&f.actor, &first, "us-east", false, None)
            .await
            .is_err()
    );
    assert!(
        f.runtime
            .register_activation(&f.actor, &request("second"), "us-west", false, None)
            .await
            .is_err()
    );
    let next = f
        .runtime
        .register_activation(&f.actor, &request("second"), "us-east", false, None)
        .await?;
    assert_eq!(next.placement.owner_epoch, 2);
    Ok(())
}

struct DiskPeers {
    stores: HashMap<String, Arc<crate::replication::FileReplicaStore>>,
    available: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl ReplicaPeers for DiskPeers {
    async fn initialize(&self, target: &ReplicaTarget, session: &str) -> Result<()> {
        use crate::replication::ReplicaStore;
        self.stores[&target.host_id]
            .initialize_session(session)
            .await
    }
    async fn head(
        &self,
        target: &ReplicaTarget,
        stream: &ReplicaStream,
    ) -> Result<crate::replication::StreamHead> {
        use crate::replication::ReplicaStore;
        ensure!(
            self.available.load(Ordering::SeqCst),
            "replicas unavailable"
        );
        self.stores[&target.host_id].stream_head(stream).await
    }
    async fn seal(
        &self,
        target: &ReplicaTarget,
        session: &str,
    ) -> Result<crate::replication::SessionHead> {
        use crate::replication::ReplicaStore;
        ensure!(
            self.available.load(Ordering::SeqCst),
            "replicas unavailable"
        );
        self.stores[&target.host_id].seal_session(session).await
    }
    async fn read(&self, target: &ReplicaTarget, object: &str) -> Result<Vec<u8>> {
        use crate::replication::ReplicaStore;
        ensure!(
            self.available.load(Ordering::SeqCst),
            "replicas unavailable"
        );
        self.stores[&target.host_id]
            .read(object)
            .await?
            .context("snapshot missing")
    }
}

#[tokio::test]
async fn combined_lease_takeover_recovers_replica_only_commits_and_seals_old_writes() -> Result<()>
{
    use crate::{replication::ReplicaStore, state_log::StateSnapshot};
    let mut f = Fixture::new()?;
    let first = f
        .runtime
        .register_activation(&f.actor, &request("first"), "us-east", true, None)
        .await?
        .placement;
    let peers = f.replicate(&first.lease).await?;
    let plan = f
        .runtime
        .prepare_actor_write(&f.actor, &first.lease, 1, 1)
        .await?;
    let previous = StateSnapshot::new(
        1,
        1,
        "previous".into(),
        serde_json::json!({"count": 1}),
        serde_json::json!(1),
    )?
    .encode()?;
    f.runtime.persist(&plan.object_name, previous).await?;
    let bytes = StateSnapshot::new(
        2,
        1,
        "committed".into(),
        serde_json::json!({"count": 42}),
        serde_json::json!(42),
    )?
    .encode()?;
    for store in peers.stores.values() {
        store.append(&plan.stream, &bytes).await?;
    }
    f.clock.0.store(11_000, Ordering::SeqCst);
    peers.available.store(false, Ordering::SeqCst);
    assert!(
        f.runtime
            .register_activation(&f.actor, &request("next"), "us-east", false, None)
            .await
            .is_err()
    );
    assert_eq!(
        f.runtime.get_owner(&f.actor.storage_key()).await?.unwrap(),
        first
    );
    peers.available.store(true, Ordering::SeqCst);
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume_list = Arc::new(tokio::sync::Semaphore::new(0));
    let resume_read = Arc::new(tokio::sync::Semaphore::new(0));
    *f.bucket.paused_list.lock().unwrap() = Some((entered.clone(), resume_list.clone()));
    *f.bucket.session_read_wait.lock().unwrap() = Some(resume_read.clone());
    f.bucket.reads.store(0, Ordering::SeqCst);
    f.bucket.session_reads.store(0, Ordering::SeqCst);
    let next = request("next");
    let activation = f
        .runtime
        .register_activation(&f.actor, &next, "us-east", false, None);
    let release = async {
        entered.acquire().await?.forget();
        resume_read.add_permits(1);
        resume_list.add_permits(1);
        anyhow::Ok(())
    };
    let (recovered, released) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(activation, release)
    })
    .await?;
    released?;
    let recovered = recovered?;
    assert_eq!(f.bucket.session_reads.load(Ordering::SeqCst), 2);
    assert_eq!(recovered.placement.owner_epoch, 2);
    assert_eq!(recovered.placement.state_version, 2);
    assert_eq!(recovered.state.unwrap().as_ref(), bytes);
    for store in peers.stores.values() {
        assert!(store.append(&plan.stream, &bytes).await.is_err());
    }
    assert!(
        f.runtime
            .renew_activation(&f.actor, &request("first"), Default::default())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn ownership_without_a_combined_lease_is_rejected() -> Result<()> {
    for missing in [true, false] {
        let f = Fixture::new()?;
        let mut owner = serde_json::json!({
            "actor": f.actor, "owner": "legacy", "session": "legacy",
            "epoch": 1, "region": "us-east", "base": null, "mutation": "old-write",
        });
        if !missing {
            owner["lease"] = serde_json::Value::Null;
        }
        let key = ownership_key(&f.actor.storage_key())?;
        let bytes = serde_json::to_vec(&owner)?;
        assert!(
            f.bucket
                .inner
                .compare_and_swap(&key, None, bytes.clone())
                .await?
        );
        assert!(f.runtime.get_owner(&f.actor.storage_key()).await.is_err());
        assert!(
            f.runtime
                .register_activation(&f.actor, &request("next"), "us-east", false, None)
                .await
                .is_err()
        );
        assert_eq!(f.bucket.inner.get(&key).await?.unwrap().bytes, bytes);
        assert_eq!(f.bucket.writes.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn ambiguous_claim_and_renewal_responses_reconcile_the_exact_record() -> Result<()> {
    let f = Fixture::new()?;
    f.bucket.lose_reply.store(true, Ordering::SeqCst);
    let first = f
        .runtime
        .register_activation(&f.actor, &request("first"), "us-east", true, None)
        .await?;
    assert_eq!(first.placement.owner_epoch, 1);
    f.clock.0.store(2000, Ordering::SeqCst);
    f.bucket.lose_reply.store(true, Ordering::SeqCst);
    assert_eq!(
        f.runtime
            .renew_activation(&f.actor, &request("first"), Default::default())
            .await?
            .expires_at_ms,
        12_000
    );
    assert_eq!(
        f.runtime
            .get_owner(&f.actor.storage_key())
            .await?
            .unwrap()
            .owner_epoch,
        1
    );
    Ok(())
}

#[tokio::test]
async fn delayed_renewal_cannot_overwrite_a_completed_takeover() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    f.clock.0.store(2000, Ordering::SeqCst);
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    *f.bucket.delay_write.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let takeover = async {
        entered.acquire().await?.forget();
        f.clock.0.store(11_000, Ordering::SeqCst);
        let result = f
            .runtime
            .register_activation(&f.actor, &request("next"), "us-east", false, None)
            .await;
        resume.add_permits(1);
        result
    };
    let (renewed, claimed) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            f.runtime
                .renew_activation(&f.actor, &first, Default::default()),
            takeover
        )
    })
    .await?;
    assert!(renewed.is_err());
    assert_eq!(claimed?.placement.owner_epoch, 2);
    assert_eq!(
        f.runtime
            .get_owner(&f.actor.storage_key())
            .await?
            .unwrap()
            .owner,
        request("next").id
    );
    Ok(())
}

#[tokio::test]
async fn inventory_follows_activation_lease_without_separate_host_records() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    let initial = f.runtime.actor_inventory(&f.actor.project_id).await?;
    assert!(matches!(
        initial[0].instances[0].status,
        ActorResidency::Unknown
    ));
    let inventory = crate::host_leases::ActivationInventory {
        resident: Some(true),
        connections: vec![],
        waiting: Some(vec![crate::host_leases::WaitingOperation {
            id: "queued".into(),
            operation: "increment".into(),
        }]),
    };
    f.runtime
        .renew_activation(&f.actor, &first, inventory)
        .await?;
    let live = f.runtime.actor_inventory(&f.actor.project_id).await?;
    assert_eq!(live[0].live, 1);
    assert_eq!(
        live[0].instances[0].waiting.as_ref().unwrap()[0].operation,
        "increment"
    );
    assert_eq!(
        f.bucket.inner.list(crate::storage_paths::ROOT).await?.len(),
        1
    );
    f.runtime
        .release_activation(&f.actor, &first.id, &first.session_id)
        .await?;
    let dormant = f.runtime.actor_inventory(&f.actor.project_id).await?;
    assert_eq!(dormant[0].dormant, 1);
    assert!(dormant[0].instances[0].waiting.as_ref().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn release_retries_a_concurrent_inventory_renewal() -> Result<()> {
    let f = Fixture::new()?;
    let first = request("first");
    f.runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    *f.bucket.delay_write.lock().unwrap() = Some((entered.clone(), resume.clone()));
    let release = f
        .runtime
        .release_activation(&f.actor, &first.id, &first.session_id);
    let renewal = async {
        entered.acquire().await?.forget();
        f.runtime
            .renew_activation(&f.actor, &first, Default::default())
            .await?;
        resume.add_permits(1);
        anyhow::Ok(())
    };
    let (released, renewed) = tokio::join!(release, renewal);
    renewed?;
    released?;
    assert_eq!(
        f.runtime
            .get_owner(&f.actor.storage_key())
            .await?
            .unwrap()
            .lease
            .expires_at_ms,
        0
    );
    Ok(())
}

#[tokio::test]
async fn expired_owner_hint_cannot_fence_a_renewed_session() -> Result<()> {
    use crate::{replication::ReplicaStore, state_log::StateSnapshot};
    let mut f = Fixture::new()?;
    let first = request("first");
    let loaded = f
        .runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    let peers = f.replicate(&loaded.placement.lease).await?;
    let (_, hint) = f
        .runtime
        .get_owner_with_hint(&f.actor.storage_key())
        .await?;
    let hint: OwnershipHint = serde_json::from_str(&serde_json::to_string(&hint.unwrap())?)?;
    f.clock.0.store(10_000, Ordering::SeqCst);
    let renewed = f
        .runtime
        .renew_activation(&f.actor, &first, Default::default())
        .await?;
    f.clock.0.store(11_000, Ordering::SeqCst);
    let error = f
        .runtime
        .register_activation(&f.actor, &request("next"), "us-east", false, Some(&hint))
        .await
        .err()
        .unwrap();
    assert!(
        error
            .to_string()
            .contains("previous owner lease is still active")
    );
    let plan = f
        .runtime
        .prepare_actor_write(&f.actor, &renewed, 1, 1)
        .await?;
    let bytes = StateSnapshot::new(
        1,
        1,
        "after-renewal".into(),
        serde_json::json!({"count": 1}),
        serde_json::json!(1),
    )?
    .encode()?;
    for store in peers.stores.values() {
        store.append(&plan.stream, &bytes).await?;
    }
    let session_key = format!(
        "{}.json",
        crate::storage_paths::session(&first.id, &first.session_id)
    );
    let session = f.bucket.inner.get(&session_key).await?.unwrap();
    assert!(serde_json::from_slice::<session::Session>(&session.bytes)?.is_open());
    assert_eq!(
        f.runtime
            .get_owner(&f.actor.storage_key())
            .await?
            .unwrap()
            .lease,
        renewed
    );
    Ok(())
}

#[tokio::test]
async fn stale_owner_hint_rereads_and_preserves_the_current_owner() -> Result<()> {
    for active in [true, false] {
        let f = Fixture::new()?;
        f.runtime
            .register_activation(&f.actor, &request("first"), "us-east", true, None)
            .await?;
        f.runtime
            .finish_activation(&f.actor, &request("first").id, "first")
            .await?;
        let (_, hint) = f
            .runtime
            .get_owner_with_hint(&f.actor.storage_key())
            .await?;
        let second = f
            .runtime
            .register_activation(&f.actor, &request("second"), "us-east", false, None)
            .await?;
        let plan = f
            .runtime
            .prepare_actor_write(&f.actor, &second.placement.lease, 2, 1)
            .await?;
        let bytes = crate::state_log::StateSnapshot::new(
            1,
            2,
            "second".into(),
            serde_json::json!({"count": 42}),
            serde_json::json!(42),
        )?
        .encode()?;
        f.runtime.persist(&plan.object_name, bytes.clone()).await?;
        if !active {
            f.runtime
                .release_activation(&f.actor, &request("second").id, "second")
                .await?;
        }
        let loaded = f
            .runtime
            .register_activation(&f.actor, &request("next"), "us-east", false, hint.as_ref())
            .await;
        if active {
            assert!(
                loaded
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("previous owner lease is still active")
            );
            assert_eq!(
                f.runtime.get_owner(&f.actor.storage_key()).await?.unwrap(),
                second.placement
            );
        } else {
            let loaded = loaded?;
            assert_eq!(loaded.placement.owner_epoch, 3);
            assert_eq!(loaded.state.unwrap().as_ref(), bytes);
        }
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_session_sealing_refreshes_the_parallel_snapshot_listing() -> Result<()> {
    use crate::state_log::StateSnapshot;
    let f = Fixture::new()?;
    let first = request("first");
    let loaded = f
        .runtime
        .register_activation(&f.actor, &first, "us-east", true, None)
        .await?;
    let plan = f
        .runtime
        .prepare_actor_write(&f.actor, &loaded.placement.lease, 1, 1)
        .await?;
    let bytes = StateSnapshot::new(
        1,
        1,
        "committed".into(),
        serde_json::json!({"count": 42}),
        serde_json::json!(42),
    )?
    .encode()?;
    f.clock.0.store(11_000, Ordering::SeqCst);
    let (_, hint) = f
        .runtime
        .get_owner_with_hint(&f.actor.storage_key())
        .await?;
    let hint = hint.unwrap();
    let scope = hint.record.scope();
    let session_key = format!(
        "{}.json",
        crate::storage_paths::session(&first.id, &first.session_id)
    );
    let mut session = session::Session {
        id: scope.identity(),
        region: scope.region.clone(),
        replicas: vec![ReplicaTarget {
            host_id: "replica".into(),
            region: "us-east".into(),
            url: "http://replica".into(),
        }],
        state: session::RecoveryState::Recovering,
    };
    assert!(
        f.bucket
            .inner
            .compare_and_swap(&session_key, None, serde_json::to_vec(&session)?)
            .await?
    );
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume_list = Arc::new(tokio::sync::Semaphore::new(0));
    let resume_read = Arc::new(tokio::sync::Semaphore::new(0));
    *f.bucket.paused_list.lock().unwrap() = Some((entered.clone(), resume_list.clone()));
    *f.bucket.session_read_wait.lock().unwrap() = Some(resume_read.clone());
    let next = request("next");
    let activation = f
        .runtime
        .register_activation(&f.actor, &next, "us-east", false, Some(&hint));
    let concurrent_recovery = async {
        entered.acquire().await?.forget();
        f.runtime.persist(&plan.object_name, bytes.clone()).await?;
        let generation = f.bucket.inner.get(&session_key).await?.unwrap().generation;
        session.state = session::RecoveryState::Sealed;
        assert!(
            f.bucket
                .inner
                .compare_and_swap(
                    &session_key,
                    Some(generation),
                    serde_json::to_vec(&session)?
                )
                .await?
        );
        resume_read.add_permits(1);
        resume_list.add_permits(1);
        anyhow::Ok(())
    };
    let (activated, recovered) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(activation, concurrent_recovery)
    })
    .await?;
    recovered?;
    let activated = activated?;
    assert_eq!(activated.placement.state_version, 1);
    assert_eq!(activated.state.unwrap().as_ref(), bytes);
    Ok(())
}

#[tokio::test]
async fn clean_shutdown_reactivates_from_the_checkpoint_without_listing_or_session_reads()
-> Result<()> {
    use crate::{state_log::StateSnapshot, state_transport::SnapshotWriter};
    for written in [true, false] {
        let f = Fixture::new()?;
        let first = request("first");
        let loaded = f
            .runtime
            .register_activation(&f.actor, &first, "us-east", true, None)
            .await?;
        let bytes = StateSnapshot::new(
            2,
            1,
            "committed".into(),
            serde_json::json!({"count": 42}),
            serde_json::json!(42),
        )?
        .encode()?;
        if written {
            for version in [2, 1] {
                let plan = f
                    .runtime
                    .prepare_actor_write(&f.actor, &loaded.placement.lease, 1, version)
                    .await?;
                let snapshot = StateSnapshot::new(
                    version,
                    1,
                    "committed".into(),
                    serde_json::json!({"count": 42}),
                    serde_json::json!(42),
                )?
                .encode()?;
                f.runtime.write_snapshot(&plan, snapshot).await?;
            }
        }
        f.runtime
            .finish_activation(&f.actor, &first.id, &first.session_id)
            .await?;
        for (epoch, session) in [(2, "next"), (3, "again")] {
            let (_, hint) = f
                .runtime
                .get_owner_with_hint(&f.actor.storage_key())
                .await?;
            assert!(hint.as_ref().unwrap().record.sealed);
            f.bucket.reads.store(0, Ordering::SeqCst);
            f.bucket.session_reads.store(0, Ordering::SeqCst);
            f.bucket.lists.store(0, Ordering::SeqCst);
            let next = request(session);
            let loaded = f
                .runtime
                .register_activation(&f.actor, &next, "us-east", false, hint.as_ref())
                .await?;
            assert_eq!(loaded.placement.owner_epoch, epoch);
            assert_eq!(loaded.state.as_deref(), written.then_some(bytes.as_slice()));
            assert_eq!(f.bucket.lists.load(Ordering::SeqCst), 0);
            assert_eq!(f.bucket.reads.load(Ordering::SeqCst), u64::from(written));
            assert_eq!(f.bucket.session_reads.load(Ordering::SeqCst), 0);
            f.runtime
                .finish_activation(&f.actor, &next.id, &next.session_id)
                .await?;
        }
    }
    Ok(())
}
