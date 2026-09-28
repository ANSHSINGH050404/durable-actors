use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use anyhow::Result;
use async_trait::async_trait;
use durable_actors::{
    bucket::{Bucket, BucketObject},
    clock::Clock,
    host::HostId,
    host_leases::HostLeaseRequest,
};

use durable_actors::{
    actor::ActorKey,
    bucket::{ReplicaPeers, RuntimeStorage},
    placement::ObjectPlacementStore,
    replication::{
        FileReplicaStore, ReplicaAccess, ReplicaProvisioner, ReplicaStore, ReplicaStream,
        ReplicaTarget, SessionHead, StreamHead,
    },
    state_log::StateSnapshot,
    state_transport::SnapshotWriter,
};

async fn enable_replicas(
    runtime: &RuntimeStorage,
    actor: &ActorKey,
    lease: &durable_actors::host_leases::HostLease,
) -> Result<()> {
    let scope = durable_actors::replication::ReplicaScope {
        actor: actor.clone(),
        host: lease.id.clone(),
        session: lease.session_id.clone(),
        region: "us-east".into(),
    };
    let targets = runtime.replacement_replicas(&scope, &[]).await?;
    let membership = runtime
        .replace_replicas(
            &scope,
            targets,
            None,
            &durable_actors::state_transport::GrpcStateTransport::new(),
        )
        .await?;
    runtime.enable_replication(membership)
}

struct Fleet(Vec<ReplicaTarget>);
#[async_trait]
impl ReplicaProvisioner for Fleet {
    fn replica_regions(&self) -> Vec<String> {
        self.0
            .iter()
            .map(|replica| replica.region.clone())
            .collect()
    }

    async fn ensure(
        &self,
        _: &durable_actors::replication::ReplicaScope,
    ) -> Result<Vec<ReplicaTarget>> {
        Ok(self.0.clone())
    }
}

struct Peers {
    stores: BTreeMap<String, Arc<FileReplicaStore>>,
    unavailable: Mutex<Vec<String>>,
    seals: AtomicU64,
    initializations: AtomicU64,
    initialization_gate: Option<(Arc<tokio::sync::Semaphore>, Arc<tokio::sync::Semaphore>)>,
}
impl Peers {
    fn store(&self, peer: &ReplicaTarget) -> Result<&Arc<FileReplicaStore>> {
        anyhow::ensure!(
            !self.unavailable.lock().unwrap().contains(&peer.host_id),
            "peer unavailable"
        );
        Ok(&self.stores[&peer.host_id])
    }
}
#[async_trait]
impl ReplicaPeers for Peers {
    async fn initialize(&self, peer: &ReplicaTarget, session: &str) -> Result<()> {
        self.initializations.fetch_add(1, Ordering::SeqCst);
        if let Some((entered, resume)) = &self.initialization_gate {
            entered.add_permits(1);
            resume.acquire().await?.forget();
        }
        self.store(peer)?.initialize_session(session).await
    }
    async fn head(&self, peer: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead> {
        self.store(peer)?.stream_head(stream).await
    }
    async fn seal(&self, peer: &ReplicaTarget, session: &str) -> Result<SessionHead> {
        self.seals.fetch_add(1, Ordering::SeqCst);
        self.store(peer)?.seal_session(session).await
    }
    async fn read(&self, peer: &ReplicaTarget, object: &str) -> Result<Vec<u8>> {
        self.store(peer)?
            .read(object)
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing snapshot"))
    }
}

#[tokio::test]
async fn takeover_recovers_replica_only_writes_and_fences_the_old_epoch() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let targets: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|id| ReplicaTarget {
            host_id: id.into(),
            url: format!("http://{id}"),
            region: "us-east".into(),
        })
        .collect();
    let mut stores = BTreeMap::new();
    for peer in &targets {
        stores.insert(
            peer.host_id.clone(),
            Arc::new(FileReplicaStore::open(directory.path().join(&peer.host_id), 4096).await?),
        );
    }
    let peers = Arc::new(Peers {
        stores,
        unavailable: Mutex::new(vec![]),
        seals: AtomicU64::new(0),
        initializations: AtomicU64::new(0),
        initialization_gate: None,
    });
    let runtime = RuntimeStorage::new(
        bucket.clone(),
        Arc::new(Fleet(targets)),
        peers.clone(),
        ReplicaAccess::new("secret", clock.clone()),
        "http://control".into(),
        clock.clone(),
    )?;
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let first = runtime
        .register_activation(&actor, &request("old"), "us-east", true, None)
        .await?
        .placement;
    enable_replicas(&runtime, &actor, &first.lease).await?;
    let ticket = runtime
        .prepare_actor_write(&actor, &first.lease, first.owner_epoch, 1)
        .await?;
    let stream = &ticket.stream;
    let bytes = StateSnapshot::new(
        1,
        first.owner_epoch,
        "committed".into(),
        serde_json::json!({"count":1}),
        serde_json::json!(1),
    )?
    .encode()?;
    for store in peers.stores.values() {
        store.append(stream, &bytes).await?;
    }
    let scope = durable_actors::replication::ReplicaScope {
        actor: actor.clone(),
        host: first.lease.id.clone(),
        session: first.lease.session_id.clone(),
        region: "us-east".into(),
    };
    assert!(
        runtime.retire_replication(&scope).await.is_err(),
        "live actor replicas must survive cleanup"
    );
    clock.0.store(20_000, Ordering::SeqCst);
    *peers.unavailable.lock().unwrap() = vec!["a".into(), "b".into()];
    assert!(
        runtime
            .register_activation(&actor, &request("new"), "us-east", false, None)
            .await
            .is_err()
    );
    assert_eq!(
        runtime.get_owner(&actor.storage_key()).await?,
        Some(first.clone())
    );
    *peers.unavailable.lock().unwrap() = vec!["a".into()];
    bucket.reject_snapshots.store(true, Ordering::SeqCst);
    assert!(
        runtime.retire_replication(&scope).await.is_err(),
        "failed checkpoint must retain the recovery replicas"
    );
    assert!(!runtime.replica_members(&scope).await?.is_empty());
    bucket.reject_snapshots.store(false, Ordering::SeqCst);
    runtime.retire_replication(&scope).await?;
    assert!(runtime.replica_members(&scope).await?.is_empty());
    let next = runtime
        .register_activation(&actor, &request("new"), "us-east", false, None)
        .await?
        .placement;
    assert_eq!(next.state_version, 1);
    assert_eq!(next.last_request_id.as_deref(), Some("committed"));
    assert_eq!(
        bucket
            .get(next.state_object.as_ref().unwrap())
            .await?
            .unwrap()
            .bytes,
        bytes
    );
    assert_eq!(
        durable_actors::storage::SnapshotReader::read_snapshot(
            &runtime,
            "us-east",
            next.state_object.as_ref().unwrap()
        )
        .await?
        .as_ref(),
        bytes.as_slice()
    );
    assert!(peers.stores["b"].append(stream, &bytes).await.is_err());
    Ok(())
}

#[tokio::test]
async fn a_new_actor_claims_once_without_waiting_for_replication() -> Result<()> {
    struct UnavailableFleet(AtomicU64);
    #[async_trait]
    impl ReplicaProvisioner for UnavailableFleet {
        fn replica_regions(&self) -> Vec<String> {
            vec!["us-east".into()]
        }

        async fn ensure(
            &self,
            _: &durable_actors::replication::ReplicaScope,
        ) -> Result<Vec<ReplicaTarget>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("replica provisioning must not block a cold read")
        }
    }
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let fleet = Arc::new(UnavailableFleet(AtomicU64::new(0)));
    let runtime = RuntimeStorage::new(
        bucket.clone(),
        fleet.clone(),
        Arc::new(Peers {
            stores: BTreeMap::new(),
            unavailable: Mutex::new(vec![]),
            seals: AtomicU64::new(0),
            initializations: AtomicU64::new(0),
            initialization_gate: None,
        }),
        ReplicaAccess::new("secret", clock.clone()),
        "http://control".into(),
        clock.clone(),
    )?;
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "new".into(),
    };
    let placement = runtime
        .register_activation(&actor, &request("session"), "us-east", true, None)
        .await?
        .placement;
    assert_eq!(placement.state_version, 0);
    assert_eq!(bucket.owner_writes.load(Ordering::SeqCst), 1);
    assert_eq!(fleet.0.load(Ordering::SeqCst), 0);
    Ok(())
}

#[derive(Default)]
struct MemoryBucket {
    objects: Mutex<BTreeMap<String, BucketObject>>,
    lose_reply: AtomicBool,
    reject_snapshots: AtomicBool,
    reject_ownership: AtomicBool,
    owner_writes: AtomicU64,
    reads: Mutex<Vec<String>>,
}

#[async_trait]
impl Bucket for MemoryBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.reads.lock().unwrap().push(key.into());
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }

    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        anyhow::ensure!(
            !key.contains("/snapshots/") || !self.reject_snapshots.load(Ordering::SeqCst),
            "bucket writes unavailable"
        );
        anyhow::ensure!(
            !key.contains("/owners/") || !self.reject_ownership.load(Ordering::SeqCst),
            "ownership writes unavailable"
        );
        let mut objects = self.objects.lock().unwrap();
        let current = objects.get(key).map(|object| object.generation);
        if current != generation {
            return Ok(false);
        }
        if key.contains("/owners/") {
            self.owner_writes.fetch_add(1, Ordering::SeqCst);
        }
        objects.insert(
            key.into(),
            BucketObject {
                generation: current.unwrap_or(0) + 1,
                bytes,
            },
        );
        anyhow::ensure!(
            !self.lose_reply.swap(false, Ordering::SeqCst),
            "CAS response lost"
        );
        Ok(true)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
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

struct TestClock(AtomicU64);
impl Clock for TestClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

fn request(session: &str) -> HostLeaseRequest {
    HostLeaseRequest {
        id: HostId::new("host"),
        session_id: session.into(),
        route: "http://host".into(),
        duration_ms: 10_000,
    }
}

#[tokio::test]
async fn simultaneous_claims_from_the_same_observed_generation_have_one_winner() -> Result<()> {
    struct RacingBucket {
        inner: Arc<MemoryBucket>,
        readers: AtomicU64,
        barrier: tokio::sync::Barrier,
    }
    #[async_trait]
    impl Bucket for RacingBucket {
        async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
            let observed = self.inner.get(key).await?;
            if key.contains("/owners/") && self.readers.fetch_add(1, Ordering::SeqCst) < 2 {
                self.barrier.wait().await;
            }
            Ok(observed)
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            generation: Option<i64>,
            bytes: Vec<u8>,
        ) -> Result<bool> {
            self.inner.compare_and_swap(key, generation, bytes).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.inner.list(prefix).await
        }
    }
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let mut left = request("left");
    left.id = HostId::new("left");
    let mut right = request("right");
    right.id = HostId::new("right");
    let authority = Arc::new(RacingBucket {
        inner: bucket.clone(),
        readers: AtomicU64::new(0),
        barrier: tokio::sync::Barrier::new(2),
    });
    let runtime = RuntimeStorage::new(
        authority,
        Arc::new(Fleet(vec![])),
        Arc::new(Peers {
            stores: BTreeMap::new(),
            unavailable: Mutex::new(vec![]),
            seals: AtomicU64::new(0),
            initializations: AtomicU64::new(0),
            initialization_gate: None,
        }),
        ReplicaAccess::new("secret", clock.clone()),
        "http://control".into(),
        clock.clone(),
    )?;
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "race".into(),
    };
    let (first, second) = tokio::join!(
        runtime.register_activation(&actor, &left, "us-east", false, None),
        runtime.register_activation(&actor, &right, "us-east", false, None)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let winner = first.or(second)?.placement;
    assert_eq!(
        runtime.get_owner(&actor.storage_key()).await?.unwrap(),
        winner
    );
    Ok(())
}

#[tokio::test]
async fn grpc_replication_and_takeover_recover_unarchived_state_without_postgres() -> Result<()> {
    use durable_actors::{
        bucket::GrpcReplicaPeers,
        clock::SystemClock,
        replication::{ReplicatedStateTransport, replica_routes},
        state_transport::{GrpcStateTransport, StateTransport, StateWrite},
    };
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
    let replica = Arc::new(FileReplicaStore::open(directory.path().join("replica"), 4096).await?);
    let peer_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let peer_url = format!("http://{}", peer_listener.local_addr()?);
    let peer_stop = tokio_util::sync::CancellationToken::new();
    let peer_shutdown = peer_stop.clone();
    let routes = replica_routes(
        replica.clone(),
        access.clone(),
        "peer".into(),
        durable_actors::replication::ReplicaScope {
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "http".into(),
            },
            host: request("old").id,
            session: request("old").session_id,
            region: "us-east".into(),
        },
    );
    let peer_server = tokio::spawn(async move {
        axum::serve(peer_listener, routes)
            .with_graceful_shutdown(peer_shutdown.cancelled_owned())
            .await
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let runtime = Arc::new(RuntimeStorage::new(
        bucket.clone(),
        Arc::new(Fleet(vec![ReplicaTarget {
            host_id: "peer".into(),
            url: peer_url,
            region: "us-east".into(),
        }])),
        Arc::new(GrpcReplicaPeers::new(access.clone())?),
        access,
        origin,
        clock.clone(),
    )?);
    let stop = tokio_util::sync::CancellationToken::new();
    let shutdown = stop.clone();
    let routes = runtime.clone().router();
    let server = tokio::spawn(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await
    });
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "http".into(),
    };
    let first = runtime
        .register_activation(&actor, &request("old"), "us-east", true, None)
        .await?
        .placement;
    enable_replicas(&runtime, &actor, &first.lease).await?;
    let ticket = runtime
        .prepare_actor_write(&actor, &first.lease, first.owner_epoch, 1)
        .await?;
    let snapshot = StateSnapshot::new(
        1,
        first.owner_epoch,
        "first".into(),
        serde_json::json!({"count":1}),
        serde_json::json!(1),
    )?
    .encode()?;
    let http = Arc::new(GrpcStateTransport::new());
    let transport = ReplicatedStateTransport::new(runtime.clone(), http.clone());
    bucket.reject_snapshots.store(true, Ordering::SeqCst);
    assert_eq!(
        transport.write_snapshot(&ticket, snapshot.clone()).await?,
        StateWrite::Replicated
    );
    assert!(bucket.get(&ticket.object_name).await?.is_none());
    assert_eq!(
        http.read(&runtime.read_url("us-east", &ticket.object_name).await?)
            .await?
            .as_ref(),
        snapshot
    );
    drop(transport);
    assert_eq!(
        replica.read(&ticket.object_name).await?,
        Some(snapshot.clone())
    );
    bucket.reject_snapshots.store(false, Ordering::SeqCst);
    assert!(bucket.get(&ticket.object_name).await?.is_none());
    clock.0.store(20_000, Ordering::SeqCst);
    let next = runtime
        .register_activation(&actor, &request("new"), "us-east", false, None)
        .await?
        .placement;
    assert_eq!(next.state_version, 1);
    let late = StateSnapshot::new(
        2,
        first.owner_epoch,
        "late".into(),
        serde_json::json!({"count":99}),
        serde_json::json!(99),
    )?
    .encode()?;
    assert!(
        http.write(
            &ticket.replication.as_ref().unwrap().replicas[0].url,
            late.clone()
        )
        .await
        .is_err()
    );
    assert_eq!(
        SnapshotWriter::write_snapshot(
            runtime.as_ref(),
            &durable_actors::storage::WritePlan {
                state_version: 2,
                object_name: ticket.stream.object(2),
                ..ticket.clone()
            },
            late.clone()
        )
        .await?,
        StateWrite::Written
    );
    assert_eq!(
        bucket.get(&ticket.stream.object(2)).await?.unwrap().bytes,
        late
    );
    assert_eq!(
        runtime
            .get(&actor.storage_key())
            .await?
            .unwrap()
            .state_version,
        1
    );
    assert_eq!(
        replica.read(&ticket.object_name).await?,
        Some(snapshot.clone())
    );
    assert_eq!(
        bucket.get(&ticket.object_name).await?.unwrap().bytes,
        snapshot
    );
    stop.cancel();
    peer_stop.cancel();
    server.await??;
    peer_server.await??;
    Ok(())
}

#[tokio::test]
async fn recovery_retries_failed_checkpoint_and_reuses_sealed_state_after_a_failed_claim()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let peers = Arc::new(Peers {
        stores: BTreeMap::from([
            (
                "a".into(),
                Arc::new(FileReplicaStore::open(directory.path().join("a"), 4096).await?),
            ),
            (
                "b".into(),
                Arc::new(FileReplicaStore::open(directory.path().join("b"), 4096).await?),
            ),
        ]),
        unavailable: Mutex::new(Vec::new()),
        seals: AtomicU64::new(0),
        initializations: AtomicU64::new(0),
        initialization_gate: None,
    });
    let runtime = || {
        RuntimeStorage::new(
            bucket.clone(),
            Arc::new(Fleet(
                peers
                    .stores
                    .keys()
                    .map(|id| ReplicaTarget {
                        host_id: id.clone(),
                        url: format!("http://{id}"),
                        region: "us-east".into(),
                    })
                    .collect(),
            )),
            peers.clone(),
            ReplicaAccess::new("secret", clock.clone()),
            "http://control".into(),
            clock.clone(),
        )
    };
    let old = runtime()?;
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let placement = old
        .register_activation(&actor, &request("old"), "us-east", true, None)
        .await?
        .placement;
    enable_replicas(&old, &actor, &placement.lease).await?;
    let ticket = old
        .prepare_actor_write(&actor, &placement.lease, placement.owner_epoch, 1)
        .await?;
    let bytes = StateSnapshot::new(
        1,
        placement.owner_epoch,
        "acknowledged".into(),
        serde_json::json!({"count": 1}),
        serde_json::json!(1),
    )?
    .encode()?;
    for store in peers.stores.values() {
        store.append(&ticket.stream, &bytes).await?;
    }
    assert_eq!(peers.initializations.load(Ordering::SeqCst), 2);
    clock.0.store(20_000, Ordering::SeqCst);
    bucket.reject_snapshots.store(true, Ordering::SeqCst);
    assert!(
        old.register_activation(&actor, &request("new"), "us-east", false, None)
            .await
            .is_err()
    );
    assert_eq!(
        old.get_owner(&actor.storage_key()).await?,
        Some(placement.clone())
    );
    assert_eq!(bucket.owner_writes.load(Ordering::SeqCst), 1);
    drop(old);

    bucket.reject_snapshots.store(false, Ordering::SeqCst);
    bucket.reject_ownership.store(true, Ordering::SeqCst);
    bucket.reads.lock().unwrap().clear();
    let next = runtime()?;
    assert!(
        next.register_activation(&actor, &request("new"), "us-east", false, None)
            .await
            .is_err()
    );
    assert_eq!(
        bucket
            .reads
            .lock()
            .unwrap()
            .iter()
            .filter(|key| **key == ticket.object_name)
            .count(),
        1,
        "reuse recovered bytes instead of downloading the snapshot again"
    );
    assert_eq!(bucket.get(&ticket.object_name).await?.unwrap().bytes, bytes);
    assert_eq!(next.get_owner(&actor.storage_key()).await?, Some(placement));
    drop(next);

    bucket.reject_ownership.store(false, Ordering::SeqCst);
    let seals = peers.seals.load(Ordering::SeqCst);
    *peers.unavailable.lock().unwrap() = vec!["a".into(), "b".into()];
    let restored = runtime()?
        .register_activation(&actor, &request("new"), "us-east", false, None)
        .await?;
    assert_eq!(restored.placement.state_version, 1);
    assert_eq!(restored.state.unwrap().as_ref(), bytes);
    assert_eq!(
        peers.seals.load(Ordering::SeqCst),
        seals,
        "retry reuses completed session recovery"
    );
    assert_eq!(bucket.owner_writes.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn takeover_fences_replication_initialization_that_was_delayed_past_lease_expiry()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let resume = Arc::new(tokio::sync::Semaphore::new(0));
    let store = Arc::new(FileReplicaStore::open(directory.path().join("replica"), 4096).await?);
    let peers = Arc::new(Peers {
        stores: BTreeMap::from([("peer".into(), store)]),
        unavailable: Mutex::new(Vec::new()),
        seals: AtomicU64::new(0),
        initializations: AtomicU64::new(0),
        initialization_gate: Some((entered.clone(), resume.clone())),
    });
    let runtime = Arc::new(RuntimeStorage::new(
        bucket.clone(),
        Arc::new(Fleet(vec![ReplicaTarget {
            host_id: "peer".into(),
            url: "http://peer".into(),
            region: "us-east".into(),
        }])),
        peers,
        ReplicaAccess::new("secret", clock.clone()),
        "http://control".into(),
        clock.clone(),
    )?);
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "delayed".into(),
    };
    let first = runtime
        .register_activation(&actor, &request("old"), "us-east", true, None)
        .await?
        .placement;
    let task = {
        let (runtime, actor) = (runtime.clone(), actor.clone());
        tokio::spawn(async move { enable_replicas(&runtime, &actor, &first.lease).await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), entered.acquire())
        .await??
        .forget();
    clock.0.store(20_000, Ordering::SeqCst);
    let next = runtime
        .register_activation(&actor, &request("new"), "us-east", false, None)
        .await?
        .placement;
    assert_eq!(next.owner_epoch, 2);
    resume.add_permits(1);
    assert!(
        task.await?.is_err(),
        "old session must never receive replication authority after takeover"
    );
    Ok(())
}

#[tokio::test]
async fn same_named_actors_in_different_projects_recover_independent_state() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let targets: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|id| ReplicaTarget {
            host_id: id.into(),
            url: format!("http://{id}"),
            region: "us-east".into(),
        })
        .collect();
    let mut stores = BTreeMap::new();
    for peer in &targets {
        stores.insert(
            peer.host_id.clone(),
            Arc::new(FileReplicaStore::open(directory.path().join(&peer.host_id), 4096).await?),
        );
    }
    let peers = Arc::new(Peers {
        stores,
        unavailable: Mutex::new(vec![]),
        seals: AtomicU64::new(0),
        initializations: AtomicU64::new(0),
        initialization_gate: None,
    });
    let runtime = RuntimeStorage::new(
        bucket.clone(),
        Arc::new(Fleet(targets)),
        peers.clone(),
        ReplicaAccess::new("secret", clock.clone()),
        "http://control".into(),
        clock.clone(),
    )?;
    let mut committed = Vec::new();
    for (project, count) in [("team-a", 1), ("team-b", 3)] {
        let actor = ActorKey {
            project_id: project.into(),
            actor_name: "Counter".into(),
            actor_id: "same".into(),
        };
        let placement = runtime
            .register_activation(
                &actor,
                &request(&format!("old-{project}")),
                "us-east",
                true,
                None,
            )
            .await?
            .placement;
        enable_replicas(&runtime, &actor, &placement.lease).await?;
        let ticket = runtime
            .prepare_actor_write(&actor, &placement.lease, placement.owner_epoch, 1)
            .await?;
        let bytes = StateSnapshot::new(
            1,
            placement.owner_epoch,
            project.into(),
            serde_json::json!({"count": count}),
            serde_json::json!(count),
        )?
        .encode()?;
        for store in peers.stores.values() {
            store.append(&ticket.stream, &bytes).await?;
        }
        committed.push((actor, bytes));
    }
    clock.0.store(20_000, Ordering::SeqCst);
    for (actor, bytes) in committed {
        let restored = runtime
            .register_activation(
                &actor,
                &request(&format!("new-{}", actor.project_id)),
                "us-east",
                false,
                None,
            )
            .await?;
        assert_eq!(restored.placement.state_version, 1);
        assert_eq!(restored.state.unwrap().as_ref(), bytes);
    }
    Ok(())
}
