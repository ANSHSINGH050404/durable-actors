use super::*;
use crate::{
    bucket::{BucketObject, FileBucket},
    host::replication::ActorReplication,
    replication::{
        FileReplicaStore, ReplicaProvisioner, ReplicaScope, ReplicaStore, ReplicaTarget,
        replica_routes,
    },
    state_log::StateSnapshot,
    state_transport::{SnapshotWriter, StateWrite},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{OnceCell, Semaphore};

struct DelayedBucket {
    inner: FileBucket,
    snapshot_delay: Duration,
    snapshots: Semaphore,
    membership: Semaphore,
    registering: Semaphore,
    reject_snapshots: AtomicBool,
}

#[async_trait]
impl Bucket for DelayedBucket {
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
        if key.contains("/hosts/") {
            self.registering.add_permits(1);
            let _permit = self.membership.acquire().await?;
        }
        if key.contains("/snapshots/") {
            ensure!(
                !self.reject_snapshots.load(Ordering::SeqCst),
                "GCS unavailable"
            );
            let _permit = self.snapshots.acquire().await?;
            tokio::time::sleep(self.snapshot_delay).await;
        }
        self.inner.compare_and_swap(key, generation, bytes).await
    }
}

struct AssignedFleet {
    target: ReplicaTarget,
    store: Arc<FileReplicaStore>,
    delay: Duration,
    assigned: OnceCell<()>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ReplicaProvisioner for AssignedFleet {
    fn replica_regions(&self) -> Vec<String> {
        vec!["us-east".into()]
    }
    async fn ensure(&self, scope: &ReplicaScope) -> Result<Vec<ReplicaTarget>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.assigned
            .get_or_try_init(|| async {
                tokio::time::sleep(self.delay).await;
                self.store.initialize_session(&scope.identity()).await
            })
            .await?;
        Ok(vec![self.target.clone()])
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    _stop: tokio_util::sync::DropGuard,
    bucket: Arc<DelayedBucket>,
    storage: Arc<HostStorage>,
    scope: ReplicaScope,
    writer: Arc<ActorReplication>,
    connections: Arc<AtomicUsize>,
    connected: Arc<Semaphore>,
    pause_connections: Arc<AtomicBool>,
    assignments: Arc<AtomicUsize>,
}

struct CountedListener {
    inner: tokio::net::TcpListener,
    connections: Arc<AtomicUsize>,
    connected: Arc<Semaphore>,
    paused: Arc<AtomicBool>,
}

impl axum::serve::Listener for CountedListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let accepted = self.inner.accept().await.unwrap();
        self.connections.fetch_add(1, Ordering::SeqCst);
        self.connected.add_permits(1);
        if self.paused.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        accepted
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

impl Fixture {
    async fn new(assignment_ms: u64, snapshot_ms: u64, blocked: bool) -> Result<Self> {
        Self::with_membership(assignment_ms, snapshot_ms, blocked, false).await
    }

    async fn with_membership(
        assignment_ms: u64,
        snapshot_ms: u64,
        blocked: bool,
        unpublished: bool,
    ) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let stop = CancellationToken::new();
        let scope = ReplicaScope {
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "cold".into(),
            },
            host: HostId::new("primary"),
            session: "activation".into(),
            region: "us-east".into(),
        };
        let store = Arc::new(FileReplicaStore::open(directory.path().join("replica"), 4096).await?);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let target = ReplicaTarget {
            host_id: "replica".into(),
            region: "us-east".into(),
            url: format!("http://{}", listener.local_addr()?),
        };
        let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
        let routes = replica_routes(
            store.clone(),
            access.clone(),
            target.host_id.clone(),
            scope.clone(),
        );
        let connections = Arc::new(AtomicUsize::new(0));
        let connected = Arc::new(Semaphore::new(0));
        let pause_connections = Arc::new(AtomicBool::new(false));
        let listener = CountedListener {
            inner: listener,
            connections: connections.clone(),
            connected: connected.clone(),
            paused: pause_connections.clone(),
        };
        let shutdown = stop.clone();
        tokio::spawn(async move {
            axum::serve(listener, routes)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
        });
        let assignments = Arc::new(AtomicUsize::new(0));
        let fleet = Arc::new(AssignedFleet {
            target,
            store,
            delay: Duration::from_millis(assignment_ms),
            assigned: OnceCell::new(),
            calls: assignments.clone(),
        });
        let bucket = Arc::new(DelayedBucket {
            inner: FileBucket::new(directory.path().join("bucket"))?,
            snapshot_delay: Duration::from_millis(snapshot_ms),
            snapshots: Semaphore::new(usize::from(!blocked)),
            membership: Semaphore::new(usize::from(!unpublished)),
            registering: Semaphore::new(0),
            reject_snapshots: AtomicBool::new(false),
        });
        let runtime = Arc::new(RuntimeStorage::new(
            bucket.clone(),
            fleet.clone(),
            Arc::new(GrpcReplicaPeers::new(access.clone())?),
            access.clone(),
            "http://control".into(),
            Arc::new(SystemClock),
        )?);
        let initial_source = Arc::new(crate::bucket::access::RuntimeAccess::new(
            crate::bucket::access::BucketLocation::File {
                directory: directory.path().join("bucket"),
            },
            fleet,
            access,
            runtime.clone(),
        )?);
        let storage = Arc::new(HostStorage {
            observer: Arc::new(
                ControlPlaneClient::connect("http://127.0.0.1:1", "unavailable").await?,
            ),
            runtime,
            transport: Default::default(),
            stop: stop.clone(),
            host: scope.host.clone(),
            session: scope.session.clone(),
            region: scope.region.clone(),
            actor: Some(scope.actor.clone()),
            new_actor: true,
            owner_hint: None,
            activation: Mutex::new(None),
            fence: Mutex::new(LeaseFence::default()),
            lease: Mutex::new(None),
        });
        storage
            .register(&HostLeaseRequest {
                id: scope.host.clone(),
                session_id: scope.session.clone(),
                route: "http://primary".into(),
                duration_ms: 60_000,
            })
            .await?;
        let initial = crate::host::replication::InitialReplication::start(
            initial_source,
            scope.clone(),
            true,
            stop.clone(),
            storage.transport.clone(),
        );
        let writer = ActorReplication::start(storage.clone(), scope.clone(), stop.clone(), initial);
        Ok(Self {
            _directory: directory,
            _stop: stop.drop_guard(),
            bucket,
            storage,
            scope,
            writer,
            connections,
            connected,
            pause_connections,
            assignments,
        })
    }

    async fn write(&self) -> Result<StateWrite> {
        self.write_version(1).await
    }

    async fn write_version(&self, version: u64) -> Result<StateWrite> {
        let plan = self
            .storage
            .prepare_state_write(&self.scope.actor, &self.scope.host, 1, version - 1)
            .await?;
        let bytes = StateSnapshot::new(
            version,
            1,
            format!("write-{version}"),
            serde_json::json!({"count":version}),
            serde_json::json!(version),
        )?
        .encode()?;
        self.writer.write_snapshot(&plan, bytes).await
    }
}

#[tokio::test]
async fn connects_to_replicas_before_the_first_write_and_reuses_the_connection() -> Result<()> {
    let f = Fixture::new(0, 0, true).await?;
    tokio::time::timeout(Duration::from_secs(1), f.connected.acquire())
        .await
        .context("replica connection was not opened before the first write")??
        .forget();
    let result = tokio::time::timeout(Duration::from_secs(1), f.write()).await;
    f.bucket.snapshots.add_permits(1);
    assert_eq!(result??, StateWrite::Replicated);
    assert_eq!(f.connections.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn stalled_replica_preconnection_does_not_delay_membership_or_gcs() -> Result<()> {
    let f = Fixture::new(0, 0, false).await?;
    f.pause_connections.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(1), f.connected.acquire())
        .await??
        .forget();
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.storage.runtime.local_replica_members(&f.scope).is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), f.write()).await??,
        StateWrite::Written
    );
    Ok(())
}

#[tokio::test]
async fn replicas_ready_during_first_write_can_win() -> Result<()> {
    let f = Fixture::new(20, 0, true).await?;
    let result = tokio::time::timeout(Duration::from_secs(1), f.write()).await;
    f.bucket.snapshots.add_permits(1);
    assert_eq!(result??, StateWrite::Replicated);
    Ok(())
}

#[tokio::test]
async fn unpublished_replicas_cannot_acknowledge_a_write() -> Result<()> {
    let f = Fixture::with_membership(0, 0, true, true).await?;
    tokio::time::timeout(Duration::from_secs(1), f.bucket.registering.acquire())
        .await??
        .forget();
    tokio::time::timeout(Duration::from_secs(1), f.connected.acquire())
        .await
        .context("replica connection waited for membership publication")??
        .forget();
    let write = f.write();
    tokio::pin!(write);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut write)
            .await
            .is_err()
    );
    assert!(f.storage.runtime.local_replica_members(&f.scope).is_empty());
    f.bucket.membership.add_permits(1);
    let proof = tokio::time::timeout(Duration::from_secs(1), &mut write).await??;
    f.bucket.snapshots.add_permits(1);
    assert_eq!(proof, StateWrite::Replicated);
    assert_eq!(f.connections.load(Ordering::SeqCst), 1);
    assert_eq!(f.assignments.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn stalled_registration_does_not_delay_gcs() -> Result<()> {
    let f = Fixture::with_membership(0, 0, false, true).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), f.write()).await??,
        StateWrite::Written
    );
    assert!(f.storage.runtime.local_replica_members(&f.scope).is_empty());
    Ok(())
}

#[tokio::test]
async fn initial_replicas_accept_a_later_full_snapshot_and_recover_a_replica_only_commit()
-> Result<()> {
    let f = Fixture::with_membership(0, 0, false, true).await?;
    assert_eq!(f.write().await?, StateWrite::Written);
    f.bucket.reject_snapshots.store(true, Ordering::SeqCst);
    f.bucket.membership.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), f.write_version(2)).await??,
        StateWrite::Replicated
    );
    let plan = f
        .storage
        .prepare_state_write(&f.scope.actor, &f.scope.host, 1, 1)
        .await?;
    assert!(f.bucket.get(&plan.object_name).await?.is_none());
    f.storage
        .unregister(&f.scope.host, &f.scope.session)
        .await?;
    let owner_key = crate::storage_paths::owner(&f.scope.actor.storage_key())?;
    let owner: serde_json::Value =
        serde_json::from_slice(&f.bucket.get(&owner_key).await?.unwrap().bytes)?;
    assert_eq!(
        owner["sealed"], false,
        "failed uploads must retain crash recovery"
    );
    f.bucket.reject_snapshots.store(false, Ordering::SeqCst);
    f.storage.runtime.retire_replication(&f.scope).await?;
    let recovered = f
        .bucket
        .get(&plan.object_name)
        .await?
        .context("replica commit was not recovered")?;
    assert_eq!(StateSnapshot::decode(&recovered.bytes)?.state_version, 2);
    Ok(())
}

#[tokio::test]
#[ignore = "controlled latency benchmark; run with --ignored --nocapture"]
async fn benchmark_cold_write() -> Result<()> {
    for assignment_ms in [20, 80, 200] {
        let mut elapsed = Vec::new();
        let mut replica_wins = 0;
        for _ in 0..20 {
            let f = Fixture::new(assignment_ms, 120, false).await?;
            let started = Instant::now();
            replica_wins += usize::from(f.write().await? == StateWrite::Replicated);
            elapsed.push(started.elapsed().as_secs_f64() * 1000.0);
            tokio::time::sleep(Duration::from_millis(assignment_ms.max(120) + 25)).await;
        }
        elapsed.sort_by(f64::total_cmp);
        println!(
            "assignment_ms={assignment_ms} gcs_delay_ms=120 n=20 p50_ms={:.2} p95_ms={:.2} replica_wins={replica_wins}",
            elapsed[10], elapsed[18]
        );
    }
    Ok(())
}

#[tokio::test]
async fn clean_shutdown_waits_for_replica_acked_uploads_and_checkpoints_the_owner() -> Result<()> {
    use crate::placement::ObjectPlacementStore;
    let f = Fixture::new(0, 0, true).await?;
    assert_eq!(f.write().await?, StateWrite::Replicated);
    let mut shutdown = Box::pin(f.storage.unregister(&f.scope.host, &f.scope.session));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), shutdown.as_mut())
            .await
            .is_err(),
        "shutdown must drain the GCS upload before releasing ownership"
    );
    f.bucket.snapshots.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), shutdown).await??;
    let owner = f
        .storage
        .runtime
        .get_owner(&f.scope.actor.storage_key())
        .await?
        .unwrap();
    assert_eq!(owner.lease.expires_at_ms, 0);
    assert_eq!(owner.state_version, 1);
    let session_key = format!(
        "{}.json",
        crate::storage_paths::session(&f.scope.host, &f.scope.session)
    );
    let session: serde_json::Value =
        serde_json::from_slice(&f.bucket.get(&session_key).await?.unwrap().bytes)?;
    assert_eq!(session["state"], "Sealed");
    Ok(())
}
