use super::*;
use crate::{
    bucket::{FileBucket, GrpcReplicaPeers},
    clock::SystemClock,
    host_leases::HostLeaseRequest,
    replication::{FileReplicaStore, ReplicaSet, ReplicaStore, replica_routes},
    state_log::StateSnapshot,
    state_transport::{GrpcStateTransport, StateTransport, StateWrite},
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::{CancellationToken, DropGuard};

struct AmbiguousBucket {
    inner: Arc<FileBucket>,
    lose_cas: AtomicBool,
    lose_confirmation: AtomicBool,
}

#[async_trait]
impl Bucket for AmbiguousBucket {
    async fn get(&self, key: &str) -> Result<Option<crate::bucket::BucketObject>> {
        ensure!(
            !self.lose_confirmation.swap(false, Ordering::SeqCst),
            "confirmation unavailable"
        );
        self.inner.get(key).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        let result = self.inner.compare_and_swap(key, generation, bytes).await?;
        if self.lose_cas.swap(false, Ordering::SeqCst) {
            self.lose_confirmation.store(true, Ordering::SeqCst);
            anyhow::bail!("CAS response lost");
        }
        Ok(result)
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }
}

struct Fixture {
    _directory: tempfile::TempDir,
    _stop: DropGuard,
    bucket: Arc<FileBucket>,
    authority: Arc<AmbiguousBucket>,
    runtime: Arc<RuntimeStorage>,
    scope: ReplicaScope,
    targets: Vec<ReplicaTarget>,
    stores: Vec<Arc<FileReplicaStore>>,
    latest: (WritePlan, Vec<u8>),
}

impl Fixture {
    async fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let bucket = Arc::new(FileBucket::new(directory.path().join("bucket"))?);
        let authority = Arc::new(AmbiguousBucket {
            inner: bucket.clone(),
            lose_cas: AtomicBool::new(false),
            lose_confirmation: AtomicBool::new(false),
        });
        let stop = CancellationToken::new();
        let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
        let scope = ReplicaScope {
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "repair".into(),
            },
            host: HostId::new("primary"),
            session: "session".into(),
            region: "us-east".into(),
        };
        let mut targets = Vec::new();
        let mut stores = Vec::new();
        for id in ["first", "failed", "replacement"] {
            let store = Arc::new(FileReplicaStore::open(directory.path().join(id), 4096).await?);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            targets.push(ReplicaTarget {
                host_id: id.into(),
                url: format!("http://{}", listener.local_addr()?),
                region: "us-east".into(),
            });
            let router = replica_routes(store.clone(), access.clone(), id.into(), scope.clone());
            let shutdown = stop.clone();
            tokio::spawn(async move {
                axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown.cancelled_owned())
                    .await
            });
            stores.push(store);
        }
        let runtime = Arc::new(RuntimeStorage::new(
            authority.clone(),
            Arc::new(ReplicaSet(targets[..2].to_vec())),
            Arc::new(GrpcReplicaPeers::new(access.clone())?),
            access,
            "http://control".into(),
            Arc::new(SystemClock),
        )?);
        let lease = runtime
            .register_activation(
                &scope.actor,
                &HostLeaseRequest {
                    id: scope.host.clone(),
                    session_id: scope.session.clone(),
                    route: "http://primary".into(),
                    duration_ms: 60_000,
                },
                &scope.region,
                true,
                None,
            )
            .await?
            .placement
            .lease;
        let membership = runtime
            .replace_replicas(
                &scope,
                targets[..2].to_vec(),
                None,
                &GrpcStateTransport::new(),
            )
            .await?;
        runtime.enable_replication(membership)?;
        let plan = runtime
            .prepare_actor_write(&scope.actor, &lease, 1, 1)
            .await?;
        let bytes = StateSnapshot::new(
            1,
            1,
            "committed".into(),
            serde_json::json!({"count":1}),
            serde_json::json!(1),
        )?
        .encode()?;
        for store in &stores[..2] {
            store.append(&plan.stream, &bytes).await?;
        }
        Ok(Self {
            _directory: directory,
            _stop: stop.drop_guard(),
            bucket,
            authority,
            runtime,
            scope,
            targets,
            stores,
            latest: (plan, bytes),
        })
    }

    fn replacements(&self) -> Vec<ReplicaTarget> {
        vec![self.targets[0].clone(), self.targets[2].clone()]
    }
}

struct SeedTransport {
    fail: bool,
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

#[tokio::test]
async fn initial_registration_is_create_only_and_reuses_published_members() -> Result<()> {
    let f = Fixture::new().await?;
    let mut scope = f.scope.clone();
    scope.session = "not-yet-activated".into();
    f.runtime
        .register_initial_replicas(&scope, f.targets[..2].to_vec())
        .await?;
    let membership = f
        .runtime
        .register_initial_replicas(&scope, f.replacements())
        .await?;
    assert_eq!(f.runtime.replica_members(&scope).await?, f.targets[..2]);
    assert!(
        f.runtime.enable_replication(membership).is_err(),
        "registration alone must not authorize a different activation"
    );
    Ok(())
}

#[tokio::test]
async fn recovery_tombstone_rejects_delayed_initial_registration() -> Result<()> {
    let f = Fixture::new().await?;
    let mut scope = f.scope.clone();
    scope.session = "never-published".into();
    f.runtime.retire_replication(&scope).await?;
    assert!(
        f.runtime
            .register_initial_replicas(&scope, f.targets[..2].to_vec())
            .await
            .is_err()
    );
    assert!(f.runtime.replica_members(&scope).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn uncertain_initial_registration_requires_confirmation() -> Result<()> {
    let f = Fixture::new().await?;
    let mut scope = f.scope.clone();
    scope.session = "uncertain-initial".into();
    f.authority.lose_cas.store(true, Ordering::SeqCst);
    assert!(
        f.runtime
            .register_initial_replicas(&scope, f.targets[..2].to_vec())
            .await
            .is_err()
    );
    assert!(f.runtime.local_replica_members(&scope).is_empty());
    f.runtime
        .register_initial_replicas(&scope, f.replacements())
        .await?;
    assert_eq!(f.runtime.replica_members(&scope).await?, f.targets[..2]);
    Ok(())
}

#[async_trait]
impl StateTransport for SeedTransport {
    async fn read(&self, _: &str) -> Result<Bytes> {
        anyhow::bail!("unused")
    }
    async fn write(&self, url: &str, bytes: Vec<u8>) -> Result<StateWrite> {
        ensure!(!self.fail, "replacement disk unavailable");
        let proof = GrpcStateTransport::new().write(url, bytes).await?;
        self.entered.add_permits(1);
        self.release.acquire().await?.forget();
        Ok(proof)
    }
}

#[tokio::test]
async fn replacement_cannot_become_a_recovery_witness_until_seeded() -> Result<()> {
    let f = Fixture::new().await?;
    let broken = SeedTransport {
        fail: true,
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    };
    assert!(
        f.runtime
            .replace_replicas(&f.scope, f.replacements(), Some(&f.latest), &broken)
            .await
            .is_err()
    );
    assert_eq!(f.runtime.replica_members(&f.scope).await?, f.targets[..2]);
    assert!(
        f.stores[2]
            .stream_head(&f.latest.0.stream)
            .await?
            .latest
            .is_none()
    );
    let membership = f
        .runtime
        .replace_replicas(
            &f.scope,
            f.replacements(),
            Some(&f.latest),
            &GrpcStateTransport::new(),
        )
        .await?;
    f.runtime.enable_replication(membership)?;
    assert_eq!(
        f.stores[2].read(&f.latest.0.object_name).await?,
        Some(f.latest.1.clone())
    );
    assert_eq!(f.runtime.replica_members(&f.scope).await?, f.replacements());
    let refreshed = f.runtime.current_write_plan(&f.latest.0).await?;
    assert_eq!(
        refreshed.replication.unwrap().replicas[1].host_id,
        "replacement"
    );
    f.runtime
        .release_activation(&f.scope.actor, &f.scope.host, &f.scope.session)
        .await?;
    f.runtime.retire_replication(&f.scope).await?;
    assert_eq!(
        f.bucket.get(&f.latest.0.object_name).await?.unwrap().bytes,
        f.latest.1
    );
    Ok(())
}

#[tokio::test]
async fn takeover_fences_a_replacement_that_finishes_seeding_late() -> Result<()> {
    let f = Fixture::new().await?;
    let transport = Arc::new(SeedTransport {
        fail: false,
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    let runtime = f.runtime.clone();
    let scope = f.scope.clone();
    let latest = f.latest.clone();
    let replicas = f.replacements();
    let seeded = transport.clone();
    let repair = tokio::spawn(async move {
        runtime
            .replace_replicas(&scope, replicas, Some(&latest), seeded.as_ref())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), transport.entered.acquire_many(2))
        .await??
        .forget();
    f.runtime
        .release_activation(&f.scope.actor, &f.scope.host, &f.scope.session)
        .await?;
    f.runtime.retire_replication(&f.scope).await?;
    transport.release.add_permits(2);
    assert!(repair.await?.is_err());
    assert!(f.runtime.replica_members(&f.scope).await?.is_empty());
    assert_eq!(
        f.bucket.get(&f.latest.0.object_name).await?.unwrap().bytes,
        f.latest.1
    );
    Ok(())
}

#[tokio::test]
async fn uncertain_membership_update_uses_gcs_until_the_record_is_reconciled() -> Result<()> {
    let f = Fixture::new().await?;
    f.runtime.suspend_replication(&f.scope);
    f.authority.lose_cas.store(true, Ordering::SeqCst);
    assert!(
        f.runtime
            .replace_replicas(
                &f.scope,
                f.replacements(),
                Some(&f.latest),
                &GrpcStateTransport::new()
            )
            .await
            .is_err()
    );
    assert!(
        f.runtime
            .current_write_plan(&f.latest.0)
            .await?
            .replication
            .is_none(),
        "an uncertain membership update must not acknowledge writes through the old set"
    );
    assert_eq!(f.runtime.replica_members(&f.scope).await?, f.replacements());
    let membership = f
        .runtime
        .replace_replicas(
            &f.scope,
            f.replacements(),
            Some(&f.latest),
            &GrpcStateTransport::new(),
        )
        .await?;
    f.runtime.enable_replication(membership)?;
    assert!(
        f.runtime
            .current_write_plan(&f.latest.0)
            .await?
            .replication
            .is_some()
    );
    Ok(())
}
