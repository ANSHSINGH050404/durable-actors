use super::super::{ReplicaProvider, store::Store};
use super::*;
use crate::{
    actor::ActorKey,
    bucket::{FileBucket, ReplicaPeers},
    control_plane::admin::{AdminRegistry, HostLaunchSpec, LocalAdminRegistry},
    host::HostId,
    host_leases::HostLeaseRequest,
    postgres::{PostgresDatabase, testing::with_postgres},
    replication::ReplicaAssignment,
    replication::{
        ReplicaAccess, ReplicaProvisioner, ReplicaScope, ReplicaSet, ReplicaStream, ReplicaTarget,
        SessionHead, StreamHead,
    },
    state_transport::GrpcStateTransport,
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

#[derive(Default)]
struct Provider {
    retired: Mutex<Vec<String>>,
    reject_retirement: AtomicBool,
    fail_next: AtomicBool,
    unavailable: Mutex<Option<String>>,
}

#[async_trait::async_trait]
impl ReplicaProvider for Provider {
    async fn ensure(
        &self,
        _: &str,
        region: &str,
        assignment: &ReplicaAssignment,
    ) -> Result<ReplicaTarget> {
        tokio::task::yield_now().await;
        if self.fail_next.swap(false, Ordering::SeqCst) {
            *self.unavailable.lock().unwrap() = Some(assignment.host_id.clone());
        }
        anyhow::ensure!(
            self.unavailable.lock().unwrap().as_ref() != Some(&assignment.host_id),
            "assigned spare became unreachable before its target was recorded"
        );
        Ok(ReplicaTarget {
            host_id: assignment.host_id.clone(),
            url: format!("https://{}.test", assignment.host_id),
            region: region.into(),
        })
    }
    async fn retire(&self, host: &str) -> Result<()> {
        anyhow::ensure!(
            !self.reject_retirement.load(Ordering::SeqCst),
            "provider unavailable"
        );
        self.retired.lock().unwrap().push(host.into());
        Ok(())
    }
}

#[tokio::test]
async fn an_unreachable_pending_assignment_is_replaced_after_controller_restart() -> Result<()> {
    with_postgres(async |database| {
        let db = PostgresDatabase::connect(&database.url).await?;
        let provider = Arc::new(Provider::default());
        provider.fail_next.store(true, Ordering::SeqCst);
        let registry = Arc::new(LocalAdminRegistry::default());
        registry
            .register_test_deployment(&HostLaunchSpec {
                sandboxes: Default::default(),
                project_id: "default".into(),
                source: None,
                image_ref: "image".into(),
                code_snapshot: None,
                working_directory: "/app".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
            })
            .await?;
        let fleet = || ActorReplicaFleet {
            provider: provider.clone(),
            registry: registry.clone(),
            store: Store(db.clone()),
            secret: "secret".into(),
            replica_regions: vec!["us-east".into(), "us-central".into()],
        };
        let scope = ReplicaScope {
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "pending".into(),
            },
            host: HostId::new("primary"),
            session: "session".into(),
            region: "us-east".into(),
        };
        assert!(fleet().ensure(&scope).await.is_err());
        let targets = fleet().ensure(&scope).await?;
        assert_eq!(targets.len(), 2);
        let failed = provider.unavailable.lock().unwrap().clone().unwrap();
        assert!(targets.iter().all(|target| target.host_id != failed));
        assert!(
            provider.retired.lock().unwrap().is_empty(),
            "pending instances remain for fenced lifecycle cleanup"
        );
        Ok(())
    })
    .await
}

struct Peers;
#[async_trait::async_trait]
impl ReplicaPeers for Peers {
    async fn initialize(&self, _: &ReplicaTarget, _: &str) -> Result<()> {
        Ok(())
    }
    async fn head(&self, _: &ReplicaTarget, stream: &ReplicaStream) -> Result<StreamHead> {
        Ok(StreamHead {
            stream: stream.clone(),
            latest: None,
        })
    }
    async fn seal(&self, _: &ReplicaTarget, session: &str) -> Result<SessionHead> {
        Ok(SessionHead {
            session: session.into(),
            initialized: true,
            sealed: true,
            streams: vec![],
        })
    }
    async fn read(&self, _: &ReplicaTarget, _: &str) -> Result<Vec<u8>> {
        anyhow::bail!("unused")
    }
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn repair_survives_controller_restart_and_cleanup_waits_for_membership_switch() -> Result<()>
{
    with_postgres(async |database| {
        let db = PostgresDatabase::connect(&database.url).await?;
        let provider = Arc::new(Provider::default());
        let registry = Arc::new(LocalAdminRegistry::default());
        registry.register_test_deployment(&HostLaunchSpec {
    sandboxes: Default::default(),
    project_id: "default".into(), source: None, image_ref: "image".into(), code_snapshot: None, working_directory: "/app".into(), actor_entrypoint: None, secret_refs: vec![] }).await?;
        let fleet = || ActorReplicaFleet { provider: provider.clone(), registry: registry.clone(), store: Store(db.clone()), secret: "secret".into(), replica_regions: vec!["us-east".into(), "us-central".into()] };
        let original = fleet();
        let scope = ReplicaScope { actor: ActorKey { project_id: "default".into(), actor_name: "Counter".into(), actor_id: "repair".into() }, host: HostId::new("primary"), session: "session".into(), region: "us-east".into() };
        let first = original.ensure(&scope).await?;
        let restarted = fleet();
        let failed = [first[0].host_id.clone()];
        let (left, right) = tokio::join!(original.repair(&scope, &failed), restarted.repair(&scope, &failed));
        let replacement = left?;
        assert_eq!(replacement, right?);
        assert_eq!(replacement.iter().filter(|r| first.contains(r)).count(), 1);

        let directory = tempfile::tempdir()?;
        let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
        let storage = RuntimeStorage::new(Arc::new(FileBucket::new(directory.path().into())?), Arc::new(ReplicaSet(first.clone())), Arc::new(Peers), access, "http://control".into(), Arc::new(SystemClock))?;
        let _lease = storage.register_activation(&scope.actor, &HostLeaseRequest { id: scope.host.clone(), session_id: scope.session.clone(), route: "http://primary".into(), duration_ms: 60_000 }, &scope.region, true, None).await?.placement.lease;
        let initial = storage.replace_replicas(&scope, first.clone(), None, &GrpcStateTransport::new()).await?;
        storage.enable_replication(initial)?;
        let group = restarted.store.prepare(&scope, &restarted.replica_regions, "image", &[]).await?;
        restarted.retire_superseded(&storage, &group).await?;
        assert!(provider.retired.lock().unwrap().is_empty(), "old witnesses must survive an unfinished replacement");
        storage.replace_replicas(&scope, replacement, None, &GrpcStateTransport::new()).await?;
        restarted.retire_superseded(&storage, &group).await?;
        assert_eq!(provider.retired.lock().unwrap().len(), 1);
        assert!(restarted.store.retire(&scope.identity()).await.is_err(), "provisioning grace prevents cleanup racing an in-flight provider call");

        storage.release_activation(&scope.actor, &scope.host, &scope.session).await?;
        db.execute("UPDATE durable_actors_replica_groups SET updated_at = clock_timestamp() - interval '121 seconds'", &[]).await?;
        provider.reject_retirement.store(true, Ordering::SeqCst);
        restarted.reconcile(&storage).await?;
        assert_eq!(restarted.store.candidates().await?.len(), 1, "failed provider cleanup must remain retryable after controller restart");
        provider.reject_retirement.store(false, Ordering::SeqCst);
        fleet().reconcile(&storage).await?;
        assert!(restarted.store.candidates().await?.is_empty());
        assert_eq!(provider.retired.lock().unwrap().len(), 3);
        Ok(())
    }).await
}
