use super::*;
use crate::{
    bucket::{BucketObject, FileBucket},
    postgres::testing::with_postgres,
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

struct Nodes {
    deleted: Mutex<Vec<String>>,
}
#[async_trait]
impl ReplicaPods for Nodes {
    async fn ensure(&self, pod: &PodRecord, _: Option<&str>, _: &[String]) -> Result<PodRecord> {
        let mut pod = pod.clone();
        pod.uid = Some(format!("uid-{}", pod.name));
        pod.node = Some(format!("node-{}", pod.name));
        pod.placement = Some(ReplicaPlacement {
            id: pod.name.clone(),
            address: format!("http://{}:7200", pod.name),
            zone: pod.zone.clone(),
        });
        Ok(pod)
    }
    async fn health(&self, _: &PodRecord, _: &PodInventory) -> Result<PodHealth> {
        Ok(PodHealth {
            live: true,
            draining: false,
            current_image: true,
        })
    }
    async fn observed(&self) -> Result<PodInventory> {
        Ok(PodInventory::new())
    }
    async fn protect(&self, _: &PodRecord, _: &str) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, pod: &PodRecord) -> Result<()> {
        self.deleted.lock().unwrap().push(pod.name.clone());
        Ok(())
    }
}
struct ArchiveBucket {
    disk: FileBucket,
    fail: AtomicBool,
}
#[async_trait]
impl Bucket for ArchiveBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
        self.disk.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.disk.list(prefix).await
    }
    async fn compare_and_swap(
        &self,
        key: &str,
        generation: Option<i64>,
        bytes: Vec<u8>,
    ) -> Result<bool> {
        ensure!(!self.fail.load(Ordering::SeqCst), "archive unavailable");
        self.disk.compare_and_swap(key, generation, bytes).await
    }
}
struct Peers {
    archive: Archive,
    copies: Mutex<std::collections::HashMap<String, Option<Bytes>>>,
}
#[async_trait]
impl ReplicaPeers for Peers {
    async fn assign(&self, replica: &ReplicaPlacement, _: &Assignment) -> Result<()> {
        self.copies
            .lock()
            .unwrap()
            .entry(replica.id.clone())
            .or_insert(None);
        Ok(())
    }
    async fn seal(
        &self,
        replica: &ReplicaPlacement,
        prefix: &str,
    ) -> Result<Option<(String, Bytes)>> {
        let bytes = self
            .copies
            .lock()
            .unwrap()
            .get(&replica.id)
            .cloned()
            .context("original disk lost")?;
        Ok(bytes.map(|bytes| (format!("{prefix}1.json"), bytes)))
    }
    async fn flush(&self, replica: &ReplicaPlacement, prefix: &str) -> Result<()> {
        let bytes = self
            .copies
            .lock()
            .unwrap()
            .get(&replica.id)
            .cloned()
            .flatten()
            .context("no state")?;
        self.archive
            .write(&super::super::record::Batch {
                prefix: prefix.into(),
                records: vec![super::super::record::Record::encode(1, &bytes, None)?],
            })
            .await?;
        Ok(())
    }
}
struct Time;
impl Clock for Time {
    fn now_ms(&self) -> Result<u64> {
        Ok(1000)
    }
}

#[tokio::test]
async fn retirement_keeps_original_copies_until_final_archive_is_verified_and_survives_restart()
-> Result<()> {
    with_postgres(async |db| {
        let dir = tempfile::tempdir()?;
        let authority = Arc::new(FileBucket::new(dir.path().join("owner"))?);
        let archive = Arc::new(ArchiveBucket { disk: FileBucket::new(dir.path().join("archive"))?, fail: AtomicBool::new(true) });
        let nodes = Arc::new(Nodes { deleted: Mutex::new(vec![]) });
        let peers = Arc::new(Peers { archive: Archive(archive.clone()), copies: Mutex::new(Default::default()) });
        let fleet = ReplicaFleet {
            registry: Registry::new(PostgresDatabase::lazy(&db.url)?), pods: nodes.clone(), peers: peers.clone(),
            authority: Authority::new(authority.clone(),Arc::new(Time)), archive: Archive(archive.clone()), zones: vec!["us-west4-a".into();3], idle:0, max_starting:3,
        };
        let actor = crate::actor::ActorKey { project_id:"p".into(),actor_name:"Counter".into(),actor_id:"one".into() };
        let prefix = format!("{}{:032x}/",crate::storage_paths::snapshots(&actor)?,1);
        authority.compare_and_swap(&crate::storage_paths::owner(&actor.storage_key())?,None,serde_json::to_vec(&serde_json::json!({"actor":actor,"epoch":1,"sealed":false,"lease":{"id":"host","session_id":"session","route":"http://host","expires_at_ms":100000}}))?).await?;
        let group = fleet.prepare(&prefix).await?;
        assert_eq!(group.replicas.len(),3);
        let bytes: Bytes = crate::state_log::StateSnapshot::new(1,1,"write".into(),serde_json::json!({"count":42}),serde_json::json!(42))?.encode()?.into();
        for copy in peers.copies.lock().unwrap().values_mut() { *copy = Some(bytes.clone()); }
        peers.copies.lock().unwrap().remove(&group.replicas[0].id);
        assert!(fleet.finish(&prefix).await.is_err());
        assert_eq!(fleet.registry.lookup(&prefix).await?.state,"closing");
        assert!(nodes.deleted.lock().unwrap().is_empty());
        archive.fail.store(false,Ordering::SeqCst);
        let restored = ReplicaFleet { registry: Registry::new(PostgresDatabase::lazy(&db.url)?), ..fleet };
        restored.finish(&prefix).await?;
        restored.reconcile_group(&prefix, &PodInventory::new()).await?;
        assert_eq!(nodes.deleted.lock().unwrap().len(),3);
        let snapshots = super::super::directory::DedicatedSnapshots::new(Arc::new(restored),"unused-after-archive".into())?;
        assert_eq!(snapshots.latest(&prefix).await?.unwrap().1,bytes);
        Ok(())
    }).await
}

#[tokio::test]
async fn losing_all_original_copies_never_turns_an_old_archive_into_a_final_checkpoint()
-> Result<()> {
    with_postgres(async |db| {
        let dir = tempfile::tempdir()?;
        let bucket = Arc::new(FileBucket::new(dir.path().join("gcs"))?);
        let peers = Arc::new(Peers {
            archive: Archive(bucket.clone()),
            copies: Mutex::new(Default::default()),
        });
        let registry = Registry::new(PostgresDatabase::lazy(&db.url)?);
        let prefix = format!(
            "{}{:032x}/",
            crate::storage_paths::snapshots(&crate::actor::ActorKey {
                project_id: "p".into(),
                actor_name: "a".into(),
                actor_id: "i".into()
            })?,
            1
        );
        let nodes = Arc::new(Nodes {
            deleted: Mutex::new(vec![]),
        });
        let group = registry.claim(&prefix, &["us-west4-a".into()]).await?;
        let pod = nodes.ensure(&group.pods[0], Some(&prefix), &[]).await?;
        registry.update_pod(&pod).await?;
        registry.ready(&prefix).await?;
        let fleet = ReplicaFleet {
            registry,
            pods: nodes.clone(),
            peers,
            authority: Authority::new(bucket.clone(), Arc::new(Time)),
            archive: Archive(bucket),
            zones: vec!["us-west4-a".into()],
            idle: 0,
            max_starting: 1,
        };
        assert!(fleet.finish(&prefix).await.is_err());
        assert!(!fleet.group(&prefix).await?.archived);
        assert!(nodes.deleted.lock().unwrap().is_empty());
        Ok(())
    })
    .await
}

#[test]
fn archived_state_uses_compact_binary_encoding_on_the_directory_protocol() -> Result<()> {
    let reply: DirectoryReply =
        serde_json::from_value(serde_json::json!({"groups":[],"keys":[],"data":"AAEC/w=="}))?;
    assert_eq!(serde_json::to_value(reply)?["data"], "AAEC/w==");
    Ok(())
}

struct ActivationOwner {
    value: Mutex<serde_json::Value>,
    reads: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Bucket for ActivationOwner {
    async fn get(&self, _: &str) -> Result<Option<BucketObject>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(Some(BucketObject {
            generation: 1,
            bytes: serde_json::to_vec(&*self.value.lock().unwrap())?,
        }))
    }
    async fn list(&self, _: &str) -> Result<Vec<String>> {
        anyhow::bail!("unexpected list")
    }
    async fn compare_and_swap(&self, _: &str, _: Option<i64>, _: Vec<u8>) -> Result<bool> {
        anyhow::bail!("unexpected write")
    }
}

#[derive(Clone, Copy)]
enum DuringAssignment {
    Nothing,
    Fence,
    Expire,
    Close,
    Fail,
}
struct ActivationPeers {
    owner: Arc<ActivationOwner>,
    registry: Registry,
    action: DuringAssignment,
    assigned: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl ReplicaPeers for ActivationPeers {
    async fn assign(&self, _: &ReplicaPlacement, assignment: &Assignment) -> Result<()> {
        let assigned = self.assigned.fetch_add(1, Ordering::SeqCst);
        match self.action {
            DuringAssignment::Nothing => {}
            DuringAssignment::Fence => self.owner.value.lock().unwrap()["epoch"] = 2.into(),
            DuringAssignment::Expire => {
                self.owner.value.lock().unwrap()["lease"]["expires_at_ms"] = 0.into()
            }
            DuringAssignment::Close => {
                self.registry.close(&assignment.prefix).await?;
            }
            DuringAssignment::Fail if assigned > 0 => anyhow::bail!("replica unavailable"),
            DuringAssignment::Fail => {}
        }
        Ok(())
    }
    async fn seal(&self, _: &ReplicaPlacement, _: &str) -> Result<Option<(String, Bytes)>> {
        anyhow::bail!("unexpected seal")
    }
    async fn flush(&self, _: &ReplicaPlacement, _: &str) -> Result<()> {
        anyhow::bail!("unexpected flush")
    }
}

struct ActivationFixture {
    fleet: ReplicaFleet,
    principal: crate::control_plane::ActorPrincipal,
    owner: Arc<ActivationOwner>,
    peers: Arc<ActivationPeers>,
    prefix: String,
}
impl ActivationFixture {
    fn new(url: &str, action: DuringAssignment) -> Result<Self> {
        let actor = crate::actor::ActorKey {
            project_id: "p".into(),
            actor_name: "Counter".into(),
            actor_id: "activation".into(),
        };
        let prefix = format!("{}{:032x}/", crate::storage_paths::snapshots(&actor)?, 1);
        let owner = Arc::new(ActivationOwner {
            value: Mutex::new(
                serde_json::json!({"actor":actor,"epoch":1,"sealed":false,"lease":{"id":"host","session_id":"session","route":"http://host","expires_at_ms":100000}}),
            ),
            reads: 0.into(),
        });
        let registry = Registry::new(PostgresDatabase::lazy(url)?);
        let peers = Arc::new(ActivationPeers {
            owner: owner.clone(),
            registry: registry.clone(),
            action,
            assigned: 0.into(),
        });
        let fleet = ReplicaFleet {
            registry,
            pods: Arc::new(Nodes {
                deleted: Mutex::new(vec![]),
            }),
            peers: peers.clone(),
            authority: Authority::new(owner.clone(), Arc::new(Time)),
            archive: Archive(owner.clone()),
            zones: vec!["us-west4-a".into(); 3],
            idle: 0,
            max_starting: 3,
        };
        let principal = crate::control_plane::ActorPrincipal {
            actor,
            host_id: crate::host::HostId::new("host"),
            session_id: "session".into(),
            region: "north-america-west".into(),
            host_config_key: None,
            invocation: None,
        };
        Ok(Self {
            fleet,
            principal,
            owner,
            peers,
            prefix,
        })
    }
    async fn warm_spares(&self) -> Result<()> {
        self.fleet
            .registry
            .reserve_spares(&self.fleet.zones, 3, 3)
            .await?;
        for pod in self.fleet.registry.unassigned().await? {
            let ready = self.fleet.pods.ensure(&pod, None, &[]).await?;
            self.fleet.registry.update_pod(&ready).await?;
        }
        Ok(())
    }
    async fn activate(&self) -> Result<DirectoryReply> {
        let command = DirectoryCommand::Prepare {
            prefix: self.prefix.clone(),
        };
        self.fleet.execute_for(&self.principal, command).await
    }
}

#[tokio::test]
async fn activation_authorizes_once_and_rechecks_ownership_after_assigning_all_replicas()
-> Result<()> {
    with_postgres(async |db| {
        let fixture = ActivationFixture::new(&db.url, DuringAssignment::Nothing)?;
        let reply = fixture.activate().await?;
        assert_eq!(reply.groups[0].replicas.len(), 3);
        assert_eq!(fixture.peers.assigned.load(Ordering::SeqCst), 3);
        assert_eq!(fixture.owner.reads.load(Ordering::SeqCst), 2);
        assert!(
            fixture
                .fleet
                .registry
                .lookup(&fixture.prefix)
                .await?
                .ever_ready
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn activation_rejects_foreign_actor_host_and_session_before_claiming_replicas() -> Result<()>
{
    with_postgres(async |db| {
        for kind in ["actor", "host", "session"] {
            let mut fixture = ActivationFixture::new(&db.url, DuringAssignment::Nothing)?;
            match kind {
                "actor" => fixture.principal.actor.actor_id = "other".into(),
                "host" => fixture.principal.host_id = crate::host::HostId::new("other"),
                _ => fixture.principal.session_id = "other".into(),
            }
            assert!(fixture.activate().await.is_err());
            assert!(
                fixture
                    .fleet
                    .registry
                    .lookup(&fixture.prefix)
                    .await
                    .is_err()
            );
            assert_eq!(fixture.peers.assigned.load(Ordering::SeqCst), 0);
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn activation_never_publishes_ready_after_fencing_expiry_closure_or_partial_assignment()
-> Result<()> {
    for action in [
        DuringAssignment::Fence,
        DuringAssignment::Expire,
        DuringAssignment::Close,
        DuringAssignment::Fail,
    ] {
        with_postgres(async |db| {
            let fixture = ActivationFixture::new(&db.url, action)?;
            fixture.warm_spares().await?;
            assert!(fixture.activate().await.is_err());
            assert!(fixture.peers.assigned.load(Ordering::SeqCst) > 0);
            if matches!(action, DuringAssignment::Fail) {
                assert_eq!(fixture.peers.assigned.load(Ordering::SeqCst), 2);
            }
            let group = fixture.fleet.registry.lookup(&fixture.prefix).await?;
            assert!(!group.ever_ready);
            assert_ne!(group.state, "ready");
            Ok(())
        })
        .await?;
    }
    Ok(())
}

#[tokio::test]
async fn preparing_an_unchanged_claimed_pod_needs_no_database_connection() -> Result<()> {
    with_postgres(async |db| {
        let fixture = ActivationFixture::new(&db.url, DuringAssignment::Nothing)?;
        let claimed = fixture
            .fleet
            .registry
            .claim(&fixture.prefix, &fixture.fleet.zones)
            .await?;
        let pod = fixture
            .fleet
            .pods
            .ensure(&claimed.pods[0], Some(&fixture.prefix), &[])
            .await?;
        fixture.fleet.registry.update_pod(&pod).await?;
        let database = PostgresDatabase::connect(&db.url).await?;
        let mut occupied = Vec::new();
        for _ in 0..8 {
            occupied.push(database.connection().await?);
        }
        let fleet = ReplicaFleet {
            registry: Registry::new(database),
            ..fixture.fleet
        };
        let prepared = tokio::time::timeout(
            Duration::from_secs(1),
            fleet.prepare_pod(&pod, &fixture.prefix, &[]),
        )
        .await??;
        assert_eq!(prepared, pod);
        Ok(())
    })
    .await
}
