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
