use super::*;
use crate::{
    bucket::BucketObject, replication::ReplicaSet, state_log::StateSnapshot,
    state_transport::SnapshotWriter,
};
use std::collections::HashMap;

#[derive(Default)]
struct MemoryBucket {
    objects: Mutex<HashMap<String, BucketObject>>,
    owner_reads: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl Bucket for MemoryBucket {
    async fn get(&self, key: &str) -> Result<Option<BucketObject>> {
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
    let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
    let runtime = Arc::new(RuntimeStorage::new(
        bucket.clone(),
        Arc::new(ReplicaSet(vec![])),
        Arc::new(GrpcReplicaPeers::new(access.clone())?),
        access,
        "http://control-plane-unavailable.invalid".into(),
        std::sync::Arc::new(crate::clock::SystemClock),
    )?);
    let host = HostId::new("host.v3.revision.host");
    let storage = HostStorage {
        observer: Arc::new(ControlPlaneClient::connect("http://127.0.0.1:1", "unavailable").await?),
        stop: CancellationToken::new(),
        runtime,
        transport: crate::state_transport::GrpcStateTransport::new(),
        host: host.clone(),
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
    };
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
        serde_json::json!({"count":1}),
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
