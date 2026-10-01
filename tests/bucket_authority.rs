#[path = "fixtures/sqlite.rs"]
mod sqlite;
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
    actor::ActorKey, bucket::RuntimeStorage, placement::ObjectPlacementStore,
    state_log::StateSnapshot, state_transport::SnapshotWriter,
};

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
    let runtime = RuntimeStorage::new(authority, clock.clone())?;
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
async fn same_named_actors_in_different_projects_recover_independent_state() -> Result<()> {
    let bucket = Arc::new(MemoryBucket::default());
    let clock = Arc::new(TestClock(AtomicU64::new(1000)));
    let runtime = RuntimeStorage::new(bucket, clock.clone())?;
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
        let ticket = runtime
            .prepare_actor_write(&actor, &placement.lease, placement.owner_epoch, 1)
            .await?;
        let bytes = StateSnapshot::new(
            1,
            placement.owner_epoch,
            project.into(),
            sqlite::snapshot(serde_json::json!({"count": count}))?,
            serde_json::json!(count),
        )?
        .encode()?;
        runtime.write_snapshot(&ticket, bytes.clone()).await?;
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
