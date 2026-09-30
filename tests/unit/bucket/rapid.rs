use super::*;
use crate::bucket::BucketSnapshots;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct Copy {
    _directory: tempfile::TempDir,
    store: BucketSnapshots,
    available: AtomicBool,
    writes: AtomicUsize,
    gate: Semaphore,
}

impl Copy {
    fn new(permits: usize) -> Arc<Self> {
        let directory = tempfile::tempdir().unwrap();
        let store = BucketSnapshots(Arc::new(
            crate::bucket::FileBucket::new(directory.path().into()).unwrap(),
        ));
        Arc::new(Self {
            _directory: directory,
            store,
            available: AtomicBool::new(true),
            writes: AtomicUsize::new(0),
            gate: Semaphore::new(permits),
        })
    }
    fn check(&self) -> Result<()> {
        ensure!(self.available.load(Ordering::SeqCst), "zone unavailable");
        Ok(())
    }
}

#[async_trait]
impl SnapshotStore for Copy {
    async fn put(&self, key: &str, bytes: Bytes) -> Result<()> {
        self.check()?;
        self.writes.fetch_add(1, Ordering::SeqCst);
        let _permit = self.gate.acquire().await?;
        self.store.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> Result<Option<Bytes>> {
        self.check()?;
        self.store.get(key).await
    }
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.check()?;
        self.store.list(prefix).await
    }
    async fn latest(&self, prefix: &str) -> Result<Option<(String, Bytes)>> {
        self.check()?;
        self.store.latest(prefix).await
    }
}

fn backend(archive: Arc<Copy>, zones: &[Arc<Copy>], acknowledgments: usize) -> RapidSnapshots {
    RapidSnapshots::new(
        archive,
        zones
            .iter()
            .map(|z| z.clone() as Arc<dyn SnapshotStore>)
            .collect(),
        acknowledgments,
    )
    .unwrap()
}

#[tokio::test]
async fn acknowledges_two_persisted_zones_without_waiting_for_standard_or_a_third_zone()
-> Result<()> {
    let archive = Copy::new(0);
    let zones = [Copy::new(1), Copy::new(1), Copy::new(0)];
    let store = backend(archive.clone(), &zones, 2);
    tokio::time::timeout(
        Duration::from_secs(1),
        store.put("epoch/1.json", Bytes::from_static(b"state")),
    )
    .await??;
    for zone in &zones[..2] {
        assert_eq!(
            zone.get("epoch/1.json").await?,
            Some(Bytes::from_static(b"state"))
        );
    }
    assert_eq!(archive.get("epoch/1.json").await?, None);
    archive.gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while archive.get("epoch/1.json").await.unwrap().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn fails_when_neither_standard_nor_the_rapid_quorum_persists() {
    let archive = Copy::new(1);
    let zones = [Copy::new(1), Copy::new(1)];
    zones[1].available.store(false, Ordering::SeqCst);
    archive.available.store(false, Ordering::SeqCst);
    let store = backend(archive.clone(), &zones, 2);
    assert!(
        store
            .put("epoch/1.json", Bytes::from_static(b"state"))
            .await
            .is_err()
    );
    assert_eq!(archive.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn standard_can_acknowledge_while_both_rapid_zones_are_stalled() -> Result<()> {
    let archive = Copy::new(1);
    let zones = [Copy::new(0), Copy::new(0)];
    let store = backend(archive.clone(), &zones, 2);
    tokio::time::timeout(
        Duration::from_secs(1),
        store.put("epoch/1.json", Bytes::from_static(b"state")),
    )
    .await??;
    assert_eq!(
        archive.get("epoch/1.json").await?,
        Some(Bytes::from_static(b"state"))
    );
    Ok(())
}

#[tokio::test]
async fn one_rapid_zone_waits_for_standard_or_the_remaining_zone() -> Result<()> {
    let archive = Copy::new(0);
    let zones = [Copy::new(1), Copy::new(0)];
    let store = backend(archive.clone(), &zones, 2);
    let write = store.put("epoch/1.json", Bytes::from_static(b"state"));
    tokio::pin!(write);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut write)
            .await
            .is_err()
    );
    assert_eq!(archive.writes.load(Ordering::SeqCst), 1);
    assert!(
        zones
            .iter()
            .all(|zone| zone.writes.load(Ordering::SeqCst) == 1)
    );
    archive.gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), write).await??;
    Ok(())
}

#[tokio::test]
async fn rapid_failure_still_allows_standard_to_finish() -> Result<()> {
    let archive = Copy::new(0);
    let zones = [Copy::new(1), Copy::new(1)];
    zones[1].available.store(false, Ordering::SeqCst);
    let store = backend(archive.clone(), &zones, 2);
    let write = store.put("epoch/1.json", Bytes::from_static(b"state"));
    tokio::pin!(write);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut write)
            .await
            .is_err()
    );
    archive.gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), write).await??;
    Ok(())
}

#[tokio::test]
async fn standard_failure_still_allows_the_rapid_quorum_to_finish() -> Result<()> {
    let archive = Copy::new(1);
    archive.available.store(false, Ordering::SeqCst);
    let zones = [Copy::new(1), Copy::new(1)];
    let store = backend(archive, &zones, 2);
    tokio::time::timeout(
        Duration::from_secs(1),
        store.put("epoch/1.json", Bytes::from_static(b"state")),
    )
    .await??;
    for zone in zones {
        assert_eq!(
            zone.get("epoch/1.json").await?,
            Some(Bytes::from_static(b"state"))
        );
    }
    Ok(())
}

#[tokio::test]
async fn recovery_intersects_the_acknowledged_zones_even_when_standard_lags() -> Result<()> {
    let archive = Copy::new(1);
    let zones = [Copy::new(1), Copy::new(1), Copy::new(1)];
    for zone in &zones[..2] {
        zone.put("epoch/9.json", Bytes::from_static(b"latest"))
            .await?;
    }
    zones[0].available.store(false, Ordering::SeqCst);
    let recovered = backend(archive, &zones, 2);
    assert_eq!(
        recovered.latest("epoch/").await?,
        Some(("epoch/9.json".into(), Bytes::from_static(b"latest")))
    );
    zones[1].available.store(false, Ordering::SeqCst);
    assert!(recovered.latest("epoch/").await.is_err());
    Ok(())
}

#[tokio::test]
async fn standard_recovers_snapshots_after_rapid_expiration() -> Result<()> {
    let archive = Copy::new(1);
    archive
        .put("epoch/12.json", Bytes::from_static(b"archived"))
        .await?;
    let recovered = backend(archive, &[Copy::new(1), Copy::new(1)], 2);
    assert_eq!(
        recovered.latest("epoch/").await?,
        Some(("epoch/12.json".into(), Bytes::from_static(b"archived")))
    );
    Ok(())
}

#[tokio::test]
async fn recovery_rejects_conflicting_copies() -> Result<()> {
    let zones = [Copy::new(1), Copy::new(1)];
    zones[0]
        .put("epoch/1.json", Bytes::from_static(b"one"))
        .await?;
    zones[1]
        .put("epoch/1.json", Bytes::from_static(b"two"))
        .await?;
    assert!(
        backend(Copy::new(1), &zones, 2)
            .latest("epoch/")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn recovery_does_not_treat_an_unavailable_archive_as_empty_history() {
    let archive = Copy::new(1);
    archive.available.store(false, Ordering::SeqCst);
    assert!(
        backend(archive, &[Copy::new(1), Copy::new(1)], 2)
            .latest("epoch/")
            .await
            .is_err()
    );
}

#[test]
fn physical_snapshot_names_are_flat_bounded_and_actor_scoped() -> Result<()> {
    let first = crate::actor::ActorKey {
        project_id: "tenant".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let mut second = first.clone();
    second.actor_id = "two".into();
    let prefix = crate::storage_paths::snapshots(&first)?;
    let other = crate::storage_paths::snapshots(&second)?;
    let key = format!("{prefix}00000000000000000000000000000001/42.json");
    let physical = object_name(&key)?;
    assert!(
        !physical.contains('/'),
        "Rapid writes must not create folders"
    );
    assert!(physical.len() < 200);
    assert!(physical.starts_with(&object_name(&prefix)?));
    assert!(!physical.starts_with(&object_name(&other)?));
    assert_eq!(
        physical
            .strip_prefix(&object_name(&prefix)?)
            .unwrap()
            .replace('~', "/"),
        "00000000000000000000000000000001/42.json"
    );
    Ok(())
}
