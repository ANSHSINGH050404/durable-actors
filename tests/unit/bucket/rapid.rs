use super::*;
use crate::{
    actor::ActorKey,
    bucket::{BucketSnapshots, FileBucket},
    state_log::StateSnapshot,
    storage::StateStream,
};
use anyhow::ensure;
use std::{
    collections::BTreeMap,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

#[tokio::test]
async fn hot_writes_reuse_streams_and_a_fresh_reader_discovers_acknowledged_state() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    for version in 1..=3 {
        store
            .put(&f.stream.object(version), state(version, 1)?)
            .await?;
    }
    assert!(
        f.zones
            .iter()
            .all(|zone| zone.opens.load(Ordering::SeqCst) == 1)
    );
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(3), state(3, 1)?))
    );
    assert_eq!(
        f.store()?.get(&f.stream.object(2)).await?,
        Some(state(2, 1)?)
    );
    Ok(())
}

fn state(version: u64, epoch: u64) -> Result<Bytes> {
    Ok(StateSnapshot::new(
        version,
        epoch,
        format!("request-{version}"),
        serde_json::json!({"count":version}),
        serde_json::json!(version),
    )?
    .encode()?
    .into())
}

#[tokio::test]
async fn actor_assignment_opens_both_connections_before_activation_and_reuses_them() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    let actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    store.prepare(&actor)?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while f
            .zones
            .iter()
            .any(|zone| zone.opens.load(Ordering::SeqCst) != 1)
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(f.archive.list("").await?.is_empty());
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    assert!(
        f.zones
            .iter()
            .all(|zone| zone.opens.load(Ordering::SeqCst) == 1)
    );
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn prepared_connections_cannot_be_reassigned_to_another_actor() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    let mut actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    actor.actor_id = "different".into();
    store.prepare(&actor)?;
    store.start(&f.stream).await?;
    assert!(store.put(&f.stream.object(1), state(1, 1)?).await.is_err());
    assert!(store.finish(&f.stream).await.is_err());
    assert!(f.archive.list("").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn failed_manifest_publication_prevents_log_writes() -> Result<()> {
    struct UnavailableArchive;
    #[async_trait]
    impl Bucket for UnavailableArchive {
        async fn get(&self, _key: &str) -> Result<Option<crate::bucket::BucketObject>> {
            anyhow::bail!("archive unavailable")
        }
        async fn list(&self, _prefix: &str) -> Result<Vec<String>> {
            anyhow::bail!("archive unavailable")
        }
        async fn compare_and_swap(
            &self,
            _key: &str,
            _generation: Option<i64>,
            _bytes: Vec<u8>,
        ) -> Result<bool> {
            anyhow::bail!("archive unavailable")
        }
    }
    let f = Fixture::new()?;
    let store = RapidSnapshots::new(
        Arc::new(UnavailableArchive),
        Arc::new(BucketSnapshots(f.archive.clone())),
        f.zones
            .iter()
            .map(|zone| zone.clone() as Arc<dyn LogZone>)
            .collect(),
        CancellationToken::new(),
    )?;
    store.start(&f.stream).await?;
    assert!(store.put(&f.stream.object(1), state(1, 1)?).await.is_err());
    assert!(store.finish(&f.stream).await.is_err());
    assert!(f.zones.iter().all(|zone| {
        zone.objects
            .lock()
            .unwrap()
            .values()
            .all(|object| object.bytes.is_empty())
    }));
    Ok(())
}

#[tokio::test]
async fn runtime_crash_recovery_advances_ownership_before_enabling_new_writes() -> Result<()> {
    use crate::{
        bucket::{PersistenceConfig, RapidBucket, RuntimeStorage},
        clock::Clock,
        host::HostId,
        host_leases::HostLeaseRequest,
        state_transport::SnapshotWriter,
    };
    struct TestClock(AtomicU64);
    impl Clock for TestClock {
        fn now_ms(&self) -> Result<u64> {
            Ok(self.0.load(Ordering::SeqCst))
        }
    }
    let f = Fixture::new()?;
    let clock = Arc::new(TestClock(AtomicU64::new(1)));
    let config = PersistenceConfig::Rapid {
        archive_bucket: "archive".into(),
        buckets: vec![
            RapidBucket {
                bucket: "a".into(),
                zone: "us-west4-a".into(),
            },
            RapidBucket {
                bucket: "b".into(),
                zone: "us-west4-b".into(),
            },
        ],
    };
    let actor = crate::storage_paths::actor_from_snapshot(&f.stream.object(1))?;
    let runtime = || {
        RuntimeStorage::new(f.archive.clone(), clock.clone())?
            .with_persistence(config.clone(), Arc::new(f.store()?))
    };
    let first = runtime()?;
    let request = |name: &str| HostLeaseRequest {
        id: HostId::new(name),
        session_id: name.into(),
        route: format!("http://{name}"),
        duration_ms: 1000,
    };
    let active = first
        .register_activation(&actor, &request("first"), "us-west", true, None)
        .await?;
    let plan = first
        .prepare_actor_write(&actor, &active.placement.lease, 1, 1)
        .await?;
    first.write_snapshot(&plan, state(1, 1)?.to_vec()).await?;
    assert!(
        runtime()?
            .register_activation(&actor, &request("early"), "us-west", false, None)
            .await
            .is_err()
    );
    first
        .write_snapshot(
            &first
                .prepare_actor_write(&actor, &active.placement.lease, 1, 2)
                .await?,
            state(2, 1)?.to_vec(),
        )
        .await?;
    clock.0.store(2000, Ordering::SeqCst);
    f.zones[1].offline.store(true, Ordering::SeqCst);
    let second = runtime()?;
    let recovered = second
        .register_activation(&actor, &request("second"), "us-west", false, None)
        .await?;
    assert_eq!(recovered.placement.owner_epoch, 2);
    assert_eq!(recovered.state, Some(state(2, 1)?));
    assert!(
        first
            .write_snapshot(
                &first
                    .prepare_actor_write(&actor, &active.placement.lease, 1, 3)
                    .await?,
                state(3, 1)?.to_vec()
            )
            .await
            .is_err()
    );
    let plan = second
        .prepare_actor_write(&actor, &recovered.placement.lease, 2, 3)
        .await?;
    second.write_snapshot(&plan, state(3, 2)?.to_vec()).await?;
    second
        .finish_activation(
            &actor,
            &recovered.placement.owner,
            &recovered.placement.lease.session_id,
        )
        .await?;
    let third = runtime()?
        .register_activation(&actor, &request("third"), "us-west", false, None)
        .await?;
    assert_eq!(third.placement.owner_epoch, 3);
    assert_eq!(third.state, Some(state(3, 2)?));
    Ok(())
}

#[tokio::test]
async fn segment_rotation_preserves_all_versions_and_reclaims_archived_replicas() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    for version in 1..=10 {
        let snapshot = StateSnapshot::new(
            version,
            1,
            format!("request-{version}"),
            serde_json::json!({"data":"x".repeat(1_000_000)}),
            serde_json::json!(version),
        )?;
        store
            .put(&f.stream.object(version), snapshot.encode()?.into())
            .await?;
    }
    assert_eq!(f.zones[0].opens.load(Ordering::SeqCst), 2);
    assert_eq!(f.zones[0].objects.lock().unwrap().len(), 1);
    assert_eq!(f.store()?.list(&f.stream.prefix).await?.len(), 10);
    store.finish(&f.stream).await?;
    assert_eq!(f.store()?.list(&f.stream.prefix).await?.len(), 10);
    Ok(())
}

#[test]
fn lifecycle_rules_must_exclude_unarchived_logs() -> Result<()> {
    use google_cloud_storage::model::bucket::{
        Lifecycle,
        lifecycle::{
            Rule,
            rule::{Action, Condition},
        },
    };
    let bucket = |prefixes: Vec<&str>| {
        google_cloud_storage::model::Bucket::new().set_lifecycle(
            Lifecycle::new().set_rule([Rule::new()
                .set_action(Action::new().set_type("Delete"))
                .set_condition(
                    Condition::new()
                        .set_age_days(7)
                        .set_matches_prefix(prefixes),
                )]),
        )
    };
    super::gcs::validate_retention(&bucket(vec![
        "durable-actors-v3-snapshots-",
        "durable-actors-v3-uploads-",
    ]))?;
    assert!(super::gcs::validate_retention(&bucket(vec![])).is_err());
    assert!(super::gcs::validate_retention(&bucket(vec!["durable-actors-v3-"])).is_err());
    assert!(
        super::gcs::validate_retention(&bucket(vec!["durable-actors-v3-logs-tenant"])).is_err()
    );
    Ok(())
}

#[tokio::test]
async fn oversized_states_use_standard_and_preserve_existing_log_history() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    let bytes: Bytes = StateSnapshot::new(
        2,
        1,
        "large".into(),
        serde_json::json!({"data":"x".repeat(5*1024*1024)}),
        serde_json::Value::Null,
    )?
    .encode()?
    .into();
    store.put(&f.stream.object(2), bytes.clone()).await?;
    store.finish(&f.stream).await?;
    assert_eq!(f.store()?.get(&f.stream.object(2)).await?, Some(bytes));
    assert_eq!(
        f.store()?.get(&f.stream.object(1)).await?,
        Some(state(1, 1)?)
    );
    Ok(())
}

#[tokio::test]
async fn failed_archival_never_deletes_a_durable_rapid_copy() -> Result<()> {
    struct RejectArchive(Arc<FileBucket>);
    #[async_trait]
    impl Bucket for RejectArchive {
        async fn get(&self, key: &str) -> Result<Option<crate::bucket::BucketObject>> {
            self.0.get(key).await
        }
        async fn list(&self, prefix: &str) -> Result<Vec<String>> {
            self.0.list(prefix).await
        }
        async fn compare_and_swap(
            &self,
            key: &str,
            generation: Option<i64>,
            bytes: Vec<u8>,
        ) -> Result<bool> {
            ensure!(!key.ends_with(".segment"), "archive unavailable");
            self.0.compare_and_swap(key, generation, bytes).await
        }
    }
    let f = Fixture::new()?;
    let store = RapidSnapshots::new(
        Arc::new(RejectArchive(f.archive.clone())),
        Arc::new(BucketSnapshots(f.archive.clone())),
        f.zones
            .iter()
            .map(|z| z.clone() as Arc<dyn LogZone>)
            .collect(),
        tokio_util::sync::CancellationToken::new(),
    )?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    assert!(store.finish(&f.stream).await.is_err());
    assert!(
        f.zones
            .iter()
            .all(|z| !z.objects.lock().unwrap().is_empty())
    );
    assert_eq!(
        f.store()?.recover(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn takeover_with_one_zone_unavailable_fences_old_streams_and_checkpoints_state() -> Result<()>
{
    let f = Fixture::new()?;
    let old = f.store()?;
    old.start(&f.stream).await?;
    old.put(&f.stream.object(1), state(1, 1)?).await?;
    f.zones[1].offline.store(true, Ordering::SeqCst);
    let restored = f.store()?.recover(&f.stream.prefix).await?;
    assert_eq!(restored, Some((f.stream.object(1), state(1, 1)?)));
    assert!(old.put(&f.stream.object(2), state(2, 1)?).await.is_err());
    assert_eq!(
        f.archive.get(&f.stream.object(1)).await?.unwrap().bytes,
        state(1, 1)?
    );
    Ok(())
}

#[tokio::test]
async fn shutdown_archives_history_before_deleting_rapid_replicas() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    for v in 1..=3 {
        store.put(&f.stream.object(v), state(v, 1)?).await?;
    }
    store.finish(&f.stream).await?;
    assert!(f.zones.iter().all(|z| z.objects.lock().unwrap().is_empty()));
    for zone in &f.zones {
        zone.offline.store(true, Ordering::SeqCst);
    }
    let reader = f.store()?;
    assert_eq!(
        reader.latest(&f.stream.prefix).await?,
        Some((f.stream.object(3), state(3, 1)?))
    );
    assert_eq!(reader.list(&f.stream.prefix).await?.len(), 3);
    assert_eq!(reader.get(&f.stream.object(1)).await?, Some(state(1, 1)?));
    Ok(())
}

#[tokio::test]
async fn a_failed_flush_poisons_the_session_and_prevents_clean_shutdown() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    f.zones[1].offline.store(true, Ordering::SeqCst);
    assert!(store.put(&f.stream.object(2), state(2, 1)?).await.is_err());
    f.zones[1].offline.store(false, Ordering::SeqCst);
    assert!(store.put(&f.stream.object(3), state(3, 1)?).await.is_err());
    assert!(store.finish(&f.stream).await.is_err());
    assert!(
        f.zones
            .iter()
            .all(|z| !z.objects.lock().unwrap().is_empty())
    );
    Ok(())
}

#[tokio::test]
async fn recovery_ignores_incomplete_tail_but_rejects_corrupt_complete_records() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    f.zones[0]
        .objects
        .lock()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .bytes
        .extend_from_slice(b"RLG1partial");
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    f.zones[0]
        .objects
        .lock()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()
        .bytes[48] ^= 1;
    assert!(f.store()?.latest(&f.stream.prefix).await.is_err());
    Ok(())
}

#[tokio::test]
async fn unavailable_rapid_at_activation_uses_standard_without_losing_state() -> Result<()> {
    let f = Fixture::new()?;
    f.zones[0].offline.store(true, Ordering::SeqCst);
    let store = f.store()?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    store.finish(&f.stream).await?;
    assert_eq!(
        f.archive.get(&f.stream.object(1)).await?.unwrap().bytes,
        state(1, 1)?
    );
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn writes_require_an_activated_epoch_and_consecutive_versions() -> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    assert!(store.put(&f.stream.object(1), state(1, 1)?).await.is_err());
    store.start(&f.stream).await?;
    assert!(store.put(&f.stream.object(2), state(2, 1)?).await.is_err());
    assert!(store.put(&f.stream.object(1), state(1, 2)?).await.is_err());
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    assert_eq!(f.zones[0].opens.load(Ordering::SeqCst), 1);
    Ok(())
}

struct Fixture {
    _directory: tempfile::TempDir,
    archive: Arc<FileBucket>,
    zones: Vec<Arc<MemoryZone>>,
    stream: StateStream,
}

#[tokio::test]
async fn activation_does_not_wait_for_the_manifest_but_writes_require_it() -> Result<()> {
    struct GatedManifest {
        inner: Arc<FileBucket>,
        release: tokio::sync::Semaphore,
    }
    #[async_trait]
    impl Bucket for GatedManifest {
        async fn get(&self, key: &str) -> Result<Option<crate::bucket::BucketObject>> {
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
            if key.ends_with(".manifest") {
                self.release.acquire().await?.forget();
            }
            self.inner.compare_and_swap(key, generation, bytes).await
        }
    }
    let f = Fixture::new()?;
    let archive = Arc::new(GatedManifest {
        inner: f.archive.clone(),
        release: tokio::sync::Semaphore::new(0),
    });
    let store = Arc::new(RapidSnapshots::new(
        archive.clone(),
        Arc::new(BucketSnapshots(f.archive.clone())),
        f.zones
            .iter()
            .map(|z| z.clone() as Arc<dyn LogZone>)
            .collect(),
        tokio_util::sync::CancellationToken::new(),
    )?);
    tokio::time::timeout(Duration::from_millis(30), store.start(&f.stream)).await??;
    let writer = store.clone();
    let object = f.stream.object(1);
    let pending = tokio::spawn(async move { writer.put(&object, state(1, 1)?).await });
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!pending.is_finished());
    assert!(f.zones.iter().all(|z| {
        z.objects
            .lock()
            .unwrap()
            .values()
            .all(|object| object.bytes.is_empty())
    }));
    archive.release.add_permits(1);
    pending.await??;
    assert_eq!(
        f.store()?.latest(&f.stream.prefix).await?,
        Some((f.stream.object(1), state(1, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn cancellation_during_flush_fences_subsequent_writes_and_preserves_an_uncertain_tail()
-> Result<()> {
    let f = Fixture::new()?;
    let store = f.store()?;
    store.start(&f.stream).await?;
    store.put(&f.stream.object(1), state(1, 1)?).await?;
    f.zones[1].stalled.store(true, Ordering::SeqCst);
    let object = f.stream.object(2);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), store.put(&object, state(2, 1)?))
            .await
            .is_err()
    );
    f.zones[1].stalled.store(false, Ordering::SeqCst);
    assert!(store.put(&object, state(2, 1)?).await.is_err());
    assert!(store.finish(&f.stream).await.is_err());
    assert_eq!(
        f.store()?.recover(&f.stream.prefix).await?,
        Some((object, state(2, 1)?))
    );
    Ok(())
}

#[tokio::test]
async fn manifests_cannot_redirect_a_reader_to_another_actors_log() -> Result<()> {
    let f = Fixture::new()?;
    let first = f.store()?;
    first.start(&f.stream).await?;
    first.put(&f.stream.object(1), state(1, 1)?).await?;
    let actor = ActorKey {
        project_id: "log-tests".into(),
        actor_name: "Counter".into(),
        actor_id: "other".into(),
    };
    let object = crate::storage::snapshot_object_name(&actor, 1, &format!("{:032x}", 1))?;
    let other_stream = StateStream {
        prefix: object.strip_suffix("1.json").unwrap().into(),
        ..f.stream.clone()
    };
    let other = f.store()?;
    other.start(&other_stream).await?;
    other.put(&object, state(1, 1)?).await?;
    let manifests = |stream: &StateStream| crate::bucket::rapid::object_name(&stream.prefix);
    let left = f.archive.list(&manifests(&f.stream)?).await?.pop().unwrap();
    let right = f
        .archive
        .list(&manifests(&other_stream)?)
        .await?
        .pop()
        .unwrap();
    let stored = f.archive.get(&left).await?.unwrap();
    let mut manifest: Manifest = serde_json::from_slice(&stored.bytes)?;
    let foreign: Manifest = serde_json::from_slice(&f.archive.get(&right).await?.unwrap().bytes)?;
    manifest.replicas = foreign.replicas;
    assert!(
        f.archive
            .compare_and_swap(
                &left,
                Some(stored.generation),
                serde_json::to_vec(&manifest)?
            )
            .await?
    );
    assert!(f.store()?.latest(&f.stream.prefix).await.is_err());
    Ok(())
}
impl Fixture {
    fn new() -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let archive = Arc::new(FileBucket::new(directory.path().into())?);
        let actor = ActorKey {
            project_id: "log-tests".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        let object = crate::storage::snapshot_object_name(&actor, 1, &format!("{:032x}", 1))?;
        Ok(Self {
            _directory: directory,
            archive,
            zones: vec![
                Arc::new(MemoryZone::new("a")),
                Arc::new(MemoryZone::new("b")),
            ],
            stream: StateStream {
                session: "session".into(),
                prefix: object.strip_suffix("1.json").unwrap().into(),
                owner_epoch: 1,
                base_version: 0,
            },
        })
    }
    fn store(&self) -> Result<RapidSnapshots> {
        RapidSnapshots::new(
            self.archive.clone(),
            Arc::new(BucketSnapshots(self.archive.clone())),
            self.zones
                .iter()
                .map(|z| z.clone() as Arc<dyn LogZone>)
                .collect(),
            tokio_util::sync::CancellationToken::new(),
        )
    }
}

#[derive(Default)]
struct MemoryObject {
    fence: u64,
    bytes: Vec<u8>,
}
struct MemoryZone {
    name: String,
    objects: Arc<Mutex<BTreeMap<String, MemoryObject>>>,
    offline: Arc<AtomicBool>,
    stalled: Arc<AtomicBool>,
    opens: AtomicU64,
}
impl MemoryZone {
    fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            objects: Default::default(),
            offline: Default::default(),
            stalled: Default::default(),
            opens: AtomicU64::new(0),
        }
    }
}
#[async_trait]
impl LogZone for MemoryZone {
    fn bucket(&self) -> &str {
        &self.name
    }
    async fn open(&self, object: &str) -> Result<(Replica, Box<dyn LogWriter>)> {
        ensure!(!self.offline.load(Ordering::SeqCst), "zone offline");
        let mut objects = self.objects.lock().unwrap();
        ensure!(!objects.contains_key(object), "already exists");
        objects.insert(object.into(), MemoryObject::default());
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok((
            Replica {
                bucket: self.name.clone(),
                object: object.into(),
                generation: 1,
            },
            Box::new(MemoryWriter {
                objects: self.objects.clone(),
                offline: self.offline.clone(),
                stalled: self.stalled.clone(),
                object: object.into(),
                fence: 0,
            }),
        ))
    }
    async fn read(&self, replica: &Replica, fence: bool) -> Result<Bytes> {
        ensure!(!self.offline.load(Ordering::SeqCst), "zone offline");
        let mut objects = self.objects.lock().unwrap();
        let object = objects
            .get_mut(&replica.object)
            .ok_or_else(|| anyhow::anyhow!("missing replica"))?;
        if fence {
            object.fence += 1;
        }
        Ok(Bytes::copy_from_slice(&object.bytes))
    }
    async fn delete(&self, replica: &Replica) -> Result<()> {
        ensure!(!self.offline.load(Ordering::SeqCst), "zone offline");
        self.objects.lock().unwrap().remove(&replica.object);
        Ok(())
    }
}
struct MemoryWriter {
    objects: Arc<Mutex<BTreeMap<String, MemoryObject>>>,
    offline: Arc<AtomicBool>,
    stalled: Arc<AtomicBool>,
    object: String,
    fence: u64,
}
#[async_trait]
impl LogWriter for MemoryWriter {
    async fn append_and_flush(&mut self, bytes: Bytes) -> Result<u64> {
        ensure!(!self.offline.load(Ordering::SeqCst), "zone offline");
        if self.stalled.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let mut objects = self.objects.lock().unwrap();
        let object = objects
            .get_mut(&self.object)
            .ok_or_else(|| anyhow::anyhow!("missing replica"))?;
        ensure!(object.fence == self.fence, "stream fenced");
        object.bytes.extend_from_slice(&bytes);
        Ok(object.bytes.len() as u64)
    }
}
