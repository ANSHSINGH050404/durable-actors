use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{
    actor::ActorKey,
    actor_state::ActorStorageKey,
    host::HostId,
    host_leases::{ActivationInventory, HostLease},
    placement::{
        ActorConnectionInventory, ActorInstanceOverview, ActorInventory, ActorInventoryReader,
        ActorResidency, ObjectPlacement, ObjectPlacementStore,
    },
    storage::{SnapshotReader, SnapshotRef, StateStream, WritePlan, snapshot_object_name},
};

use super::{Bucket, replace};

mod activation;
mod history;
pub(crate) use history::ActorStateReader;
mod session;

#[cfg(test)]
#[path = "../../tests/unit/bucket/activation.rs"]
mod activation_tests;
pub struct RuntimeStorage {
    reader: RuntimeStorageReader,
    owned: Mutex<HashMap<String, Ownership>>,
    uploaded: Mutex<HashMap<String, UploadedSnapshots>>,
}

pub struct RuntimeStorageReader {
    clock: Arc<dyn crate::clock::Clock>,
    authority: Arc<dyn Bucket>,
    snapshots: Arc<dyn super::SnapshotStore>,
    pub(crate) persistence: super::PersistenceConfig,
}

#[derive(Clone, Serialize, Deserialize)]
struct Ownership {
    persistence: super::PersistenceConfig,
    #[serde(default)]
    sealed: bool,
    inventory: ActivationInventory,
    lease: HostLease,
    mutation: String,
    actor: ActorKey,
    epoch: u64,
    region: String,
    base: Option<SnapshotRef>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct OwnershipHint {
    generation: i64,
    record: Ownership,
}

pub struct LoadedActor {
    pub placement: ObjectPlacement,
    pub state: Option<Bytes>,
}

#[derive(Default)]
struct UploadedSnapshots {
    started: u64,
    completed: u64,
    latest: Option<SnapshotRef>,
}

struct SessionCheckpoint {
    snapshot: Option<SnapshotRef>,
}

struct LoadedSnapshot {
    reference: SnapshotRef,
    bytes: Bytes,
}

impl Ownership {
    fn placement(&self) -> ObjectPlacement {
        let mut placement = ObjectPlacement {
            lease: self.lease.clone(),
            object: self.actor.storage_key(),
            owner: self.lease.id.clone(),
            owner_epoch: self.epoch,
            home_region: self.region.clone(),
            state_version: 0,
            state_object: None,
            last_request_id: None,
        };
        apply_snapshot(&mut placement, self.base.as_ref());
        placement
    }

    fn stream(&self) -> Result<StateStream> {
        let name = snapshot_object_name(&self.actor, 1, &format!("{:032x}", self.epoch))?;
        Ok(StateStream {
            session: self.lease.session_id.clone(),
            prefix: name.strip_suffix("1.json").unwrap().into(),
            owner_epoch: self.epoch,
            base_version: self.base.as_ref().map_or(0, |base| base.state_version),
        })
    }
}

impl RuntimeStorage {
    pub fn new(authority: Arc<dyn Bucket>, clock: Arc<dyn crate::clock::Clock>) -> Result<Self> {
        Ok(Self {
            reader: RuntimeStorageReader::new(authority, clock)?,
            owned: Mutex::new(HashMap::new()),
            uploaded: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn with_persistence(
        mut self,
        config: super::PersistenceConfig,
        snapshots: Arc<dyn super::SnapshotStore>,
    ) -> Result<Self> {
        self.reader = self.reader.with_persistence(config, snapshots)?;
        Ok(self)
    }
}

impl std::ops::Deref for RuntimeStorage {
    type Target = RuntimeStorageReader;
    fn deref(&self) -> &Self::Target {
        &self.reader
    }
}

impl RuntimeStorageReader {
    pub fn new(authority: Arc<dyn Bucket>, clock: Arc<dyn crate::clock::Clock>) -> Result<Self> {
        Ok(Self {
            clock,
            snapshots: Arc::new(super::BucketSnapshots(authority.clone())),
            persistence: super::PersistenceConfig::Local,
            authority,
        })
    }

    pub(crate) fn with_persistence(
        mut self,
        config: super::PersistenceConfig,
        snapshots: Arc<dyn super::SnapshotStore>,
    ) -> Result<Self> {
        config.validate()?;
        self.persistence = config;
        self.snapshots = snapshots;
        Ok(self)
    }
}

#[async_trait]
impl ObjectPlacementStore for RuntimeStorageReader {
    async fn get_owner_with_hint(
        &self,
        object: &ActorStorageKey,
    ) -> Result<(Option<ObjectPlacement>, Option<OwnershipHint>)> {
        Ok(match self.load(object).await? {
            Some((generation, record)) => (
                Some(record.placement()),
                Some(OwnershipHint { generation, record }),
            ),
            None => (None, None),
        })
    }
    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        match self.load(object).await? {
            Some((_, record)) => Ok(Some(self.current_placement(&record).await?)),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl SnapshotReader for RuntimeStorageReader {
    async fn read_snapshot(&self, _region: &str, object: &str) -> Result<Bytes> {
        self.read_persisted(object)
            .await?
            .context("snapshot unavailable")
    }
}

impl RuntimeStorage {
    pub(crate) async fn activate_actor(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        region: &str,
    ) -> Result<LoadedActor> {
        let (_, record) = self
            .load(&actor.storage_key())
            .await?
            .context("actor has no ownership")?;
        ensure!(
            record.actor == *actor
                && record.region == region
                && record.lease.id == lease.id
                && record.lease.session_id == lease.session_id,
            "actor ownership changed"
        );
        self.load_actor(record).await
    }

    pub(crate) async fn load_owned_actor(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        epoch: u64,
    ) -> Result<LoadedActor> {
        let (_, record) = self
            .load(&actor.storage_key())
            .await?
            .context("actor has no ownership")?;
        ensure!(
            record.lease.id == lease.id
                && record.lease.session_id == lease.session_id
                && record.epoch == epoch,
            "actor ownership changed"
        );
        self.load_actor(record).await
    }

    pub async fn prepare_actor_write(
        &self,
        actor: &ActorKey,
        lease: &HostLease,
        epoch: u64,
        version: u64,
    ) -> Result<WritePlan> {
        let record = self
            .owned
            .lock()
            .unwrap()
            .get(actor.storage_key().as_str())
            .cloned()
            .context("actor is not locally activated")?;
        ensure!(
            record.lease.id == lease.id
                && record.lease.session_id == lease.session_id
                && record.epoch == epoch,
            "actor ownership changed"
        );
        self.write_plan(&record, version)
    }

    async fn load_actor(&self, record: Ownership) -> Result<LoadedActor> {
        let snapshot = self.latest(&record, None).await?;
        Ok(self.remember(record, snapshot))
    }

    fn remember(&self, record: Ownership, snapshot: Option<LoadedSnapshot>) -> LoadedActor {
        let mut placement = record.placement();
        apply_snapshot(&mut placement, snapshot.as_ref().map(|s| &s.reference));
        self.owned
            .lock()
            .unwrap()
            .insert(record.actor.storage_key().as_str().into(), record);
        LoadedActor {
            placement,
            state: snapshot.map(|s| s.bytes),
        }
    }

    fn write_plan(&self, record: &Ownership, version: u64) -> Result<WritePlan> {
        ensure!(version > 0, "state version must be positive");
        let stream = record.stream()?;
        Ok(WritePlan {
            state_version: version,
            object_name: stream.object(version),
            stream,
        })
    }
}

impl RuntimeStorageReader {
    async fn latest(
        &self,
        record: &Ownership,
        known: Option<LoadedSnapshot>,
    ) -> Result<Option<LoadedSnapshot>> {
        let latest = self
            .snapshots
            .latest(&record.stream()?.prefix)
            .await?
            .map(|(key, bytes)| decode_snapshot(key, bytes.to_vec()))
            .transpose()?;
        self.load_latest(record, latest.or(known)).await
    }

    async fn load_latest(
        &self,
        record: &Ownership,
        mut loaded: Option<LoadedSnapshot>,
    ) -> Result<Option<LoadedSnapshot>> {
        let mut candidate = record.base.clone();
        advance(&mut candidate, loaded.as_ref().map(|s| s.reference.clone()))?;
        if let Some(snapshot) = candidate
            && loaded.as_ref().is_none_or(|s| s.reference != snapshot)
        {
            let bytes = self
                .read_persisted(&snapshot.object)
                .await?
                .context("acknowledged snapshot is unavailable")?;
            snapshot.verify(&bytes)?;
            loaded = Some(LoadedSnapshot {
                reference: snapshot,
                bytes,
            });
        }
        Ok(loaded)
    }

    async fn persist(&self, object: &str, bytes: Vec<u8>) -> Result<()> {
        self.snapshots.put(object, bytes.into()).await
    }

    async fn read_persisted(&self, object: &str) -> Result<Option<Bytes>> {
        self.snapshots.get(object).await
    }

    async fn current_placement(&self, record: &Ownership) -> Result<ObjectPlacement> {
        let mut placement = record.placement();
        apply_snapshot(
            &mut placement,
            self.latest(record, None)
                .await?
                .as_ref()
                .map(|s| &s.reference),
        );
        Ok(placement)
    }

    async fn load(&self, object: &ActorStorageKey) -> Result<Option<(i64, Ownership)>> {
        self.authority
            .get(&ownership_key(object)?)
            .await?
            .map(|value| {
                let record: Ownership = serde_json::from_slice(&value.bytes)?;
                ensure!(record.persistence == self.persistence, "actor persistence configuration changed; an explicit state migration is required");
                Ok((value.generation, record))
            })
            .transpose()
    }
}

#[async_trait]
impl ObjectPlacementStore for RuntimeStorage {
    async fn get_owner_with_hint(
        &self,
        object: &ActorStorageKey,
    ) -> Result<(Option<ObjectPlacement>, Option<OwnershipHint>)> {
        self.reader.get_owner_with_hint(object).await
    }
    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        self.reader.get(object).await
    }
}
#[async_trait]
impl SnapshotReader for RuntimeStorage {
    async fn read_snapshot(&self, region: &str, object: &str) -> Result<Bytes> {
        self.reader.read_snapshot(region, object).await
    }
}
#[async_trait]
impl ActorInventoryReader for RuntimeStorage {
    async fn actor_inventory(&self, project: &str) -> Result<Vec<ActorInventory>> {
        self.reader.actor_inventory(project).await
    }
}

fn ownership_key(object: &ActorStorageKey) -> Result<String> {
    crate::storage_paths::owner(object)
}

fn apply_snapshot(placement: &mut ObjectPlacement, snapshot: Option<&SnapshotRef>) {
    if let Some(snapshot) = snapshot {
        placement.state_version = snapshot.state_version;
        placement.state_object = Some(snapshot.object.clone());
        placement.last_request_id = Some(snapshot.request_id.clone());
    }
}

fn advance(current: &mut Option<SnapshotRef>, candidate: Option<SnapshotRef>) -> Result<()> {
    if let Some(candidate) = candidate {
        if let Some(current) = current.as_ref() {
            ensure!(
                snapshot_position(&current.object) != snapshot_position(&candidate.object)
                    || current == &candidate,
                "conflicting snapshot witnesses"
            );
        }
        if current.as_ref().is_none_or(|current| {
            snapshot_position(&candidate.object) > snapshot_position(&current.object)
        }) {
            *current = Some(candidate);
        }
    }
    Ok(())
}

fn snapshot_position(object: &str) -> Option<(u64, u64)> {
    let mut parts = object.rsplit('/');
    let version = parts.next()?.strip_suffix(".json")?.parse().ok()?;
    let epoch = u64::from_str_radix(parts.next()?, 16).ok()?;
    Some((epoch, version))
}

fn decode_snapshot(object: String, bytes: Vec<u8>) -> Result<LoadedSnapshot> {
    let snapshot = crate::state_log::StateSnapshot::decode(&bytes)?;
    ensure!(
        snapshot_position(&object) == Some((snapshot.owner_epoch, snapshot.state_version)),
        "snapshot identity does not match its object"
    );
    Ok(LoadedSnapshot {
        reference: SnapshotRef::new(object, &snapshot, &bytes),
        bytes: bytes.into(),
    })
}

#[async_trait]
impl crate::state_transport::SnapshotWriter for RuntimeStorage {
    async fn write_snapshot(
        &self,
        plan: &WritePlan,
        bytes: Vec<u8>,
    ) -> Result<crate::state_transport::StateWrite> {
        let stream = &plan.stream;
        let snapshot = stream.snapshot(&bytes)?;
        ensure!(
            snapshot.object == plan.object_name && snapshot.state_version == plan.state_version,
            "write plan does not match snapshot"
        );
        self.uploaded
            .lock()
            .unwrap()
            .entry(stream.session.clone())
            .or_default()
            .started += 1;
        self.persist(&snapshot.object, bytes).await?;
        let mut uploaded = self.uploaded.lock().unwrap();
        let uploaded = uploaded.entry(stream.session.clone()).or_default();
        advance(&mut uploaded.latest, Some(snapshot))?;
        uploaded.completed += 1;
        Ok(crate::state_transport::StateWrite::Written)
    }
}

#[async_trait]
impl ActorInventoryReader for RuntimeStorageReader {
    async fn actor_inventory(&self, project_id: &str) -> Result<Vec<ActorInventory>> {
        let mut actors = std::collections::BTreeMap::new();
        let prefix = format!("{}owners/", crate::storage_paths::ROOT);
        for key in self.authority.list(&prefix).await? {
            let Some(object) = self.authority.get(&key).await? else {
                continue;
            };
            let record: Ownership = serde_json::from_slice(&object.bytes)?;
            if record.actor.project_id != project_id {
                continue;
            }
            let row = actors
                .entry(record.actor.actor_name.clone())
                .or_insert_with(|| ActorInventory {
                    actor_name: record.actor.actor_name.clone(),
                    ..Default::default()
                });
            let instance = actor_instance_overview(&record, self.clock.now_ms()?);
            match instance.status {
                ActorResidency::Live => row.live += 1,
                ActorResidency::Dormant => row.dormant += 1,
                ActorResidency::Unknown => row.unknown += 1,
            }
            row.instances.push(instance);
        }
        Ok(actors
            .into_values()
            .map(|mut actor| {
                actor
                    .instances
                    .sort_by(|left, right| left.actor_id.cmp(&right.actor_id));
                actor
            })
            .collect())
    }
}

fn actor_instance_overview(record: &Ownership, now: u64) -> ActorInstanceOverview {
    if record.lease.expires_at_ms <= now {
        return ActorInstanceOverview {
            actor_id: record.actor.actor_id.clone(),
            status: ActorResidency::Dormant,
            connections: vec![],
            waiting: Some(vec![]),
        };
    }
    ActorInstanceOverview {
        actor_id: record.actor.actor_id.clone(),
        status: match record.inventory.resident {
            Some(true) => ActorResidency::Live,
            Some(false) => ActorResidency::Dormant,
            None => ActorResidency::Unknown,
        },
        connections: record
            .inventory
            .connections
            .iter()
            .map(|connection| ActorConnectionInventory {
                id: connection.id.clone(),
                metadata: connection.metadata.clone(),
            })
            .collect(),
        waiting: record.inventory.waiting.clone(),
    }
}
