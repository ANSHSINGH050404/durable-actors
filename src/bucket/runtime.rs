use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::grpc::{
    proto,
    transport::{MAX_STORAGE_MESSAGE_BYTES, token, unavailable},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::Router;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tonic::{Request, Response, Status};

use crate::{
    actor::ActorKey,
    actor_state::ActorStorageKey,
    host::HostId,
    host_leases::{ActivationInventory, HostLease},
    placement::{
        ActorConnectionInventory, ActorInstanceOverview, ActorInventory, ActorInventoryReader,
        ActorResidency, ObjectPlacement, ObjectPlacementStore,
    },
    replication::{
        ReplicaAccess, ReplicaGrant, ReplicaProvisioner, ReplicaScope, ReplicaStream,
        ReplicaTarget, ReplicationTicket, SnapshotRef,
    },
    storage::{SnapshotReader, WritePlan, snapshot_object_name, snapshot_prefix},
};

use super::{Bucket, ReplicaPeers, peers::grant, replace};

mod activation;
mod history;
pub(crate) use history::ActorStateReader;
mod repair;
mod session;
mod startup;
pub use repair::ReplicaMembership;

#[cfg(test)]
#[path = "../../tests/unit/bucket/activation.rs"]
mod activation_tests;
#[cfg(test)]
#[path = "../../tests/unit/bucket/repair.rs"]
mod repair_tests;

pub struct RuntimeStorage {
    clock: Arc<dyn crate::clock::Clock>,
    owned: Mutex<HashMap<String, Ownership>>,
    uploads: tokio_util::task::TaskTracker,
    uploaded: Mutex<HashMap<String, UploadedSnapshots>>,
    sessions: Mutex<HashMap<String, Vec<ReplicaTarget>>>,
    authority: Arc<dyn Bucket>,
    fleet: Arc<dyn ReplicaProvisioner>,
    peers: Arc<dyn ReplicaPeers>,
    access: ReplicaAccess,
    origin: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Ownership {
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
    fn scope(&self) -> ReplicaScope {
        ReplicaScope {
            actor: self.actor.clone(),
            host: self.lease.id.clone(),
            session: self.lease.session_id.clone(),
            region: self.region.clone(),
        }
    }

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

    fn stream(&self) -> Result<ReplicaStream> {
        let name = snapshot_object_name(&self.actor, 1, &format!("{:032x}", self.epoch))?;
        Ok(ReplicaStream {
            session: session::identity(&self.lease.id, &self.lease.session_id),
            prefix: name.strip_suffix("1.json").unwrap().into(),
            owner_epoch: self.epoch,
            base_version: self.base.as_ref().map_or(0, |base| base.state_version),
        })
    }
}

impl RuntimeStorage {
    pub fn new(
        authority: Arc<dyn Bucket>,
        fleet: Arc<dyn ReplicaProvisioner>,
        peers: Arc<dyn ReplicaPeers>,
        access: ReplicaAccess,
        origin: String,
        clock: Arc<dyn crate::clock::Clock>,
    ) -> Result<Self> {
        ensure!(
            fleet.replica_regions().len() <= crate::replication::MAX_REPLICAS,
            "invalid runtime storage configuration"
        );
        Ok(Self {
            clock,
            owned: Mutex::new(HashMap::new()),
            uploads: tokio_util::task::TaskTracker::new(),
            uploaded: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            authority,
            fleet,
            peers,
            access,
            origin,
        })
    }

    pub fn router(self: Arc<Self>) -> Router {
        let service = RuntimeStorageService(self);
        tonic::service::Routes::from(Router::new())
            .add_service(
                proto::snapshot_service_server::SnapshotServiceServer::new(service)
                    .max_decoding_message_size(MAX_STORAGE_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_STORAGE_MESSAGE_BYTES),
            )
            .into_axum_router()
    }
}

#[async_trait]
impl ObjectPlacementStore for RuntimeStorage {
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
impl SnapshotReader for RuntimeStorage {
    async fn read_snapshot(&self, region: &str, object: &str) -> Result<Bytes> {
        self.fetch_snapshot(&grant("RUNTIME_READ", region, object, 60_000)?)
            .await
    }
}

impl RuntimeStorage {
    pub(crate) fn upload_tracker(&self) -> tokio_util::task::TaskTracker {
        self.uploads.clone()
    }

    pub async fn read_url(&self, region: &str, object: &str) -> Result<String> {
        self.access.url(
            &self.origin,
            &grant("RUNTIME_READ", region, object, 60_000)?,
        )
    }
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
        let replicas = self.local_replica_members(&record.scope());
        self.write_plan(&record, version, &replicas)
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

    fn write_plan(
        &self,
        record: &Ownership,
        version: u64,
        replicas: &[ReplicaTarget],
    ) -> Result<WritePlan> {
        ensure!(version > 0, "state version must be positive");
        let stream = record.stream()?;
        let mut capability = grant("APPEND", &record.region, &stream.prefix, 60_000)?;
        capability.stream = Some(stream.clone());
        let expires_at_ms = i64::try_from(capability.expires_at_ms)?;
        let targets = replicas
            .iter()
            .map(|peer| {
                Ok(ReplicaTarget {
                    host_id: peer.host_id.clone(),
                    region: peer.region.clone(),
                    url: self.access.url(
                        &peer.url,
                        &ReplicaGrant {
                            host_id: peer.host_id.clone(),
                            ..capability.clone()
                        },
                    )?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(WritePlan {
            state_version: version,
            object_name: stream.object(version),
            expires_at_ms,
            stream,
            replication: (!targets.is_empty()).then_some(ReplicationTicket { replicas: targets }),
        })
    }

    async fn latest(
        &self,
        record: &Ownership,
        known: Option<LoadedSnapshot>,
    ) -> Result<Option<LoadedSnapshot>> {
        let newest = self.latest_snapshot_key(record).await?;
        let replicas = self.replica_members(&record.scope()).await?;
        self.load_latest(record, known, newest, &replicas).await
    }

    async fn latest_snapshot_key(&self, record: &Ownership) -> Result<Option<String>> {
        Ok(self
            .authority
            .list(&record.stream()?.prefix)
            .await?
            .into_iter()
            .filter_map(|key| {
                snapshot_position(&key)
                    .filter(|(epoch, _)| *epoch == record.epoch)
                    .map(|position| (position, key))
            })
            .max_by_key(|(position, _)| *position)
            .map(|(_, key)| key))
    }

    async fn load_latest(
        &self,
        record: &Ownership,
        known: Option<LoadedSnapshot>,
        newest: Option<String>,
        replicas: &[ReplicaTarget],
    ) -> Result<Option<LoadedSnapshot>> {
        let stream = record.stream()?;
        let mut loaded = match newest {
            Some(key)
                if known.as_ref().is_some_and(|s| {
                    snapshot_position(&s.reference.object) >= snapshot_position(&key)
                }) =>
            {
                known
            }
            Some(key) => {
                let object = self
                    .authority
                    .get(&key)
                    .await?
                    .context("listed snapshot disappeared")?;
                Some(decode_snapshot(key, object.bytes)?)
            }
            None => known,
        };
        let mut candidate = record.base.clone();
        advance(&mut candidate, loaded.as_ref().map(|s| s.reference.clone()))?;
        let mut pending = JoinSet::new();
        for target in replicas {
            let (peers, target, stream) = (self.peers.clone(), target.clone(), stream.clone());
            pending.spawn(async move { peers.head(&target, &stream).await });
        }
        let mut witnesses = 0;
        while let Some(result) = pending.join_next().await {
            if let Ok(Ok(head)) = result {
                ensure!(head.stream == stream, "replica returned another stream");
                witnesses += 1;
                advance(&mut candidate, head.latest)?;
            }
        }
        ensure!(
            replicas.is_empty() || witnesses > 0,
            "no complete replica witness; refusing to lose acknowledged state"
        );
        if let Some(snapshot) = candidate
            && loaded.as_ref().is_none_or(|s| s.reference != snapshot)
        {
            let bytes = self.recover_snapshot(replicas, &snapshot).await?;
            loaded = Some(LoadedSnapshot {
                reference: snapshot,
                bytes: bytes.into(),
            });
        }
        Ok(loaded)
    }

    async fn recover_snapshot(
        &self,
        replicas: &[ReplicaTarget],
        snapshot: &SnapshotRef,
    ) -> Result<Vec<u8>> {
        if let Some(object) = self.authority.get(&snapshot.object).await? {
            snapshot.verify(&object.bytes)?;
            return Ok(object.bytes);
        }
        for peer in replicas {
            if let Ok(bytes) = self.peers.read(peer, &snapshot.object).await {
                snapshot.verify(&bytes)?;
                self.persist(&snapshot.object, bytes.clone()).await?;
                return Ok(bytes);
            }
        }
        anyhow::bail!("acknowledged snapshot is unavailable")
    }

    async fn persist(&self, object: &str, bytes: Vec<u8>) -> Result<()> {
        ensure!(
            replace(self.authority.as_ref(), object, None, bytes).await?,
            "conflicting immutable snapshot"
        );
        Ok(())
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
            .map(|value| Ok((value.generation, serde_json::from_slice(&value.bytes)?)))
            .transpose()
    }

    async fn fetch_snapshot(&self, grant: &ReplicaGrant) -> Result<Bytes> {
        if let Some(object) = self.authority.get(&grant.object).await? {
            return Ok(Bytes::from(object.bytes));
        }
        let actor = actor_from_object(&grant.object)?;
        let (_, record) = self
            .load(&actor.storage_key())
            .await?
            .context("missing ownership")?;
        let mut pending = JoinSet::new();
        for peer in self.replica_members(&record.scope()).await? {
            let peers = self.peers.clone();
            let object = grant.object.clone();
            pending.spawn(async move { peers.read(&peer, &object).await });
        }
        while let Some(result) = pending.join_next().await {
            if let Ok(Ok(bytes)) = result {
                return Ok(Bytes::from(bytes));
            }
        }
        anyhow::bail!("snapshot unavailable")
    }
}

#[derive(Clone)]
struct RuntimeStorageService(Arc<RuntimeStorage>);

#[tonic::async_trait]
impl proto::snapshot_service_server::SnapshotService for RuntimeStorageService {
    async fn read(
        &self,
        request: Request<proto::Empty>,
    ) -> Result<Response<proto::SnapshotData>, Status> {
        let grant = self
            .0
            .access
            .verify(token(&request)?, "RUNTIME_READ")
            .map_err(|_| Status::permission_denied("snapshot read capability rejected"))?;
        let data = self.0.fetch_snapshot(&grant).await.map_err(unavailable)?;
        Ok(Response::new(proto::SnapshotData {
            data: data.to_vec(),
        }))
    }

    async fn write(
        &self,
        _: Request<proto::SnapshotData>,
    ) -> Result<Response<proto::SnapshotWriteReply>, Status> {
        Err(Status::unimplemented(
            "write snapshots directly to the bucket",
        ))
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

fn actor_from_object(object: &str) -> Result<ActorKey> {
    crate::storage_paths::actor_from_snapshot(object)
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
impl ActorInventoryReader for RuntimeStorage {
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
