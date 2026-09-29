use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, stream};

use super::{
    Assignment,
    archive::Archive,
    client::{Command, ReplicaClient, position},
    directory::{DirectoryCommand, DirectoryReply, Group, ReplicaDirectory},
};
use crate::{
    bucket::{Bucket, ReplicaPlacement, SnapshotStore},
    clock::Clock,
    postgres::PostgresDatabase,
};

mod authority;
mod kubernetes;
mod registry;
use authority::Authority;
pub(crate) use kubernetes::{KubernetesReplicas, ReplicaPodConfig};
pub(crate) use registry::Checkpoint;
use registry::{GroupRecord, PodRecord, Registry};

pub(crate) struct ReplicaFleet {
    registry: Registry,
    pods: Arc<dyn ReplicaPods>,
    peers: Arc<dyn ReplicaPeers>,
    authority: Authority,
    archive: Archive,
    zones: Vec<String>,
    idle: usize,
    max_starting: usize,
}

#[derive(Clone, Copy)]
struct PodHealth {
    live: bool,
    draining: bool,
    current_image: bool,
}

struct ObservedPod {
    pod: PodRecord,
    prefix: Option<String>,
    health: PodHealth,
}
type PodInventory = HashMap<String, ObservedPod>;

#[async_trait]
trait ReplicaPods: Send + Sync {
    async fn ensure(
        &self,
        pod: &PodRecord,
        group: Option<&str>,
        excluded_nodes: &[String],
    ) -> Result<PodRecord>;
    async fn health(&self, pod: &PodRecord, inventory: &PodInventory) -> Result<PodHealth>;
    async fn observed(&self) -> Result<PodInventory>;
    async fn protect(&self, pod: &PodRecord, group: &str) -> Result<()>;
    async fn retire(&self, pod: &PodRecord) -> Result<()>;
}

#[async_trait]
trait ReplicaPeers: Send + Sync {
    async fn assign(&self, replica: &ReplicaPlacement, assignment: &Assignment) -> Result<()>;
    async fn seal(
        &self,
        replica: &ReplicaPlacement,
        prefix: &str,
    ) -> Result<Option<(String, Bytes)>>;
    async fn flush(&self, replica: &ReplicaPlacement, prefix: &str) -> Result<()>;
}

impl ReplicaFleet {
    pub fn new(
        database: PostgresDatabase,
        pods: KubernetesReplicas,
        authority: Arc<dyn Bucket>,
        archive: Arc<dyn Bucket>,
        config: &crate::bucket::PersistenceConfig,
        secret: String,
        idle: usize,
        max_starting: usize,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let crate::bucket::PersistenceConfig::Replicated { placements, .. } = config else {
            anyhow::bail!("dedicated replicas require a replicated policy");
        };
        config.validate()?;
        ensure!(
            max_starting > 0,
            "replica starting capacity must be positive"
        );
        Ok(Self {
            registry: Registry::new(database),
            pods: Arc::new(pods),
            peers: Arc::new(HttpPeers::new(secret)?),
            authority: Authority::new(authority, clock),
            archive: Archive(archive),
            zones: placements.clone(),
            idle,
            max_starting,
        })
    }

    pub fn start(self: &Arc<Self>, stop: tokio_util::sync::CancellationToken) {
        let fleet = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = stop.cancelled() => return, _ = tick.tick() => {} }
                tokio::select! { _ = stop.cancelled() => return, result = fleet.reconcile() => if let Err(error) = result { tracing::warn!(%error,"replica fleet reconciliation deferred"); } }
            }
        });
    }

    pub async fn authorize(
        &self,
        principal: &crate::control_plane::ActorPrincipal,
        command: &DirectoryCommand,
    ) -> Result<()> {
        let actor_prefix = crate::storage_paths::snapshots(&principal.actor)?;
        ensure!(
            command.scope().starts_with(&actor_prefix),
            "replica directory belongs to another actor"
        );
        match command {
            DirectoryCommand::Prepare { prefix } => {
                self.authority
                    .authorize_writer(prefix, &principal.host_id, &principal.session_id)
                    .await
            }
            DirectoryCommand::Finish { prefix } => {
                self.authority
                    .authorize_finish(prefix, &principal.host_id, &principal.session_id)
                    .await
            }
            _ => Ok(()),
        }
    }

    async fn prepare(&self, prefix: &str) -> Result<Group> {
        self.authority.require_live(prefix).await?;
        let group = self.registry.claim(prefix, &self.zones).await?;
        ensure!(
            group.state == "creating" || group.state == "ready",
            "replica group is closed"
        );
        let excluded: Vec<_> = group
            .pods
            .iter()
            .filter_map(|pod| pod.node.clone())
            .collect();
        let pods = futures_util::future::try_join_all(group.pods.iter().map(|pod| async {
            let pod = self.pods.ensure(pod, Some(prefix), &excluded).await?;
            if let Err(error) = self.registry.update_pod(&pod).await {
                if self.registry.lookup(prefix).await?.state == "archived" {
                    self.pods.retire(&pod).await?;
                }
                return Err(error);
            }
            self.pods.protect(&pod, prefix).await?;
            Ok::<_, anyhow::Error>(pod)
        }))
        .await?;
        ensure!(
            pods.iter()
                .filter_map(|pod| pod.node.as_ref())
                .collect::<HashSet<_>>()
                .len()
                == self.zones.len(),
            "replicas must occupy distinct nodes"
        );
        let assignment = Assignment {
            prefix: prefix.into(),
            replicas: placements(&pods)?,
        };
        futures_util::future::try_join_all(
            assignment
                .replicas
                .iter()
                .map(|replica| self.peers.assign(replica, &assignment)),
        )
        .await?;
        self.authority.require_live(prefix).await?;
        self.registry.ready(prefix).await?;
        self.group(prefix).await
    }

    async fn finish(&self, prefix: &str) -> Result<Group> {
        let group = self.registry.close(prefix).await?;
        if group.state != "archived" {
            let checkpoint = if group.ever_ready {
                self.archive_final(&group).await?
            } else {
                None
            };
            self.registry.archived(prefix, checkpoint.as_ref()).await?;
        }
        self.group(prefix).await
    }

    async fn archive_final(&self, group: &GroupRecord) -> Result<Option<Checkpoint>> {
        let copies = placements(&group.pods)?;
        let results = futures_util::future::join_all(
            copies
                .iter()
                .map(|replica| self.peers.seal(replica, &group.prefix)),
        )
        .await;
        let mut witness = false;
        let mut selected: Option<(&ReplicaPlacement, String, Bytes)> = None;
        for (replica, result) in copies.iter().zip(results) {
            let Ok(candidate) = result else {
                continue;
            };
            witness = true;
            if let Some((object, bytes)) = candidate {
                if let Some((_, previous, previous_bytes)) = &selected {
                    ensure!(
                        object != *previous || bytes == *previous_bytes,
                        "conflicting sealed replica copies"
                    );
                    if position(previous)?.1 >= position(&object)?.1 {
                        continue;
                    }
                }
                selected = Some((replica, object, bytes));
            }
        }
        ensure!(
            witness,
            "no original replica can witness the final state; recovery blocked"
        );
        let Some((replica, object, bytes)) = selected else {
            return Ok(None);
        };
        self.peers.flush(replica, &group.prefix).await?;
        let (_, version) = position(&object)?;
        let archived = self
            .archive
            .get(&group.prefix, version)
            .await?
            .context("final archive missing")?;
        ensure!(
            archived.as_slice() == bytes.as_ref(),
            "final archive differs from sealed state"
        );
        Ok(Some(Checkpoint {
            object,
            digest: super::record::checksum(&bytes),
        }))
    }

    async fn group(&self, prefix: &str) -> Result<Group> {
        let record = self.registry.lookup(prefix).await?;
        Ok(Group {
            prefix: prefix.into(),
            replicas: record
                .pods
                .iter()
                .filter_map(|pod| pod.placement.clone())
                .collect(),
            archived: record.state == "archived",
            checkpoint: record.checkpoint,
        })
    }

    async fn reconcile(&self) -> Result<()> {
        let inventory = &self.pods.observed().await?;
        self.registry
            .reserve_spares(&self.zones, self.idle, self.max_starting)
            .await?;
        let spares = self.registry.unassigned().await?;
        let work = stream::iter(
            spares
                .into_iter()
                .enumerate()
                .map(|(index, pod)| async move {
                    if pod.placement.is_some() {
                        let health = self.pods.health(&pod, inventory).await?;
                        if index >= self.idle
                            || !health.live
                            || health.draining
                            || !health.current_image
                        {
                            if self.registry.retire_spare(&pod.name).await? {
                                self.delete(&pod).await?;
                            }
                            return Ok(());
                        }
                        return Ok(());
                    }
                    match self.pods.ensure(&pod, None, &[]).await {
                        Ok(ready) => self.registry.update_pod(&ready).await,
                        Err(error) => {
                            if self.registry.retire_spare(&pod.name).await? {
                                self.delete(&pod).await?;
                            }
                            Err(error)
                        }
                    }
                }),
        )
        .buffer_unordered(self.max_starting)
        .collect::<Vec<Result<()>>>();
        let groups = self.reconcile_groups(inventory);
        let (spares, groups) = tokio::join!(work, groups);
        for result in spares {
            if let Err(error) = result {
                tracing::warn!(%error, "replica spare reconciliation failed");
            }
        }
        groups?;
        self.reconcile_orphans(inventory).await
    }

    async fn reconcile_groups(&self, inventory: &PodInventory) -> Result<()> {
        let groups = self.registry.maintenance().await?;
        let results = stream::iter(
            groups
                .into_iter()
                .map(|prefix| async move { self.reconcile_group(&prefix, inventory).await }),
        )
        .buffer_unordered(self.max_starting)
        .collect::<Vec<_>>()
        .await;
        for result in results {
            if let Err(error) = result {
                tracing::warn!(%error, "replica group reconciliation deferred");
            }
        }
        Ok(())
    }

    async fn reconcile_group(&self, prefix: &str, inventory: &PodInventory) -> Result<()> {
        let mut group = self.registry.lookup(prefix).await?;
        if group.state != "archived" && group.state != "closing" {
            let mut disrupted = false;
            for pod in &group.pods {
                if pod.uid.is_some() {
                    let health = self.pods.health(&pod, inventory).await?;
                    disrupted |= !health.live || health.draining;
                }
            }
            if !self.authority.retire_if_inactive(prefix, disrupted).await? {
                return Ok(());
            }
        }
        if group.state != "archived" {
            self.finish(prefix).await?;
            group = self.registry.lookup(prefix).await?;
        }
        for pod in &group.pods {
            self.delete(&pod).await?;
        }
        Ok(())
    }

    async fn reconcile_orphans(&self, inventory: &PodInventory) -> Result<()> {
        let registered = self.registry.registered_names().await?;
        for observed in inventory.values() {
            let pod = &observed.pod;
            let prefix = &observed.prefix;
            if registered.contains(&pod.name) || self.registry.registered(&pod.name).await? {
                continue;
            }
            let safe = match prefix {
                Some(prefix) => self
                    .registry
                    .lookup(&prefix)
                    .await
                    .is_ok_and(|group| group.state == "archived"),
                None => true,
            };
            if safe {
                self.pods.retire(&pod).await?;
            }
        }
        Ok(())
    }

    async fn delete(&self, pod: &PodRecord) -> Result<()> {
        self.pods.retire(pod).await?;
        self.registry.remove_pod(&pod.name).await
    }
}

#[async_trait]
impl ReplicaDirectory for ReplicaFleet {
    async fn execute(&self, command: DirectoryCommand) -> Result<DirectoryReply> {
        let mut reply = DirectoryReply::default();
        match command {
            DirectoryCommand::Prepare { prefix } => reply.groups.push(self.prepare(&prefix).await?),
            DirectoryCommand::Lookup { prefix } => reply.groups.push(self.group(&prefix).await?),
            DirectoryCommand::Finish { prefix } => reply.groups.push(self.finish(&prefix).await?),
            DirectoryCommand::ReadArchive { object } => {
                let (prefix, version) = position(&object)?;
                use base64::{Engine, engine::general_purpose::STANDARD};
                reply.data = self
                    .archive
                    .get(&prefix, version)
                    .await?
                    .map(|bytes| STANDARD.encode(bytes));
            }
            DirectoryCommand::ListArchive { prefix } => {
                reply.keys = self.archive.list(&prefix).await?
            }
            DirectoryCommand::Groups { prefix } => {
                for key in self.registry.groups(&prefix).await? {
                    reply.groups.push(self.group(&key).await?);
                }
            }
        }
        Ok(reply)
    }
}

fn placements(pods: &[PodRecord]) -> Result<Vec<ReplicaPlacement>> {
    let replicas: Vec<_> = pods
        .iter()
        .map(|pod| pod.placement.clone().context("replica is not registered"))
        .collect::<Result<_>>()?;
    ensure!(!replicas.is_empty(), "replica group has no members");
    ensure!(
        replicas
            .iter()
            .map(|r| &r.id)
            .collect::<BTreeSet<_>>()
            .len()
            == replicas.len(),
        "duplicate replica disk identities"
    );
    Ok(replicas)
}

struct HttpPeers {
    http: reqwest::Client,
    secret: String,
}
impl HttpPeers {
    fn new(secret: String) -> Result<Self> {
        Ok(Self {
            secret,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(120))
                .build()?,
        })
    }
    fn client(&self, replica: &ReplicaPlacement) -> Result<ReplicaClient> {
        ReplicaClient::new(replica.clone(), self.secret.clone())
    }
}
#[async_trait]
impl ReplicaPeers for HttpPeers {
    async fn assign(&self, replica: &ReplicaPlacement, assignment: &Assignment) -> Result<()> {
        self.http
            .post(format!("{}/assign", replica.address))
            .bearer_auth(&self.secret)
            .json(assignment)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }
    async fn seal(
        &self,
        replica: &ReplicaPlacement,
        prefix: &str,
    ) -> Result<Option<(String, Bytes)>> {
        let client = self.client(replica)?;
        client.seal(prefix).await?;
        client.latest(prefix).await
    }
    async fn flush(&self, replica: &ReplicaPlacement, prefix: &str) -> Result<()> {
        self.client(replica)?
            .send(Command::Flush {
                prefix: prefix.into(),
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/replicas/fleet.rs"]
mod tests;
