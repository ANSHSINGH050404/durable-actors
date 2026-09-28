use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc, watch};
use tokio_util::sync::CancellationToken;

use super::{actor_runtime::ActorStorage, storage::HostStorage};
use crate::{
    replication::{ReplicaScope, ReplicatedStateTransport},
    state_transport::{SnapshotWriter, StateWrite},
    storage::WritePlan,
};

mod startup;
pub(crate) use startup::InitialReplication;

pub(super) struct ActorReplication {
    storage: Arc<HostStorage>,
    writer: ReplicatedStateTransport,
    writing: Mutex<()>,
    latest: StdMutex<Option<(WritePlan, Vec<u8>)>>,
    initial_ready: watch::Receiver<bool>,
}

impl ActorReplication {
    pub fn start(
        storage: Arc<HostStorage>,
        scope: ReplicaScope,
        stop: CancellationToken,
        initial: InitialReplication,
    ) -> Arc<Self> {
        let (failures, reports) = mpsc::unbounded_channel();
        let (ready, initial_ready) = watch::channel(false);
        let this = Arc::new(Self {
            writer: ReplicatedStateTransport::new(
                storage.runtime.clone(),
                Arc::new(storage.transport.clone()),
            )
            .with_upload_tracker(storage.runtime.upload_tracker())
            .with_failure_reports(failures),
            storage,
            writing: Mutex::new(()),
            latest: StdMutex::new(None),
            initial_ready,
        });
        if this.storage.runtime.replication_enabled() {
            let supervisor = this.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = stop.cancelled() => {},
                    result = supervisor.initialize_and_maintain(scope, initial, ready, reports) => {
                        if let Err(error) = result {
                            tracing::warn!(event = "initial_replication_failed", %error);
                        }
                    }
                }
            });
        }
        this
    }

    async fn initialize_and_maintain(
        &self,
        scope: ReplicaScope,
        initial: InitialReplication,
        ready: watch::Sender<bool>,
        reports: mpsc::UnboundedReceiver<String>,
    ) -> Result<()> {
        let membership = initial.ready().await?;
        self.storage.ensure_authority()?;
        self.storage.runtime.enable_replication(membership)?;
        ready.send_replace(true);
        tracing::info!(event = "actor_replication_ready", actor = %scope.actor.storage_key());
        self.maintain(scope, reports).await;
        Ok(())
    }

    async fn maintain(&self, scope: ReplicaScope, mut reports: mpsc::UnboundedReceiver<String>) {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.tick().await;
        let mut first_seen = BTreeMap::new();
        loop {
            let mut failed = BTreeSet::new();
            tokio::select! {
                _ = interval.tick() => {},
                report = reports.recv() => { let Some(host) = report else { return; }; failed.insert(host); }
            }
            while let Ok(host) = reports.try_recv() {
                failed.insert(host);
            }
            if self.storage.ensure_authority().is_err() {
                return;
            }
            match self.repair_if_needed(&scope, failed, &mut first_seen).await {
                Ok(true) => {
                    tracing::info!(event = "actor_replication_repaired", actor = %scope.actor.storage_key());
                }
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(event = "actor_replication_degraded", %error, actor = %scope.actor.storage_key());
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    async fn repair_if_needed(
        &self,
        scope: &ReplicaScope,
        mut failed: BTreeSet<String>,
        first_seen: &mut BTreeMap<String, Instant>,
    ) -> Result<bool> {
        let members = self.storage.runtime.local_replica_members(scope);
        failed.retain(|host| members.iter().any(|member| &member.host_id == host));
        failed.extend(self.storage.runtime.unhealthy_replicas(scope).await?);
        first_seen.retain(|host, _| members.iter().any(|member| &member.host_id == host));
        for member in &members {
            if first_seen
                .entry(member.host_id.clone())
                .or_insert_with(Instant::now)
                .elapsed()
                >= Duration::from_secs(22 * 3600)
            {
                failed.insert(member.host_id.clone());
            }
        }
        if failed.is_empty() && !members.is_empty() {
            return Ok(false);
        }
        self.repair(scope, failed.into_iter().collect()).await?;
        Ok(true)
    }

    async fn repair(&self, scope: &ReplicaScope, failed: Vec<String>) -> Result<()> {
        let targets = self
            .storage
            .runtime
            .replacement_replicas(scope, &failed)
            .await?;
        let mut seeded = {
            let _writing = self.writing.lock().await;
            self.storage.ensure_authority()?;
            self.storage.runtime.suspend_replication(scope);
            self.latest.lock().unwrap().clone()
        };
        let membership = self
            .storage
            .runtime
            .replace_replicas(
                scope,
                targets.clone(),
                seeded.as_ref(),
                &self.storage.transport,
            )
            .await?;
        loop {
            {
                let latest = self.latest.lock().unwrap();
                self.storage.ensure_authority()?;
                if latest.as_ref().map(|(plan, _)| plan.state_version)
                    == seeded.as_ref().map(|(plan, _)| plan.state_version)
                {
                    return self.storage.runtime.enable_replication(membership);
                }
                seeded = latest.clone();
            }
            tokio::time::timeout(
                Duration::from_secs(10),
                self.storage.runtime.seed_replicas(
                    &scope.identity(),
                    &targets,
                    &targets,
                    seeded.as_ref(),
                    &self.storage.transport,
                ),
            )
            .await??;
        }
    }
}

#[async_trait]
impl SnapshotWriter for ActorReplication {
    async fn write_snapshot(&self, plan: &WritePlan, bytes: Vec<u8>) -> Result<StateWrite> {
        let _writing = self.writing.lock().await;
        self.storage.ensure_authority()?;
        let initializing = !*self.initial_ready.borrow();
        let plan = self.storage.runtime.current_write_plan(plan).await?;
        let proof = if plan.replication.is_none()
            && self.storage.runtime.replication_enabled()
            && initializing
        {
            self.writer
                .write_when_ready(&plan, bytes.clone(), self.initial_write_plan(&plan))
                .await?
        } else {
            self.writer.write_snapshot(&plan, bytes.clone()).await?
        };
        *self.latest.lock().unwrap() = Some((plan, bytes));
        Ok(proof)
    }
}

impl ActorReplication {
    async fn initial_write_plan(&self, plan: &WritePlan) -> Result<WritePlan> {
        let mut ready = self.initial_ready.clone();
        ready
            .wait_for(|ready| *ready)
            .await
            .context("initial replication stopped")?;
        self.storage.ensure_authority()?;
        self.storage.runtime.current_write_plan(plan).await
    }
}
