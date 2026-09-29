use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use google_cloud_auth::credentials::{CacheableResource, CredentialsProvider, EntityTag};
use tokio_util::sync::CancellationToken;

use super::{
    HostId,
    actor_runtime::{ActorActivation, ActorStorage},
};
use crate::{
    actor::ActorKey,
    bucket::{
        Bucket, GcsBucket, RuntimeStorage, WarmGcs,
        access::{HostStorageConfig, StorageToken},
    },
    clock::{Clock, SystemClock},
    control_plane::{ControlPlaneClient, LeaseFence},
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest},
    storage::WritePlan,
};

pub(crate) struct HostStorage {
    pub runtime: Arc<RuntimeStorage>,
    objects: Option<google_cloud_storage::client::Storage>,
    pub(super) stop: CancellationToken,
    observer: Arc<ControlPlaneClient>,
    host: HostId,
    session: String,
    region: String,
    actor: Option<ActorKey>,
    new_actor: bool,
    owner_hint: Option<crate::bucket::OwnershipHint>,
    activation: Mutex<Option<ActorActivation>>,
    fence: Mutex<LeaseFence>,
    lease: Mutex<Option<HostLease>>,
}

impl HostStorage {
    pub async fn new(
        config: HostStorageConfig,
        host: HostId,
        session: String,
        client: Arc<ControlPlaneClient>,
        stop: CancellationToken,
        warm: Option<WarmGcs>,
    ) -> Result<Self> {
        let credentials = HostCredentials::new(config.token, client.clone(), stop.clone());
        let (authority, clients): (Arc<dyn Bucket>, _) = match config.bucket {
            crate::bucket::access::BucketLocation::Gcs { bucket, .. } => {
                let bucket = match warm {
                    Some(warm) => warm.bind(&bucket, credentials.into())?,
                    None => GcsBucket::with_credentials(&bucket, credentials.into()).await?,
                };
                let clients = bucket.clients();
                (Arc::new(bucket), Some(clients))
            }
            crate::bucket::access::BucketLocation::File { directory } => {
                (Arc::new(crate::bucket::FileBucket::new(directory)?), None)
            }
        };
        let mut runtime = RuntimeStorage::new(authority, Arc::new(SystemClock))?;
        if matches!(
            config.persistence,
            crate::bucket::PersistenceConfig::Replicated { .. }
        ) {
            let snapshots = crate::replicas::directory::DedicatedSnapshots::new(
                client.clone(),
                config.replica_token.context("replica capability missing")?,
            )?;
            runtime = runtime.with_persistence(config.persistence, Arc::new(snapshots))?;
        }
        Ok(Self {
            objects: clients.map(|clients| clients.storage),
            observer: client,
            stop,
            runtime: Arc::new(runtime),
            host,
            session,
            region: config.region,
            actor: None,
            new_actor: false,
            owner_hint: None,
            activation: Mutex::new(None),
            fence: Mutex::new(LeaseFence::default()),
            lease: Mutex::new(None),
        })
    }

    pub(crate) async fn install_code(
        &self,
        artifact: &crate::artifacts::ArtifactManifest,
    ) -> Result<()> {
        artifact
            .install(
                std::path::Path::new("/customer"),
                self.objects
                    .as_ref()
                    .context("GCS artifact client missing")?,
            )
            .await
    }

    pub(crate) fn with_actor(
        mut self,
        actor: Option<ActorKey>,
        new_actor: bool,
        owner_hint: Option<crate::bucket::OwnershipHint>,
    ) -> Self {
        self.actor = actor;
        self.new_actor = new_actor;
        self.owner_hint = owner_hint;
        self
    }

    pub(super) fn current_lease(&self) -> Result<HostLease> {
        self.ensure_authority()?;
        self.lease
            .lock()
            .unwrap()
            .clone()
            .context("activation lease missing")
    }

    fn notify_observer(&self) {
        let client = self.observer.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(2), client.notify_inventory_changed())
                .await;
        });
    }

    fn authorize(&self, actor: &ActorKey, host: &HostId) -> Result<()> {
        actor.validate()?;
        ensure!(
            self.actor.as_ref().is_none_or(|bound| bound == actor),
            "actor identity mismatch"
        );
        ensure!(*host == self.host, "host storage scope mismatch");
        self.ensure_authority()
    }
}

#[async_trait]
impl ActorStorage for HostStorage {
    fn ensure_authority(&self) -> Result<()> {
        ensure!(
            !self.stop.is_cancelled(),
            "host session is permanently fenced"
        );
        self.fence
            .lock()
            .map_err(|_| anyhow::anyhow!("lease fence poisoned"))?
            .check(Instant::now())
    }

    async fn acquire_actor(&self, actor: &ActorKey, host: &HostId) -> Result<ActorActivation> {
        self.authorize(actor, host)?;
        if let Some(activation) = self.activation.lock().unwrap().take() {
            return Ok(activation);
        }
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self
            .runtime
            .activate_actor(actor, &lease, &self.region)
            .await?;
        self.ensure_authority()?;
        Ok(ActorActivation {
            owner_epoch: loaded.placement.owner_epoch,
            state_version: loaded.placement.state_version,
            state: loaded.state,
        })
    }

    async fn load_actor_state(
        &self,
        actor: &ActorKey,
        host: &HostId,
        epoch: u64,
    ) -> Result<(u64, bytes::Bytes)> {
        self.authorize(actor, host)?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let loaded = self.runtime.load_owned_actor(actor, &lease, epoch).await?;
        self.ensure_authority()?;
        Ok((
            loaded.placement.state_version,
            loaded.state.unwrap_or_default(),
        ))
    }

    async fn prepare_state_write(
        &self,
        actor: &ActorKey,
        host: &HostId,
        epoch: u64,
        version: u64,
    ) -> Result<WritePlan> {
        self.authorize(actor, host)?;
        let lease = self
            .lease
            .lock()
            .unwrap()
            .clone()
            .context("host lease missing")?;
        let ticket = self
            .runtime
            .prepare_actor_write(
                actor,
                &lease,
                epoch,
                version.checked_add(1).context("state version overflow")?,
            )
            .await?;
        self.ensure_authority()?;
        Ok(ticket)
    }
}

#[async_trait]
impl HostLeaseRegistry for HostStorage {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        self.register_with_residents(request, None).await
    }

    async fn register_with_residents(
        &self,
        request: &HostLeaseRequest,
        residents: Option<&[crate::actor::ActorKey]>,
    ) -> Result<HostLease> {
        self.register_with_inventory(request, residents, &[], None)
            .await
    }

    async fn register_with_inventory(
        &self,
        request: &HostLeaseRequest,
        residents: Option<&[crate::actor::ActorKey]>,
        sockets: &[crate::host_leases::ActorSocketInventory],
        queues: Option<&[crate::host_leases::ActorQueueInventory]>,
    ) -> Result<HostLease> {
        ensure!(
            request.id == self.host && request.session_id == self.session,
            "host lease scope mismatch"
        );
        let started = Instant::now();
        self.fence.lock().unwrap().begin(started)?;
        let actor = self.actor.as_ref().context("host actor identity missing")?;
        ensure!(
            residents.is_none_or(
                |actors| actors.len() <= 1 && actors.iter().all(|candidate| candidate == actor)
            ),
            "resident actor scope mismatch"
        );
        ensure!(
            sockets.iter().all(|entry| entry.actor == *actor)
                && queues.is_none_or(|entries| entries.iter().all(|entry| entry.actor == *actor)),
            "inventory actor scope mismatch"
        );
        let inventory = crate::host_leases::ActivationInventory {
            resident: residents.map(|actors| actors.contains(actor)),
            connections: sockets
                .iter()
                .flat_map(|entry| entry.connections.clone())
                .collect(),
            waiting: queues.map(|entries| {
                entries
                    .iter()
                    .flat_map(|entry| entry.waiting.clone())
                    .collect()
            }),
        };
        let first = self.lease.lock().unwrap().is_none();
        let lease = if first {
            let loaded = self
                .runtime
                .register_activation(
                    actor,
                    request,
                    &self.region,
                    self.new_actor,
                    self.owner_hint.as_ref(),
                )
                .await?;
            let lease = loaded.placement.lease;
            *self.activation.lock().unwrap() = Some(ActorActivation {
                owner_epoch: loaded.placement.owner_epoch,
                state_version: loaded.placement.state_version,
                state: loaded.state,
            });
            lease
        } else {
            self.runtime
                .renew_activation(actor, request, inventory)
                .await?
        };
        self.fence.lock().unwrap().confirm(
            started,
            Duration::from_millis(request.duration_ms),
            Instant::now(),
        )?;
        self.lease.lock().unwrap().replace(lease.clone());
        self.notify_observer();
        Ok(lease)
    }
    async fn unregister(&self, host: &HostId, session: &str) -> Result<()> {
        ensure!(
            *host == self.host && session == self.session,
            "host lease scope mismatch"
        );
        self.fence.lock().unwrap().fenced = true;
        let actor = self.actor.as_ref().context("host actor identity missing")?;
        let completed = tokio::time::timeout(
            Duration::from_secs(5),
            self.runtime.finish_activation(actor, host, session),
        )
        .await
        .context("snapshot drain timed out")
        .and_then(|result| result);
        if let Err(error) = completed {
            tracing::warn!(%error, "clean session shutdown deferred to recovery");
            self.runtime
                .release_activation(actor, host, session)
                .await?;
        }
        self.notify_observer();
        Ok(())
    }
}

#[derive(Clone)]
struct HostCredentials(Arc<std::sync::RwLock<Option<StorageToken>>>);
impl std::fmt::Debug for HostCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("HostCredentials")
    }
}
impl HostCredentials {
    fn new(
        token: Option<StorageToken>,
        client: Arc<ControlPlaneClient>,
        stop: CancellationToken,
    ) -> Self {
        let credentials = Self(Arc::new(std::sync::RwLock::new(token)));
        let refreshed = credentials.clone();
        tokio::spawn(async move {
            loop {
                let now = SystemClock.now_ms().unwrap_or(u64::MAX);
                let host_expiry = client.token_expires_at_ms().unwrap_or(now);
                let expires = refreshed
                    .0
                    .read()
                    .unwrap()
                    .as_ref()
                    .map_or(host_expiry, |token| token.expires_at_ms.min(host_expiry));
                let delay = Duration::from_millis(
                    expires
                        .saturating_sub(now.saturating_add(30_000))
                        .clamp(1_000, 600_000),
                );
                tokio::select! { _ = stop.cancelled() => return, _ = tokio::time::sleep(delay) => {} }
                match client.refresh_storage_access().await {
                    Ok(token) => {
                        *refreshed.0.write().unwrap() = token;
                    }
                    Err(error) => {
                        tracing::warn!(%error, "could not refresh host storage credentials")
                    }
                }
            }
        });
        credentials
    }
}
impl CredentialsProvider for HostCredentials {
    async fn headers(
        &self,
        _: axum::http::Extensions,
    ) -> std::result::Result<
        CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        let stored = self.0.read().unwrap();
        let token = stored.as_ref().ok_or_else(|| {
            google_cloud_auth::errors::CredentialsError::from_msg(false, "GCS credentials missing")
        })?;
        let mut headers = axum::http::HeaderMap::new();
        let mut value: axum::http::HeaderValue = format!("Bearer {}", token.access_token)
            .parse()
            .map_err(|_| {
            google_cloud_auth::errors::CredentialsError::from_msg(
                false,
                "invalid storage credential",
            )
        })?;
        value.set_sensitive(true);
        headers.insert(axum::http::header::AUTHORIZATION, value);
        Ok(CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: headers,
        })
    }
    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/storage.rs"]
mod tests;
