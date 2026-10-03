use std::{collections::HashMap, path::PathBuf, process::Stdio, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tempfile::TempDir;
use tokio::{
    process::{Child, ChildStdin, Command},
    sync::watch,
};
use tokio_util::{
    sync::{CancellationToken, DropGuard},
    task::TaskTracker,
};
use tracing::warn;

use super::local_store::{HostRecord, LocalHostStore};
use super::{
    ActorHostHandle, EnsureHostRequest, HostTermination, SandboxProvider, TerminateHostsRequest,
};
use crate::{
    clock::{Clock, SystemClock},
    placement::ObjectPlacementStore,
};

pub(crate) struct LocalSandboxProvider {
    runtime: Arc<LocalRuntime>,
    placements: Arc<dyn ObjectPlacementStore>,
    _stop: DropGuard,
}

impl LocalSandboxProvider {
    pub(crate) async fn new(
        executable: PathBuf,
        project: PathBuf,
        placements: Arc<dyn ObjectPlacementStore>,
        sdk_host: Option<PathBuf>,
        database: PathBuf,
    ) -> Result<Self> {
        let stop = CancellationToken::new();
        Ok(Self {
            runtime: Arc::new(LocalRuntime {
                executable,
                project,
                sdk_host,
                store: LocalHostStore::open(database).await?,
                tasks: TaskTracker::new(),
                changes: watch::channel(()).0,
                stop: stop.clone(),
            }),
            placements,
            _stop: stop.drop_guard(),
        })
    }

    pub(crate) async fn shutdown(&self) {
        self.runtime.stop.cancel();
        if let Err(error) = self.runtime.store.shutdown().await {
            warn!(%error, "failed to retire local host reservations");
        }
        self.runtime.changes.send_replace(());
        self.runtime.tasks.close();
        self.runtime.tasks.wait().await;
    }
}

#[async_trait]
impl SandboxProvider for LocalSandboxProvider {
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        anyhow::bail!("local hosts use their own process registry")
    }

    async fn wait_ready(&self, host: &crate::host::HostId) -> Result<()> {
        let mut changes = self.runtime.changes.subscribe();
        loop {
            let record = self
                .runtime
                .store
                .host(host.as_str())
                .await?
                .ok_or(super::HostNotReady)?;
            match record.status.as_str() {
                "ready" => return Ok(()),
                "starting" => self.runtime.changed(&mut changes).await?,
                _ => return Err(super::HostNotReady.into()),
            }
        }
    }

    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        ensure!(
            !self.runtime.stop.is_cancelled(),
            "local runtime is shutting down"
        );
        ensure!(
            PathBuf::from(&request.working_directory) == self.runtime.project,
            "local deployments must use the current project directory"
        );
        ensure!(
            request.secret_refs.is_empty(),
            "Kubernetes secret references are unavailable in local mode"
        );
        request
            .actor
            .as_ref()
            .context("local actor identity missing")?
            .validate()?;
        let mut changes = self.runtime.changes.subscribe();
        loop {
            let runtime = self.runtime.clone();
            let request = request.clone();
            // Reservation and process tracking must complete even if this caller disconnects.
            let token = self
                .runtime
                .tasks
                .spawn(async move { runtime.reserve(request).await })
                .await??;
            if let Some(host) = self.wait_for_host(&token, &mut changes).await? {
                return Ok(host);
            }
        }
    }

    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        let runtime = self.runtime.clone();
        let config = request.host_config_key.clone();
        self.runtime
            .tasks
            .spawn(async move { runtime.retire_config(&config).await })
            .await?
    }
}

impl LocalSandboxProvider {
    async fn wait_for_host(
        &self,
        token: &str,
        changes: &mut watch::Receiver<()>,
    ) -> Result<Option<ActorHostHandle>> {
        loop {
            ensure!(
                !self.runtime.stop.is_cancelled(),
                "local runtime is shutting down"
            );
            let Some(record) = self.runtime.store.get(token).await? else {
                anyhow::bail!("local actor startup was retired");
            };
            match record.status.as_str() {
                "ready" => {
                    if let Some(lease) = self.active_lease(&record).await? {
                        return Ok(Some(handle(lease, &record.region, record.epoch)));
                    }
                    self.runtime.store.retire(token).await?;
                    self.runtime.changes.send_replace(());
                    while self
                        .runtime
                        .store
                        .get(token)
                        .await?
                        .is_some_and(|record| record.status == "retiring")
                    {
                        self.runtime.changed(changes).await?;
                    }
                    return Ok(None);
                }
                "failed" => anyhow::bail!(
                    "{}",
                    record
                        .error
                        .as_deref()
                        .unwrap_or("local actor startup failed")
                ),
                "retiring" => anyhow::bail!("local actor startup was retired"),
                _ => {}
            }
            self.runtime.changed(changes).await?;
        }
    }

    async fn active_lease(
        &self,
        record: &HostRecord,
    ) -> Result<Option<crate::host_leases::HostLease>> {
        let expected = record
            .lease
            .as_ref()
            .context("local readiness omitted lease")?;
        let placement = self
            .placements
            .get_owner(&record.actor.storage_key())
            .await?;
        Ok(placement.map(|placement| placement.lease).filter(|lease| {
            lease.id == expected.id
                && lease.session_id == expected.session_id
                && lease.expires_at_ms > SystemClock.now_ms().unwrap_or(u64::MAX)
        }))
    }
}

struct LocalRuntime {
    executable: PathBuf,
    project: PathBuf,
    sdk_host: Option<PathBuf>,
    store: LocalHostStore,
    tasks: TaskTracker,
    changes: watch::Sender<()>,
    stop: CancellationToken,
}

impl LocalRuntime {
    async fn reserve(self: Arc<Self>, request: EnsureHostRequest) -> Result<String> {
        ensure!(!self.stop.is_cancelled(), "local runtime is shutting down");
        let reservation = self.store.reserve(&request).await?;
        if reservation.created {
            let runtime = self.clone();
            let token = reservation.token.clone();
            self.tasks
                .spawn(async move { runtime.run_host(request, token).await });
        }
        Ok(reservation.token)
    }

    async fn retire_config(&self, config: &str) -> Result<HostTermination> {
        let mut changes = self.changes.subscribe();
        let hosts = self.store.retire_config(config).await?;
        self.changes.send_replace(());
        for (token, _) in &hosts {
            while self.store.get(token).await?.is_some() {
                changes.changed().await?;
            }
        }
        Ok(HostTermination {
            provider: "local".into(),
            resource_ids: hosts.into_iter().map(|(_, id)| id).collect(),
        })
    }

    async fn run_host(&self, request: EnsureHostRequest, token: String) {
        let result = self.serve_host(&request, &token).await;
        let error = match result {
            Ok(()) => "local actor host stopped".into(),
            Err(error) => format!("{error:#}"),
        };
        if let Err(error) = self.store.finish(&token, &error).await {
            warn!(%error, "failed to finish local host reservation");
        }
        self.changes.send_replace(());
    }

    async fn serve_host(&self, request: &EnsureHostRequest, token: &str) -> Result<()> {
        let retired = self.retired(token);
        tokio::pin!(retired);
        let mut host = self.spawn_host(request)?;
        let result = async {
            tokio::select! {
                biased;
                result = &mut retired => { result?; anyhow::bail!("local actor startup was retired"); },
                result = tokio::time::timeout(Duration::from_secs(30), host.wait_ready()) => result.context("local actor host did not become ready within 30 seconds")??,
            }
            let lease = host.lease.as_ref().context("local readiness omitted lease")?;
            ensure!(self.store.publish(token, lease, host.owner_epoch).await?, "local actor startup was retired");
            self.changes.send_replace(());
            tokio::select! {
                biased;
                result = &mut retired => result,
                status = host.child.wait() => { anyhow::bail!("local actor host exited with {}", status?); },
            }
        }.await;
        host.stop().await;
        result
    }

    fn spawn_host(&self, request: &EnsureHostRequest) -> Result<LocalHost> {
        let directory = tempfile::Builder::new().prefix("ldo-").tempdir_in("/tmp")?;
        let mut environment = host_environment(request, &directory);
        if let Some(module) = &self.sdk_host {
            environment.insert(
                "DURABLE_ACTORS_SDK_HOST".into(),
                module.display().to_string(),
            );
        }
        let mut child = Command::new(&self.executable)
            .current_dir(&self.project)
            .env_clear()
            .envs(environment)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .context("start local actor host")?;
        Ok(LocalHost {
            // Child::wait closes stdin unless the parent lifetime pipe is held separately.
            lifetime: child.stdin.take(),
            child,
            directory,
            owner_epoch: 0,
            lease: None,
        })
    }

    async fn retired(&self, token: &str) -> Result<()> {
        let mut changes = self.changes.subscribe();
        loop {
            if self.stop.is_cancelled()
                || self
                    .store
                    .get(token)
                    .await?
                    .is_none_or(|record| record.status == "retiring")
            {
                return Ok(());
            }
            if self.changed(&mut changes).await.is_err() {
                return Ok(());
            }
        }
    }

    async fn changed(&self, changes: &mut watch::Receiver<()>) -> Result<()> {
        tokio::select! {
            biased;
            () = self.stop.cancelled() => anyhow::bail!("local runtime is shutting down"),
            result = changes.changed() => result.context("local host tracking stopped"),
        }
    }
}

struct LocalHost {
    lifetime: Option<ChildStdin>,
    lease: Option<crate::host_leases::HostLease>,
    child: Child,
    directory: TempDir,
    owner_epoch: u64,
}

impl LocalHost {
    async fn wait_ready(&mut self) -> Result<()> {
        loop {
            if let Some(status) = self.child.try_wait()? {
                anyhow::bail!("local actor host exited with {status}; check its logs above");
            }
            let path = self.directory.path().join("ready");
            if path.exists() {
                let ready: serde_json::Value =
                    serde_json::from_slice(&tokio::fs::read(path).await?)?;
                self.owner_epoch = ready["ownerEpoch"]
                    .as_u64()
                    .context("host readiness omitted ownership epoch")?;
                ensure!(
                    self.owner_epoch > 0,
                    "host readiness returned no ownership epoch"
                );
                self.lease = Some(serde_json::from_value(ready["lease"].clone())?);
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(mut self) {
        drop(self.lifetime.take());
        if tokio::time::timeout(Duration::from_secs(7), self.child.wait())
            .await
            .is_err()
        {
            let _ = self.child.kill().await;
        }
    }
}

fn handle(lease: crate::host_leases::HostLease, region: &str, owner_epoch: u64) -> ActorHostHandle {
    ActorHostHandle {
        lease: Some(lease.clone()),
        owner_epoch,
        host_id: lease.id,
        route: lease.route,
        canonical_region: region.to_owned(),
        provisioning: None,
    }
}

fn host_environment(request: &EnsureHostRequest, directory: &TempDir) -> HashMap<String, String> {
    let mut environment = std::env::vars()
        .filter(|(key, _)| !key.starts_with("DURABLE_ACTORS_") || key == "DURABLE_ACTORS_PYTHON")
        .collect::<HashMap<_, _>>();
    if let Some(actor) = &request.actor {
        environment.insert(
            "DURABLE_ACTORS_ACTOR".into(),
            serde_json::to_string(actor).expect("actor identity serializable"),
        );
    }
    environment.insert(
        "DURABLE_ACTORS_ACTOR_IS_NEW".into(),
        request.actor_is_new.to_string(),
    );
    if let Some(hint) = &request.owner_hint {
        environment.insert("DURABLE_ACTORS_OWNER_HINT".into(), hint.clone());
    }
    for (key, value) in [
        ("DURABLE_ACTORS_PROCESS_ROLE", "host".to_owned()),
        ("DURABLE_ACTORS_LOG_MODE", "development".into()),
        ("DURABLE_ACTORS_PARENT_LIFETIME_STDIN", "1".into()),
        ("DURABLE_ACTORS_HOST_BIND", "127.0.0.1:0".into()),
        (
            "DURABLE_ACTORS_HOST_ID",
            request.host_id.as_str().to_owned(),
        ),
        ("DURABLE_ACTORS_SESSION_ID", request.session_id.clone()),
        ("DURABLE_ACTORS_HOST_TOKEN", request.host_token.clone()),
        (
            "DURABLE_ACTORS_JWT_PUBLIC_KEYS",
            request.jwt_public_keys.clone(),
        ),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL",
            request.control_plane_url.clone(),
        ),
        ("DURABLE_ACTORS_JWT_ISSUER", request.jwt_issuer.clone()),
        (
            "DURABLE_ACTORS_INVOKE_JWT_AUDIENCE",
            request.invocation_jwt_audience.clone(),
        ),
        (
            "DURABLE_ACTORS_EXECUTOR_SOCKET",
            directory.path().join("executor.sock").display().to_string(),
        ),
        (
            "DURABLE_ACTORS_HOST_READY_FILE",
            directory.path().join("ready").display().to_string(),
        ),
        (
            "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS",
            request.host_idle_timeout_ms.to_string(),
        ),
    ] {
        environment.insert(key.into(), value);
    }
    if let Some(config) = &request.runtime_config {
        environment.insert("DURABLE_ACTORS_RUNTIME_CONFIG".into(), config.clone());
    }
    if let Some(entrypoint) = &request.actor_entrypoint {
        environment.insert("DURABLE_ACTORS_ENTRYPOINT".into(), entrypoint.clone());
    }
    environment
}

#[cfg(test)]
#[path = "../../tests/unit/sandbox/local.rs"]
mod tests;
