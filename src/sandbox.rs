use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::host::HostId;

mod command_process;
mod local;
mod local_store;
pub(crate) mod pool;

pub(crate) use local::LocalSandboxProvider;

const PROVIDER_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const PROVIDER_BUILD_TIMEOUT: Duration = Duration::from_secs(12 * 60);
const MAX_PROVIDER_OUTPUT_BYTES: usize = 5 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLimits {
    pub cpu_millis: u32,
    pub memory_mib: u32,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            cpu_millis: 1000,
            memory_mib: 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpareHandle {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub control_route: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub control_token: String,
    pub name: String,
    pub resource_id: String,
    pub route: String,
    pub canonical_region: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SpareKind {
    Actor,
    Replica,
}

impl SpareKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Actor => "actor",
            Self::Replica => "replica",
        }
    }
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSpareRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub control_plane_url: Option<String>,
    pub kind: SpareKind,
    pub name: String,
    pub image_ref: String,
    pub canonical_region: String,
    pub resources: ResourceLimits,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildCodeRequest {
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: String,
    pub canonical_region: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuiltActorCode {
    pub code_snapshot: String,
    pub contract: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnsureHostRequest {
    pub actor_is_new: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_hint: Option<String>,
    pub actor: Option<crate::actor::ActorKey>,
    pub code_snapshot: Option<String>,
    pub spare: Option<SpareHandle>,
    pub resources: ResourceLimits,
    pub runtime_config: Option<String>,

    pub host_config_key: String,
    pub canonical_region: String,
    pub host_id: HostId,
    pub session_id: String,
    pub host_token: String,
    pub jwt_public_keys: String,
    pub control_plane_url: String,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub socket_jwt_audience: String,
    pub image_ref: String,
    pub working_directory: String,
    pub actor_entrypoint: Option<String>,
    pub secret_refs: Vec<String>,
    pub host_idle_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostHandle {
    pub lease: Option<crate::host_leases::HostLease>,
    #[serde(default)]
    pub owner_epoch: u64,
    pub host_id: HostId,
    pub route: String,
    pub canonical_region: String,
    pub provisioning: Option<ActorHostProvisioning>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorHostProvisioning {
    pub provider: String,
    pub resource_id: String,
    pub reused: bool,
    pub started_at_ms: u64,
    pub input_parsed_at_ms: Option<u64>,
    pub sdk_loaded_at_ms: Option<u64>,
    pub resources_resolved_at_ms: Option<u64>,
    pub sandbox_scheduled_at_ms: Option<u64>,
    pub host_ready_observed_at_ms: Option<u64>,
    pub route_read_at_ms: Option<u64>,
    pub completed_at_ms: u64,
    #[serde(default)]
    pub command_spawned_at_ms: Option<u64>,
    #[serde(default)]
    pub request_written_at_ms: Option<u64>,
    #[serde(default)]
    pub process_completed_at_ms: Option<u64>,
    #[serde(default)]
    pub response_decoded_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TerminateHostsRequest {
    pub host_config_key: String,
    pub canonical_regions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostTermination {
    pub provider: String,
    pub resource_ids: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SocketCredentialsRequest {
    pub resource_id: Option<String>,
    pub canonical_region: String,
    pub host_id: HostId,
    pub session_id: String,
}

#[derive(Deserialize)]
pub struct SocketCredentials {
    pub url: String,
    #[serde(default)]
    pub token: String,
}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    async fn build_code(&self, request: &BuildCodeRequest) -> Result<BuiltActorCode>;
    async fn wait_ready(&self, _host: &HostId) -> Result<()> {
        Ok(())
    }

    async fn create_spare(&self, _request: &CreateSpareRequest) -> Result<SpareHandle> {
        anyhow::bail!("provider does not support generic spares")
    }
    async fn retire_spare(&self, _request: &SpareHandle) -> Result<()> {
        anyhow::bail!("provider does not support generic spares")
    }
    async fn socket_credentials(
        &self,
        request: &SocketCredentialsRequest,
    ) -> Result<SocketCredentials>;
    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle>;
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination>;
}

#[derive(Clone)]
pub struct HostSandboxRuntimeConfig {
    pub control_plane_url: String,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub host_idle_timeout_ms: u64,
}

pub struct CommandSandboxProvider {
    provider_name: String,
    command: String,
    environment: HashMap<String, String>,
    process: command_process::Process,
}

impl CommandSandboxProvider {
    pub fn new(
        provider_name: String,
        command: String,
        mut environment: HashMap<String, String>,
    ) -> Result<Self> {
        ensure!(
            !provider_name.is_empty() && provider_name.trim() == provider_name,
            "sandbox provider name must be non-empty without surrounding whitespace"
        );
        ensure!(
            !command.is_empty() && command.trim() == command,
            "DURABLE_ACTORS_SANDBOX_COMMAND must be non-empty without surrounding whitespace"
        );
        if let Ok(path) = std::env::var("PATH") {
            environment.entry("PATH".into()).or_insert(path);
        }
        Ok(Self {
            provider_name,
            command,
            environment,
            process: command_process::Process::default(),
        })
    }
}

#[async_trait]
impl SandboxProvider for CommandSandboxProvider {
    async fn build_code(&self, request: &BuildCodeRequest) -> Result<BuiltActorCode> {
        self.execute("build_code", request).await
    }
    async fn create_spare(&self, request: &CreateSpareRequest) -> Result<SpareHandle> {
        self.execute("create_spare", request).await
    }

    async fn retire_spare(&self, request: &SpareHandle) -> Result<()> {
        self.execute::<_, serde_json::Value>("retire_spare", request)
            .await?;
        Ok(())
    }
    async fn socket_credentials(
        &self,
        request: &SocketCredentialsRequest,
    ) -> Result<SocketCredentials> {
        self.execute("socket_credentials", request).await
    }

    async fn ensure_host(&self, request: &EnsureHostRequest) -> Result<ActorHostHandle> {
        let (mut response, command): (ActorHostHandle, _) =
            self.execute_timed("ensure_host", request).await?;
        if let Some(provisioning) = &mut response.provisioning {
            provisioning.command_spawned_at_ms = command.spawned_at_ms;
            provisioning.request_written_at_ms = command.request_written_at_ms;
            provisioning.process_completed_at_ms = command.process_completed_at_ms;
            provisioning.response_decoded_at_ms = command.response_decoded_at_ms;
        }
        ensure!(
            response.canonical_region == request.canonical_region,
            "{} sandbox command returned a host in the wrong canonical region",
            self.provider_name
        );
        ensure!(
            !response.host_id.as_str().is_empty() && !response.route.is_empty(),
            "{} sandbox command returned an invalid host",
            self.provider_name
        );
        Ok(response)
    }

    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        let _ = request;
        anyhow::bail!("cloud hosts must be retired through the spare registry")
    }
}

impl CommandSandboxProvider {
    async fn execute<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
    ) -> Result<Reply> {
        Ok(self.execute_timed(operation, request).await?.0)
    }

    async fn execute_timed<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
    ) -> Result<(Reply, ProviderCommandTimings)> {
        let started_at = Instant::now();
        let mut timings = ProviderCommandTimings::default();
        match self
            .execute_timed_inner(operation, request, started_at, &mut timings)
            .await
        {
            Ok(response) => Ok((response, timings)),
            Err(source) => Err(ProviderCommandFailure { source, timings }.into()),
        }
    }

    async fn execute_timed_inner<Request: Serialize, Reply: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        request: &Request,
        started_at: Instant,
        timings: &mut ProviderCommandTimings,
    ) -> Result<Reply> {
        let command = ProviderCommand { operation, request };
        let execution = self.process.exchange(
            &self.command,
            &self.environment,
            &command,
            started_at,
            timings,
        );
        let timeout = if operation == "build_code" {
            PROVIDER_BUILD_TIMEOUT
        } else {
            PROVIDER_REQUEST_TIMEOUT
        };
        tokio::time::timeout(timeout, execution)
            .await
            .context("sandbox provider command timed out; outcome may be unknown")?
    }
}

#[derive(Debug, Default)]
struct ProviderCommandTimings {
    spawned_at_ms: Option<u64>,
    request_written_at_ms: Option<u64>,
    process_completed_at_ms: Option<u64>,
    response_decoded_at_ms: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct ProviderCommandFailure {
    source: anyhow::Error,
    timings: ProviderCommandTimings,
}

impl ProviderCommandFailure {
    pub(crate) fn spawned_at_ms(&self) -> Option<u64> {
        self.timings.spawned_at_ms
    }

    pub(crate) fn request_written_at_ms(&self) -> Option<u64> {
        self.timings.request_written_at_ms
    }

    pub(crate) fn process_completed_at_ms(&self) -> Option<u64> {
        self.timings.process_completed_at_ms
    }

    pub(crate) fn response_decoded_at_ms(&self) -> Option<u64> {
        self.timings.response_decoded_at_ms
    }
}

impl std::fmt::Display for ProviderCommandFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for ProviderCommandFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.source()
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[derive(Serialize)]
struct ProviderCommand<'a, Request> {
    operation: &'a str,
    request: &'a Request,
}

#[cfg(test)]
#[path = "../tests/unit/sandbox.rs"]
mod tests;
