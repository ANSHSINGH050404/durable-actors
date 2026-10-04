use crate::request_tracking::HostState;
use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::{
    actor::{ActorKey, ActorSocketEvent},
    bucket::OwnershipHint,
    grpc::proto::{
        ControlPlaneReply, ControlPlaneRequest,
        actor_control_plane_service_server::{
            ActorControlPlaneService, ActorControlPlaneServiceServer,
        },
    },
    host::HostId,
    host_leases::HostLease,
    placement::{ObjectPlacement, ObjectPlacementStore},
    sandbox::{
        EnsureHostRequest, HostSandboxRuntimeConfig, HostTermination, SandboxProvider,
        TerminateHostsRequest,
    },
};

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminRegistry, AdminService, HostLaunchSpec},
    auth::{ActorJwtVerifier, ActorPrincipal},
    issuer::ActorJwtIssuer,
    protocol::{ControlPlaneCommand, ControlPlaneCommandReply, decode_command, encode_reply},
};

const FALLBACK_REGION: &str = "north-america-central";

#[derive(Clone)]
pub struct ControlPlaneService {
    pub(super) traces: crate::request_traces::TraceStore,
    pub(super) changes: crate::postgres::notifications::ChangeFeed,
    pub(super) region: Option<String>,
    runtime_access: Option<Arc<crate::bucket::access::RuntimeAccess>>,
    pub(super) gateway: Option<super::gateway::Gateway>,
    placements: Arc<dyn ObjectPlacementStore>,
    pub(super) auth: ActorJwtVerifier,
    host_token_issuer: ActorJwtIssuer,
    registry: Arc<dyn AdminRegistry>,
    provisioner: Arc<dyn HostProvisioner>,
    socket_events: Option<Arc<dyn super::event_sink::SocketMessageEventSink>>,
    local_builds: Option<Arc<super::local_build::LocalBuilds>>,
}

impl ControlPlaneService {
    pub(super) fn with_local_builds(
        mut self,
        builds: Arc<super::local_build::LocalBuilds>,
    ) -> Self {
        self.local_builds = Some(builds);
        self
    }

    pub(crate) fn with_runtime_access(
        mut self,
        access: Arc<crate::bucket::access::RuntimeAccess>,
    ) -> Self {
        self.runtime_access = Some(access);
        self
    }

    pub(crate) fn new(
        placements: Arc<dyn ObjectPlacementStore>,
        auth: ActorJwtVerifier,
        registry: Arc<dyn AdminRegistry>,
        issuer: ActorJwtIssuer,
        provisioner: Arc<dyn HostProvisioner>,
    ) -> Self {
        Self {
            traces: crate::request_traces::TraceStore::default(),
            changes: Default::default(),
            runtime_access: None,
            gateway: None,
            region: None,
            placements,
            auth,
            host_token_issuer: issuer,
            registry,
            provisioner,
            socket_events: None,
            local_builds: None,
        }
    }

    pub(crate) fn with_socket_event_sink(
        mut self,
        sink: Option<Arc<dyn super::event_sink::SocketMessageEventSink>>,
    ) -> Self {
        self.socket_events = sink;
        self
    }

    pub(crate) fn with_traces(mut self, traces: crate::request_traces::TraceStore) -> Self {
        self.traces = traces;
        self
    }

    pub fn into_internal_service(self) -> ActorControlPlaneServiceServer<Self> {
        ActorControlPlaneServiceServer::new(self)
            .max_decoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_CONTROL_PLANE_MESSAGE_BYTES)
    }

    pub(super) fn default_region(&self) -> &str {
        self.region.as_deref().unwrap_or(FALLBACK_REGION)
    }

    pub(super) async fn runtime_deployment(
        &self,
        project_id: &str,
    ) -> Result<Option<HostLaunchSpec>> {
        self.registry.launch_spec(project_id).await
    }

    pub(super) async fn register_deployment(
        &self,
        admin: &AdminService,
        update: &mut dyn super::admin::DeploymentUpdate,
        spec: &HostLaunchSpec,
        contract: Option<&super::contracts::PublicActorContract>,
    ) -> Result<bool> {
        let previous = admin.current_deployment(&spec.project_id).await?;
        spec.validate()?;
        let replacing = previous.is_some();
        if let Some(previous) = previous {
            self.terminate_deployment_hosts(&previous).await?;
        }
        let changed = update.register(spec, contract).await?;
        self.changes.notify().await;
        Ok(changed || replacing)
    }

    pub(super) async fn deploy_source(
        &self,
        admin: &AdminService,
        source: &HostLaunchSpec,
        supplied_contract: Option<&super::contracts::PublicActorContract>,
    ) -> Result<bool> {
        source.validate()?;
        let mut update = admin.lock_deployment(&source.project_id).await?;
        let previous = admin.current_deployment(&source.project_id).await?;
        let local_build = match &self.local_builds {
            Some(builds) => Some(builds.prepare(source).await?),
            None => None,
        };
        let (mut prepared, mut compiled_contract) = match &local_build {
            Some(build) => (build.spec.clone(), Some(build.contract.clone())),
            None => {
                self.provisioner
                    .prepare_deployment(source, previous.as_ref(), self.default_region())
                    .await?
            }
        };
        if compiled_contract.is_none()
            && prepared.code_snapshot.is_some()
            && supplied_contract.is_none()
        {
            previous
                .as_ref()
                .filter(|old| old.code_snapshot == prepared.code_snapshot)
                .context("a new bundle requires its actor contract")?;
            let record = admin
                .deployment_contract(&source.project_id)
                .await?
                .context("compiled deployment contract is missing")?;
            compiled_contract = Some(super::contracts::PublicActorContract::new(record.contract)?);
        }
        if let (Some(compiled), Some(supplied)) = (&compiled_contract, supplied_contract) {
            ensure!(
                compiled.hash() == supplied.hash(),
                "supplied actor contract differs from compiled actor code"
            );
        }
        let contract = compiled_contract.as_ref().or(supplied_contract);
        prepared.sandboxes = contract
            .map(|contract| contract.sandboxes())
            .transpose()?
            .unwrap_or_default();
        let changed = self
            .register_deployment(admin, update.as_mut(), &prepared, contract)
            .await?;
        if let Some(build) = local_build {
            build.commit().await;
        }
        Ok(changed)
    }

    pub(super) async fn delete_deployment(
        &self,
        admin: &AdminService,
        project_id: &str,
    ) -> Result<bool> {
        let mut update = admin.lock_deployment(project_id).await?;
        let Some(previous) = admin.current_deployment(project_id).await? else {
            return Ok(false);
        };
        self.terminate_deployment_hosts(&previous).await?;
        update.remove().await?;
        self.changes.notify().await;
        Ok(true)
    }

    async fn terminate_deployment_hosts(&self, spec: &HostLaunchSpec) -> Result<()> {
        let started_at = Instant::now();
        match self
            .provisioner
            .terminate_hosts(
                spec,
                &super::regions::ALL
                    .iter()
                    .map(|r| (*r).into())
                    .collect::<Vec<_>>(),
            )
            .await
        {
            Ok(termination) => info!(
                event = "actor_hosts_terminated",
                project_id = %spec.project_id,
                provider = %termination.provider,
                resource_count = termination.resource_ids.len(),
                total_ms = elapsed_ms(started_at),
                outcome = "terminated",
                "replaced deployment hosts terminated"
            ),
            Err(error) => {
                return Err(error.context(
                    "previous actor hosts could not be terminated; retry the deployment update",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn resolve_actor_target_timed(
        &self,
        actor: &ActorKey,
        home_region: Option<&str>,
        timings: &mut TargetResolutionTimings,
        grant: Option<super::session::InvocationGrant>,
    ) -> Result<ActorTarget> {
        self.resolve_actor_route(actor, home_region, Some(timings), grant)
            .await
    }

    pub(super) fn validate_home_region(&self, assignment: Option<&str>) -> Result<()> {
        if let Some(assignment) = assignment {
            crate::placement::validate_region(assignment)?;
        }
        Ok(())
    }

    pub(super) fn deliver_socket_message_event(
        &self,
        actor: &ActorKey,
        trigger_id: Option<String>,
        event: &ActorSocketEvent,
    ) {
        let ActorSocketEvent::Message {
            connection_id,
            message,
        } = event
        else {
            return;
        };
        if let Some(sink) = self.socket_events.clone() {
            let event = super::event_sink::SocketMessageEvent::new(
                actor,
                trigger_id,
                connection_id,
                message,
            );
            tokio::spawn(async move {
                if let Err(error) = sink.deliver(event).await {
                    warn!(error = %format!("{error:#}"), "actor socket message trigger delivery failed");
                }
            });
        }
        info!(
            event = "actor_socket_message_committed",
            actor_name = %actor.actor_name,
            actor_id = %actor.actor_id,
            connection_id,
            message_kind = match message {
                crate::actor::ActorSocketMessage::Text { .. } => "text",
                crate::actor::ActorSocketMessage::Binary { .. } => "binary",
            },
            "actor socket message committed"
        );
    }

    pub(super) async fn resolve_actor_route(
        &self,
        actor: &ActorKey,
        home_region: Option<&str>,
        mut timings: Option<&mut TargetResolutionTimings>,
        grant: Option<super::session::InvocationGrant>,
    ) -> Result<ActorTarget> {
        actor.validate()?;
        let target = self
            .route_actor(
                actor,
                self.default_region(),
                home_region,
                timings.as_deref_mut(),
            )
            .await?;
        let resolved_at_ms = crate::clock::Clock::now_ms(&crate::clock::SystemClock)?;
        let idle_expires_at_ms = resolved_at_ms.saturating_add(
            target
                .spec
                .sandboxes
                .get(&actor.actor_name)
                .and_then(|options| options.idle_timeout_ms)
                .unwrap_or_else(|| self.provisioner.host_idle_timeout_ms()),
        );
        let issued = self.host_token_issuer.issue_invocation_target(
            actor,
            &target.lease.id,
            &target.lease.session_id,
            &target.spec.host_config_key(),
            &target.placement.home_region,
            target.placement.owner_epoch,
            grant,
            &target.lease.route,
        )?;
        let expires_at_ms = issued.expires_at_ms.min(i64::try_from(
            idle_expires_at_ms.min(target.lease.expires_at_ms),
        )?);
        if let Some(timings) = timings.as_deref_mut() {
            timings.invocation_token_issued_at_ms = Some(timings.elapsed_ms());
        }
        let route = target.lease.route;
        if let Some(timings) = timings {
            timings.route_selected_at_ms = Some(timings.elapsed_ms());
        }
        Ok(ActorTarget {
            host_state: target.host_state,
            home_region: target.placement.home_region,
            route,
            token: issued.token,
            owner_epoch: target.placement.owner_epoch,
            expires_at_ms,
        })
    }
}

#[derive(Clone)]
pub(super) struct ActorTarget {
    pub host_state: HostState,
    pub home_region: String,
    pub route: String,
    pub token: String,
    pub owner_epoch: u64,
    pub expires_at_ms: i64,
}

pub(super) struct TargetResolutionTimings {
    started_at: Instant,
    pub request_validated_at_ms: Option<f64>,
    pub client_authenticated_at_ms: Option<f64>,
    pub deployment_loaded_at_ms: Option<f64>,
    pub placement_loaded_at_ms: Option<f64>,
    pub lease_checked_at_ms: Option<f64>,
    pub host_ensured_at_ms: Option<f64>,
    pub placement_claimed_at_ms: Option<f64>,
    pub invocation_token_issued_at_ms: Option<f64>,
    pub route_selected_at_ms: Option<f64>,
}

impl TargetResolutionTimings {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            request_validated_at_ms: None,
            client_authenticated_at_ms: None,
            deployment_loaded_at_ms: None,
            placement_loaded_at_ms: None,
            lease_checked_at_ms: None,
            host_ensured_at_ms: None,
            placement_claimed_at_ms: None,
            invocation_token_issued_at_ms: None,
            route_selected_at_ms: None,
        }
    }

    pub fn elapsed_ms(&self) -> f64 {
        elapsed_ms(self.started_at)
    }
}

#[tonic::async_trait]
impl ActorControlPlaneService for ControlPlaneService {
    async fn execute(
        &self,
        request: Request<ControlPlaneRequest>,
    ) -> std::result::Result<Response<ControlPlaneReply>, Status> {
        let principal = self.auth.authenticate(&request).await?;
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let command = decode_command(request.into_inner())
            .map_err(|error| Status::invalid_argument(format!("invalid command: {error:#}")))?;
        let reply = match command {
            ControlPlaneCommand::SocketOperation { actor, operation } => {
                self.authorize_socket_host(&principal, &actor)
                    .await
                    .map_err(failed_precondition)?;
                let gateway = self
                    .gateway
                    .as_ref()
                    .ok_or_else(|| Status::unavailable("socket gateway is unavailable"))?;
                let reply = gateway
                    .connections
                    .operation(&actor, operation, &authorization)
                    .await
                    .map_err(failed_precondition)?;
                ControlPlaneCommandReply::SocketOperation { reply }
            }
            command => self
                .execute_command(&principal, command)
                .await
                .map_err(failed_precondition)?,
        };
        Ok(Response::new(encode_reply(reply).map_err(internal)?))
    }
}

impl ControlPlaneService {
    async fn execute_command(
        &self,
        principal: &ActorPrincipal,
        command: ControlPlaneCommand,
    ) -> Result<ControlPlaneCommandReply> {
        match command {
            ControlPlaneCommand::SocketOperation { .. } => {
                anyhow::bail!("socket operation requires authenticated routing")
            }
            ControlPlaneCommand::RequestTraces { traces, dropped } => {
                self.require_active_host(principal).await?;
                ensure!(
                    traces.len() <= crate::request_traces::TRACE_BATCH_SIZE,
                    "trace batch too large"
                );
                for trace in &traces {
                    trace.validate()?;
                }
                self.traces
                    .record(
                        &principal.actor.project_id,
                        principal.host_id.as_str(),
                        &principal.session_id,
                        traces,
                        dropped,
                    )
                    .await?;
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::InventoryChanged => {
                let owner = self
                    .placements
                    .get_owner(&principal.actor.storage_key())
                    .await?;
                ensure!(
                    owner.is_some_and(|owner| owner.owner == principal.host_id
                        && owner.lease.session_id == principal.session_id),
                    "host session does not match"
                );
                self.changes.notify().await;
                Ok(ControlPlaneCommandReply::Unit)
            }
            ControlPlaneCommand::RefreshStorageAccess => {
                self.require_active_host(principal).await?;
                let spec = self
                    .registry
                    .launch_spec(&principal.actor.project_id)
                    .await?
                    .context("deployment removed")?;
                ensure!(
                    principal.host_config_key.as_deref() == Some(spec.host_config_key().as_str()),
                    "host deployment changed"
                );
                let token = self
                    .runtime_access
                    .as_ref()
                    .context("direct storage is not configured")?
                    .issue(&principal.actor, spec.code_snapshot.as_deref())
                    .await?;
                let replacement_token = self
                    .host_token_issuer
                    .issue_host(
                        &principal.host_id,
                        &principal.session_id,
                        principal
                            .host_config_key
                            .as_deref()
                            .context("host configuration missing")?,
                        &principal.region,
                        &principal.actor,
                    )?
                    .token;
                Ok(ControlPlaneCommandReply::StorageAccess {
                    token,
                    replacement_token,
                })
            }
        }
    }

    pub(super) async fn authorize_socket_host(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
    ) -> Result<()> {
        actor.validate()?;
        ensure!(&principal.actor == actor, "host actor scope mismatch");
        self.require_active_host(principal).await?;
        Ok(())
    }

    async fn route_actor(
        &self,
        actor: &ActorKey,
        storage_region: &str,
        home_region: Option<&str>,
        mut timings: Option<&mut TargetResolutionTimings>,
    ) -> Result<RoutedActor> {
        self.validate_home_region(home_region)?;
        let spec = self
            .runtime_deployment(&actor.project_id)
            .await?
            .context("project has no registered actor code")?;
        let overrides_regions = spec
            .sandboxes
            .get(&actor.actor_name)
            .is_some_and(|options| options.regions.is_some());
        if !overrides_regions && let (Some(default), Some(assigned)) = (&self.region, home_region) {
            if default != assigned {
                return Err(RegionConflict.into());
            }
        }
        if let Some(timings) = timings.as_deref_mut() {
            timings.deployment_loaded_at_ms = Some(timings.elapsed_ms());
        }
        let (current, owner_hint) = self
            .placements
            .get_owner_with_hint(&actor.storage_key())
            .await?;
        if let (Some(assigned), Some(placement)) = (home_region, current.as_ref())
            && assigned != placement.home_region
        {
            return Err(RegionConflict.into());
        }
        let regions =
            placement_regions(&spec, actor, storage_region, home_region, current.as_ref())?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.placement_loaded_at_ms = Some(timings.elapsed_ms());
        }
        let lease_checked = current.as_ref().is_some_and(|placement| {
            host_matches_config(&placement.owner, &spec.host_config_key())
        });
        let active = self.active_target(&current, &spec).await?;
        if lease_checked && let Some(timings) = timings.as_deref_mut() {
            timings.lease_checked_at_ms = Some(timings.elapsed_ms());
        }
        if let Some(target) = active {
            return Ok(target);
        }
        let target = self
            .provision_in_regions(
                actor,
                &spec,
                current.as_ref(),
                owner_hint.as_ref(),
                &regions,
            )
            .await?;
        if let Some(timings) = timings.as_deref_mut() {
            timings.host_ensured_at_ms = Some(timings.elapsed_ms());
        }
        if let Some(timings) = timings {
            timings.placement_claimed_at_ms = Some(timings.elapsed_ms());
        }
        Ok(target)
    }

    async fn provision_in_regions(
        &self,
        actor: &ActorKey,
        spec: &HostLaunchSpec,
        current: Option<&ObjectPlacement>,
        owner_hint: Option<&OwnershipHint>,
        regions: &[String],
    ) -> Result<RoutedActor> {
        let mut last_error = None;
        for region in regions {
            match self
                .ensure_actor_host(actor, spec, current, region, owner_hint)
                .await
            {
                Ok(target) => return Ok(target),
                Err(error) => {
                    let current = self.placements.get_owner(&actor.storage_key()).await?;
                    if let Some(placement) = &current {
                        if !regions.contains(&placement.home_region) {
                            return Err(RegionConflict.into());
                        }
                        if let Some(mut target) = self.active_target(&current, spec).await? {
                            target.host_state = HostState::Cold;
                            return Ok(target);
                        }
                        return Err(error);
                    }
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.context("actor has no eligible sandbox region")?)
    }

    async fn ensure_actor_host(
        &self,
        actor: &ActorKey,
        spec: &HostLaunchSpec,
        current: Option<&ObjectPlacement>,
        requested_region: &str,
        owner_hint: Option<&OwnershipHint>,
    ) -> Result<RoutedActor> {
        let region = current
            .map(|p| p.home_region.clone())
            .unwrap_or_else(|| requested_region.to_owned());
        let (lease, owner_epoch) = self
            .provisioner
            .ensure_actor_host(spec, &region, actor, current.is_none(), owner_hint)
            .await?;
        ensure!(
            owner_epoch > 0,
            "host readiness returned no ownership epoch"
        );
        Ok(RoutedActor {
            host_state: HostState::Cold,
            placement: ObjectPlacement {
                lease: lease.clone(),
                object: actor.storage_key(),
                owner: lease.id.clone(),
                owner_epoch,
                home_region: region,
                state_version: 0,
                state_object: None,
                last_request_id: None,
            },
            lease,
            spec: spec.clone(),
        })
    }

    async fn require_active_host(&self, principal: &ActorPrincipal) -> Result<HostLease> {
        let placement = self.current_placement(&principal.actor).await?;
        ensure!(
            placement.owner == principal.host_id
                && placement.lease.id == principal.host_id
                && placement.home_region == principal.region,
            "host no longer owns actor"
        );
        let lease = placement.lease;
        ensure!(
            lease.expires_at_ms > crate::clock::Clock::now_ms(&crate::clock::SystemClock)?,
            "host lease is not active"
        );
        ensure!(
            lease.session_id == principal.session_id,
            "host lease belongs to another session"
        );
        Ok(lease)
    }

    async fn current_placement(&self, actor: &ActorKey) -> Result<ObjectPlacement> {
        self.placements
            .get_owner(&actor.storage_key())
            .await?
            .context("actor has no current placement")
    }

    async fn active_target(
        &self,
        current: &Option<ObjectPlacement>,
        spec: &HostLaunchSpec,
    ) -> Result<Option<RoutedActor>> {
        let Some(placement) = current else {
            return Ok(None);
        };
        if !host_matches_config(&placement.owner, &spec.host_config_key()) {
            return Ok(None);
        }
        let lease = &placement.lease;
        if lease.id != placement.owner
            || lease.expires_at_ms <= crate::clock::Clock::now_ms(&crate::clock::SystemClock)?
        {
            return Ok(None);
        }
        self.provisioner.wait_ready(&placement.owner).await?;
        Ok(Some(RoutedActor {
            host_state: HostState::Warm,
            placement: placement.clone(),
            lease: lease.clone(),
            spec: spec.clone(),
        }))
    }
}

fn select_target_region(current: Option<&ObjectPlacement>, requested: &str) -> Result<String> {
    if let Some(placement) = current {
        return Ok(placement.home_region.clone());
    }
    let region = super::regions::storage_region(requested).unwrap_or(FALLBACK_REGION);
    crate::placement::validate_region(region)?;
    Ok(region.into())
}

fn placement_regions(
    spec: &HostLaunchSpec,
    actor: &ActorKey,
    requested: &str,
    assignment: Option<&str>,
    current: Option<&ObjectPlacement>,
) -> Result<Vec<String>> {
    let allowed = spec
        .sandboxes
        .get(&actor.actor_name)
        .and_then(|options| options.regions.as_ref());
    if let Some(region) =
        assignment.or_else(|| current.map(|placement| placement.home_region.as_str()))
    {
        if allowed.is_some_and(|regions| !regions.iter().any(|allowed| allowed == region)) {
            return Err(RegionConflict.into());
        }
        return Ok(vec![region.into()]);
    }
    let requested = select_target_region(None, requested)?;
    let Some(allowed) = allowed else {
        return Ok(vec![requested]);
    };
    let mut regions = allowed.clone();
    regions.sort_by_cached_key(|region| {
        let identity =
            serde_json::to_vec(&(actor, region)).expect("placement identity is serializable");
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &identity);
        (region != &requested, digest.as_ref().to_vec())
    });
    Ok(regions)
}

#[derive(Debug)]
pub(super) struct RegionConflict;

impl std::fmt::Display for RegionConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "homeRegion conflicts with the control-plane region, allowed sandbox regions, or existing actor ownership",
        )
    }
}

impl std::error::Error for RegionConflict {}

#[async_trait]
pub(crate) trait HostProvisioner: Send + Sync {
    fn host_idle_timeout_ms(&self) -> u64;

    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        previous: Option<&HostLaunchSpec>,
        region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::contracts::PublicActorContract>,
    )>;
    async fn wait_ready(&self, _host: &HostId) -> Result<()> {
        Ok(())
    }

    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
    ) -> Result<(HostLease, u64)>;
    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination>;
}

pub(crate) struct SandboxHostProvisioner {
    runtime_image: Option<String>,
    pool: Option<Arc<crate::sandbox::pool::SparePool>>,
    runtime_access: Option<Arc<crate::bucket::access::RuntimeAccess>>,
    provider: Arc<dyn SandboxProvider>,
    runtime: HostSandboxRuntimeConfig,
    issuer: ActorJwtIssuer,
}

impl SandboxHostProvisioner {
    pub(crate) fn with_pool(mut self, pool: Arc<crate::sandbox::pool::SparePool>) -> Self {
        self.pool = Some(pool);
        self
    }

    pub(crate) fn with_runtime_access(
        mut self,
        access: Arc<crate::bucket::access::RuntimeAccess>,
    ) -> Self {
        self.runtime_access = Some(access);
        self
    }
    pub(crate) fn new(
        provider: Arc<dyn SandboxProvider>,
        runtime: HostSandboxRuntimeConfig,
        issuer: ActorJwtIssuer,
        runtime_image: Option<String>,
    ) -> Self {
        Self {
            runtime_image,
            pool: None,
            runtime_access: None,
            provider,
            runtime,
            issuer,
        }
    }
}

#[async_trait]
impl HostProvisioner for SandboxHostProvisioner {
    fn host_idle_timeout_ms(&self) -> u64 {
        self.runtime.host_idle_timeout_ms
    }

    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _previous: Option<&HostLaunchSpec>,
        _region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<super::contracts::PublicActorContract>,
    )> {
        let snapshot = source
            .code_snapshot
            .as_deref()
            .context("hosted deployments require a compiled bundle")?;
        let manifest = crate::artifacts::ArtifactManifest::decode(snapshot)?;
        self.runtime_access
            .as_ref()
            .context("hosted deployments require artifact storage access")?
            .validate_code(&manifest)?;
        let mut prepared = source.clone();
        prepared.image_ref = self
            .runtime_image
            .clone()
            .context("hosted deployments require a runtime image")?;
        prepared.validate()?;
        Ok((prepared, None))
    }
    async fn wait_ready(&self, host: &HostId) -> Result<()> {
        match &self.pool {
            Some(pool) => pool.wait_ready(host.as_str()).await,
            None => self.provider.wait_ready(host).await,
        }
    }

    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
    ) -> Result<(HostLease, u64)> {
        self.launch(spec, region, actor, new_actor, owner_hint)
            .await
    }

    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination> {
        if let Some(pool) = &self.pool {
            return Ok(HostTermination {
                provider: "gke".into(),
                resource_ids: pool.retire_config(&spec.host_config_key()).await?,
            });
        }
        self.provider
            .terminate_hosts(&TerminateHostsRequest {
                host_config_key: spec.host_config_key(),
                canonical_regions: regions.to_vec(),
            })
            .await
    }
}

impl SandboxHostProvisioner {
    async fn launch(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
    ) -> Result<(HostLease, u64)> {
        let started_at = Instant::now();
        let mut request = self.request(spec, region, actor)?;
        request.actor_is_new = new_actor;
        request.owner_hint = owner_hint.map(serde_json::to_string).transpose()?;
        let token = async {
            match &self.runtime_access {
                Some(access) => Ok(Some(
                    access
                        .bootstrap(region, actor, spec.code_snapshot.as_deref())
                        .await?,
                )),
                None => anyhow::Ok(None),
            }
        };
        let spare = async {
            match &self.pool {
                Some(pool) => {
                    pool.claim(spec, region, request.host_id.as_str(), &request.resources)
                        .await
                }
                None => Ok(None),
            }
        };
        let (runtime_config, spare) = tokio::join!(token, spare);
        request.spare = spare?;
        if let Some(pool) = &self.pool {
            if request.spare.is_none() {
                pool.reserve_host(
                    &request.session_id,
                    request.host_id.as_str(),
                    &spec.host_config_key(),
                )
                .await?;
            }
        }
        request.runtime_config = match runtime_config {
            Ok(config) => config,
            Err(error) => {
                if let Some(pool) = &self.pool {
                    let _ = pool.failed(request.host_id.as_str()).await;
                }
                return Err(error);
            }
        };
        let handle = match self.provider.ensure_host(&request).await {
            Ok(handle) => handle,
            Err(error) => {
                if let Some(pool) = &self.pool {
                    let _ = pool.failed(request.host_id.as_str()).await;
                }
                warn!(
                    event = "actor_host_provisioning",
                    project_id = %spec.project_id,
                    region,
                    host_id = %request.host_id,
                    started_at_ms = 0,
                    completed_at_ms = elapsed_ms(started_at),
                    outcome = "provider_failed",
                    error = %format!("{error:#}"),
                    "actor host provisioning failed"
                );
                return Err(error);
            }
        };
        let provisioning = handle.provisioning.clone();
        let owner_epoch = handle.owner_epoch;
        let ready = async {
            ensure!(
                owner_epoch > 0,
                "host readiness returned no ownership epoch"
            );
            if self.pool.is_some() {
                ensure!(
                    handle.host_id == request.host_id,
                    "provider returned a different host identity"
                );
            }
            let lease = ready_lease(&handle, &request)?;
            if let Some(pool) = &self.pool {
                let provisioning = provisioning
                    .as_ref()
                    .context("provider did not identify the assigned sandbox")?;
                pool.remember(
                    lease.id.as_str(),
                    &spec.host_config_key(),
                    &crate::sandbox::SpareHandle {
                        control_route: String::new(),
                        control_token: String::new(),
                        name: request
                            .spare
                            .as_ref()
                            .map(|spare| spare.name.clone())
                            .unwrap_or_else(|| format!("do-actor-{}", request.session_id)),
                        resource_id: provisioning.resource_id.clone(),
                        route: lease.route.clone(),
                        canonical_region: region.into(),
                    },
                )
                .await?;
            }
            anyhow::Ok(lease)
        }
        .await;
        let lease = match ready {
            Ok(lease) => lease,
            Err(error) => {
                if let Some(pool) = &self.pool {
                    let _ = pool.failed(request.host_id.as_str()).await;
                }
                return Err(error);
            }
        };
        let lease_validated_at_ms = elapsed_ms(started_at);
        info!(
            event = "actor_host_provisioning",
            project_id = %spec.project_id,
            region,
            host_id = %lease.id,
            provider = provisioning.as_ref().map(|value| value.provider.as_str()).unwrap_or("unknown"),
            provider_resource_id = provisioning.as_ref().map(|value| value.resource_id.as_str()).unwrap_or(""),
            provider_reused = provisioning.as_ref().is_some_and(|value| value.reused),
            started_at_ms = 0,
            provider_process_spawned_at_ms = provisioning.as_ref().and_then(|value| value.command_spawned_at_ms),
            provider_request_written_at_ms = provisioning.as_ref().and_then(|value| value.request_written_at_ms),
            provider_process_completed_at_ms = provisioning.as_ref().and_then(|value| value.process_completed_at_ms),
            provider_response_decoded_at_ms = provisioning.as_ref().and_then(|value| value.response_decoded_at_ms),
            provider_provider_started_at_ms = provisioning.as_ref().map(|value| value.started_at_ms),
            provider_input_parsed_at_ms = provisioning.as_ref().and_then(|value| value.input_parsed_at_ms),
            provider_sdk_loaded_at_ms = provisioning.as_ref().and_then(|value| value.sdk_loaded_at_ms),
            provider_resources_resolved_at_ms = provisioning.as_ref().and_then(|value| value.resources_resolved_at_ms),
            provider_sandbox_scheduled_at_ms = provisioning.as_ref().and_then(|value| value.sandbox_scheduled_at_ms),
            provider_host_ready_observed_at_ms = provisioning.as_ref().and_then(|value| value.host_ready_observed_at_ms),
            provider_route_read_at_ms = provisioning.as_ref().and_then(|value| value.route_read_at_ms),
            provider_provider_completed_at_ms = provisioning.as_ref().map(|value| value.completed_at_ms),
            lease_validated_at_ms,
            completed_at_ms = elapsed_ms(started_at),
            outcome = "ready",
            "actor host provisioning completed"
        );
        Ok((lease, owner_epoch))
    }

    fn request(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        actor: &ActorKey,
    ) -> Result<EnsureHostRequest> {
        let options = spec
            .sandboxes
            .get(&actor.actor_name)
            .cloned()
            .unwrap_or_default();
        options.validate()?;
        let defaults = self
            .pool
            .as_ref()
            .map(|pool| pool.config.resources.clone())
            .unwrap_or_default();
        let config_key = spec.host_config_key();
        let host_id = HostId::new(format!("host.v3.{}.{}", config_key, uuid::Uuid::new_v4()));
        let session_id = uuid::Uuid::new_v4().to_string();
        let host_token = self
            .issuer
            .issue_host(&host_id, &session_id, &config_key, region, actor)?
            .token;
        Ok(EnsureHostRequest {
            actor_is_new: false,
            owner_hint: None,
            actor: Some(actor.clone()),
            code_snapshot: spec.code_snapshot.clone(),
            spare: None,
            resources: options.resources(defaults),
            runtime_config: None,
            host_config_key: config_key,
            canonical_region: region.to_owned(),
            host_id,
            session_id,
            host_token,
            jwt_public_keys: self.issuer.verifier_keys_json()?,
            control_plane_url: self.runtime.control_plane_url.clone(),
            jwt_issuer: self.runtime.jwt_issuer.clone(),
            invocation_jwt_audience: self.runtime.invocation_jwt_audience.clone(),
            image_ref: spec.image_ref.clone(),
            working_directory: spec.working_directory.clone(),
            actor_entrypoint: spec
                .actor_entrypoint
                .clone()
                .or_else(|| spec.code_snapshot.as_ref().map(|_| "actors.mjs".into())),
            secret_refs: spec.secret_refs.clone(),
            host_idle_timeout_ms: options
                .idle_timeout_ms
                .unwrap_or(self.runtime.host_idle_timeout_ms),
        })
    }
}

fn ready_lease(
    handle: &crate::sandbox::ActorHostHandle,
    request: &EnsureHostRequest,
) -> Result<HostLease> {
    use crate::clock::{Clock, SystemClock};
    ensure!(
        handle.host_id == request.host_id && handle.canonical_region == request.canonical_region,
        "sandbox readiness scope mismatch"
    );
    validate_host_route(&handle.route)?;
    let lease = handle
        .lease
        .clone()
        .context("sandbox readiness omitted activation lease")?;
    ensure!(
        lease.id == request.host_id
            && lease.session_id == request.session_id
            && lease.route == handle.route,
        "sandbox readiness lease mismatch"
    );
    let now = SystemClock.now_ms()?;
    ensure!(
        lease.expires_at_ms > now
            && lease.expires_at_ms
                <= now.saturating_add(crate::host_leases::MAX_HOST_LEASE_DURATION_MS + 5_000),
        "sandbox readiness lease expired or invalid"
    );
    Ok(lease)
}

struct RoutedActor {
    host_state: HostState,
    placement: ObjectPlacement,
    lease: HostLease,
    spec: HostLaunchSpec,
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn host_matches_config(host: &HostId, config_key: &str) -> bool {
    host.as_str().starts_with(&format!("host.v3.{config_key}."))
}

fn validate_host_route(route: &str) -> Result<()> {
    let route = reqwest::Url::parse(route)?;
    ensure!(
        matches!(route.scheme(), "http" | "https") && route.host_str().is_some(),
        "host route must be an HTTP origin"
    );
    ensure!(
        route.username().is_empty() && route.password().is_none(),
        "host route must not contain credentials"
    );
    ensure!(
        route.path() == "/" && route.query().is_none() && route.fragment().is_none(),
        "host route must not contain a path, query, or fragment"
    );
    Ok(())
}

fn failed_precondition(error: impl std::fmt::Display) -> Status {
    Status::failed_precondition(error.to_string())
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/streaming_tests.rs"]
mod streaming_tests;

#[cfg(test)]
#[path = "../../tests/unit/control_plane/deployment.rs"]
mod deployment_tests;

#[cfg(test)]
#[path = "../../tests/unit/control_plane/service.rs"]
pub(crate) mod tests;
