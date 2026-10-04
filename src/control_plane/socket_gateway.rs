use super::{
    service::{ActorTarget, ControlPlaneService},
    socket_directory::{GATEWAY_LEASE, GatewayOwner, SocketDirectory},
    socket_ticket::SocketTicket,
};
use crate::request_tracking::HostState;
use crate::{
    actor::{ActorKey, ActorSocketEffect, ActorSocketInvocation},
    sockets::{SocketRegistry, browser::SocketDispatcher},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use axum::{
    Json,
    extract::State,
    http::{HeaderMap, StatusCode, header},
};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(crate) use crate::sockets::operations::{SocketOperation, SocketOperationReply};
#[derive(Serialize, Deserialize)]
pub(crate) struct SocketEventRequest {
    #[serde(default)]
    pub host_state: HostState,
    pub owner_epoch: u64,
    pub invocation: ActorSocketInvocation,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum SocketEventReply {
    Completed { effects: Vec<ActorSocketEffect> },
    NotExecuted,
    Failed { message: String },
}

#[derive(Serialize, Deserialize)]
pub(super) struct GatewayOperationRequest {
    actor: ActorKey,
    gateway_id: String,
    operation: SocketOperation,
}

type ActorRoute = Arc<tokio::sync::Mutex<Option<ActorTarget>>>;

pub(super) struct SocketGateway {
    pub owner: GatewayOwner,
    pub registry: SocketRegistry,
    pub stop: CancellationToken,
    pub(super) directory: Arc<dyn SocketDirectory>,
    deadline: Mutex<Instant>,
    owners: moka::future::Cache<ActorKey, GatewayOwner>,
    routes: moka::future::Cache<ActorKey, ActorRoute>,
    pub(super) http: reqwest::Client,
}

impl SocketGateway {
    pub(super) async fn start(
        route: String,
        directory: Arc<dyn SocketDirectory>,
        max_connections: usize,
        accepts_rooms: bool,
        stop: CancellationToken,
    ) -> Result<Arc<Self>> {
        super::gateway::backend_origin(&route)?;
        let owner = GatewayOwner {
            id: uuid::Uuid::new_v4().to_string(),
            route,
        };
        let started = Instant::now();
        directory.register(&owner, accepts_rooms).await?;
        let gateway = Arc::new(Self {
            owner,
            directory,
            stop,
            registry: SocketRegistry::with_max_connections(max_connections),
            deadline: Mutex::new(started + GATEWAY_LEASE - Duration::from_secs(5)),
            owners: moka::future::Cache::builder()
                .max_capacity(100_000)
                .time_to_live(Duration::from_secs(5))
                .build(),
            routes: moka::future::Cache::builder()
                .max_capacity(100_000)
                .time_to_idle(Duration::from_secs(300))
                .build(),
            http: reqwest::Client::builder()
                .http2_prior_knowledge()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(5))
                .build()?,
        });
        gateway.maintain_lease();
        Ok(gateway)
    }

    fn maintain_lease(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let stop = self.stop.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = tokio::time::sleep(Duration::from_secs(5)) => {} }
                let Some(gateway) = weak.upgrade() else {
                    break;
                };
                if gateway.ensure_authority().is_err() {
                    stop.cancel();
                    break;
                }
                let started = Instant::now();
                let renewed = tokio::time::timeout(
                    Duration::from_secs(5),
                    gateway.directory.renew(&gateway.owner),
                )
                .await;
                match renewed {
                    Ok(Ok(())) if gateway.ensure_authority().is_ok() => {
                        *gateway.deadline.lock().unwrap() =
                            started + GATEWAY_LEASE - Duration::from_secs(5)
                    }
                    _ => {
                        tracing::warn!(gateway_id = %gateway.owner.id, "socket gateway lease renewal failed");
                    }
                }
            }
        });
    }

    pub(super) fn ensure_authority(&self) -> Result<()> {
        ensure!(
            !self.stop.is_cancelled() && Instant::now() < *self.deadline.lock().unwrap(),
            "socket gateway lease expired"
        );
        Ok(())
    }

    pub(super) async fn owner(&self, actor: &ActorKey) -> Result<GatewayOwner> {
        self.ensure_authority()?;
        self.owners
            .try_get_with(actor.clone(), self.directory.claim(actor, &self.owner))
            .await
            .map_err(|error| anyhow::anyhow!("{error:#}"))
    }

    pub(super) async fn operation(
        &self,
        actor: &ActorKey,
        operation: SocketOperation,
        authorization: &str,
    ) -> Result<SocketOperationReply> {
        let owner = self.owner(actor).await?;
        if owner.id == self.owner.id {
            return self.apply(actor, operation).await;
        }
        let response = self
            .http
            .post(format!(
                "{}/internal/socket-operation",
                owner.route.trim_end_matches('/')
            ))
            .header(header::AUTHORIZATION, authorization)
            .json(&GatewayOperationRequest {
                actor: actor.clone(),
                gateway_id: owner.id,
                operation,
            })
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!(
                "gateway operation failed ({status}): {}",
                response.text().await?
            );
        }
        response
            .json()
            .await
            .context("read gateway operation reply")
    }

    pub(super) async fn apply(
        &self,
        actor: &ActorKey,
        operation: SocketOperation,
    ) -> Result<SocketOperationReply> {
        self.ensure_authority()?;
        match operation {
            SocketOperation::Publish { effects } => {
                crate::actor::validate_socket_effects(&effects)?;
                self.registry.apply(actor, effects).await;
                Ok(SocketOperationReply::Published)
            }
            SocketOperation::Connections { tag } => Ok(SocketOperationReply::Connections {
                connections: self
                    .registry
                    .connections_with_tag(actor, tag.as_deref())
                    .await,
            }),
            SocketOperation::Count => Ok(SocketOperationReply::Count {
                count: self.registry.count(actor).await,
            }),
        }
    }

    async fn target(
        &self,
        service: &ControlPlaneService,
        ticket: &SocketTicket,
    ) -> Result<ActorTarget> {
        let route = self.route(&ticket.actor).await;
        let mut cached = route.lock().await;
        let now = crate::clock::Clock::now_ms(&crate::clock::SystemClock)? as i64;
        if let Some(target) = cached.as_ref().filter(|target| target.expires_at_ms > now) {
            return Ok(target.clone());
        }
        let target = service
            .resolve_actor_route(&ticket.actor, ticket.home_region.as_deref(), None, None)
            .await?;
        let mut cached_target = target.clone();
        cached_target.host_state = HostState::Warm;
        *cached = Some(cached_target);
        Ok(target)
    }

    async fn route(&self, actor: &ActorKey) -> ActorRoute {
        self.routes
            .get_with(actor.clone(), async { Arc::default() })
            .await
    }

    async fn invalidate(&self, actor: &ActorKey, target: &ActorTarget) {
        let route = self.route(actor).await;
        let mut cached = route.lock().await;
        if cached.as_ref().is_some_and(|current| {
            current.owner_epoch == target.owner_epoch && current.route == target.route
        }) {
            *cached = None;
        }
    }

    async fn dispatch(
        &self,
        service: &ControlPlaneService,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        let retry_deadline = Instant::now() + super::CONTROL_PLANE_REQUEST_TIMEOUT;
        let mut host_state = HostState::Warm;
        loop {
            self.ensure_authority()?;
            ensure!(
                Instant::now() < retry_deadline,
                "actor remained unavailable during socket handoff"
            );
            let target = tokio::time::timeout_at(retry_deadline, self.target(service, ticket))
                .await
                .context("actor remained unavailable during socket handoff")?;
            let target = match target {
                Ok(target) => target,
                Err(error) if error.is::<crate::sandbox::HostNotReady>() => {
                    self.wait_to_retry().await?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if target.host_state == HostState::Cold {
                host_state = HostState::Cold;
            }
            let actor = &ticket.actor;
            let url = format!(
                "{}/v1/projects/{}/actors/{}/{}/socket-events",
                target.route.trim_end_matches('/'),
                actor.project_id,
                actor.actor_name,
                actor.actor_id
            );
            let reply = self
                .http
                .post(url)
                .bearer_auth(&target.token)
                .json(&SocketEventRequest {
                    host_state,
                    owner_epoch: target.owner_epoch,
                    invocation: invocation.clone(),
                })
                .send()
                .await;
            match reply {
                Err(error) if error.is_connect() => {
                    self.invalidate(actor, &target).await;
                }
                Err(error) => {
                    return Err(error)
                        .context("socket event outcome unknown; event was not replayed");
                }
                Ok(reply)
                    if matches!(
                        reply.status(),
                        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
                    ) =>
                {
                    self.invalidate(actor, &target).await;
                }
                Ok(reply) => match reply.error_for_status()?.json::<SocketEventReply>().await? {
                    SocketEventReply::Completed { effects } => {
                        self.ensure_authority()?;
                        return Ok(effects);
                    }
                    SocketEventReply::NotExecuted => {
                        self.invalidate(actor, &target).await;
                    }
                    SocketEventReply::Failed { message } => {
                        anyhow::bail!("socket handler failed: {message}")
                    }
                },
            }
            self.wait_to_retry().await?;
        }
    }

    async fn wait_to_retry(&self) -> Result<()> {
        tokio::select! {
            _ = self.stop.cancelled() => anyhow::bail!("socket gateway stopped"),
            _ = tokio::time::sleep(Duration::from_millis(100)) => Ok(()),
        }
    }
}

pub(super) struct GatewaySocketDispatcher {
    pub gateway: Arc<SocketGateway>,
    pub service: ControlPlaneService,
}

#[async_trait]
impl SocketDispatcher for GatewaySocketDispatcher {
    fn ensure_authority(&self) -> Result<()> {
        self.gateway.ensure_authority()
    }
    async fn dispatch(
        &self,
        ticket: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        let result = self
            .gateway
            .dispatch(&self.service, ticket, invocation)
            .await;
        if let Err(error) = &result {
            tracing::warn!(actor = %ticket.actor.storage_key().as_str(), error = %format!("{error:#}"), "socket event delivery failed");
        }
        result
    }
    fn notify(&self, ticket: &SocketTicket, event: &crate::actor::ActorSocketEvent) {
        self.service
            .deliver_socket_message_event(&ticket.actor, None, event);
    }
}

pub(super) async fn internal_operation(
    State(service): State<ControlPlaneService>,
    headers: HeaderMap,
    Json(request): Json<GatewayOperationRequest>,
) -> Result<Json<SocketOperationReply>, (StatusCode, String)> {
    let result = async {
        let authorization = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        let principal = service.auth.authenticate_authorization(authorization)?;
        service
            .authorize_socket_host(&principal, &request.actor)
            .await?;
        let gateway = &service
            .gateway
            .as_ref()
            .context("socket gateway unavailable")?
            .connections;
        ensure!(
            gateway.owner(&request.actor).await?.id == gateway.owner.id
                && gateway.owner.id == request.gateway_id,
            "socket gateway ownership changed"
        );
        gateway.apply(&request.actor, request.operation).await
    }
    .await;
    result
        .map(Json)
        .map_err(|error: anyhow::Error| (StatusCode::FORBIDDEN, format!("{error:#}")))
}
