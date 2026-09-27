use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::actor::ActorKey;

use super::{
    MAX_CONTROL_PLANE_MESSAGE_BYTES,
    admin::{AdminService, HostLaunchSpec},
    contracts::PublicActorContract,
    service::{ControlPlaneService, TargetResolutionTimings},
};

#[derive(Clone)]
struct PublicApiState {
    invocations: ControlPlaneService,
    admin: AdminService,
}

pub(super) fn router(invocations: ControlPlaneService, admin: AdminService) -> Router {
    let contracts = super::contract_api::router(admin.clone());
    Router::new()
        .route("/openapi.yaml", get(openapi))
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/projects/{project_id}/sessions", post(issue_session))
        .route(
            "/v1/projects/{project_id}/deployment",
            put(register_deployment)
                .get(get_deployment)
                .delete(delete_deployment),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/find-actor",
            post(find_actor),
        )
        .route(
            "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/find-websocket",
            post(find_websocket),
        )
        .layer(DefaultBodyLimit::max(MAX_CONTROL_PLANE_MESSAGE_BYTES))
        .with_state(PublicApiState { invocations, admin })
        .merge(contracts)
}

async fn openapi() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/yaml")],
        include_str!("../../docs/reference/openapi.yaml"),
    )
}

async fn issue_session(
    State(state): State<PublicApiState>,
    path: Path<ProjectPath>,
    headers: HeaderMap,
    request: Result<Json<IssueSessionRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    state
        .admin
        .authorize_session_issuance(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
        )
        .map_err(|_| ApiError::unauthorized("administrative credential is required"))?;
    let project = project_id(path)?;
    let Json(request) = request.map_err(ApiError::json)?;
    let issued = state
        .admin
        .issue_session(project.clone(), request.subject, request.expires_at_ms)
        .map_err(ApiError::bad_request)?;
    if state
        .admin
        .current_deployment(&project)
        .await
        .map_err(ApiError::internal)?
        .is_none()
    {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "not_found",
            "project deployment not found",
        ));
    }
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({"token":issued.token, "expiresAtMs":issued.expires_at_ms})),
    )
        .into_response())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct IssueSessionRequest {
    subject: String,
    expires_at_ms: i64,
}

async fn find_websocket(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    request: Result<Json<FindWebSocketRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Json(request) = request.map_err(ApiError::json)?;
    state
        .invocations
        .validate_home_region(request.home_region.as_deref())
        .map_err(ApiError::assignment)?;
    let mut grant = super::socket_ticket::SocketGrant {
        actor: path.into_actor(),
        region: request
            .home_region
            .clone()
            .unwrap_or_else(|| state.invocations.default_region().into()),
        target: None,
        metadata: request.metadata,
        authorization_lifetime_ms: request.authorization_lifetime_ms,
    };
    grant.validate().map_err(ApiError::bad_request)?;
    let (region, target, credentials) = state
        .invocations
        .socket_destination(&grant.actor, &grant.region, request.home_region.as_deref())
        .await
        .map_err(ApiError::routing)?;
    grant.region = region;
    grant.target = Some(target);
    let issued = state
        .admin
        .issue_direct_socket(grant, credentials)
        .map_err(ApiError::internal)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(issued)).into_response())
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FindWebSocketRequest {
    #[serde(default)]
    home_region: Option<String>,
    metadata: Value,
    #[serde(default = "socket_authorization_lifetime")]
    authorization_lifetime_ms: i64,
}

fn socket_authorization_lifetime() -> i64 {
    900_000
}

async fn get_deployment(
    State(state): State<PublicApiState>,
    path: Path<ProjectPath>,
    headers: HeaderMap,
) -> Result<Json<RegisterDeploymentRequest>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let project = project_id(path)?;
    let spec = state
        .admin
        .current_deployment(&project)
        .await
        .map_err(ApiError::internal)?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "deployment not found"))?;
    let source = spec
        .source
        .unwrap_or_else(|| super::admin::DeploymentSource {
            image_ref: spec.image_ref,
            working_directory: spec.working_directory,
            actor_entrypoint: spec.actor_entrypoint,
        });
    Ok(Json(RegisterDeploymentRequest {
        contract: None,
        image_ref: source.image_ref,
        working_directory: source.working_directory,
        actor_entrypoint: source.actor_entrypoint,
        secret_refs: spec.secret_refs,
    }))
}

async fn delete_deployment(
    State(state): State<PublicApiState>,
    path: Path<ProjectPath>,
    headers: HeaderMap,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let project = project_id(path)?;
    let changed = state
        .invocations
        .delete_deployment(&state.admin, &project)
        .await
        .map_err(ApiError::internal)?;
    Ok(Json(DeploymentReply { changed }))
}

async fn register_deployment(
    State(state): State<PublicApiState>,
    path: Path<ProjectPath>,
    headers: HeaderMap,
    request: Result<Json<RegisterDeploymentRequest>, JsonRejection>,
) -> Result<Json<DeploymentReply>, ApiError> {
    authorized_admin(&state.admin, &headers)?;
    let Json(request) = request.map_err(ApiError::json)?;
    let contract = request
        .contract
        .map(PublicActorContract::new)
        .transpose()
        .map_err(ApiError::bad_request)?;
    let spec = HostLaunchSpec {
        project_id: project_id(path)?,
        source: None,
        code_snapshot: None,
        image_ref: request.image_ref,
        working_directory: request.working_directory,
        actor_entrypoint: request.actor_entrypoint,
        secret_refs: request.secret_refs,
    };
    let changed = state
        .invocations
        .deploy_source(&state.admin, &spec, contract.as_ref())
        .await
        .map_err(ApiError::bad_request)?;
    Ok(Json(DeploymentReply { changed }))
}

async fn find_actor(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    request: Result<Json<FindActorRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let mut timings = TargetResolutionTimings::new();
    let request_id = headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned();
    let actor = path.into_actor();
    let grant = state
        .admin
        .authorize_discovery(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
            &actor.project_id,
        )
        .map_err(|_| ApiError::unauthorized("actor discovery credential was rejected"))?;
    let Json(request) = request.map_err(ApiError::json)?;
    actor.validate().map_err(ApiError::bad_request)?;
    state
        .invocations
        .validate_home_region(request.home_region.as_deref())
        .map_err(ApiError::assignment)?;
    let result: Result<Json<ActorTargetReply>, ApiError> = async {
        actor.validate().map_err(ApiError::bad_request)?;
        timings.request_validated_at_ms = Some(timings.elapsed_ms());
        timings.client_authenticated_at_ms = Some(timings.elapsed_ms());
        let grant = match grant {
            Some(grant) => {
                let contract = state
                    .admin
                    .deployment_contract(&actor.project_id)
                    .await
                    .map_err(ApiError::internal)?
                    .ok_or_else(|| {
                        ApiError::new(
                            StatusCode::NOT_FOUND,
                            "not_found",
                            "actor contract not found",
                        )
                    })?;
                Some(
                    grant
                        .invocation(
                            &actor,
                            contract.rpc_methods(&actor.actor_name).map_err(|_| {
                                ApiError::new(StatusCode::NOT_FOUND, "not_found", "actor not found")
                            })?,
                        )
                        .map_err(|_| {
                            ApiError::new(
                                StatusCode::FORBIDDEN,
                                "forbidden",
                                "actor has no published RPC methods",
                            )
                        })?,
                )
            }
            None => None,
        };
        let target = state
            .invocations
            .resolve_actor_target_timed(&actor, request.home_region.as_deref(), &mut timings, grant)
            .await
            .map_err(ApiError::routing)?;
        Ok(Json(ActorTargetReply {
            home_region: target.home_region,
            route: target.route,
            token: target.token,
            owner_epoch: target.owner_epoch,

            expires_at_ms: target.expires_at_ms,
        }))
    }
    .await;
    let completed_at_ms = timings.elapsed_ms();
    match &result {
        Ok(_) => info!(
            event = "actor_target_resolution",
            request_id,
            actor_name = %actor.actor_name,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            client_authenticated_at_ms = timings.client_authenticated_at_ms,
            deployment_loaded_at_ms = timings.deployment_loaded_at_ms,
            placement_loaded_at_ms = timings.placement_loaded_at_ms,
            lease_checked_at_ms = timings.lease_checked_at_ms,
            host_ensured_at_ms = timings.host_ensured_at_ms,
            placement_claimed_at_ms = timings.placement_claimed_at_ms,

            invocation_token_issued_at_ms = timings.invocation_token_issued_at_ms,
            route_selected_at_ms = timings.route_selected_at_ms,
            completed_at_ms,
            outcome = "resolved",
            "actor target resolution completed"
        ),
        Err(error) => warn!(
            event = "actor_target_resolution",
            request_id,
            actor_name = %actor.actor_name,
            actor_id = %actor.actor_id,
            started_at_ms = 0,
            request_validated_at_ms = timings.request_validated_at_ms,
            client_authenticated_at_ms = timings.client_authenticated_at_ms,
            deployment_loaded_at_ms = timings.deployment_loaded_at_ms,
            placement_loaded_at_ms = timings.placement_loaded_at_ms,
            lease_checked_at_ms = timings.lease_checked_at_ms,
            host_ensured_at_ms = timings.host_ensured_at_ms,
            placement_claimed_at_ms = timings.placement_claimed_at_ms,

            invocation_token_issued_at_ms = timings.invocation_token_issued_at_ms,
            route_selected_at_ms = timings.route_selected_at_ms,
            completed_at_ms,
            outcome = "failed",
            error_code = %error.code,
            error = %error.message,
            "actor target resolution failed"
        ),
    }
    result.map(|target| ([(header::CACHE_CONTROL, "no-store")], target).into_response())
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FindActorRequest {
    home_region: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct ActorPath {
    project_id: String,
    actor_name: String,
    actor_id: String,
}

impl ActorPath {
    pub(super) fn into_actor(self) -> ActorKey {
        ActorKey {
            project_id: self.project_id,
            actor_name: self.actor_name,
            actor_id: self.actor_id,
        }
    }
}

#[derive(Deserialize)]
pub(super) struct ProjectPath {
    pub(super) project_id: String,
}

pub(super) fn project_id(path: Path<ProjectPath>) -> Result<String, ApiError> {
    let Path(ProjectPath {
        project_id: project,
    }) = path;
    super::admin::validate_component("project ID", &project, 64).map_err(ApiError::bad_request)?;
    Ok(project)
}

pub(super) fn authorized_admin(admin: &AdminService, headers: &HeaderMap) -> Result<(), ApiError> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    admin
        .authenticate(authorization)
        .map_err(|_| ApiError::unauthorized("admin credential was rejected"))?;
    Ok(())
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RegisterDeploymentRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contract: Option<Value>,
    image_ref: String,
    working_directory: String,
    #[serde(default)]
    actor_entrypoint: Option<String>,
    #[serde(default)]
    secret_refs: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeploymentReply {
    changed: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ActorTargetReply {
    home_region: String,
    route: String,
    token: String,
    owner_epoch: u64,

    expires_at_ms: i64,
}

pub(super) struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
}

impl ApiError {
    fn json(error: JsonRejection) -> Self {
        if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload_too_large",
                error.body_text(),
            )
        } else {
            Self::bad_request(error.body_text())
        }
    }

    pub(super) fn assignment(error: anyhow::Error) -> Self {
        if error.is::<super::service::RegionConflict>() {
            Self::conflict(error.to_string())
        } else {
            Self::bad_request(error)
        }
    }

    pub(super) fn routing(error: anyhow::Error) -> Self {
        if error.is::<super::service::RegionConflict>() {
            Self::conflict(error.to_string())
        } else {
            Self::unavailable(format!("actor host is unavailable: {error:#}"))
        }
    }

    pub(super) fn bad_request(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            error.to_string(),
        )
    }

    fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthenticated", message)
    }

    fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub(super) fn unavailable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, "unavailable", message)
    }

    pub(super) fn internal(error: impl std::fmt::Display) -> Self {
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            error.to_string(),
        )
    }

    pub(super) fn new(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            code: code.into(),
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ErrorDocument {
                error: ErrorBody {
                    code: self.code,
                    message: self.message,
                },
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct ErrorDocument {
    error: ErrorBody,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorBody {
    code: String,
    message: String,
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/public_api.rs"]
mod tests;
