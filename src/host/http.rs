use crate::request_tracking::HostState;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{info, warn};

use crate::{
    actor::{ActorExecutionResult, ActorInvocation, ActorKey, ActorSocketEffect},
    control_plane::{ActorJwtVerifier, ActorPrincipal},
    host::{ActorHost, HostId, sockets::HostSockets},
};

pub(crate) struct ActorHostHttpService {
    host: Arc<ActorHost>,
    session_id: String,
    auth: ActorJwtVerifier,
    sockets: Arc<HostSockets>,
}

impl ActorHostHttpService {
    pub(crate) fn new(
        host: Arc<ActorHost>,
        session_id: String,
        auth: ActorJwtVerifier,
        sockets: Arc<HostSockets>,
    ) -> Self {
        Self {
            host,
            session_id,
            auth,
            sockets,
        }
    }

    pub(crate) fn router(self) -> Router {
        Router::new()
            .route(
                "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/socket-events",
                post(socket_event),
            )
            .route(
                "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/invoke",
                post(invoke),
            )
            .route(
                "/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/socket-effects",
                post(publish),
            )
            .layer(DefaultBodyLimit::disable())
            .with_state(Arc::new(self))
    }

    fn authenticate(&self, headers: &HeaderMap) -> Result<ActorPrincipal, HttpError> {
        let authorization = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        self.auth
            .authenticate_authorization(authorization)
            .map_err(|_| {
                HttpError(
                    StatusCode::UNAUTHORIZED,
                    "invalid actor invocation ticket".into(),
                )
            })
    }

    fn authorize(
        &self,
        principal: &ActorPrincipal,
        actor: &ActorKey,
        owner_epoch: u64,
    ) -> Result<(), HttpError> {
        actor.validate().map_err(bad_request)?;
        validate_host_request(
            principal,
            self.host.id(),
            &self.session_id,
            actor,
            owner_epoch,
        )
    }

    async fn execute(
        &self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        host_state: HostState,
        routing_ms: f64,
    ) -> InvocationResponse {
        let actor = invocation.actor.clone();
        let request_id = invocation.request_id.clone();
        let (result, metadata) = self
            .host
            .invoke_actor(invocation, owner_epoch, host_state, routing_ms)
            .await;
        let outcome = match result {
            Ok(ActorExecutionResult::Completed { result, effects }) => {
                if !effects.is_empty()
                    && let Err(error) = self
                        .sockets
                        .publish_authorized(&actor, self.host.id(), owner_epoch, effects)
                        .await
                {
                    warn!(request_id, error = %format!("{error:#}"), "actor completed but socket delivery failed");
                    return InvocationReply::failed(
                        "outcome_unknown",
                        "actor completed but socket effects could not be delivered",
                    )
                    .with_metadata(metadata);
                }
                InvocationReply::Completed { result }
            }
            Ok(ActorExecutionResult::Failed { failure }) => InvocationReply::Failed {
                code: failure.code,
                message: failure.message,
            },
            Ok(ActorExecutionResult::Reroute) => InvocationReply::NotExecuted {
                reason: RejectionReason::StaleOwner,
            },
            Ok(ActorExecutionResult::HostUnavailable) => InvocationReply::NotExecuted {
                reason: RejectionReason::HostUnavailable,
            },
            Err(error) => {
                warn!(request_id, error = %format!("{error:#}"), "actor invocation failed before execution");
                InvocationReply::failed(
                    "unavailable",
                    "actor could not start because its state was unavailable",
                )
            }
        };
        outcome.with_metadata(metadata)
    }
}

async fn socket_event(
    State(service): State<Arc<ActorHostHttpService>>,
    Path(actor): Path<ActorKey>,
    headers: HeaderMap,
    Json(request): Json<crate::control_plane::socket_gateway::SocketEventRequest>,
) -> Result<Json<crate::control_plane::socket_gateway::SocketEventReply>, HttpError> {
    use crate::control_plane::socket_gateway::SocketEventReply;
    let principal = service.authenticate(&headers)?;
    service.authorize(&principal, &actor, request.owner_epoch)?;
    authorize_grant(
        principal
            .invocation
            .as_ref()
            .and_then(|capability| capability.grant.as_ref()),
        None,
    )?;
    if request.invocation.actor != actor {
        return Err(HttpError(
            StatusCode::FORBIDDEN,
            "socket event actor mismatch".into(),
        ));
    }
    let result = service
        .host
        .handle_socket_event_since(
            request.invocation,
            request.owner_epoch,
            std::time::Instant::now(),
            request.host_state,
            request.routing_ms,
        )
        .await;
    let reply = match result {
        Ok(ActorExecutionResult::Completed { effects, .. }) => {
            SocketEventReply::Completed { effects }
        }
        Ok(ActorExecutionResult::Reroute | ActorExecutionResult::HostUnavailable) => {
            SocketEventReply::NotExecuted
        }
        Ok(ActorExecutionResult::Failed { failure }) => SocketEventReply::Failed {
            message: failure.message,
        },
        Err(error) => SocketEventReply::Failed {
            message: format!("{error:#}"),
        },
    };
    Ok(Json(reply))
}

async fn invoke(
    State(service): State<Arc<ActorHostHttpService>>,
    Path(actor): Path<ActorKey>,
    headers: HeaderMap,
    body: Result<Json<InvokeRequest>, JsonRejection>,
) -> Result<Json<InvocationResponse>, HttpError> {
    let principal = service.authenticate(&headers)?;
    let Json(request) = body.map_err(json_error)?;
    service.authorize(&principal, &actor, request.owner_epoch)?;
    let grant = principal
        .invocation
        .as_ref()
        .and_then(|capability| capability.grant.as_ref());
    if let Err(error) = authorize_grant(grant, Some(&request.method)) {
        return Ok(Json(
            InvocationReply::failed("forbidden", &error.1).with_metadata(None),
        ));
    }
    if let Some(grant) = grant {
        info!(event = "delegated_actor_invocation", subject = %grant.subject, grant_id = %grant.grant_id, project_id = %actor.project_id, actor_name = %actor.actor_name, actor_id = %actor.actor_id, request_id = %request.request_id, method = %request.method);
    }
    let invocation = ActorInvocation {
        actor,
        request_id: request.request_id,
        method: request.method,
        args: request.args,
    };
    invocation.validate().map_err(bad_request)?;
    Ok(Json(
        service
            .execute(
                invocation,
                request.owner_epoch,
                request.host_state,
                request.routing_ms,
            )
            .await,
    ))
}

async fn publish(
    State(service): State<Arc<ActorHostHttpService>>,
    Path(actor): Path<ActorKey>,
    headers: HeaderMap,
    body: Result<Json<PublishRequest>, JsonRejection>,
) -> Result<StatusCode, HttpError> {
    let principal = service.authenticate(&headers)?;
    authorize_grant(
        principal
            .invocation
            .as_ref()
            .and_then(|capability| capability.grant.as_ref()),
        None,
    )?;
    let Json(request) = body.map_err(json_error)?;
    service.authorize(&principal, &actor, request.owner_epoch)?;
    crate::actor::validate_socket_effects(&request.effects).map_err(bad_request)?;
    service
        .sockets
        .publish_authorized(
            &actor,
            service.host.id(),
            request.owner_epoch,
            request.effects,
        )
        .await
        .map_err(|error| {
            warn!(error = %format!("{error:#}"), "socket effects could not be delivered");
            HttpError(
                StatusCode::SERVICE_UNAVAILABLE,
                "socket effects could not be delivered".into(),
            )
        })?;
    Ok(StatusCode::NO_CONTENT)
}

fn authorize_grant(
    grant: Option<&crate::control_plane::session::InvocationGrant>,
    method: Option<&str>,
) -> Result<(), HttpError> {
    if let Some(grant) = grant {
        let allowed = method.is_some_and(|method| {
            !matches!(
                method,
                "onConnect" | "onMessage" | "onDisconnect" | "connect" | "broadcast" | "then"
            ) && grant.methods.iter().any(|allowed| allowed == method)
        });
        if !allowed {
            return Err(HttpError(
                StatusCode::FORBIDDEN,
                "operation is outside the actor grant".into(),
            ));
        }
    }
    Ok(())
}

fn validate_host_request(
    principal: &ActorPrincipal,
    host_id: &HostId,
    session_id: &str,
    actor: &ActorKey,
    owner_epoch: u64,
) -> Result<(), HttpError> {
    if owner_epoch == 0 || owner_epoch > 9_007_199_254_740_991 {
        return Err(HttpError(
            StatusCode::BAD_REQUEST,
            "actor ownership capability is incomplete".into(),
        ));
    }
    if principal.host_id != *host_id
        || principal.session_id != session_id
        || principal.actor != *actor
    {
        return Err(HttpError(
            StatusCode::FORBIDDEN,
            "actor credential belongs to another actor or host session".into(),
        ));
    }
    if let Some(capability) = &principal.invocation
        && (capability.actor != *actor
            || capability.host_id != *host_id
            || capability.owner_epoch != owner_epoch)
    {
        return Err(HttpError(
            StatusCode::FORBIDDEN,
            "actor invocation does not match its direct capability".into(),
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct InvokeRequest {
    #[serde(default)]
    host_state: HostState,
    routing_ms: f64,
    request_id: String,
    owner_epoch: u64,
    method: String,
    args: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublishRequest {
    owner_epoch: u64,
    effects: Vec<ActorSocketEffect>,
}

#[derive(Serialize)]
struct InvocationResponse {
    #[serde(flatten)]
    outcome: InvocationReply,
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<crate::request_tracking::RequestMetadata>,
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InvocationReply {
    Completed {
        result: Value,
    },
    Failed {
        code: String,
        message: String,
    },
    /// The invocation did not execute and cannot execute later from a queue.
    NotExecuted {
        reason: RejectionReason,
    },
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum RejectionReason {
    StaleOwner,
    HostUnavailable,
}

impl InvocationReply {
    fn with_metadata(
        self,
        metadata: Option<crate::request_tracking::RequestMetadata>,
    ) -> InvocationResponse {
        InvocationResponse {
            outcome: self,
            metadata,
        }
    }

    fn failed(code: &str, message: &str) -> Self {
        Self::Failed {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug)]
struct HttpError(StatusCode, String);
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (
            self.0,
            Json(serde_json::json!({"error": {"code": self.0.as_str(), "message": self.1}})),
        )
            .into_response()
    }
}
fn bad_request(error: anyhow::Error) -> HttpError {
    HttpError(StatusCode::BAD_REQUEST, error.to_string())
}
fn json_error(error: JsonRejection) -> HttpError {
    HttpError(error.status(), error.body_text())
}

#[cfg(test)]
#[path = "../../tests/unit/host/http.rs"]
mod tests;
