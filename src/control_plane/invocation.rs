use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::public_api::{
    ActorPath, ApiError, FindActorRequest, PublicApiState, resolve_actor_target,
};
use crate::actor::ActorInvocation;

pub(super) async fn invoke(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    body: Result<Json<InvokeRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = path.into_actor();
    let Json(request) = body.map_err(ApiError::json)?;
    if let Some(epoch) = request.owner_epoch {
        return invoke_cached(&state, actor, &headers, request, epoch).await;
    }
    state
        .admin
        .authorize_discovery(
            headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or(""),
            &actor.project_id,
        )
        .map_err(|_| ApiError::unauthorized("actor invocation credential was rejected"))?;
    let invocation = ActorInvocation {
        actor,
        request_id: request.request_id,
        method: request.method,
        args: request.args,
    };
    invocation.validate().map_err(ApiError::bad_request)?;
    let target = resolve_actor_target(
        &state,
        &invocation.actor,
        &headers,
        Ok(Json(FindActorRequest {
            home_region: request.home_region,
        })),
    )
    .await?;
    let outcome = dispatch(
        &state.hosts,
        &target.backend_route,
        &target.token,
        target.owner_epoch,
        &invocation,
    )
    .await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"target": target, "outcome": outcome})),
    )
        .into_response())
}

async fn invoke_cached(
    state: &PublicApiState,
    actor: crate::actor::ActorKey,
    headers: &HeaderMap,
    request: InvokeRequest,
    epoch: u64,
) -> Result<Response, ApiError> {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let gateway = state
        .invocations
        .gateway
        .as_ref()
        .ok_or_else(|| ApiError::unauthorized("gateway is not configured"))?;
    let route = gateway
        .invocation_route(&actor, epoch, authorization)
        .map_err(|_| ApiError::unauthorized("actor invocation credential was rejected"))?;
    let token = authorization
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::unauthorized("bearer token required"))?;
    let invocation = ActorInvocation {
        actor,
        request_id: request.request_id,
        method: request.method,
        args: request.args,
    };
    invocation.validate().map_err(ApiError::bad_request)?;
    let outcome = dispatch(&state.hosts, &route, token, epoch, &invocation).await?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(outcome)).into_response())
}

pub(super) async fn publish(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    body: Result<Json<PublishRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = path.into_actor();
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let gateway = state
        .invocations
        .gateway
        .as_ref()
        .ok_or_else(|| ApiError::unauthorized("gateway is not configured"))?;
    if authorization.is_empty() {
        return Err(ApiError::unauthorized("bearer token required"));
    }
    let Json(request) = body.map_err(ApiError::json)?;
    let route = gateway
        .invocation_route(&actor, request.owner_epoch, authorization)
        .map_err(|_| ApiError::unauthorized("actor invocation credential was rejected"))?;
    crate::actor::validate_socket_effects(&request.effects).map_err(ApiError::bad_request)?;
    let url = format!(
        "{}/v1/projects/{}/actors/{}/{}/socket-effects",
        route.trim_end_matches('/'),
        actor.project_id,
        actor.actor_name,
        actor.actor_id
    );
    let response = state
        .hosts
        .post(url)
        .header(header::AUTHORIZATION, authorization)
        .json(&request)
        .send()
        .await
        .map_err(|_| outcome_unknown())?;
    let status = response.status();
    let body = response.bytes().await.map_err(|_| outcome_unknown())?;
    Ok((status, [(header::CACHE_CONTROL, "no-store")], body).into_response())
}

#[derive(serde::Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PublishRequest {
    owner_epoch: u64,
    effects: Vec<crate::actor::ActorSocketEffect>,
}

async fn dispatch(
    client: &reqwest::Client,
    route: &str,
    token: &str,
    owner_epoch: u64,
    invocation: &ActorInvocation,
) -> Result<Value, ApiError> {
    let actor = &invocation.actor;
    let url = format!(
        "{}/v1/projects/{}/actors/{}/{}/invoke",
        route.trim_end_matches('/'),
        actor.project_id,
        actor.actor_name,
        actor.actor_id
    );
    let response = client
        .post(url)
        .bearer_auth(token)
        .json(&json!({
            "requestId":invocation.request_id, "ownerEpoch":owner_epoch,
            "method":invocation.method, "args":invocation.args,
        }))
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) if error.is_connect() => {
            return Ok(json!({"type":"not_executed", "reason":"upstream_not_reached"}));
        }
        Err(_) => return Err(outcome_unknown()),
    };
    if response.status() == StatusCode::UNAUTHORIZED {
        return Ok(json!({"type":"unauthenticated"}));
    }
    if !response.status().is_success() {
        return Err(outcome_unknown());
    }
    let reply = read_reply(response).await?;
    match reply.get("type").and_then(Value::as_str) {
        Some("not_executed")
            if matches!(
                reply["reason"].as_str(),
                Some("stale_owner" | "host_unavailable" | "upstream_not_reached")
            ) =>
        {
            Ok(reply)
        }
        Some("completed") if reply.get("result").is_some() => Ok(reply),
        Some("failed")
            if reply["code"].as_str().is_some_and(|code| !code.is_empty())
                && reply["message"].is_string() =>
        {
            Ok(reply)
        }
        _ => Err(outcome_unknown()),
    }
}

async fn read_reply(response: reqwest::Response) -> Result<Value, ApiError> {
    response.json().await.map_err(|_| outcome_unknown())
}

fn outcome_unknown() -> ApiError {
    ApiError::new(
        StatusCode::BAD_GATEWAY,
        "outcome_unknown",
        "actor invocation was dispatched but its outcome could not be confirmed",
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InvokeRequest {
    request_id: String,
    owner_epoch: Option<u64>,
    method: String,
    args: Vec<Value>,
    home_region: Option<String>,
}
