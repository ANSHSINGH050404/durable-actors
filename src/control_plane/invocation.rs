use axum::{
    Json,
    extract::{Path, State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::public_api::{
    ActorPath, ActorTargetReply, ApiError, FindActorRequest, PublicApiState, resolve_actor_target,
};
use crate::actor::{ActorInvocation, MAX_ACTOR_EXECUTOR_MESSAGE_BYTES};

pub(super) async fn invoke(
    State(state): State<PublicApiState>,
    Path(path): Path<ActorPath>,
    headers: HeaderMap,
    body: Result<Json<InvokeRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let actor = path.into_actor();
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
    let Json(request) = body.map_err(ApiError::json)?;
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
    let outcome = dispatch(&state.hosts, &target, &invocation).await?;
    Ok((
        [(header::CACHE_CONTROL, "no-store")],
        Json(json!({"target": target, "outcome": outcome})),
    )
        .into_response())
}

async fn dispatch(
    client: &reqwest::Client,
    target: &ActorTargetReply,
    invocation: &ActorInvocation,
) -> Result<Value, ApiError> {
    let actor = &invocation.actor;
    let url = format!(
        "{}/v1/projects/{}/actors/{}/{}/invoke",
        target.route.trim_end_matches('/'),
        actor.project_id,
        actor.actor_name,
        actor.actor_id
    );
    let response = client
        .post(url)
        .bearer_auth(&target.token)
        .json(&json!({
            "requestId":invocation.request_id, "ownerEpoch":target.owner_epoch,
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

async fn read_reply(mut response: reqwest::Response) -> Result<Value, ApiError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| outcome_unknown())? {
        if body.len() + chunk.len() > MAX_ACTOR_EXECUTOR_MESSAGE_BYTES {
            return Err(outcome_unknown());
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| outcome_unknown())
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
    method: String,
    args: Vec<Value>,
    home_region: Option<String>,
}
