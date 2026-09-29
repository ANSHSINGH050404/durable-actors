use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::post,
};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, oneshot};

use super::process::HostReadiness;

pub(super) struct Assignment {
    pub environment: HashMap<String, String>,
    pub ready: oneshot::Sender<HostReadiness>,
}

struct AssignmentState {
    authorization: String,
    pending: Mutex<Option<oneshot::Sender<Assignment>>>,
}

pub(super) fn router(token: String, pending: oneshot::Sender<Assignment>) -> Router {
    Router::new()
        .route("/assign", post(assign))
        .layer(DefaultBodyLimit::disable())
        .with_state(Arc::new(AssignmentState {
            authorization: format!("Bearer {token}"),
            pending: Mutex::new(Some(pending)),
        }))
}

async fn assign(
    State(state): State<Arc<AssignmentState>>,
    headers: HeaderMap,
    Json(environment): Json<HashMap<String, String>>,
) -> Result<Json<HostReadiness>, StatusCode> {
    let supplied = headers
        .get("authorization")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !bool::from(state.authorization.as_bytes().ct_eq(supplied.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let pending = state
        .pending
        .lock()
        .await
        .take()
        .ok_or(StatusCode::CONFLICT)?;
    let (ready, receive) = oneshot::channel();
    pending
        .send(Assignment { environment, ready })
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    receive
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

#[cfg(test)]
#[path = "../../tests/unit/host/assignment.rs"]
mod tests;
