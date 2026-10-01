use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::StatusCode,
    response::Response,
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message as UpstreamMessage, protocol::WebSocketConfig},
};

use super::{
    ActorJwtVerifier, issuer::ActorJwtIssuer, service::ControlPlaneService,
    socket_ticket::SocketTicketVerifier,
};

#[derive(Clone)]
pub(super) struct Gateway {
    pub origin: String,
    invocations: ActorJwtVerifier,
    sockets: SocketTicketVerifier,
}

impl Gateway {
    pub fn new(issuer: &ActorJwtIssuer, origin: String) -> Result<Self> {
        Ok(Self {
            origin,
            invocations: issuer.invocation_verifier()?,
            sockets: issuer.socket_verifier()?,
        })
    }

    pub fn invocation_route(
        &self,
        actor: &crate::actor::ActorKey,
        epoch: u64,
        authorization: &str,
    ) -> Result<String> {
        let principal = self.invocations.authenticate_authorization(authorization)?;
        let capability = principal
            .invocation
            .context("invocation capability missing")?;
        ensure!(
            capability.actor == *actor && capability.owner_epoch == epoch,
            "invocation target mismatch"
        );
        Ok(backend_origin(&capability.route)?.to_string())
    }

    pub fn router(self, service: ControlPlaneService) -> Router {
        Router::new()
            .route("/v1/socket", get(connect))
            .with_state((self, service))
    }
}

pub(super) fn backend_origin(route: &str) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(route)?;
    ensure!(
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.path() == "/"
            && url.query().is_none()
            && url.fragment().is_none()
            && url.username().is_empty()
            && url.password().is_none(),
        "invalid host origin"
    );
    Ok(url)
}

#[derive(Deserialize)]
struct SocketQuery {
    key: String,
}

async fn connect(
    State((gateway, service)): State<(Gateway, ControlPlaneService)>,
    Query(query): Query<SocketQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let ticket = gateway
        .sockets
        .verify(&query.key)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let (target, key) = match ticket.target.clone() {
        Some(target) => (target, query.key),
        None => service
            .bind_socket(ticket)
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?,
    };
    let mut url = backend_origin(&target.route).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    url.set_scheme(scheme)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    url.set_path("/v1/socket");
    url.query_pairs_mut().append_pair("key", &key);
    let config = WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None);
    let (upstream, _) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        connect_async_with_config(url.as_str(), Some(config), false),
    )
    .await
    .map_err(|_| StatusCode::GATEWAY_TIMEOUT)?
    .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(upgrade
        .max_message_size(usize::MAX)
        .max_frame_size(usize::MAX)
        .on_upgrade(|socket| bridge(socket, upstream)))
}

async fn bridge(
    socket: WebSocket,
    upstream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) {
    let (mut downstream_sink, mut downstream) = socket.split();
    let (mut upstream_sink, mut upstream) = upstream.split();
    let upload = async {
        while let Some(Ok(message)) = downstream.next().await {
            let closing = matches!(message, Message::Close(_));
            let message = match message {
                Message::Text(text) => UpstreamMessage::Text(text.to_string().into()),
                Message::Binary(bytes) => UpstreamMessage::Binary(bytes),
                Message::Ping(bytes) => UpstreamMessage::Ping(bytes),
                Message::Pong(bytes) => UpstreamMessage::Pong(bytes),
                Message::Close(frame) => UpstreamMessage::Close(frame.map(|frame| {
                    tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code: frame.code.into(),
                        reason: frame.reason.to_string().into(),
                    }
                })),
            };
            if upstream_sink.send(message).await.is_err() || closing {
                break;
            }
        }
        let _ = upstream_sink.close().await;
    };
    let download = async {
        while let Some(Ok(message)) = upstream.next().await {
            let closing = matches!(message, UpstreamMessage::Close(_));
            let message = match message {
                UpstreamMessage::Text(text) => Message::Text(text.to_string().into()),
                UpstreamMessage::Binary(bytes) => Message::Binary(bytes),
                UpstreamMessage::Ping(bytes) => Message::Ping(bytes),
                UpstreamMessage::Pong(bytes) => Message::Pong(bytes),
                UpstreamMessage::Close(frame) => {
                    Message::Close(frame.map(|frame| axum::extract::ws::CloseFrame {
                        code: frame.code.into(),
                        reason: frame.reason.to_string().into(),
                    }))
                }
                UpstreamMessage::Frame(_) => continue,
            };
            if downstream_sink.send(message).await.is_err() || closing {
                break;
            }
        }
        let _ = downstream_sink.close().await;
    };
    tokio::pin!(upload, download);
    tokio::select! {
        () = &mut upload => { let _ = tokio::time::timeout(std::time::Duration::from_secs(5), download).await; },
        () = &mut download => { let _ = tokio::time::timeout(std::time::Duration::from_secs(5), upload).await; },
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/gateway.rs"]
mod tests;
