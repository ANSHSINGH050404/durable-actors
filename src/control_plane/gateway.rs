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
    pub(super) connections: std::sync::Arc<super::socket_gateway::SocketGateway>,
}

impl Gateway {
    pub(super) fn new(
        issuer: &ActorJwtIssuer,
        origin: String,
        connections: std::sync::Arc<super::socket_gateway::SocketGateway>,
    ) -> Result<Self> {
        Ok(Self {
            origin,
            connections,
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

    pub fn router(self, service: ControlPlaneService, admin: super::admin::AdminService) -> Router {
        let inventory = super::socket_inventory::router(self.connections.clone(), admin);
        Router::new()
            .route("/v1/socket", get(connect))
            .with_state((self, service.clone()))
            .merge(inventory)
            .merge(
                Router::new()
                    .route(
                        "/internal/socket-operation",
                        axum::routing::post(super::socket_gateway::internal_operation),
                    )
                    .layer(axum::extract::DefaultBodyLimit::disable())
                    .with_state(service),
            )
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
    key: Option<String>,
}

async fn connect(
    State((gateway, service)): State<(Gateway, ControlPlaneService)>,
    Query(query): Query<SocketQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, StatusCode> {
    let key = query.key.ok_or(StatusCode::UNAUTHORIZED)?;
    let ticket = gateway
        .sockets
        .verify(&key)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let owner = gateway
        .connections
        .owner(&ticket.actor)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    if owner.id == gateway.connections.owner.id {
        let state = crate::sockets::browser::SocketServerState {
            registry: gateway.connections.registry.clone(),
            dispatcher: std::sync::Arc::new(super::socket_gateway::GatewaySocketDispatcher {
                gateway: gateway.connections.clone(),
                service,
            }),
            stop: gateway.connections.stop.clone(),
        };
        return Ok(upgrade
            .read_buffer_size(crate::sockets::READ_BUFFER_BYTES)
            .write_buffer_size(crate::sockets::WRITE_BUFFER_BYTES)
            .max_message_size(crate::sockets::MAX_MESSAGE_BYTES)
            .max_frame_size(crate::sockets::MAX_MESSAGE_BYTES)
            .on_upgrade(move |socket| crate::sockets::browser::run(socket, state, ticket)));
    }
    let mut url = backend_origin(&owner.route).map_err(|_| StatusCode::BAD_GATEWAY)?;
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    url.set_scheme(scheme)
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    url.set_path("/v1/socket");
    url.query_pairs_mut().append_pair("key", &key);
    let config = WebSocketConfig::default()
        .read_buffer_size(crate::sockets::READ_BUFFER_BYTES)
        .write_buffer_size(crate::sockets::WRITE_BUFFER_BYTES)
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
        .read_buffer_size(crate::sockets::READ_BUFFER_BYTES)
        .write_buffer_size(crate::sockets::WRITE_BUFFER_BYTES)
        .max_message_size(crate::sockets::MAX_MESSAGE_BYTES)
        .max_frame_size(crate::sockets::MAX_MESSAGE_BYTES)
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
    let oversized = {
        let upload = async {
            while let Some(message) = downstream.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(error) => return crate::sockets::message_too_large(&error),
                };
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
            false
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
            oversized = &mut upload => {
                if !oversized { let _ = tokio::time::timeout(std::time::Duration::from_secs(5), download).await; }
                oversized
            },
            () = &mut download => tokio::time::timeout(std::time::Duration::from_secs(5), upload).await.unwrap_or(false),
        }
    };
    if oversized {
        let _ = downstream_sink
            .send(Message::Close(Some(axum::extract::ws::CloseFrame {
                code: 1009,
                reason: "message exceeds 32 MiB".into(),
            })))
            .await;
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/gateway.rs"]
mod tests;
