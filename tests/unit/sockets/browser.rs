use super::*;
use axum::{Router, extract::WebSocketUpgrade, routing::get};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::{
    Message as ClientMessage, protocol::CloseFrame as ClientClose,
};

struct Dispatcher(tokio::sync::mpsc::UnboundedSender<ActorSocketEvent>);

#[tokio::test]
async fn accepted_socket_outlives_its_ticket_without_waking_the_actor() -> Result<()> {
    let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
    let state = SocketServerState {
        registry: SocketRegistry::default(),
        dispatcher: Arc::new(Dispatcher(events)),
        stop: CancellationToken::new(),
    };
    let now = now_ms();
    let ticket = SocketTicket {
        iss: "issuer".into(),
        aud: "socket".into(),
        scope: "actor:socket".into(),
        iat: now / 1000,
        nbf: now / 1000,
        exp: (now + 1999) / 1000,
        actor: crate::actor::ActorKey {
            project_id: "test".into(),
            actor_name: "Room".into(),
            actor_id: "one".into(),
        },
        region: "us-west".into(),
        home_region: None,
        metadata: serde_json::json!({"user":"one"}),
        connect_by_ms: now + 1000,
    };
    ticket.validate(now)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server_state = state.clone();
    let admitted = ticket.clone();
    let router = Router::new().route(
        "/",
        get(move |upgrade: WebSocketUpgrade| {
            let state = server_state.clone();
            let ticket = admitted.clone();
            async move { upgrade.on_upgrade(|socket| run(socket, state, ticket)) }
        }),
    );
    let _server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, router).await
    }));
    tokio::time::timeout(Duration::from_secs(5), async {
        let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/")).await?;
        assert_eq!(socket.next().await.transpose()?, Some(ClientMessage::Text("ready".into())));
        assert!(matches!(received.recv().await, Some(ActorSocketEvent::Connect { .. })));
        let connections = state.registry.connections_with_tag(&ticket.actor, None).await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(ticket.validate(now_ms()).is_err(), "expired tickets cannot open new connections");
        socket.send(ClientMessage::Ping("alive".into())).await?;
        assert_eq!(socket.next().await.transpose()?, Some(ClientMessage::Pong("alive".into())));
        assert!(received.try_recv().is_err(), "idle sockets must not deliver actor events");
        assert_eq!(state.registry.connections_with_tag(&ticket.actor, None).await, connections);
        state.registry.apply(&ticket.actor, vec![ActorSocketEffect::Send {
            connection_id: connections[0].id.clone(),
            message: ActorSocketMessage::Text { data: "still connected".into() },
        }]).await;
        assert_eq!(socket.next().await.transpose()?, Some(ClientMessage::Text("still connected".into())));
        state.stop.cancel();
        assert!(matches!(socket.next().await.transpose()?, Some(ClientMessage::Close(Some(frame))) if u16::from(frame.code) == 1012));
        anyhow::Ok(())
    }).await??;
    Ok(())
}

#[async_trait]
impl SocketDispatcher for Dispatcher {
    fn ensure_authority(&self) -> Result<()> {
        Ok(())
    }

    async fn dispatch(
        &self,
        _: &SocketTicket,
        invocation: ActorSocketInvocation,
    ) -> Result<Vec<ActorSocketEffect>> {
        self.0.send(invocation.event.clone())?;
        Ok(match invocation.event {
            ActorSocketEvent::Connect { connection } if connection.metadata == "reject" => vec![
                ActorSocketEffect::Broadcast {
                    message: ActorSocketMessage::Text {
                        data: "ready".into(),
                    },
                    except_connection_ids: vec![],
                    tags: vec![],
                    tag_match: Default::default(),
                },
                ActorSocketEffect::Reject {
                    connection_id: connection.id,
                    code: 4403,
                    reason: "denied".into(),
                },
            ],
            ActorSocketEvent::Connect { .. } => vec![ActorSocketEffect::Broadcast {
                message: ActorSocketMessage::Text {
                    data: "ready".into(),
                },
                except_connection_ids: vec![],
                tags: vec![],
                tag_match: Default::default(),
            }],
            ActorSocketEvent::Message { connection_id, .. } => vec![ActorSocketEffect::Close {
                connection_id,
                code: 4001,
                reason: "room finished".into(),
            }],
            ActorSocketEvent::Disconnect { .. } => vec![],
        })
    }
}

#[tokio::test]
async fn joining_broadcasts_and_close_handshakes_reach_the_actor() -> Result<()> {
    for (mode, code, reason, clean) in [
        ("client", 1001, "leaving", true),
        ("actor", 4001, "room finished", true),
        ("reject", 4403, "denied", true),
        ("drop", 1006, "transport closed", false),
    ] {
        let (events, mut received) = tokio::sync::mpsc::unbounded_channel();
        let state = SocketServerState {
            registry: SocketRegistry::default(),
            dispatcher: Arc::new(Dispatcher(events)),
            stop: CancellationToken::new(),
        };
        let ticket = SocketTicket {
            iss: "issuer".into(),
            aud: "socket".into(),
            scope: "actor:socket".into(),
            iat: 0,
            nbf: 0,
            exp: i64::MAX,
            actor: crate::actor::ActorKey {
                project_id: "test".into(),
                actor_name: "Room".into(),
                actor_id: "one".into(),
            },
            region: "us-west".into(),
            home_region: None,
            metadata: serde_json::json!(mode),
            connect_by_ms: i64::MAX,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let router = Router::new().route(
            "/",
            get(move |upgrade: WebSocketUpgrade| {
                let state = state.clone();
                let ticket = ticket.clone();
                async move { upgrade.on_upgrade(|socket| run(socket, state, ticket)) }
            }),
        );
        let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, router).await
        }));
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut socket, _) = tokio_tungstenite::connect_async(format!("ws://{address}/")).await?;
            if mode != "reject" {
                assert_eq!(socket.next().await.transpose()?, Some(ClientMessage::Text("ready".into())));
            }
            match mode {
                "client" => socket.send(ClientMessage::Close(Some(ClientClose { code: code.into(), reason: reason.into() }))).await?,
                "actor" => socket.send(ClientMessage::Text("close".into())).await?,
                _ => {},
            }
            if clean {
                assert!(matches!(socket.next().await.transpose()?, Some(ClientMessage::Close(Some(frame))) if u16::from(frame.code) == code && frame.reason == reason));
                let _ = socket.flush().await;
            }
            drop(socket);
            loop {
                if let Some(ActorSocketEvent::Disconnect { code: actual, reason: why, was_clean, .. }) = received.recv().await {
                    assert_eq!((actual, why.as_str(), was_clean), (code, reason, clean), "{mode}");
                    break;
                }
            }
            anyhow::Ok(())
        }).await??;
        drop(server);
    }
    Ok(())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}
