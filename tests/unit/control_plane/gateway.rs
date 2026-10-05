use super::*;
use crate::request_tracking::HostState;

#[tokio::test]
async fn signed_route_is_bound_to_the_actor_and_epoch() -> Result<()> {
    use base64::Engine;
    let key = aws_lc_rs::signature::Ed25519KeyPair::generate_pkcs8(
        &aws_lc_rs::rand::SystemRandom::new(),
    )?;
    let issuer = ActorJwtIssuer::from_base64_pkcs8(
        &base64::engine::general_purpose::STANDARD.encode(key.as_ref()),
        "test",
        "issuer",
        "authority",
        "invoke",
        std::time::Duration::from_secs(60),
    )?;
    let gateway = test_gateway(
        &issuer,
        "http://127.0.0.1:7100".into(),
        std::sync::Arc::new(super::super::socket_directory::MemorySocketDirectory::default()),
    )
    .await?;
    let actor = crate::actor::ActorKey {
        project_id: "test".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let token = issuer.issue_invocation_target(
        &actor,
        &crate::host::HostId::new("host.v3.test.one"),
        "00000000-0000-4000-8000-000000000001",
        "test",
        "north-america-west",
        7,
        None,
        "http://10.1.2.3:7101",
    )?;
    let authorization = format!("Bearer {}", token.token);
    assert_eq!(
        gateway.invocation_route(&actor, 7, &authorization)?,
        "http://10.1.2.3:7101/"
    );
    assert!(gateway.invocation_route(&actor, 8, &authorization).is_err());
    let other = crate::actor::ActorKey {
        actor_id: "other".into(),
        ..actor
    };
    assert!(gateway.invocation_route(&other, 7, &authorization).is_err());
    assert!(
        gateway
            .invocation_route(&other, 7, "Bearer unsigned")
            .is_err()
    );
    Ok(())
}

async fn test_gateway(
    issuer: &ActorJwtIssuer,
    origin: String,
    directory: std::sync::Arc<dyn super::super::socket_directory::SocketDirectory>,
) -> Result<Gateway> {
    let sockets = super::super::socket_gateway::SocketGateway::start(
        origin.clone(),
        directory,
        32768,
        true,
        tokio_util::sync::CancellationToken::new(),
    )
    .await?;
    Gateway::new(issuer, origin, sockets)
}

#[tokio::test]
async fn gateways_keep_connections_and_metadata_when_the_actor_host_changes() -> Result<()> {
    use super::super::socket_directory::SocketDirectory;
    use super::super::{
        ActorTokenPurpose,
        admin::{AdminRegistry, AdminService, HostLaunchSpec, LocalAdminRegistry},
        socket_gateway::{SocketEventReply, SocketEventRequest},
    };
    use crate::actor::{ActorKey, ActorSocketEffect, ActorSocketEvent, ActorSocketMessage};
    use axum::response::IntoResponse;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    let issuer = super::super::service::tests::test_issuer()?;
    let retiring = Arc::new(AtomicBool::new(false));
    let rejections = Arc::new(AtomicUsize::new(4));
    let uncertain_executions = Arc::new(AtomicUsize::new(0));
    let host_states = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut hosts = Vec::new();
    let mut host_routes = Vec::new();
    for instance in [1, 2] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        host_routes.push(format!("http://{}", listener.local_addr()?));
        let retiring = retiring.clone();
        let rejections = rejections.clone();
        let uncertain_executions = uncertain_executions.clone();
        let host_states = host_states.clone();
        let handler = move |axum::Json(request): axum::Json<SocketEventRequest>| {
            assert!(request.routing_ms.is_finite() && request.routing_ms > 0.0);
            host_states.lock().unwrap().push(request.host_state);
            let retiring = retiring.clone();
            let rejections = rejections.clone();
            let uncertain_executions = uncertain_executions.clone();
            async move {
                if instance == 2
                    && rejections
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                            value.checked_sub(1)
                        })
                        .is_ok()
                {
                    return axum::Json(SocketEventReply::NotExecuted).into_response();
                }
                if instance == 1 && retiring.load(Ordering::SeqCst) {
                    return axum::Json(SocketEventReply::NotExecuted).into_response();
                }
                if matches!(&request.invocation.event, ActorSocketEvent::Message {
                    message: ActorSocketMessage::Text { data }, ..
                } if data == "uncertain")
                {
                    uncertain_executions.fetch_add(1, Ordering::SeqCst);
                    return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
                let effects = match request.invocation.event {
                    ActorSocketEvent::Connect { connection } => vec![
                        ActorSocketEffect::SetMetadata {
                            connection_id: connection.id.clone(),
                            metadata: serde_json::json!({"retained":true}),
                        },
                        ActorSocketEffect::Send {
                            connection_id: connection.id,
                            message: ActorSocketMessage::Text {
                                data: "ready".into(),
                            },
                        },
                    ],
                    ActorSocketEvent::Message { connection_id, .. } => {
                        assert_eq!(request.invocation.connections.len(), 1);
                        assert_eq!(
                            request.invocation.connections[0].metadata,
                            serde_json::json!({"retained":true})
                        );
                        vec![ActorSocketEffect::Send {
                            connection_id,
                            message: ActorSocketMessage::Text {
                                data: instance.to_string(),
                            },
                        }]
                    }
                    ActorSocketEvent::Disconnect { .. } => vec![],
                };
                axum::Json(SocketEventReply::Completed { effects }).into_response()
            }
        };
        let routes = Router::new().route(
            "/v1/projects/default/actors/Counter/one/socket-events",
            axum::routing::post(handler),
        );
        hosts.push(tokio::spawn(
            async move { axum::serve(listener, routes).await },
        ));
    }
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            project_id: "default".into(),
            sandboxes: [(
                "Counter".into(),
                super::super::contracts::SandboxOptions {
                    regions: Some(vec!["north-america-east".into()]),
                    ..Default::default()
                },
            )]
            .into(),
            source: None,
            code_snapshot: None,
            image_ref: "image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let provisioner = Arc::new(PausedProvisioner {
        route: std::sync::Mutex::new(host_routes[0].clone()),
        started: tokio::sync::Semaphore::new(0),
        ready: tokio::sync::Semaphore::new(0),
        unavailable: AtomicUsize::new(0),
    });
    let service = ControlPlaneService::new(
        Arc::new(crate::placement::testing::LocalObjectPlacementStore::default()),
        ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            std::time::Duration::from_secs(60),
        )?,
        registry.clone(),
        issuer.clone(),
        provisioner.clone(),
    );
    let directory = Arc::new(super::super::socket_directory::MemorySocketDirectory::default());
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let mut servers = Vec::new();
    let mut gateways = Vec::new();
    for _ in 0..2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let gateway = test_gateway(&issuer, origin, directory.clone()).await?;
        if gateways.is_empty() {
            gateway.connections.owner(&actor).await?;
        }
        let mut service = service.clone();
        service.gateway = Some(gateway.clone());
        let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
        let routes = super::super::public_api::router(service, admin);
        servers.push(tokio::spawn(
            async move { axum::serve(listener, routes).await },
        ));
        gateways.push(gateway);
    }
    let result = async {
        let client = reqwest::Client::new();
        let mut sockets = Vec::new();
        for gateway in &gateways {
            let grant: serde_json::Value = client
                .post(format!(
                    "{}/v1/projects/default/actors/Counter/one/find-websocket",
                    gateway.origin
                ))
                .bearer_auth("api-key")
                .json(&serde_json::json!({"metadata":{}}))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let (socket, _) =
                tokio_tungstenite::connect_async(grant["websocketUrl"].as_str().unwrap()).await?;
            sockets.push(socket);
        }
        provisioner.started.acquire().await?.forget();
        assert_eq!(
            provisioner.started.available_permits(),
            0,
            "one coordinated actor activation"
        );
        provisioner.ready.add_permits(1);
        for socket in &mut sockets {
            assert_eq!(
                socket.next().await.transpose()?,
                Some(UpstreamMessage::Text("ready".into()))
            );
        }
        assert_eq!(
            host_states
                .lock()
                .unwrap()
                .iter()
                .filter(|&&state| state == HostState::Cold)
                .count(),
            1
        );
        assert_eq!(
            host_states
                .lock()
                .unwrap()
                .iter()
                .filter(|&&state| state == HostState::Warm)
                .count(),
            1
        );
        assert_eq!(gateways[0].connections.registry.count(&actor).await, 2);
        assert_eq!(gateways[1].connections.registry.count(&actor).await, 0);
        struct EmptyInventory;
        #[async_trait::async_trait]
        impl crate::placement::ActorInventoryReader for EmptyInventory {
            async fn actor_inventory(
                &self,
                _: &str,
            ) -> Result<crate::placement::ActorInventorySnapshot> {
                Ok(crate::placement::ActorInventorySnapshot {
                    actors: vec![],
                    connections_complete: true,
                })
            }
        }
        let reader = super::super::socket_inventory::GatewayInventoryReader::new(
            Arc::new(EmptyInventory),
            gateways[1].connections.clone(),
            Some("api-key".into()),
        );
        let overview =
            crate::placement::ActorInventoryReader::actor_inventory(&reader, "default").await?;
        assert!(overview.connections_complete);
        let overview = overview.actors;
        assert_eq!(overview.len(), 1);
        assert_eq!(overview[0].dormant, 1);
        assert_eq!(overview[0].instances[0].connections.len(), 2);
        assert_eq!(
            overview[0].instances[0].connections[0].metadata,
            serde_json::json!({"retained":true})
        );
        assert!(
            gateways[1]
                .connections
                .inventory("default", "Bearer wrong")
                .await
                .is_err()
        );
        assert!(
            gateways[1]
                .connections
                .inventory("unrelated", "Bearer api-key")
                .await?
                .rooms
                .is_empty()
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let unavailable = super::super::socket_directory::GatewayOwner {
            id: "unavailable".into(),
            route: format!("http://{}", listener.local_addr()?),
        };
        drop(listener);
        directory.register(&unavailable, true).await?;
        directory
            .claim(
                &ActorKey {
                    actor_id: "unavailable".into(),
                    ..actor.clone()
                },
                &unavailable,
            )
            .await?;
        let partial =
            crate::placement::ActorInventoryReader::actor_inventory(&reader, "default").await?;
        assert!(!partial.connections_complete);
        assert_eq!(partial.actors[0].instances[0].connections.len(), 2);
        retiring.store(true, Ordering::SeqCst);
        *provisioner.route.lock().unwrap() = host_routes[1].clone();
        provisioner.unavailable.store(2, Ordering::SeqCst);
        provisioner.ready.add_permits(5);
        for socket in &mut sockets {
            socket.send(UpstreamMessage::Text("message".into())).await?;
            assert_eq!(
                socket.next().await.transpose()?,
                Some(UpstreamMessage::Text("2".into()))
            );
        }
        assert_eq!(
            provisioner.started.available_permits(),
            5,
            "known non-execution may retry through the handoff"
        );
        assert_eq!(provisioner.unavailable.load(Ordering::SeqCst), 0);
        sockets[0]
            .send(UpstreamMessage::Text("uncertain".into()))
            .await?;
        assert!(matches!(
            sockets[0].next().await.transpose()?,
            Some(UpstreamMessage::Close(_))
        ));
        assert_eq!(uncertain_executions.load(Ordering::SeqCst), 1);
        for socket in &mut sockets[1..] {
            socket.close(None).await?;
        }
        let health = format!("{}/healthz", gateways[0].origin);
        assert_eq!(client.get(&health).send().await?.status(), StatusCode::OK);
        gateways[0].connections.stop.cancel();
        assert_eq!(
            client.get(&health).send().await?.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        anyhow::Ok(())
    };
    let result = tokio::time::timeout(std::time::Duration::from_secs(15), result).await;
    for gateway in gateways {
        gateway.connections.stop.cancel();
    }
    for server in servers.into_iter().chain(hosts) {
        server.abort();
    }
    result??;
    Ok(())
}

struct PausedProvisioner {
    route: std::sync::Mutex<String>,
    started: tokio::sync::Semaphore,
    ready: tokio::sync::Semaphore,
    unavailable: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl super::super::service::HostProvisioner for PausedProvisioner {
    fn host_idle_timeout_ms(&self) -> u64 {
        300_000
    }

    async fn prepare_deployment(
        &self,
        _: &super::super::admin::HostLaunchSpec,
        _: Option<&super::super::admin::HostLaunchSpec>,
        _: &str,
    ) -> Result<(
        super::super::admin::HostLaunchSpec,
        Option<super::super::contracts::PublicActorContract>,
    )> {
        unreachable!()
    }

    async fn ensure_actor_host(
        &self,
        _: &super::super::admin::HostLaunchSpec,
        region: &str,
        actor: &crate::actor::ActorKey,
        new_actor: bool,
        _: Option<&crate::bucket::OwnershipHint>,
    ) -> Result<(crate::host_leases::HostLease, u64)> {
        assert_eq!(region, "north-america-east");
        assert_eq!(actor.actor_id, "one");
        assert!(new_actor);
        if self
            .unavailable
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |value| value.checked_sub(1),
            )
            .is_ok()
        {
            return Err(crate::sandbox::HostNotReady.into());
        }
        self.started.add_permits(1);
        self.ready.acquire().await?.forget();
        Ok((
            crate::host_leases::HostLease {
                id: crate::host::HostId::new("host"),
                session_id: "session".into(),
                route: self.route.lock().unwrap().clone(),
                expires_at_ms: u64::MAX,
            },
            7,
        ))
    }

    async fn terminate_hosts(
        &self,
        _: &super::super::admin::HostLaunchSpec,
        _: &[String],
    ) -> Result<crate::sandbox::HostTermination> {
        unreachable!()
    }
}

#[tokio::test]
async fn gateway_accepts_32_mib_and_closes_oversize_with_1009() -> Result<()> {
    let backend = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let backend_address = backend.local_addr()?;
    let echo = tokio::spawn(async move {
        let (stream, _) = backend.accept().await?;
        let config = WebSocketConfig::default()
            .max_frame_size(None)
            .max_message_size(None);
        let mut socket = tokio_tungstenite::accept_async_with_config(stream, Some(config)).await?;
        while let Some(Ok(message)) = socket.next().await {
            if message.is_text() {
                socket.send(message).await?;
            }
        }
        anyhow::Ok(())
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let router = Router::new().route(
        "/",
        get(move |upgrade: WebSocketUpgrade| async move {
            let (upstream, _) = connect_async_with_config(
                format!("ws://{backend_address}"),
                Some(
                    WebSocketConfig::default()
                        .max_frame_size(None)
                        .max_message_size(None),
                ),
                false,
            )
            .await
            .unwrap();
            upgrade
                .max_frame_size(crate::sockets::MAX_MESSAGE_BYTES)
                .max_message_size(crate::sockets::MAX_MESSAGE_BYTES)
                .on_upgrade(|socket| bridge(socket, upstream))
        }),
    );
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let result = async {
        let config = WebSocketConfig::default()
            .max_frame_size(None)
            .max_message_size(None);
        let (mut socket, _) =
            connect_async_with_config(format!("ws://{address}"), Some(config), false).await?;
        let message = UpstreamMessage::Text("x".repeat(32 * 1024 * 1024).into());
        socket.send(message.clone()).await?;
        assert_eq!(socket.next().await.transpose()?, Some(message));
        let _ = socket
            .send(UpstreamMessage::Text(
                "x".repeat(32 * 1024 * 1024 + 1).into(),
            ))
            .await;
        assert!(
            matches!(socket.next().await.transpose()?, Some(UpstreamMessage::Close(Some(frame))) if frame.code == 1009.into())
        );
        anyhow::Ok(())
    };
    let result = tokio::time::timeout(std::time::Duration::from_secs(15), result).await;
    server.abort();
    echo.abort();
    result??;
    Ok(())
}
