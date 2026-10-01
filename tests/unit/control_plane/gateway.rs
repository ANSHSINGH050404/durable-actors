use super::*;

#[test]
fn signed_route_is_bound_to_the_actor_and_epoch() -> Result<()> {
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
    let gateway = Gateway::new(&issuer, "https://actors.example.com".into())?;
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

#[tokio::test]
async fn websocket_grant_defers_activation_and_gateway_preserves_frames_and_close() -> Result<()> {
    use super::super::{
        ActorTokenPurpose,
        admin::{AdminRegistry, AdminService, HostLaunchSpec, LocalAdminRegistry},
        service::ControlPlaneService,
    };
    use std::sync::Arc;
    let issuer = super::super::service::tests::test_issuer()?;
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let upstream_route = format!("http://{}", upstream.local_addr()?);
    let (accepted, receipt) = tokio::sync::oneshot::channel();
    let verifier = issuer.socket_verifier()?;
    let backend_ticket = std::sync::Mutex::new(None);
    let backend = tokio::spawn(async move {
        let (stream, _) = upstream.accept().await?;
        let mut socket = tokio_tungstenite::accept_hdr_async(
            stream,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                let url = reqwest::Url::parse(&format!("http://host{}", request.uri())).unwrap();
                let key = url.query_pairs().find(|(name, _)| name == "key").unwrap().1;
                *backend_ticket.lock().unwrap() = Some(verifier.verify(&key).unwrap());
                Ok(response)
            },
        )
        .await?;
        assert!(
            accepted
                .send(backend_ticket.into_inner().unwrap().unwrap())
                .is_ok()
        );
        while let Some(message) = socket.next().await.transpose()? {
            if message.is_close() {
                socket.flush().await?;
                break;
            }
            if message.is_text() || message.is_binary() {
                socket.send(message).await?;
            }
        }
        anyhow::Ok(())
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
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
        route: upstream_route.clone(),
        started: tokio::sync::Semaphore::new(0),
        ready: tokio::sync::Semaphore::new(0),
    });
    let mut service = ControlPlaneService::new(
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
    service.region = Some("north-america-west".into());
    service.gateway = Some(Gateway::new(&issuer, format!("http://{address}"))?);
    let admin = AdminService::new(Some("api-key".into()), registry, issuer.clone())?;
    let routes = super::super::public_api::router(service, admin);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let result = async {
        let invalid =
            tokio_tungstenite::connect_async(format!("ws://{address}/v1/socket?key=invalid"))
                .await
                .unwrap_err();
        assert!(
            matches!(invalid, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == StatusCode::UNAUTHORIZED)
        );
        assert_eq!(provisioner.started.available_permits(), 0);
        let client = reqwest::Client::new();
        let constrained: serde_json::Value = client
            .post(format!(
                "http://{address}/v1/projects/default/actors/Counter/one/find-websocket"
            ))
            .bearer_auth("api-key")
            .json(&serde_json::json!({"metadata":{},"homeRegion":"north-america-west"}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let rejected =
            tokio_tungstenite::connect_async(constrained["websocketUrl"].as_str().unwrap())
                .await
                .unwrap_err();
        assert!(
            matches!(rejected, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == StatusCode::BAD_GATEWAY)
        );
        assert_eq!(provisioner.started.available_permits(), 0);
        let request = client
            .post(format!(
                "http://{address}/v1/projects/default/actors/Counter/one/find-websocket"
            ))
            .bearer_auth("api-key")
            .json(&serde_json::json!({"metadata":{"userId":"alice"}}));
        let response = tokio::select! {
            response = request.send() => response?,
            _ = provisioner.started.acquire() => anyhow::bail!("issuing a socket URL started the actor"),
        };
        let grant: serde_json::Value = response.error_for_status()?.json().await?;
        assert_eq!(provisioner.started.available_permits(), 0);
        let url = grant["websocketUrl"]
            .as_str()
            .context("socket URL")?
            .to_owned();
        let parsed = reqwest::Url::parse(&url)?;
        assert_eq!(parsed.port(), Some(address.port()));
        let key = parsed
            .query_pairs()
            .find(|(name, _)| name == "key")
            .unwrap()
            .1;
        let unbound = issuer.verify_socket(&key)?;
        assert!(unbound.target.is_none());
        let connecting = tokio::spawn(async move { tokio_tungstenite::connect_async(url).await });
        provisioner.started.acquire().await?.forget();
        assert!(
            !connecting.is_finished(),
            "the upgrade must await host readiness"
        );
        provisioner.ready.add_permits(1);
        let (mut socket, _) = connecting.await??;
        let bound = receipt.await?;
        assert_eq!(bound.actor, unbound.actor);
        assert_eq!(bound.metadata, serde_json::json!({"userId":"alice"}));
        assert_eq!(bound.authorized_until_ms, unbound.authorized_until_ms);
        assert_eq!(bound.region, "north-america-east");
        let target = bound.target.context("host binding")?;
        assert_eq!(target.route, upstream_route);
        assert_eq!(target.host_id, crate::host::HostId::new("host"));
        assert_eq!(target.session_id, "session");
        assert_eq!(target.owner_epoch, 7);
        for message in [
            UpstreamMessage::Text("hello".into()),
            UpstreamMessage::Binary(vec![0, 1, 255].into()),
        ] {
            socket.send(message.clone()).await?;
            assert_eq!(socket.next().await.transpose()?, Some(message));
        }
        socket
            .close(Some(tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: 1000.into(),
                reason: "done".into(),
            }))
            .await?;
        assert!(
            matches!(socket.next().await.transpose()?, Some(UpstreamMessage::Close(Some(frame))) if frame.code == 1000.into())
        );
        anyhow::Ok(())
    };
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), result).await;
    server.abort();
    backend.abort();
    result??;
    Ok(())
}

struct PausedProvisioner {
    route: String,
    started: tokio::sync::Semaphore,
    ready: tokio::sync::Semaphore,
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
        self.started.add_permits(1);
        self.ready.acquire().await?.forget();
        Ok((
            crate::host_leases::HostLease {
                id: crate::host::HostId::new("host"),
                session_id: "session".into(),
                route: self.route.clone(),
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
