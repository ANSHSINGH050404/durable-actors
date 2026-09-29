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
async fn websocket_gateway_authenticates_and_preserves_frames_and_close() -> Result<()> {
    use super::super::socket_ticket::{SocketGrant, SocketTarget};
    let issuer = super::super::service::tests::test_issuer()?;
    let upstream = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let upstream_route = format!("http://{}", upstream.local_addr()?);
    let (accepted, receipt) = tokio::sync::oneshot::channel();
    let backend = tokio::spawn(async move {
        let (stream, _) = upstream.accept().await?;
        let mut socket = tokio_tungstenite::accept_hdr_async(
            stream,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert!(request.uri().query().unwrap().starts_with("key="));
                Ok(response)
            },
        )
        .await?;
        accepted.send(()).unwrap();
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
    let (key, _, _) = issuer.issue_socket(SocketGrant {
        actor: crate::actor::ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        },
        region: "north-america-west".into(),
        target: Some(SocketTarget {
            route: upstream_route,
            host_id: crate::host::HostId::new("host"),
            session_id: "session".into(),
            owner_epoch: 1,
        }),
        metadata: serde_json::json!({}),
        authorization_lifetime_ms: 60_000,
    })?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let gateway = Gateway::new(&issuer, format!("http://{address}"))?;
    let server = tokio::spawn(async move { axum::serve(listener, gateway.router()).await });
    let result = async {
        let invalid =
            tokio_tungstenite::connect_async(format!("ws://{address}/v1/socket?key=invalid"))
                .await
                .unwrap_err();
        assert!(
            matches!(invalid, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == StatusCode::UNAUTHORIZED)
        );
        let (mut socket, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/v1/socket?key={key}")).await?;
        receipt.await?;
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
