use super::tests::test_issuer;
use super::*;
use crate::{
    actor::{ActorExecutorListener, ActorSocketPublisher, ActorSocketSource},
    control_plane::{ActorTokenPurpose, ControlPlaneClient, admin::LocalAdminRegistry},
    host::{ActorHost, HostEndpoint},
    host_leases::{HostLeaseRegistry, HostLeaseRequest},
};
use futures_util::{SinkExt, StreamExt};
use std::{process::Stdio, time::Duration};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn browser_receives_only_emittable_state_after_commit_and_on_reconnect() -> Result<()> {
    let mut stack = Stack::start().await?;
    let grant = stack
        .grant(serde_json::json!({"user":"one"}), 30_000)
        .await?;
    let url = grant["websocketUrl"].as_str().context("socket URL")?;
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await?;
    let initial = receive(&mut socket).await?;
    assert_eq!(initial["type"], "state");
    assert_eq!(initial["state"], serde_json::json!({"count":0}));
    stack.invoke("change", vec![]).await?;
    let update = receive(&mut socket).await?;
    assert_eq!(update["type"], "state_update");
    assert_eq!(update["changes"], serde_json::json!({"count":2}));
    assert!(update["version"].as_u64() > initial["version"].as_u64());
    assert!(stack.invoke("fail", vec![]).await.is_err());
    stack.invoke("change", vec![]).await?;
    assert_eq!(
        receive(&mut socket).await?["changes"],
        serde_json::json!({"count":4})
    );
    socket.close(None).await?;
    let (mut reconnected, _) = tokio_tungstenite::connect_async(url).await?;
    assert_eq!(
        receive(&mut reconnected).await?["state"],
        serde_json::json!({"count":4})
    );
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn ordinary_calls_skip_connection_lookup_and_explicit_lookup_failures_are_isolated()
-> Result<()> {
    #[derive(Default)]
    struct UnavailableConnections(std::sync::atomic::AtomicUsize);
    #[async_trait]
    impl ActorSocketSource for UnavailableConnections {
        async fn connections(
            &self,
            actor: &ActorKey,
        ) -> Result<Vec<crate::actor::ActorSocketConnection>> {
            assert_eq!(actor.actor_id, "counter-1");
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            anyhow::bail!("gateway unavailable")
        }
    }
    let source = Arc::new(UnavailableConnections::default());
    let mut stack = Stack::start_with_connections(Some(source.clone())).await?;
    assert_eq!(stack.invoke("readHistory", vec![]).await?, "");
    assert_eq!(stack.invoke("appendHistory", vec![]).await?, "saved");
    assert_eq!(source.0.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert!(stack.invoke("clients", vec![]).await.is_err());
    assert_eq!(source.0.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(stack.invoke("readHistory", vec![]).await?, "saved");
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn signed_url_connects_without_a_protocol_or_handshake_and_exchanges_plain_json() -> Result<()>
{
    let mut stack = Stack::start().await?;
    let grant = stack
        .grant(serde_json::json!({"user":"one"}), 5_000)
        .await?;
    let url = grant["websocketUrl"].as_str().context("socket URL")?;
    assert_ne!(
        reqwest::Url::parse(url)?.port_or_known_default(),
        reqwest::Url::parse(&stack.gateway)?.port_or_known_default(),
        "browser sockets must connect to the actor host"
    );
    assert_eq!(
        reqwest::Url::parse(url)?
            .query_pairs()
            .find(|(name, _)| name == "key")
            .map(|(_, key)| key.into_owned()),
        Some(socket_key(&grant)?)
    );
    let (mut socket, response) = tokio_tungstenite::connect_async(url).await?;
    assert!(response.headers().get("sec-websocket-protocol").is_none());
    assert_eq!(
        receive(&mut socket).await?["state"],
        serde_json::json!({"count":0})
    );
    socket
        .send(Message::Text(r#"{"type":"start"}"#.into()))
        .await?;
    assert_eq!(
        receive(&mut socket).await?,
        serde_json::json!({"delta":"first"})
    );
    std::fs::write(stack.directory.path().join("release"), "")?;
    assert_eq!(
        receive(&mut socket).await?,
        serde_json::json!({"delta":"last"})
    );
    assert_eq!(
        stack.invoke("clients", vec![]).await?[0]["metadata"]["name"],
        "member"
    );
    let clients = stack.invoke("clients", vec![]).await?;
    stack
        .invoke("notifyClient", vec![clients[0]["id"].clone()])
        .await?;
    assert_eq!(
        receive(&mut socket).await?,
        serde_json::json!({"text":"from method"})
    );
    stack
        .storage
        .unregister(&stack.host_id, "00000000-0000-4000-8000-000000000001")
        .await?;
    let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await?
        .context("close")??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 1012));
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn signed_socket_rejects_missing_invalid_and_backend_keys_before_upgrading() -> Result<()> {
    let mut stack = Stack::start().await?;
    let grant = stack.grant(serde_json::json!({}), 3_000).await?;
    let mut url = reqwest::Url::parse(grant["websocketUrl"].as_str().context("socket URL")?)?;
    for key in [
        "",
        "test-api-key",
        &stack.host_token,
        &format!("{}tampered", socket_key(&grant)?),
    ] {
        url.set_query(None);
        if !key.is_empty() {
            url.query_pairs_mut().append_pair("key", key);
        }
        let error = tokio_tungstenite::connect_async(url.as_str())
            .await
            .unwrap_err();
        assert!(
            matches!(error, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == 401)
        );
    }
    let original = stack.issuer.verify_socket(&socket_key(&grant)?)?;
    for mismatch in [
        "project",
        "actor",
        "actor_name",
        "host",
        "session",
        "epoch",
        "unbound",
    ] {
        let mut ticket = original.clone();
        match mismatch {
            "project" => ticket.actor.project_id = "other-project".into(),
            "actor" => ticket.actor.actor_id = "other-actor".into(),
            "actor_name" => ticket.actor.actor_name = "OtherActor".into(),
            "host" => ticket.target.as_mut().unwrap().host_id = HostId::new("other-host"),
            "session" => ticket.target.as_mut().unwrap().session_id = "replacement-session".into(),
            "epoch" => ticket.target.as_mut().unwrap().owner_epoch += 1,
            _ => ticket.target = None,
        }
        let (key, _, _) = stack
            .issuer
            .issue_socket(super::super::socket_ticket::SocketGrant {
                actor: ticket.actor,
                region: ticket.region,
                target: ticket.target,
                metadata: serde_json::json!({}),
                authorization_lifetime_ms: 3_000,
            })?;
        url.set_query(None);
        url.query_pairs_mut().append_pair("key", &key);
        let error = tokio_tungstenite::connect_async(url.as_str())
            .await
            .unwrap_err();
        assert!(
            matches!(error, tokio_tungstenite::tungstenite::Error::Http(response) if response.status() == 401),
            "{mismatch}"
        );
    }
    assert_eq!(
        stack.invoke("clients", vec![]).await?,
        serde_json::json!([])
    );
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn signed_socket_expires_while_idle_or_running_a_handler_and_rejects_invalid_messages()
-> Result<()> {
    let mut stack = Stack::start().await?;
    for active in [false, true] {
        let grant = stack.grant(serde_json::json!({}), 1_000).await?;
        let (mut socket, _) =
            tokio_tungstenite::connect_async(grant["websocketUrl"].as_str().unwrap()).await?;
        assert_eq!(receive(&mut socket).await?["type"], "state");
        if active {
            socket
                .send(Message::Text(r#"{"type":"start"}"#.into()))
                .await?;
            assert_eq!(
                receive(&mut socket).await?,
                serde_json::json!({"delta":"first"})
            );
        }
        let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
            .await?
            .context("close")??;
        assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 4408));
    }
    std::fs::write(stack.directory.path().join("release"), "")?;
    let grant = stack.grant(serde_json::json!({}), 3_000).await?;
    let (mut socket, _) =
        tokio_tungstenite::connect_async(grant["websocketUrl"].as_str().unwrap()).await?;
    receive(&mut socket).await?;
    socket.send(Message::Text("{".into())).await?;
    let frame = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await?
        .context("close")??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 4400));
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn generated_backend_calls_http_and_authorizes_a_native_websocket() -> Result<()> {
    let mut stack = Stack::start().await?;
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/dist");
    let script = stack.directory.path().join("browser-test.mjs");
    std::fs::write(
        &script,
        format!(
            r#"
import assert from 'node:assert/strict';
import {{ writeFile }} from 'node:fs/promises';
import {{ ActorCompiler }} from {compiler};
import {{ generateClient }} from {generator};

const directory = {directory};
await generateClient(new ActorCompiler().compileContract(directory + '/actors.ts'), directory + '/generated');
const {{ actors, createActorTransport, ActorInvocationError }} = await import(directory + '/generated/index.js');
const transport = createActorTransport({{projectId:'default',controlPlaneUrl:{gateway},apiKey:'test-api-key'}});
const counter = actors.Counter.get('counter-1', transport);
assert.equal(await counter.readHistory(), '');
assert.equal(await counter.appendHistory(), 'saved');
assert.equal(await counter.readHistory(), 'saved');
await assert.rejects(counter.fail(), error => error instanceof ActorInvocationError && error.code === 'actor_error');
const grant = await actors.Counter.prepareWebsocket({{actorId:'counter-1',metadata:{{user:'one'}}}},
    {{projectId:'default',controlPlaneUrl:{gateway},apiKey:'test-api-key'}});
assert.ok(new URL(grant.websocketUrl).searchParams.get('key'));
assert.ok(grant.authorizedUntilMs >= grant.connectByMs);
assert.equal(grant.key, undefined);
await writeFile(directory + '/release', '');
const socket = new WebSocket(grant.websocketUrl);
const messages = [];
await new Promise((resolve, reject) => {{
    socket.onopen = () => socket.send(JSON.stringify({{type:'start'}}));
    socket.onerror = reject;
    socket.onmessage = event => {{
        messages.push(JSON.parse(event.data));
        if (messages.length === 3) resolve();
    }};
}});
assert.equal(messages[0].type, 'state');
assert.deepEqual(messages[0].state, {{count:0}});
assert.deepEqual(messages.slice(1), [{{delta:'first'}}, {{delta:'last'}}]);
socket.close();
"#,
            compiler = serde_json::to_string(&format!(
                "file://{}",
                sdk.join("compiler/actor-compiler.js").display()
            ))?,
            generator = serde_json::to_string(&format!(
                "file://{}",
                sdk.join("compiler/generators/client-generator.js")
                    .display()
            ))?,
            directory = serde_json::to_string(stack.directory.path())?,
            gateway = serde_json::to_string(&stack.gateway)?,
        ),
    )?;
    let result = tokio::time::timeout(
        Duration::from_secs(15),
        tokio::process::Command::new("node")
            .arg(script)
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        result.status.success(),
        "native WebSocket integration failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn broadcast_tag_modes_reach_only_matching_websockets() -> Result<()> {
    let mut stack = Stack::start().await?;
    let mut first = stack.connect().await?;
    receive(&mut first).await?;
    let first_id = stack.invoke("clients", vec![]).await?[0]["id"].clone();
    stack
        .invoke(
            "watchFiles",
            vec![first_id.clone(), serde_json::json!(["file:a"])],
        )
        .await?;

    let mut second = stack.connect().await?;
    receive(&mut second).await?;
    let clients = stack.invoke("clients", vec![]).await?;
    let second_id = clients
        .as_array()
        .unwrap()
        .iter()
        .find(|client| client["id"] != first_id)
        .unwrap()["id"]
        .clone();
    stack
        .invoke(
            "watchFiles",
            vec![second_id, serde_json::json!(["file:a", "file:b"])],
        )
        .await?;

    for mode in ["all", "any"] {
        stack
            .invoke("notifyFiles", vec![serde_json::json!(mode)])
            .await?;
        if mode == "any" {
            assert_eq!(
                receive(&mut first).await?,
                serde_json::json!({"text":"matched"})
            );
        }
        assert_eq!(
            receive(&mut first).await?,
            serde_json::json!({"text":"done"})
        );
        assert_eq!(
            receive(&mut second).await?,
            serde_json::json!({"text":"matched"})
        );
        assert_eq!(
            receive(&mut second).await?,
            serde_json::json!({"text":"done"})
        );
    }
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn ordinary_methods_list_and_address_gateway_connections() -> Result<()> {
    let mut stack = Stack::start().await?;
    assert_eq!(
        stack.invoke("clients", vec![]).await?,
        serde_json::json!([])
    );
    let mut socket = stack.connect().await?;
    receive(&mut socket).await?;
    let clients = stack.invoke("clients", vec![]).await?;
    assert_eq!(clients.as_array().unwrap().len(), 1);
    assert_eq!(
        clients[0]["metadata"],
        serde_json::json!({"name": "member"})
    );
    assert_eq!(clients[0]["tags"], serde_json::json!(["member"]));
    let mut outside = stack.actor.clone();
    outside.actor_id = "another-actor".into();
    assert!(stack.publisher.connections(&outside).await?.is_empty());
    stack
        .invoke("notifyClient", vec![clients[0]["id"].clone()])
        .await?;
    assert_eq!(
        receive(&mut socket).await?,
        serde_json::json!({"text": "from method"})
    );
    assert_eq!(
        stack.invoke("clients", vec![]).await?[0]["metadata"]["notified"],
        true
    );
    socket.close(None).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if stack.invoke("clients", vec![]).await? == serde_json::json!([]) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    stack
        .storage
        .unregister(&stack.host_id, "00000000-0000-4000-8000-000000000001")
        .await?;
    assert!(stack.publisher.connections(&stack.actor).await.is_err());
    assert!(stack.invoke("clients", vec![]).await.is_err());
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build; exercises a handler longer than 30 seconds"]
async fn streams_through_real_worker_host_and_gateway_then_catches_up_reconnect() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .try_init();
    let mut stack = Stack::start().await?;
    let mut first = stack.connect().await?;
    assert_eq!(
        receive(&mut first).await?["state"],
        serde_json::json!({"count":0})
    );
    first
        .send(Message::Text(
            serde_json::json!({"type": "start"}).to_string().into(),
        ))
        .await?;
    assert_eq!(receive(&mut first).await?["delta"], "first");

    let mut late = tokio::time::timeout(Duration::from_secs(5), stack.connect()).await??;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), late.next())
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_secs(31)).await;
    std::fs::write(stack.directory.path().join("release"), "")?;
    assert_eq!(receive(&mut first).await?["delta"], "last");
    assert_eq!(
        receive(&mut late).await?["state"],
        serde_json::json!({"count":0})
    );
    assert_eq!(stack.invoke("readHistory", vec![]).await?, "firstlast");

    let mut outside = stack.actor.clone();
    outside.actor_id = "another-actor".into();
    assert!(stack.publisher.connections(&outside).await?.is_empty());
    stack
        .storage
        .unregister(&stack.host_id, "00000000-0000-4000-8000-000000000001")
        .await?;
    assert!(stack.publisher.publish(&stack.actor, vec![]).await.is_err());
    first.close(None).await?;
    late.close(None).await?;
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn socket_lease_fencing_is_not_postponed_by_incoming_control_frames() -> Result<()> {
    let mut stack = Stack::start().await?;
    let mut socket = stack.connect().await?;
    receive(&mut socket).await?;
    let (mut outgoing, mut incoming) = socket.split();
    stack
        .storage
        .unregister(&stack.host_id, "00000000-0000-4000-8000-000000000001")
        .await?;
    let mut pongs = tokio::time::interval(Duration::from_millis(20));
    let frame = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            tokio::select! {
                _ = pongs.tick() => outgoing.send(Message::Pong(vec![1].into())).await?,
                frame = incoming.next() => return anyhow::Ok(frame.context("socket closed before fence notification")??),
            }
        }
    })
    .await??;
    assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == 1012));
    stack.child.kill().await?;
    Ok(())
}

struct Stack {
    issuer: ActorJwtIssuer,
    directory: tempfile::TempDir,
    tasks: JoinSet<()>,
    child: tokio::process::Child,
    gateway: String,
    host_token: String,
    actor: ActorKey,
    host_id: HostId,
    _runtime: crate::bucket::testing::RuntimeFixture,
    publisher: Arc<crate::host::sockets::HostSockets>,
    host: Arc<ActorHost>,
    storage: Arc<crate::host::storage::HostStorage>,
}

impl Stack {
    async fn grant(&self, metadata: serde_json::Value, lifetime: u64) -> Result<serde_json::Value> {
        reqwest::Client::new()
            .post(format!(
                "{}/v1/projects/default/actors/Counter/counter-1/find-websocket",
                self.gateway
            ))
            .bearer_auth("test-api-key")
            .json(&serde_json::json!({"metadata":metadata,"authorizationLifetimeMs":lifetime}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await
            .map_err(Into::into)
    }
    async fn start() -> Result<Self> {
        Self::start_with_connections(None).await
    }

    async fn start_with_connections(source: Option<Arc<dyn ActorSocketSource>>) -> Result<Self> {
        let directory = tempfile::TempDir::new_in(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk"),
        )?;
        let mut tasks = JoinSet::new();
        let issuer = test_issuer()?;
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "counter-1".into(),
        };
        let spec = HostLaunchSpec {
            sandboxes: Default::default(),
            source: None,
            code_snapshot: None,
            project_id: "default".into(),

            image_ref: "test-image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        };
        let revision = spec.host_config_key();
        let host_id = HostId::new(format!("host.v3.{revision}.session"));
        let host_listener = TcpListener::bind("127.0.0.1:0").await?;
        let host_route = format!("http://{}", host_listener.local_addr()?);
        let runtime = crate::bucket::testing::RuntimeFixture::new()?;
        let registry = Arc::new(LocalAdminRegistry::default());
        registry.register_test_deployment(&spec).await?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?;
        let service = ControlPlaneService::new(
            runtime.runtime.clone(),
            auth,
            registry.clone(),
            issuer.clone(),
            Arc::new(SocketTestProvisioner),
        );
        let control_plane = serve_control_plane(&mut tasks, service.clone()).await?;
        let gateway = serve_gateway(
            &mut tasks,
            service,
            AdminService::new(Some("test-api-key".into()), registry, issuer.clone())?,
        )
        .await?;
        let token = issuer
            .issue_host(
                &host_id,
                "00000000-0000-4000-8000-000000000001",
                &revision,
                "us-east",
                &actor,
            )?
            .token;
        let publisher = Arc::new(ControlPlaneClient::connect(control_plane, token.clone()).await?);
        let storage = Arc::new(
            crate::host::storage::HostStorage::new(
                crate::bucket::access::HostStorageConfig {
                    persistence: crate::bucket::PersistenceConfig::Local,
                    bucket: crate::bucket::access::BucketLocation::File {
                        directory: runtime.directory.path().into(),
                    },
                    region: "us-east".into(),
                    token: None,
                },
                host_id.clone(),
                "00000000-0000-4000-8000-000000000001".into(),
                publisher.clone(),
                tokio_util::sync::CancellationToken::new(),
                None,
                Some(actor.clone()),
            )
            .await?
            .with_activation(true, None),
        );
        storage
            .register(&HostLeaseRequest {
                id: host_id.clone(),
                session_id: "00000000-0000-4000-8000-000000000001".into(),
                route: host_route.clone(),
                duration_ms: 60_000,
            })
            .await?;

        let local_sockets = Arc::new(crate::host::sockets::HostSockets::new(storage.clone()));
        let (child, connection) = start_worker(directory.path()).await?;
        connection
            .mark_ready(
                Some(local_sockets.clone()),
                Some(source.unwrap_or_else(|| local_sockets.clone())),
            )
            .await?;
        let host = Arc::new(ActorHost::new(
            HostEndpoint {
                id: host_id.clone(),
                route: host_route,
            },
            connection.executor(),
            storage.clone(),
            storage.runtime.clone(),
            local_sockets.clone(),
            Arc::new(crate::litestream::Litestream::start("litestream".into()).await?),
        ));
        host.activate_actor(actor.clone()).await?;
        tasks.spawn(async move {
            let _ = connection
                .run(tokio_util::sync::CancellationToken::new())
                .await;
        });
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let serving_host = host.clone();
        let socket_routes =
            crate::sockets::browser::router(crate::sockets::browser::SocketServerState {
                registry: local_sockets.registry.clone(),
                verifier: issuer.socket_verifier()?,
                dispatcher: Arc::new(crate::host::sockets::HostSocketDispatcher::new(
                    host.clone(),
                    local_sockets.clone(),
                    "00000000-0000-4000-8000-000000000001".into(),
                )),
                stop: tokio_util::sync::CancellationToken::new(),
            });
        let http_sockets = local_sockets.clone();
        tasks.spawn(async move {
            let routes = socket_routes.merge(
                crate::host::http::ActorHostHttpService::new(
                    serving_host,
                    "00000000-0000-4000-8000-000000000001".into(),
                    auth,
                    http_sockets,
                )
                .router(),
            );
            let _ = axum::serve(host_listener, routes).await;
        });
        Ok(Self {
            issuer,
            directory,
            _runtime: runtime,
            tasks,
            child,
            gateway,
            host_token: token,
            actor,
            host_id,
            publisher: local_sockets,
            host,
            storage,
        })
    }

    async fn invoke(
        &self,
        method: &str,
        args: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let result = self
            .host
            .invoke_actor(
                crate::actor::ActorInvocation {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    actor: self.actor.clone(),
                    method: method.into(),
                    args,
                },
                1,
            )
            .await?;
        match result {
            crate::actor::ActorExecutionResult::Completed { result, .. } => Ok(result),
            other => anyhow::bail!("actor call failed: {other:?}"),
        }
    }

    async fn connect(&self) -> Result<Socket> {
        let grant = self.grant(serde_json::json!({}), 60_000).await?;
        let (socket, _) =
            tokio_tungstenite::connect_async(grant["websocketUrl"].as_str().context("socket URL")?)
                .await?;
        Ok(socket)
    }
}

impl Drop for Stack {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}

async fn serve_control_plane(
    tasks: &mut JoinSet<()>,
    service: ControlPlaneService,
) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    tasks.spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(service.into_internal_service())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Ok(url)
}

async fn serve_gateway(
    tasks: &mut JoinSet<()>,
    service: ControlPlaneService,
    admin: AdminService,
) -> Result<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    tasks.spawn(async move {
        let _ = axum::serve(listener, super::super::public_api::router(service, admin)).await;
    });
    Ok(url)
}

async fn start_worker(
    directory: &std::path::Path,
) -> Result<(tokio::process::Child, crate::actor::ActorExecutorConnection)> {
    let sdk = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("sdk/dist");
    ensure!(
        sdk.join("host.js").exists(),
        "run pnpm --dir sdk build before this test"
    );
    let entrypoint = directory.join("actors.ts");
    std::fs::write(
        &entrypoint,
        format!(
            r#"
import {{ Actor, Persisted, Emittable, type ActorSocket }} from {};
import {{ existsSync }} from 'node:fs';
import {{ setTimeout }} from 'node:timers/promises';
export class Counter extends Actor<{{name?:string; notified?:boolean; user?:string}}, {{type:"start"}}, {{delta:string}} | {{text:string}}> {{
    @Persisted history = '';
    @Persisted @Emittable count = 0;
    @Persisted private secret = 'private';
    @Persisted protected internal = 'internal';
    async readHistory() {{ return this.history; }}
    async appendHistory() {{ this.history += 'saved'; return this.history; }}
    async change() {{ this.count++; this.count++; this.secret = 'changed'; }}
    async fail() {{ this.count++; throw new Error('failed'); }}
    async onConnect(socket: ActorSocket<{{name?:string; notified?:boolean; user?:string}}, {{delta:string}} | {{text:string}}, "member">) {{
        socket.metadata = {{ name: 'member' }};
        socket.setTags('member');
    }}
    async clients() {{
        return (await this.getConnections()).map(socket => ({{ id: socket.id, metadata: socket.metadata, tags: socket.tags }}));
    }}
    async watchFiles(id: string, tags: string[]) {{
        (await this.getConnections()).find(socket => socket.id === id)!.setTags(...tags);
    }}
    async notifyFiles(tagMatch: "all" | "any") {{
        this.broadcast({{ text: 'matched' }}, {{ tags: ['file:a', 'file:b'], tagMatch }});
        this.broadcast({{ text: 'done' }});
    }}
    async notifyClient(id: string) {{
        const socket = (await this.getConnections()).find(socket => socket.id === id)!;
        socket.metadata = {{ ...socket.metadata, notified: true }};
        socket.send({{ text: 'from method' }});
    }}
    async onMessage() {{
        if (process.env.TEST_ACTOR_SECRET !== 'injected') throw new Error('actor secret missing');
        this.history += 'first';
        this.broadcast({{ delta: 'first' }});
        while (!existsSync({})) await setTimeout(10);
        this.history += 'last';
        this.broadcast({{ delta: 'last' }});
    }}
}}
"#,
            serde_json::to_string(&format!("{}", sdk.join("index.js").display()))?,
            serde_json::to_string(&directory.join("release"))?
        ),
    )?;
    std::fs::write(
        directory.join("tsconfig.json"),
        r#"{"compilerOptions":{"target":"ES2022","module":"NodeNext","moduleResolution":"NodeNext","strict":true,"skipLibCheck":true,"types":["node"],"typeRoots":["../node_modules/@types"]},"include":["actors.ts"]}"#,
    )?;
    let code = directory.join("built");
    let build = tokio::time::timeout(
        Duration::from_secs(30),
        tokio::process::Command::new("bun")
            .arg(sdk.join("compiler/deployment-build.js"))
            .arg(directory)
            .arg("actors.ts")
            .arg(&code)
            .arg("local")
            .kill_on_drop(true)
            .output(),
    )
    .await??;
    ensure!(
        build.status.success(),
        "actor build failed: {}",
        String::from_utf8_lossy(&build.stderr)
    );
    let socket_directory = tempfile::tempdir()?;
    let socket = socket_directory.path().join("executor.sock");
    let listener = ActorExecutorListener::bind(&socket).await?;
    let bootstrap = directory.join("host.mjs");
    std::fs::write(
        &bootstrap,
        format!(
            "import {{ runActorHost }} from {}; await runActorHost();",
            serde_json::to_string(&format!("file://{}", sdk.join("host.js").display()))?
        ),
    )?;
    let child = tokio::process::Command::new("bun")
        .arg(bootstrap)
        .env("DURABLE_ACTORS_ENTRYPOINT", code.join("actors.mjs"))
        .env("DURABLE_ACTORS_EXECUTOR_SOCKET", socket)
        .env("TEST_ACTOR_SECRET", "injected")
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()?;
    let connection = tokio::time::timeout(Duration::from_secs(10), listener.accept()).await??;
    Ok((child, connection))
}

async fn receive(socket: &mut Socket) -> Result<serde_json::Value> {
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await?
        .context("socket closed")??;
    ensure!(frame.is_text(), "unexpected socket frame: {frame:?}");
    serde_json::from_str(frame.to_text()?)
        .with_context(|| format!("invalid socket JSON: {frame:?}"))
}

struct SocketTestProvisioner;

#[async_trait]
impl HostProvisioner for SocketTestProvisioner {
    fn host_idle_timeout_ms(&self) -> u64 {
        10_000
    }

    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _previous: Option<&HostLaunchSpec>,
        _region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<crate::control_plane::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }

    async fn socket_credentials(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        lease: &HostLease,
    ) -> Result<crate::sandbox::SocketCredentials> {
        Ok(crate::sandbox::SocketCredentials {
            url: lease.route.clone(),
        })
    }
    async fn ensure_actor_host(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
        _owner_hint: Option<&crate::bucket::OwnershipHint>,
    ) -> Result<(HostLease, u64)> {
        anyhow::bail!("fixture host must be active")
    }
    async fn terminate_hosts(
        &self,
        _spec: &HostLaunchSpec,
        _regions: &[String],
    ) -> Result<HostTermination> {
        anyhow::bail!("unused")
    }
}

fn socket_key(grant: &serde_json::Value) -> Result<String> {
    let url = reqwest::Url::parse(
        grant["websocketUrl"]
            .as_str()
            .context("socket URL missing")?,
    )?;
    url.query_pairs()
        .find(|(name, _)| name == "key")
        .map(|(_, value)| value.into_owned())
        .context("socket key missing")
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn http_invocations_and_socket_delivery_are_actor_bound() -> Result<()> {
    let mut stack = Stack::start().await?;
    let mut socket = stack.connect().await?;
    receive(&mut socket).await?;
    let http = reqwest::Client::new();
    let target: serde_json::Value = http
        .post(format!(
            "{}/v1/projects/default/actors/Counter/counter-1/find-actor",
            stack.gateway
        ))
        .bearer_auth("test-api-key")
        .json(&serde_json::json!({}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let route = target["route"].as_str().context("route missing")?;
    let token = target["token"].as_str().context("token missing")?;
    let epoch = target["ownerEpoch"].as_u64().context("epoch missing")?;
    let url = format!("{route}/v1/projects/default/actors/Counter/counter-1");
    let invocation = serde_json::json!({"requestId":"http-change", "ownerEpoch":epoch, "method":"change", "args":[]});
    assert_eq!(
        http.post(format!("{url}/invoke"))
            .json(&invocation)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let reply: serde_json::Value = http
        .post(format!("{url}/invoke"))
        .bearer_auth(token)
        .json(&invocation)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        reply,
        serde_json::json!({"type":"completed", "result":null})
    );
    let update = receive(&mut socket).await?;
    assert_eq!(update["changes"]["count"], 2);
    let command = serde_json::json!({"ownerEpoch":epoch, "effects":[{"type":"broadcast", "message":{"type":"text", "data":"{\"notice\":\"hello\"}"}, "except_connection_ids":[], "tags":[]}]});
    assert_eq!(
        http.post(format!("{url}/socket-effects"))
            .bearer_auth(token)
            .json(&command)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NO_CONTENT
    );
    assert_eq!(
        receive(&mut socket).await?,
        serde_json::json!({"notice":"hello"})
    );
    for (actor_id, owner_epoch) in [("counter-1", epoch + 1), ("another", epoch)] {
        let mut wrong = invocation.clone();
        wrong["ownerEpoch"] = owner_epoch.into();
        assert_eq!(
            http.post(format!(
                "{route}/v1/projects/default/actors/Counter/{actor_id}/invoke"
            ))
            .bearer_auth(token)
            .json(&wrong)
            .send()
            .await?
            .status(),
            reqwest::StatusCode::FORBIDDEN
        );
        let mut wrong = command.clone();
        wrong["ownerEpoch"] = owner_epoch.into();
        assert_eq!(
            http.post(format!(
                "{route}/v1/projects/default/actors/Counter/{actor_id}/socket-effects"
            ))
            .bearer_auth(token)
            .json(&wrong)
            .send()
            .await?
            .status(),
            reqwest::StatusCode::FORBIDDEN
        );
    }
    let mut malformed = invocation.clone();
    malformed["args"] = serde_json::json!({});
    assert_eq!(
        http.post(format!("{url}/invoke"))
            .bearer_auth(token)
            .json(&malformed)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );
    stack
        .storage
        .unregister(&stack.host_id, "00000000-0000-4000-8000-000000000001")
        .await?;
    assert_eq!(
        http.post(format!("{url}/socket-effects"))
            .bearer_auth(token)
            .json(&command)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn delegated_http_invocations_enforce_methods_and_socket_boundaries() -> Result<()> {
    let mut stack = Stack::start().await?;
    let http = reqwest::Client::new();
    let target: serde_json::Value = http
        .post(format!(
            "{}/v1/projects/default/actors/Counter/counter-1/find-actor",
            stack.gateway
        ))
        .bearer_auth("test-api-key")
        .json(&serde_json::json!({}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let url = format!(
        "{}/v1/projects/default/actors/Counter/counter-1",
        target["route"].as_str().unwrap()
    );
    let epoch = target["ownerEpoch"].as_u64().unwrap();
    let expiry = crate::control_plane::auth::unix_seconds()? + 60;
    let ticket = stack
        .issuer
        .issue_invocation_target(
            &stack.actor,
            &stack.host_id,
            "00000000-0000-4000-8000-000000000001",
            "revision",
            "us-east",
            epoch,
            Some(crate::control_plane::session::InvocationGrant {
                subject: "credential-a".into(),
                grant_id: "grant-a".into(),
                expires_at: expiry,
                methods: vec!["readHistory".into()],
            }),
            "http://10.1.2.3:7101",
        )?
        .token;
    for method in ["change", "onConnect", "onMessage", "onDisconnect"] {
        let reply: serde_json::Value = http.post(format!("{url}/invoke")).bearer_auth(&ticket)
            .json(&serde_json::json!({"requestId":uuid::Uuid::new_v4().to_string(), "ownerEpoch":epoch, "method":method, "args":[]}))
            .send().await?.error_for_status()?.json().await?;
        assert_eq!(reply["code"], "forbidden");
    }
    assert_eq!(
        http.post(format!("{url}/socket-effects"))
            .bearer_auth(&ticket)
            .json(&serde_json::json!({"ownerEpoch":epoch,"effects":[]}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let reply: serde_json::Value = http.post(format!("{url}/invoke")).bearer_auth(&ticket)
        .json(&serde_json::json!({"requestId":uuid::Uuid::new_v4().to_string(), "ownerEpoch":epoch, "method":"readHistory", "args":[]}))
        .send().await?.error_for_status()?.json().await?;
    assert_eq!(reply["type"], "completed");
    stack.child.kill().await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires pnpm --dir sdk build"]
async fn combined_invocation_commits_a_real_actor_call_and_returns_its_result() -> Result<()> {
    let mut stack = Stack::start().await?;
    let reply: serde_json::Value = reqwest::Client::new()
        .post(format!(
            "{}/v1/projects/default/actors/Counter/counter-1/invoke",
            stack.gateway
        ))
        .bearer_auth("test-api-key")
        .json(
            &serde_json::json!({"requestId":"combined-call", "method":"appendHistory", "args":[]}),
        )
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        reply["outcome"],
        serde_json::json!({"type":"completed", "result":"saved"})
    );
    let warm: serde_json::Value = reqwest::Client::new()
        .post(format!(
            "{}/v1/projects/default/actors/Counter/counter-1/invoke",
            reply["target"]["route"].as_str().unwrap()
        ))
        .bearer_auth(reply["target"]["token"].as_str().unwrap())
        .json(&serde_json::json!({
            "requestId":"warm-call", "ownerEpoch":reply["target"]["ownerEpoch"],
            "method":"readHistory", "args":[]
        }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(
        warm,
        serde_json::json!({"type":"completed", "result":"saved"})
    );
    stack.child.kill().await?;
    Ok(())
}
