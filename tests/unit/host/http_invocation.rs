use super::*;
use crate::{
    control_plane::{
        ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, session::InvocationGrant,
    },
    host::{http::ActorHostHttpService, sockets::HostSockets},
};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};

#[tokio::test]
async fn draining_host_rejects_http_invocations_without_executing_them() -> Result<()> {
    let mut fixture = HttpHost::start(None).await?;
    assert_eq!(
        fixture.invoke("warm", "counter-1").await?,
        json!({"type":"completed", "result":1})
    );
    assert_eq!(fixture.started.recv().await.as_deref(), Some("warm"));
    fixture.host.drain(Duration::from_secs(1)).await?;
    assert_eq!(
        fixture.invoke("rejected", "counter-1").await?,
        json!({"type":"not_executed", "reason":"host_unavailable"})
    );
    fixture.host.drain(Duration::from_secs(1)).await?;
    assert!(fixture.started.try_recv().is_err());
    assert!(fixture.host.queues().inventory().is_empty());
    Ok(())
}

#[tokio::test]
async fn host_assigned_to_another_actor_rejects_before_execution() -> Result<()> {
    let mut fixture = HttpHost::start(None).await?;
    fixture.invoke("warm", "counter-1").await?;
    assert_eq!(fixture.started.recv().await.as_deref(), Some("warm"));
    assert_eq!(
        fixture.invoke("wrong-host", "another").await?,
        json!({"type":"not_executed", "reason":"stale_owner"})
    );
    assert!(fixture.started.try_recv().is_err());
    assert_eq!(
        fixture.invoke("next", "counter-1").await?,
        json!({"type":"completed", "result":2})
    );
    Ok(())
}

#[tokio::test]
async fn interrupted_execution_is_an_unknown_outcome_over_http() -> Result<()> {
    let mut fixture = HttpHost::start(None).await?;
    let reply = fixture.invoke("panic", "counter-1").await?;
    assert_eq!(fixture.started.recv().await.as_deref(), Some("panic"));
    assert_eq!(reply["type"], "failed");
    assert_eq!(reply["code"], "outcome_unknown");
    Ok(())
}

#[tokio::test]
async fn delegated_permissions_are_enforced_before_retryable_admission() -> Result<()> {
    for allowed in [true, false] {
        let grant = InvocationGrant {
            subject: "caller".into(),
            grant_id: "session".into(),
            expires_at: i64::MAX,
            methods: if allowed {
                vec!["increment".into()]
            } else {
                vec!["read".into()]
            },
        };
        let mut fixture = HttpHost::start(Some(grant)).await?;
        fixture.host.drain(Duration::from_secs(1)).await?;
        let reply = fixture.invoke("delegated", "counter-1").await?;
        if allowed {
            assert_eq!(
                reply,
                json!({"type":"not_executed", "reason":"host_unavailable"})
            );
        } else {
            assert_eq!(reply["type"], "failed");
            assert_eq!(reply["code"], "forbidden");
        }
        assert!(fixture.started.try_recv().is_err());
    }
    Ok(())
}

struct HttpHost {
    host: Arc<ActorHost>,
    started: mpsc::UnboundedReceiver<String>,
    issuer: ActorJwtIssuer,
    grant: Option<InvocationGrant>,
    origin: String,
    client: reqwest::Client,
    _tasks: JoinSet<()>,
}

impl HttpHost {
    async fn start(grant: Option<InvocationGrant>) -> Result<Self> {
        let (started_tx, started) = mpsc::unbounded_channel();
        let storage = Arc::new(FakeAuthority::default());
        let sockets = Arc::new(HostSockets::new(
            storage.clone(),
            Arc::new(crate::sockets::SocketRegistry::default()),
        ));
        let host = Arc::new(ActorHost::new(
            HostEndpoint {
                id: crate::host::HostId::new("host.v3.revision-1.host-1"),
                route: "http://host.invalid".into(),
            },
            Arc::new(ControlledExecutor {
                started: started_tx,
                release: Arc::new(tokio::sync::Semaphore::new(0)),
            }),
            storage,
            Arc::new(FakeStateTransport::default()),
            sockets.clone(),
            replication().await,
        ));
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let service = ActorHostHttpService::new(
            host.clone(),
            "00000000-0000-4000-8000-000000000001".into(),
            auth,
            sockets,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            axum::serve(listener, service.router()).await.unwrap();
        });
        Ok(Self {
            host,
            started,
            issuer,
            grant,
            origin,
            client: reqwest::Client::new(),
            _tasks: tasks,
        })
    }

    async fn invoke(&self, request_id: &str, actor_id: &str) -> Result<Value> {
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: actor_id.into(),
        };
        let token = self.issuer.issue_invocation_target(
            &actor,
            self.host.id(),
            "00000000-0000-4000-8000-000000000001",
            "revision-1",
            "us-east",
            1,
            self.grant.clone(),
            "http://10.1.2.3:7101",
        )?;
        Ok(self.client.post(format!("{}/v1/projects/default/actors/Counter/{actor_id}/invoke", self.origin))
            .bearer_auth(token.token)
            .json(&json!({"requestId": request_id, "ownerEpoch": 1, "method": "increment", "args": []}))
            .send().await?.error_for_status()?.json().await?)
    }
}
