use super::*;
use crate::request_tracking::HostState;
use crate::{
    control_plane::{
        ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, session::InvocationGrant,
    },
    host::{http::ActorHostHttpService, sockets::HostSockets},
};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};

#[tokio::test]
async fn invocation_response_reuses_observability_timings() -> Result<()> {
    let mut fixture = HttpHost::start(None).await?;
    let reply = fixture.invoke("timed", "counter-1").await?;
    assert_eq!(reply["result"], 1);
    assert_eq!(reply["metadata"]["hostState"], "warm");
    assert_trace_metadata(&reply, fixture.traces.recv().await.unwrap());
    Ok(())
}

#[tokio::test]
async fn response_and_observability_preserve_routing_start_context() -> Result<()> {
    let mut fixture = HttpHost::start(None).await?;
    for (request_id, host_state) in [("cold", HostState::Cold), ("warm", HostState::Warm)] {
        let reply = fixture
            .invoke_with_start(request_id, "counter-1", host_state)
            .await?;
        assert_eq!(reply["metadata"]["hostState"], host_state.as_str());
        assert_eq!(reply["metadata"]["routingMs"], 500.0);
        assert_trace_metadata(&reply, fixture.traces.recv().await.unwrap());
    }
    Ok(())
}

#[test]
fn diagnostic_logs_use_finalized_request_timing() -> Result<()> {
    let log = tempfile::NamedTempFile::new()?;
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_writer(log.reopen()?)
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let (mut tracker, response) = RequestTracker::new(Instant::now(), HostState::Warm, 0.0, None);
    tracker.admitted();
    tracker.mark(crate::request_tracking::RequestStage::StateCacheChecked);
    tracker.mark(crate::request_tracking::RequestStage::ActorExecutionCompleted);
    let result = Ok(ActorExecutionResult::Completed {
        result: Value::Null,
        effects: vec![],
    });
    tracker.complete(&result);
    let metadata: Value =
        serde_json::from_str(&serde_json::to_string(&response.blocking_recv()?)?)?;
    let reply = json!({"metadata": metadata});
    ActorRuntime::log_invocation(
        &HostEndpoint {
            id: crate::host::HostId::new("host"),
            route: "http://host.invalid".into(),
        },
        &ActorInvocation {
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "one".into(),
            },
            request_id: "timed".into(),
            method: "increment".into(),
            args: vec![],
        },
        tracker.completion(),
        &result,
    );
    let logs = std::fs::read_to_string(log.path())?;
    let invocations: Vec<Value> = logs
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .filter(|entry| entry["fields"]["event"] == "actor_host_invocation")
        .collect();
    assert_eq!(invocations.len(), 1);
    let fields = &invocations[0]["fields"];
    assert_eq!(fields["completed_at_ms"], reply["metadata"]["durationMs"]);
    assert_eq!(fields["host_state"], reply["metadata"]["hostState"]);
    assert_eq!(
        fields["queue_admitted_at_ms"],
        reply["metadata"]["queueWaitMs"]
    );
    for stage in [
        "state_cache_checked_at_ms",
        "actor_execution_completed_at_ms",
    ] {
        let elapsed = fields[stage].as_f64().expect("stage checkpoint");
        assert!(elapsed >= reply["metadata"]["queueWaitMs"].as_f64().unwrap());
        assert!(elapsed <= reply["metadata"]["durationMs"].as_f64().unwrap());
    }
    Ok(())
}

#[tokio::test]
async fn queued_response_reuses_observability_queue_wait() -> Result<()> {
    let mut fixture = HttpHost::start(None).await?;
    let mut activity = fixture.host.activity();
    let (first, second, released) = tokio::join!(
        fixture.invoke("first", "counter-1"),
        async {
            activity.wait_for(|value| value.active == 1).await?;
            fixture.invoke("second", "counter-1").await
        },
        async {
            let mut activity = fixture.host.activity();
            activity.wait_for(|value| value.active == 2).await?;
            tokio::time::sleep(Duration::from_millis(25)).await;
            fixture.release.add_permits(1);
            Ok::<_, anyhow::Error>(())
        }
    );
    released?;
    assert_trace_metadata(&first?, fixture.traces.recv().await.unwrap());
    let second = second?;
    assert!(second["metadata"]["queueWaitMs"].as_f64().unwrap() >= 25.0);
    assert_trace_metadata(&second, fixture.traces.recv().await.unwrap());
    Ok(())
}

fn assert_trace_metadata(reply: &Value, trace: crate::request_traces::RequestTrace) {
    // Decode both through JSON, as the HTTP response and observability API do.
    let trace: Value = serde_json::from_str(&serde_json::to_string(&trace).unwrap()).unwrap();
    assert_eq!(
        reply["metadata"],
        json!({
            "durationMs": trace["durationMs"],
            "queueWaitMs": trace["queueWaitMs"],
            "hostState": trace["hostState"],
            "routingMs": trace["routingMs"],
        })
    );
}

#[tokio::test]
async fn draining_host_rejects_http_invocations_without_executing_them() -> Result<()> {
    let mut fixture = HttpHost::start(None).await?;
    assert_eq!(
        without_metadata(fixture.invoke("warm", "counter-1").await?),
        json!({"type":"completed", "result":1})
    );
    assert_eq!(fixture.started.recv().await.as_deref(), Some("warm"));
    fixture.host.drain(Duration::from_secs(1)).await?;
    assert_eq!(
        without_metadata(fixture.invoke("rejected", "counter-1").await?),
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
        without_metadata(fixture.invoke("wrong-host", "another").await?),
        json!({"type":"not_executed", "reason":"stale_owner"})
    );
    assert!(fixture.started.try_recv().is_err());
    assert_eq!(
        without_metadata(fixture.invoke("next", "counter-1").await?),
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
    assert_trace_metadata(&reply, fixture.traces.recv().await.unwrap());
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
            assert!(reply["metadata"]["queueWaitMs"].is_null());
            assert_trace_metadata(&reply, fixture.traces.recv().await.unwrap());
            assert_eq!(
                without_metadata(reply),
                json!({"type":"not_executed", "reason":"host_unavailable"})
            );
        } else {
            assert_eq!(reply["type"], "failed");
            assert_eq!(reply["code"], "forbidden");
            assert!(reply.get("metadata").is_none());
            assert!(fixture.traces.try_recv().is_err());
        }
        assert!(fixture.started.try_recv().is_err());
    }
    Ok(())
}

fn without_metadata(mut reply: Value) -> Value {
    let metadata = reply
        .as_object_mut()
        .unwrap()
        .remove("metadata")
        .expect("response metadata");
    let duration = metadata["durationMs"].as_f64().expect("host duration");
    assert!(duration.is_finite() && duration >= 0.0);
    assert!(metadata.get("queueWaitMs").is_some());
    reply
}

struct HttpHost {
    host: Arc<ActorHost>,
    started: mpsc::UnboundedReceiver<String>,
    traces: mpsc::Receiver<crate::request_traces::RequestTrace>,
    release: Arc<tokio::sync::Semaphore>,
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
        let (trace_sender, traces) = crate::request_traces::TraceSender::channel(16);
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let sockets = Arc::new(HostSockets::new(
            storage.clone(),
            Arc::new(crate::sockets::SocketRegistry::default()),
        ));
        let host = Arc::new(
            ActorHost::new(
                HostEndpoint {
                    id: crate::host::HostId::new("host.v3.revision-1.host-1"),
                    route: "http://host.invalid".into(),
                },
                Arc::new(ControlledExecutor {
                    started: started_tx,
                    release: release.clone(),
                }),
                storage,
                Arc::new(FakeStateTransport::default()),
                sockets.clone(),
                replication().await,
            )
            .with_traces(trace_sender),
        );
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
            traces,
            release,
            issuer,
            grant,
            origin,
            client: reqwest::Client::new(),
            _tasks: tasks,
        })
    }

    async fn invoke(&self, request_id: &str, actor_id: &str) -> Result<Value> {
        self.invoke_with_start(request_id, actor_id, HostState::Warm)
            .await
    }

    async fn invoke_with_start(
        &self,
        request_id: &str,
        actor_id: &str,
        host_state: HostState,
    ) -> Result<Value> {
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
            .json(&json!({"requestId": request_id, "ownerEpoch": 1, "method": "increment", "args": [], "hostState": host_state, "routingMs": 500.0}))
            .send().await?.error_for_status()?.json().await?)
    }
}
