use crate::request_tracking::HostState;
use std::{sync::Arc, time::Duration};

use anyhow::Result;
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::{
    actor::ActorKey,
    bucket::{RuntimeStorage, testing::RuntimeFixture},
    host::HostId,
    host_leases::HostLeaseRequest,
};

use super::{ActorJwtIssuer, admin::AdminService, inspection::ActorInspector};

#[tokio::test]
async fn inventory_reads_and_streams_filter_projects_before_grouping() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (project, id) in [("alpha", "same"), ("beta", "same"), ("beta", "extra")] {
        let actor = ActorKey {
            project_id: project.into(),
            ..fixture.actor(id)
        };
        fixture
            .store
            .register_activation(
                &actor,
                &HostLeaseRequest {
                    id: HostId::new(format!("host-{project}-{id}")),
                    session_id: "session".into(),
                    route: "http://localhost:7101".into(),
                    duration_ms: 60_000,
                },
                "north-america-east",
                true,
                None,
            )
            .await?;
    }
    let page: Value = fixture
        .get("/v1/projects/alpha/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["actors"][0]["unknown"], 1);
    assert_eq!(page["actors"][0]["instances"].as_array().unwrap().len(), 1);
    let mut stream = fixture
        .get("/v1/projects/alpha/observe/events")
        .await?
        .error_for_status()?;
    assert_eq!(stream_inventory(&mut stream).await?, page);
    let empty: Value = fixture
        .get("/v1/projects/empty/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(empty["actors"], json!([]));
    Ok(())
}

#[tokio::test]
async fn project_observability_isolates_history_metrics_and_streams() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (project, duration) in [("alpha", 10), ("beta", 90)] {
        for index in 0..2 {
            let trace = serde_json::from_value(json!({
                "projectId": project, "requestId": format!("{project}-{index}"),
                "actorName": "Room", "actorId": "same", "kind": "websocket",
                "operation": "onConnect", "connectionId": "same",
                "startedAtMs": index + 1, "durationMs": duration,
                "queueWaitMs": duration, "outcome": "completed"
            }))?;
            fixture
                .traces
                .record(project, "host", "session", vec![trace], 0)
                .await?;
        }
    }
    let base = "/v1/projects/alpha/observe";
    let page: Value = fixture
        .get(&format!("{base}/requests?limit=1"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["records"].as_array().unwrap().len(), 1);
    assert_eq!(page["records"][0]["projectId"], "alpha");
    let metrics: Value = fixture
        .get(&format!("{base}/metrics"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(metrics["total"]["count"], 2);
    assert_eq!(metrics["total"]["p95"], 10.0);
    let waits: Value = fixture
        .get(&format!("{base}/queue-waits"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(waits[0]["admitted"], 2);
    assert_eq!(waits[0]["averageMs"], 10.0);
    let sockets: Value = fixture
        .get(&format!("{base}/websockets"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(sockets.as_array().unwrap().len(), 1);
    let mut stream = fixture
        .get(&format!("{base}/requests/events"))
        .await?
        .error_for_status()?;
    let chunk = tokio::time::timeout(Duration::from_secs(5), stream.chunk())
        .await??
        .unwrap();
    let event = String::from_utf8(chunk.to_vec())?;
    assert!(event.contains("alpha"));
    assert!(!event.contains("beta"));
    for (route, cursor) in [
        ("requests?cursor=", "nextCursor"),
        ("requests/events?after=", "resumeCursor"),
    ] {
        let url = format!(
            "/v1/projects/beta/observe/{route}{}",
            page[cursor].as_str().unwrap()
        );
        assert_eq!(fixture.get(&url).await?.status(), StatusCode::BAD_REQUEST);
    }
    for route in [
        "requests",
        "requests/events",
        "metrics",
        "queue-waits",
        "websockets",
    ] {
        let url = format!("/v1/projects/bad!/observe/{route}");
        assert_eq!(fixture.get(&url).await?.status(), StatusCode::BAD_REQUEST);
    }
    Ok(())
}

#[tokio::test]
async fn durability_endpoint_is_not_exposed() -> Result<()> {
    let fixture = Fixture::start().await?;
    assert_eq!(
        fixture.get("/v1/durability").await?.status(),
        StatusCode::NOT_FOUND
    );
    Ok(())
}

#[tokio::test]
async fn observability_requires_admin_credentials() -> Result<()> {
    let fixture = Fixture::start().await?;
    let token = fixture
        .issuer
        .issue_host(
            &fixture.host,
            &uuid::Uuid::new_v4().to_string(),
            "r1",
            "north-america-east",
            &fixture.actor("one"),
        )?
        .token;
    for path in [
        "/v1/projects/default/observe/actors",
        "/v1/projects/default/observe/events",
        "/v1/projects/default/observe/requests/events",
        "/v1/projects/default/observe/requests",
        "/v1/projects/default/observe/metrics",
        "/v1/projects/default/observe/queue-waits",
        "/v1/projects/default/observe/websockets",
        "/v1/projects/default/observe/state?actorName=Room&actorId=one",
        "/v1/projects/default/observe/state/history?actorName=Room&actorId=one",
    ] {
        for credential in [None, Some(""), Some("wrong"), Some(token.as_str())] {
            let mut request = fixture.client.get(format!("{}{path}", fixture.origin));
            if let Some(credential) = credential {
                request = request.bearer_auth(credential);
            }
            assert_eq!(request.send().await?.status(), StatusCode::UNAUTHORIZED);
        }
    }
    Ok(())
}

struct Fixture {
    traces: crate::request_traces::TraceStore,
    changes: crate::postgres::notifications::ChangeFeed,
    admin: AdminService,
    _runtime: RuntimeFixture,
    store: Arc<RuntimeStorage>,
    host: HostId,
    issuer: ActorJwtIssuer,
    origin: String,
    client: reqwest::Client,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Fixture {
    async fn start() -> Result<Self> {
        Self::start_with_traces(crate::request_traces::TraceStore::default()).await
    }

    async fn start_with_traces(traces: crate::request_traces::TraceStore) -> Result<Self> {
        let runtime = RuntimeFixture::new()?;
        let store = runtime.runtime.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let origin = format!("http://{}", listener.local_addr()?);
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
        let issuer = ActorJwtIssuer::from_base64_pkcs8(
            &STANDARD.encode(pkcs8.as_ref()),
            "key",
            "issuer",
            "authority",
            "invocation",
            Duration::from_secs(60),
        )?;
        let admin = AdminService::new(
            Some("api-key".into()),
            Arc::new(super::admin::LocalAdminRegistry::default()),
            issuer.clone(),
        )?;
        let changes = crate::postgres::notifications::ChangeFeed::default();
        let inspector = ActorInspector::new(store.clone(), store.clone(), changes.clone())
            .with_traces(traces.clone());
        let routes = super::inspection::router(inspector, admin.clone());
        let server = tokio::spawn(async { axum::serve(listener, routes).await });
        let host = HostId::new("host.v3.test");
        Ok(Self {
            traces,
            changes,
            admin,
            _runtime: runtime,
            store,
            host,
            issuer,
            origin,
            client: reqwest::Client::new(),
            server,
        })
    }

    fn actor(&self, id: &str) -> ActorKey {
        ActorKey {
            project_id: "default".into(),
            actor_name: "Room.with.dots".into(),
            actor_id: id.into(),
        }
    }

    async fn get(&self, path: &str) -> Result<reqwest::Response> {
        Ok(self
            .client
            .get(format!("{}{path}", self.origin))
            .bearer_auth("api-key")
            .send()
            .await?)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn inventory_stream_reports_host_queues_and_fences_expired_sessions() -> Result<()> {
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("unsaved");
    let request = HostLeaseRequest {
        id: fixture.host.clone(),
        session_id: "session".into(),
        route: "http://localhost:7101".into(),
        duration_ms: 60_000,
    };
    fixture
        .store
        .register_activation(&actor, &request, "north-america-east", true, None)
        .await?;
    let mut stream = fixture
        .get("/v1/projects/default/observe/events")
        .await?
        .error_for_status()?;
    let first = stream_inventory(&mut stream).await?;
    assert!(first.get("namespaceId").is_none());
    assert_eq!(first["actors"][0]["unknown"], 1);
    let queues = vec![crate::host_leases::ActorQueueInventory {
        actor: actor.clone(),
        waiting: vec![crate::host_leases::WaitingOperation {
            id: "queued-1".into(),
            operation: "sendMessage".into(),
        }],
    }];
    fixture
        .store
        .renew_activation(
            &actor,
            &request,
            crate::host_leases::ActivationInventory {
                resident: Some(true),
                waiting: Some(queues[0].waiting.clone()),
            },
        )
        .await?;
    fixture.changes.notify().await;
    let connected = stream_inventory(&mut stream).await?;
    assert_eq!(connected["actors"][0]["live"], 1);
    assert_eq!(
        connected["actors"][0]["instances"][0]["waiting"][0]["operation"],
        "sendMessage"
    );
    fixture
        .store
        .release_activation(&actor, &fixture.host, "session")
        .await?;
    fixture.changes.notify().await;
    let expired = stream_inventory(&mut stream).await?;
    assert_eq!(expired["actors"][0]["dormant"], 1);
    assert_eq!(expired["actors"][0]["instances"][0]["waiting"], json!([]));
    assert_eq!(
        expired["actors"][0]["instances"][0]["connections"],
        json!([])
    );
    let replacement = HostLeaseRequest {
        session_id: "replacement".into(),
        ..request
    };
    assert!(
        fixture
            .store
            .renew_activation(&actor, &replacement, Default::default())
            .await
            .is_err()
    );
    let inventory: Value = fixture
        .get("/v1/projects/default/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(inventory["actors"][0]["dormant"], 1);
    assert_eq!(inventory["actors"][0]["instances"][0]["waiting"], json!([]));
    assert_eq!(
        inventory["actors"][0]["instances"][0]["connections"],
        json!([])
    );
    Ok(())
}

async fn stream_inventory(stream: &mut reqwest::Response) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = Vec::new();
        loop {
            bytes.extend(
                stream
                    .chunk()
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("stream ended"))?,
            );
            let text = String::from_utf8_lossy(&bytes);
            if text.ends_with("\n\n") {
                if let Some(data) = text.lines().find_map(|line| line.strip_prefix("data: ")) {
                    return serde_json::from_str(data).map_err(Into::into);
                }
            }
        }
    })
    .await?
}

#[tokio::test]
async fn inventory_includes_unused_deployed_types_without_loading_actors() -> Result<()> {
    let fixture = Fixture::start().await?;
    let contract = super::contracts::PublicActorContract::new(serde_json::from_str(
        include_str!("../../../sdk/tests/fixtures/public-contract.json"),
    )?)?;
    fixture
        .admin
        .register_deployment(
            &super::admin::HostLaunchSpec {
                sandboxes: Default::default(),
                project_id: "default".into(),
                source: None,
                code_snapshot: Some(crate::sandbox::testing::code_artifact(1)),
                image_ref: "test-image".into(),
                working_directory: "/customer".into(),
                actor_entrypoint: Some("actors.mjs".into()),
                secret_refs: vec![],
            },
            Some(&contract),
        )
        .await?;
    let inventory: Value = fixture
        .get("/v1/projects/default/observe/actors")
        .await?
        .error_for_status()?
        .json()
        .await?;
    let row = inventory["actors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["actorName"] == "ChatRoom")
        .unwrap();
    assert_eq!(row["live"], 0);
    assert_eq!(row["dormant"], 0);
    assert_eq!(row["instances"], json!([]));
    Ok(())
}

#[tokio::test]
async fn request_history_streams_distinct_records_and_replays_on_reconnect() -> Result<()> {
    use crate::request_traces::{
        RequestKind, RequestOutcome, RequestTrace, TraceStore,
        persistence::sqlite::SqliteTracePersistence,
    };
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("request-traces.sqlite3");
    let open = || TraceStore::open(Arc::new(SqliteTracePersistence::new(path.clone())));
    let fixture = Fixture::start_with_traces(open().await?).await?;
    let mut stream = fixture
        .get("/v1/projects/default/observe/requests/events")
        .await?
        .error_for_status()?;
    assert_eq!(stream.headers()["cache-control"], "no-store");
    let empty = stream_inventory(&mut stream).await?;
    assert_eq!(empty["records"], json!([]));
    for id in ["first", "second"] {
        fixture
            .traces
            .record(
                "default",
                "host",
                "session",
                vec![RequestTrace {
                    state_version: None,
                    project_id: "default".into(),
                    request_id: id.into(),
                    actor_name: "Room".into(),
                    actor_id: "one".into(),
                    kind: RequestKind::Method,
                    operation: "post".into(),
                    connection_id: None,
                    started_at_ms: 1000,
                    duration_ms: 25.0,
                    queue_wait_ms: Some(10.0),
                    host_state: HostState::Warm,
                    outcome: RequestOutcome::Completed,
                    metadata: None,
                }],
                0,
            )
            .await?;
        let page = stream_inventory(&mut stream).await?;
        assert_eq!(page["records"].as_array().unwrap().len(), 1);
        assert_eq!(page["records"][0]["requestId"], id);
        assert_eq!(page["records"][0]["queueWaitMs"], 10.0);
        assert_eq!(page["epoch"], empty["epoch"]);
    }
    drop(stream);
    let snapshot = serde_json::to_value(
        fixture
            .traces
            .replay("default", &Default::default())
            .await?,
    )?;
    let mut replay = fixture
        .get("/v1/projects/default/observe/requests/events")
        .await?;
    assert_eq!(stream_inventory(&mut replay).await?, snapshot);
    assert_eq!(snapshot["records"].as_array().unwrap().len(), 2);
    drop(replay);
    drop(fixture);
    let restarted = Fixture::start_with_traces(open().await?).await?;
    let mut replay = restarted
        .get("/v1/projects/default/observe/requests/events")
        .await?;
    let restored = stream_inventory(&mut replay).await?;
    assert_eq!(restored["records"], snapshot["records"]);
    assert_eq!(restored["epoch"], snapshot["epoch"]);
    Ok(())
}

#[tokio::test]
async fn request_history_validates_filters_and_returns_empty_pages() -> Result<()> {
    let fixture = Fixture::start().await?;
    let response = fixture.get("/v1/projects/default/observe/requests").await?;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["cache-control"], "no-store");
    let page: Value = response.json().await?;
    assert_eq!(page["records"], json!([]));
    assert_eq!(page["nextCursor"], Value::Null);
    assert_eq!(page["capacity"], 100);
    for query in [
        "limit=0",
        "limit=501",
        "limit=-1",
        "limit=1.5",
        "outcome=unknown",
        "fromMs=-1",
        "fromMs=9007199254740992",
        "fromMs=2&toMs=1",
        "actorName=",
        "actorId=bad%2Fid",
        "cursor=broken",
        "unknown=true",
    ] {
        assert_eq!(
            fixture
                .get(&format!("/v1/projects/default/observe/requests?{query}"))
                .await?
                .status(),
            StatusCode::BAD_REQUEST,
            "{query}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn request_history_filters_and_pages_without_including_later_appends() -> Result<()> {
    let fixture = Fixture::start().await?;
    for (id, name, actor, time, outcome) in [
        (
            "old",
            "Room",
            "one",
            999,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "first",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "second",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "other-class",
            "Counter",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "other-actor",
            "Room",
            "two",
            1000,
            crate::request_traces::RequestOutcome::Failed,
        ),
        (
            "completed",
            "Room",
            "one",
            1000,
            crate::request_traces::RequestOutcome::Completed,
        ),
        (
            "future",
            "Room",
            "one",
            1001,
            crate::request_traces::RequestOutcome::Failed,
        ),
    ] {
        record_request(&fixture, id, name, actor, time, outcome).await?;
    }
    let filters = "actorName=Room&actorId=one&outcome=failed&fromMs=1000&toMs=1000&limit=1";
    let first: Value = fixture
        .get(&format!("/v1/projects/default/observe/requests?{filters}"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first["records"].as_array().unwrap().len(), 1);
    assert_eq!(first["records"][0]["requestId"], "second");
    let cursor = first["nextCursor"].as_str().unwrap();
    record_request(
        &fixture,
        "late",
        "Room",
        "one",
        1000,
        crate::request_traces::RequestOutcome::Failed,
    )
    .await?;
    let next: Value = fixture
        .get(&format!(
            "/v1/projects/default/observe/requests?{filters}&cursor={cursor}"
        ))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(next["records"].as_array().unwrap().len(), 1);
    assert_eq!(next["records"][0]["requestId"], "first");
    assert_eq!(next["nextCursor"], Value::Null);
    assert_eq!(next["reset"], false);
    assert_eq!(
        fixture
            .get(&format!(
                "/v1/projects/default/observe/requests?actorId=two&cursor={cursor}"
            ))
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

async fn record_request(
    fixture: &Fixture,
    id: &str,
    actor_name: &str,
    actor_id: &str,
    time: u64,
    outcome: crate::request_traces::RequestOutcome,
) -> Result<()> {
    fixture
        .traces
        .record(
            "default",
            "host",
            "session",
            vec![crate::request_traces::RequestTrace {
                state_version: None,
                project_id: "default".into(),
                request_id: id.into(),
                actor_name: actor_name.into(),
                actor_id: actor_id.into(),
                kind: crate::request_traces::RequestKind::Method,
                operation: "post".into(),
                connection_id: None,
                started_at_ms: time,
                duration_ms: 25.0,
                queue_wait_ms: Some(10.0),
                host_state: HostState::Warm,
                outcome,
                metadata: None,
            }],
            0,
        )
        .await
}

#[tokio::test]
async fn request_stream_resumes_saved_cursors() -> Result<()> {
    let fixture = Fixture::start().await?;
    let snapshot = serde_json::to_value(
        fixture
            .traces
            .replay("default", &Default::default())
            .await?,
    )?;
    let cursor = snapshot["resumeCursor"].as_str().unwrap();
    let mut replay = fixture
        .get(&format!(
            "/v1/projects/default/observe/requests/events?after={cursor}"
        ))
        .await?;
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(stream_inventory(&mut replay).await?["records"], json!([]));
    assert_eq!(
        fixture
            .get("/v1/projects/default/observe/requests/events?after=broken")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn typed_observer_metrics_validate_ranges_and_return_uncached_results() -> Result<()> {
    let fixture = Fixture::start().await?;
    for path in ["metrics", "queue-waits", "websockets"] {
        let response = fixture
            .get(&format!("/v1/projects/default/observe/{path}"))
            .await?;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(response.headers()["cache-control"], "no-store");
        let body: Value = response.json().await?;
        if path == "metrics" {
            assert_eq!(body["total"]["count"], 0);
            assert_eq!(body["total"]["p95"], Value::Null);
            assert_eq!(body["classes"], json!([]));
        } else {
            assert_eq!(body, json!([]));
        }
        for query in [
            "fromMs=-1",
            "fromMs=9007199254740992",
            "fromMs=2&toMs=1",
            "unknown=true",
        ] {
            assert_eq!(
                fixture
                    .get(&format!("/v1/projects/default/observe/{path}?{query}"))
                    .await?
                    .status(),
                StatusCode::BAD_REQUEST,
                "{path}?{query}"
            );
        }
    }
    assert_eq!(
        fixture
            .get("/v1/projects/default/observe/queue-waits?actorName=")
            .await?
            .status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}

#[tokio::test]
async fn postgres_request_stream_observes_writes_from_another_control_plane() -> Result<()> {
    use crate::{
        postgres::{PostgresDatabase, testing::with_postgres},
        request_traces::{TraceStore, persistence::postgres::PostgresTracePersistence},
    };
    with_postgres(async |database| {
        let open = async || TraceStore::open(Arc::new(PostgresTracePersistence::new(
            PostgresDatabase::lazy(&database.url)?, Duration::from_secs(86400),
        ))).await;
        let reader = Fixture::start_with_traces(open().await?).await?;
        let writer = open().await?;
        let mut stream = reader.get("/v1/projects/alpha/observe/requests/events").await?.error_for_status()?;
        let initial = tokio::time::timeout(Duration::from_secs(2), stream.chunk()).await??.unwrap();
        assert!(String::from_utf8(initial.to_vec())?.contains("\"records\":[]"));
        writer.record("alpha", "host", "session", vec![serde_json::from_value(json!({
            "projectId": "alpha", "requestId": "remote-instance-request", "actorName": "Room", "actorId": "one",
            "kind": "method", "operation": "increment", "startedAtMs": 1000, "durationMs": 10.0,
            "queueWaitMs": 1.0, "outcome": "completed"
        }))?], 0).await?;
        tokio::time::timeout(Duration::from_secs(7), async {
            while let Some(chunk) = stream.chunk().await? {
                if String::from_utf8_lossy(&chunk).contains("remote-instance-request") { return anyhow::Ok(()); }
            }
            anyhow::bail!("request stream closed before the shared event arrived")
        }).await??;
        let history: Value = reader.get("/v1/projects/alpha/observe/requests").await?.error_for_status()?.json().await?;
        assert_eq!(history["records"][0]["requestId"], "remote-instance-request");
        Ok(())
    }).await
}

#[tokio::test]
async fn state_inspection_reads_committed_values_and_retained_attribution() -> Result<()> {
    use crate::{state_log::StateSnapshot, state_transport::SnapshotWriter};
    let fixture = Fixture::start().await?;
    let actor = fixture.actor("state-demo");
    let loaded = fixture
        .store
        .register_activation(
            &actor,
            &HostLeaseRequest {
                id: fixture.host.clone(),
                session_id: "state-session".into(),
                route: "http://localhost:7101".into(),
                duration_ms: 60_000,
            },
            "north-america-east",
            true,
            None,
        )
        .await?;
    for version in 1..=3 {
        let plan = fixture
            .store
            .prepare_actor_write(&actor, &loaded.placement.lease, 1, version)
            .await?;
        let snapshot: StateSnapshot = serde_json::from_value(json!({
            "stateVersion": version, "ownerEpoch": 1, "requestId": format!("req-{version}"),
            "sqlite": crate::test_sqlite::snapshot(json!({"count": version, "settings": {"enabled": true}}))?, "result": null,
            "attribution": {"operation": "onMessage", "connectionId": "socket-a", "committedAtMs": 1234, "interleaved": false}
        }))?;
        fixture
            .store
            .write_snapshot(&plan, snapshot.encode()?)
            .await?;
    }
    let path = "/v1/projects/default/observe/state?actorName=Room.with.dots&actorId=state-demo";
    let response = fixture.get(path).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let current: Value = response.json().await?;
    assert_eq!(current["snapshot"]["state"]["count"], 3);
    assert_eq!(current["snapshot"]["requestId"], "req-3");
    assert!(current["snapshot"].get("result").is_none());
    let page: Value = fixture
        .get(&path.replace("/state?", "/state/history?"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(page["records"].as_array().unwrap().len(), 3);
    assert_eq!(
        page["records"][0]["attribution"]["connectionId"],
        "socket-a"
    );
    assert!(page["records"][0].get("state").is_none());
    let page_path = path.replace("/state?", "/state/history?");
    let first: Value = fixture
        .get(&format!("{page_path}&limit=2"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(first["records"].as_array().unwrap().len(), 2);
    assert_eq!(first["nextBefore"], 2);
    let next: Value = fixture
        .get(&format!("{page_path}&before=2&limit=2"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(next["records"][0]["stateVersion"], 1);
    assert!(next["nextBefore"].is_null());
    let old: Value = fixture
        .get(&format!("{path}&version=1"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(old["snapshot"]["state"]["count"], 1);
    let isolated: Value = fixture
        .get(&path.replace("projects/default", "projects/other"))
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert!(isolated["snapshot"].is_null());
    assert_eq!(
        fixture.get(&format!("{path}&version=0")).await?.status(),
        StatusCode::BAD_REQUEST
    );
    Ok(())
}
