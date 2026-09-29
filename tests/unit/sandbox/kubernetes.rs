use super::*;

#[test]
fn customer_pod_enforces_isolation_and_allows_node_scale_down() -> Result<()> {
    let request = CreateSpareRequest {
        control_plane_url: Some("http://control:7100".into()),
        kind: SpareKind::Actor,
        name: "warm-one".into(),
        image_ref: "runtime@sha256:abc".into(),
        canonical_region: "north-america-west".into(),
        resources: ResourceLimits {
            cpu_millis: 2000,
            memory_mib: 4096,
        },
    };
    let pod = spare_pod(&request, "us-west4-a", "test-token")?;
    assert_eq!(
        pod.metadata
            .annotations
            .as_ref()
            .and_then(
                |annotations| annotations.get("cluster-autoscaler.kubernetes.io/safe-to-evict")
            )
            .map(String::as_str),
        Some("true"),
    );
    let spec = pod.spec.unwrap();
    assert_eq!(spec.runtime_class_name.as_deref(), Some("gvisor"));
    assert_eq!(spec.automount_service_account_token, Some(false));
    assert_eq!(
        spec.node_selector.unwrap()["topology.kubernetes.io/zone"],
        "us-west4-a"
    );
    assert_eq!(
        spec.containers[0]
            .resources
            .as_ref()
            .unwrap()
            .limits
            .as_ref()
            .unwrap()["memory"]
            .0,
        "4096Mi"
    );
    assert_eq!(
        spec.containers[0]
            .security_context
            .as_ref()
            .unwrap()
            .allow_privilege_escalation,
        Some(false)
    );
    Ok(())
}

#[tokio::test]
async fn retirement_recovers_a_lost_create_reply_and_uses_a_uid_precondition() -> Result<()> {
    use axum::{Json, Router, routing::get};
    let deleted = Arc::new(std::sync::Mutex::new(None));
    let recorded = deleted.clone();
    let pod = json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":"reserved-pod","namespace":"sandboxes","uid":"created-uid","labels":{"app.kubernetes.io/managed-by":"terse"}}});
    let routes = Router::new().route(
        "/api/v1/namespaces/sandboxes/pods/reserved-pod",
        get(move || async move { Json(pod) }).delete(
            move |Json(body): Json<serde_json::Value>| async move {
                *recorded.lock().unwrap() = Some(body);
                Json(json!({"apiVersion":"v1","kind":"Status","status":"Success"}))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let cluster = Kubernetes::new(
        Client::try_from(config)?,
        GkeConfig {
            namespace: "sandboxes".into(),
            zones: BTreeMap::new(),
            public_origin: "https://actors.example.com".into(),
            artifact_bucket: "code".into(),
        },
    );
    let result = cluster
        .retire_spare(&SpareHandle {
            name: "reserved-pod".into(),
            resource_id: String::new(),
            route: String::new(),
            canonical_region: String::new(),
            control_route: String::new(),
            control_token: String::new(),
        })
        .await;
    server.abort();
    result?;
    assert_eq!(
        deleted.lock().unwrap().as_ref().unwrap()["preconditions"]["uid"],
        "created-uid"
    );
    Ok(())
}

#[tokio::test]
async fn stopped_spares_identifies_missing_and_completed_pod_identities() -> Result<()> {
    use axum::{Json, Router, routing::get};
    let items: Vec<_> = [("live", "Running"), ("pending", "Pending"), ("unknown", "Unknown"), ("done", "Succeeded"), ("crashed", "Failed"), ("replaced", "Running")]
        .into_iter().map(|(name, phase)| json!({"apiVersion":"v1","kind":"Pod","metadata":{"name":name,"namespace":"sandboxes","uid":format!("{name}-uid")},"status":{"phase":phase}})).collect();
    let fail = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let fail_request = fail.clone();
    let routes = Router::new().route("/api/v1/namespaces/sandboxes/pods", get(move || {
        let fail = fail_request.clone();
        let items = items.clone();
        async move {
            if fail.load(std::sync::atomic::Ordering::SeqCst) {
                Err(axum::http::StatusCode::SERVICE_UNAVAILABLE)
            } else {
                Ok(Json(json!({"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"10"},"items":items})))
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = kube::Config::new(format!("http://{}", listener.local_addr()?).parse()?);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let cluster = Kubernetes::new(
        Client::try_from(config)?,
        GkeConfig {
            namespace: "sandboxes".into(),
            zones: BTreeMap::new(),
            public_origin: "https://actors.example.com".into(),
            artifact_bucket: "code".into(),
        },
    );
    let spares: Vec<_> = [
        "live", "pending", "unknown", "done", "crashed", "missing", "replaced",
    ]
    .into_iter()
    .map(|name| SpareHandle {
        name: name.into(),
        resource_id: format!(
            "sandboxes/{name}/{}-uid",
            if name == "replaced" { "old" } else { name }
        ),
        route: String::new(),
        canonical_region: String::new(),
        control_route: String::new(),
        control_token: String::new(),
    })
    .collect();
    let stopped = cluster.stopped_spares(&spares).await?;
    assert_eq!(
        stopped,
        spares[3..]
            .iter()
            .map(|s| s.resource_id.clone())
            .collect::<Vec<_>>()
    );
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(cluster.stopped_spares(&spares).await.is_err());
    server.abort();
    Ok(())
}
