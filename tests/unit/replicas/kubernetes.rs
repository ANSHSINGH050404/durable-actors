use super::*;

#[test]
fn replicas_on_unready_nodes_are_unavailable_even_if_the_pod_still_says_running() -> Result<()> {
    let pod: Pod = serde_json::from_value(json!({
        "metadata":{"name":"replica"},
        "spec":{"containers":[{"name":"replica","image":"runtime"}]},
        "status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}
    }))?;
    let mut node: Node = serde_json::from_value(json!({
        "metadata":{"name":"node"},
        "status":{"conditions":[{"type":"Ready","status":"Unknown"}]}
    }))?;
    assert!(!pod_health(&pod, Some(&node), "runtime").live);
    node.status.as_mut().unwrap().conditions.as_mut().unwrap()[0].status = "True".into();
    assert!(pod_health(&pod, Some(&node), "runtime").live);
    assert!(!pod_health(&pod, None, "runtime").live);
    Ok(())
}
use axum::{Json, Router, extract::State, routing::get};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test]
async fn reconciliation_reads_fresh_inventory_each_pass() -> Result<()> {
    let revision = Arc::new(AtomicUsize::new(1));
    let app = Router::new()
        .route("/api/v1/namespaces/test/pods", get(|State(revision): State<Arc<AtomicUsize>>| async move {
            let pods: Vec<_> = (0..revision.load(Ordering::SeqCst)).map(|n| json!({"metadata":{"name":format!("replica-{n}"),"uid":format!("uid-{n}")}})).collect();
            Json(json!({"apiVersion":"v1","kind":"PodList","metadata":{},"items":pods}))
        }))
        .route("/api/v1/nodes", get(|| async { Json(json!({"apiVersion":"v1","kind":"NodeList","metadata":{},"items":[]})) }))
        .with_state(revision.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let uri = format!("http://{}", listener.local_addr()?);
    let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        axum::serve(listener, app).await
    }));
    let replicas = KubernetesReplicas::new(
        Client::try_from(kube::Config::new(uri.parse()?))?,
        ReplicaPodConfig {
            namespace: "test".into(),
            image: format!("example/image@sha256:{}", "a".repeat(64)),
            archive_bucket: "archive".into(),
            credentials_secret: "secret".into(),
            resources: json!({}),
        },
        "secret".into(),
    )?;
    assert_eq!(replicas.observed().await?.len(), 1);
    revision.store(2, Ordering::SeqCst);
    assert_eq!(replicas.observed().await?.len(), 2);
    drop(server);
    Ok(())
}

struct RegistrationFixture {
    replicas: KubernetesReplicas,
    requests: Arc<AtomicUsize>,
    _server: tokio_util::task::AbortOnDropHandle<std::io::Result<()>>,
}
impl RegistrationFixture {
    async fn new() -> Result<Self> {
        let requests = Arc::new(AtomicUsize::new(0));
        let count = requests.clone();
        let app = Router::new().fallback(get(move || {
            count.fetch_add(1, Ordering::SeqCst);
            async { Json("disk") }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("http://{}", listener.local_addr()?);
        let server = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            axum::serve(listener, app).await
        }));
        let mut replicas = KubernetesReplicas::new(
            Client::try_from(kube::Config::new(address.parse()?))?,
            ReplicaPodConfig {
                namespace: "test".into(),
                image: format!("example/image@sha256:{}", "a".repeat(64)),
                archive_bucket: "archive".into(),
                credentials_secret: "secret".into(),
                resources: json!({}),
            },
            "secret".into(),
        )?;
        replicas.http = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all(address)?)
            .build()?;
        Ok(Self {
            replicas,
            requests,
            _server: server,
        })
    }
    fn pod() -> Pod {
        serde_json::from_value(json!({
            "metadata":{"name":"replica","uid":"uid"},
            "spec":{"containers":[],"nodeName":"node"},
            "status":{"podIP":"192.0.2.1","conditions":[{"type":"Ready","status":"True"}]}
        }))
        .unwrap()
    }
    fn record() -> PodRecord {
        PodRecord {
            name: "replica".into(),
            zone: "zone".into(),
            uid: Some("uid".into()),
            node: Some("node".into()),
            placement: Some(crate::bucket::ReplicaPlacement {
                id: "disk".into(),
                address: "http://192.0.2.1:7200".into(),
                zone: "zone".into(),
            }),
        }
    }
}

#[tokio::test]
async fn verified_placement_needs_no_identity_request_but_new_disks_do() -> Result<()> {
    let fixture = RegistrationFixture::new().await?;
    let expected = RegistrationFixture::record();
    assert_eq!(
        fixture
            .replicas
            .register(&expected, RegistrationFixture::pod())
            .await?,
        expected
    );
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 0);
    let pending = PodRecord {
        uid: None,
        node: None,
        placement: None,
        ..expected.clone()
    };
    assert_eq!(
        fixture
            .replicas
            .register(&pending, RegistrationFixture::pod())
            .await?,
        expected
    );
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn cached_placement_rejects_replacement_readdressing_and_node_changes() -> Result<()> {
    let fixture = RegistrationFixture::new().await?;
    for kind in ["uid", "address", "node"] {
        let mut pod = RegistrationFixture::pod();
        match kind {
            "uid" => pod.metadata.uid = Some("replacement".into()),
            "address" => pod.status.as_mut().unwrap().pod_ip = Some("192.0.2.2".into()),
            _ => pod.spec.as_mut().unwrap().node_name = Some("other-node".into()),
        }
        assert!(
            fixture
                .replicas
                .register(&RegistrationFixture::record(), pod)
                .await
                .is_err(),
            "{kind}"
        );
    }
    assert_eq!(fixture.requests.load(Ordering::SeqCst), 0);
    Ok(())
}
