use super::*;
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
