use std::collections::HashMap;

use anyhow::Context;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, Query},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use google_cloud_auth::credentials::anonymous;
use serde_json::json;

use super::*;

#[tokio::test]
async fn bulk_transfers_do_not_block_ownership_reads_and_updates() -> Result<()> {
    let server = WarmServer::start(false).await?;
    let bucket = server
        .client()
        .await?
        .bind("test-bucket", TestCredentials::new("owner").into())?;
    let _bulk = bucket.clients.transfers.acquire_many(2).await?;
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        assert_eq!(bucket.get("owner").await?.unwrap().bytes.as_ref(), b"lease");
        assert!(
            bucket
                .compare_and_swap("owner", None, bytes::Bytes::from_static(b"lease"))
                .await?
        );
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn warmed_connections_keep_credentials_isolated_and_refreshable() -> Result<()> {
    let server = WarmServer::start(false).await?;
    let warm = server.client().await?;
    warm.preconnect().await;
    let token = TestCredentials::new("first");
    let first = warm.bind("test-bucket", token.clone().into())?;
    assert_eq!(first.get("owner").await?.unwrap().bytes.as_ref(), b"lease");

    let warm = server.client().await?;
    warm.preconnect().await;
    let second = warm.bind("test-bucket", TestCredentials::new("second").into())?;
    second.get("owner").await?;
    *token.0.lock().unwrap() = "refreshed".into();
    assert!(
        first
            .compare_and_swap("owner", None, bytes::Bytes::from_static(b"lease"))
            .await?
    );

    let requests = server.requests.lock().unwrap();
    let authenticated: Vec<_> = requests
        .iter()
        .filter(|request| request.1.is_some())
        .collect();
    assert_eq!(
        authenticated
            .iter()
            .map(|request| request.1.as_deref())
            .collect::<Vec<_>>(),
        [
            Some("Bearer first"),
            Some("Bearer second"),
            Some("Bearer refreshed")
        ]
    );
    assert_eq!(authenticated[0].0, authenticated[2].0);
    assert_ne!(authenticated[0].0, authenticated[1].0);
    assert!(
        requests
            .iter()
            .filter(|request| request.1.is_none())
            .count()
            >= 2
    );
    Ok(())
}

#[tokio::test]
async fn idle_spares_refresh_their_connection_without_credentials() -> Result<()> {
    let server = WarmServer::start(false).await?;
    let warm = server.client().await?;
    warm.preconnect().await;
    let warming = warm.keep_warm();
    tokio::pin!(warming);
    tokio::select! {
        biased;
        () = &mut warming => anyhow::bail!("idle warmup stopped"),
        () = std::future::ready(()) => {}
    }
    for expected in 2..=3 {
        tokio::time::pause();
        tokio::time::advance(std::time::Duration::from_secs(20)).await;
        tokio::time::resume();
        tokio::select! {
            () = &mut warming => anyhow::bail!("idle warmup stopped"),
            result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while server.requests.lock().unwrap().len() < expected {
                    tokio::task::yield_now().await;
                }
            }) => { result?; }
        }
    }
    assert!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.1.is_none())
    );
    Ok(())
}

#[tokio::test]
async fn stalled_warmup_is_bounded_and_can_be_cancelled_for_assignment() -> Result<()> {
    let server = WarmServer::start(true).await?;
    let warm = server.client().await?;
    tokio::time::timeout(std::time::Duration::from_secs(2), warm.preconnect()).await?;
    let initial = server.requests.lock().unwrap().len();
    assert!(initial >= 1);
    {
        let warming = warm.preconnect();
        tokio::pin!(warming);
        tokio::select! {
            () = &mut warming => anyhow::bail!("probe completed before cancellation"),
            () = async {
                while server.requests.lock().unwrap().len() <= initial {
                    tokio::task::yield_now().await;
                }
            } => {}
        }
    }
    let bucket = warm.bind("test-bucket", TestCredentials::new("assigned").into())?;
    let result =
        tokio::time::timeout(std::time::Duration::from_secs(2), bucket.get("owner")).await??;
    assert_eq!(result.unwrap().bytes.as_ref(), b"lease");
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests[0].1, None);
    assert_eq!(requests[1].1, None);
    assert_eq!(
        requests
            .iter()
            .filter_map(|request| request.1.as_deref())
            .collect::<Vec<_>>(),
        ["Bearer assigned"]
    );
    Ok(())
}

#[derive(Clone)]
struct TestCredentials(std::sync::Arc<std::sync::Mutex<String>>);

impl TestCredentials {
    fn new(token: &str) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(token.into())))
    }
}

impl std::fmt::Debug for TestCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TestCredentials")
    }
}

impl google_cloud_auth::credentials::CredentialsProvider for TestCredentials {
    async fn headers(
        &self,
        _: axum::http::Extensions,
    ) -> std::result::Result<
        google_cloud_auth::credentials::CacheableResource<axum::http::HeaderMap>,
        google_cloud_auth::errors::CredentialsError,
    > {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {}", self.0.lock().unwrap())
                .parse()
                .unwrap(),
        );
        Ok(google_cloud_auth::credentials::CacheableResource::New {
            entity_tag: google_cloud_auth::credentials::EntityTag::new(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        Some("googleapis.com".into())
    }
}

type RecordedRequests =
    std::sync::Arc<std::sync::Mutex<Vec<(std::net::SocketAddr, Option<String>)>>>;

struct WarmServer {
    endpoint: String,
    requests: RecordedRequests,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl WarmServer {
    async fn start(stall: bool) -> Result<Self> {
        let requests = RecordedRequests::default();
        let record = requests.clone();
        let routes = Router::new().fallback(
            move |peer: axum::extract::ConnectInfo<std::net::SocketAddr>,
                  request: axum::extract::Request| {
                let record = record.clone();
                async move {
                    let auth = request
                        .headers()
                        .get("authorization")
                        .map(|h| h.to_str().unwrap().to_owned());
                    record.lock().unwrap().push((peer.0, auth.clone()));
                    if auth.is_none() {
                        if stall {
                            std::future::pending::<()>().await;
                        }
                        return error(StatusCode::UNAUTHORIZED);
                    }
                    if request.method() == axum::http::Method::POST {
                        return Json(json!({"generation":"43"})).into_response();
                    }
                    ([("x-goog-generation", "42")], "lease").into_response()
                }
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                routes.into_make_service_with_connect_info::<std::net::SocketAddr>(),
            )
            .await
        });
        Ok(Self {
            endpoint,
            requests,
            task,
        })
    }

    async fn client(&self) -> Result<WarmGcs> {
        let credentials = PendingCredentials::default();
        Ok(WarmGcs {
            clients: GcsClients {
                transfers: Arc::new(tokio::sync::Semaphore::new(2)),
                storage: Storage::builder()
                    .with_endpoint(&self.endpoint)
                    .with_credentials(credentials.clone())
                    .build()
                    .await?,
                control: StorageControl::builder()
                    .with_endpoint(&self.endpoint)
                    .with_credentials(credentials.clone())
                    .build()
                    .await?,
            },
            credentials,
        })
    }
}

impl Drop for WarmServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn gcs_adapter_preserves_generation_conditions_and_pagination() -> Result<()> {
    let routes = Router::new()
        .route("/storage/v1/b/test-bucket/o/{*key}", get(read))
        .route("/google.storage.v2.Storage/ListObjects", post(list))
        .route("/upload/storage/v1/b/test-bucket/o", post(write));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let bucket = GcsBucket {
        bucket: "projects/_/buckets/test-bucket".into(),
        clients: GcsClients {
            transfers: Arc::new(tokio::sync::Semaphore::new(2)),
            storage: Storage::builder()
                .with_endpoint(&endpoint)
                .with_credentials(anonymous::Builder::new().build())
                .build()
                .await?,
            control: StorageControl::builder()
                .with_endpoint(&endpoint)
                .with_credentials(anonymous::Builder::new().build())
                .build()
                .await?,
        },
    };
    let key = "runtime/lease.json";
    let object = bucket.get(key).await.context("read object")?.unwrap();
    assert_eq!(object.generation, 42);
    assert_eq!(object.bytes.as_ref(), b"lease");
    assert!(bucket.get("missing").await?.is_none());
    assert!(bucket.get("forbidden").await.is_err());
    assert!(
        bucket
            .compare_and_swap(key, None, bytes::Bytes::from_static(b"lease"))
            .await
            .context("create object")?
    );
    assert!(
        bucket
            .compare_and_swap(
                key,
                Some(object.generation),
                bytes::Bytes::from_static(b"lease")
            )
            .await
            .context("replace object")?
    );
    assert!(
        !bucket
            .compare_and_swap(key, Some(41), bytes::Bytes::from_static(b"lease"))
            .await?
    );
    assert!(
        bucket
            .compare_and_swap(key, Some(43), bytes::Bytes::from_static(b"lease"))
            .await
            .is_err()
    );
    assert_eq!(
        bucket.list("runtime/").await.context("list objects")?,
        ["runtime/one.json", "runtime/two.json"]
    );
    server.abort();
    Ok(())
}

async fn read(Path(key): Path<String>) -> Response {
    match key.as_str() {
        "runtime/lease.json" => ([("x-goog-generation", "42")], "lease").into_response(),
        "missing" => error(StatusCode::NOT_FOUND),
        _ => error(StatusCode::FORBIDDEN),
    }
}

async fn write(Query(query): Query<HashMap<String, String>>, body: Bytes) -> Response {
    let body = String::from_utf8_lossy(&body);
    if query.get("uploadType").map(String::as_str) != Some("multipart")
        || query.get("name").map(String::as_str) != Some("runtime/lease.json")
        || !body.contains("lease")
    {
        return error(StatusCode::BAD_REQUEST);
    }
    match query.get("ifGenerationMatch").map(String::as_str) {
        Some("0" | "42") => Json(json!({"generation":"43"})).into_response(),
        Some("41") => error(StatusCode::PRECONDITION_FAILED),
        Some("43") => error(StatusCode::FORBIDDEN),
        _ => error(StatusCode::BAD_REQUEST),
    }
}

async fn list(request: axum::extract::Request) -> Response {
    tonic::server::Grpc::new(tonic_prost::ProstCodec::<ListResponse, ListRequest>::default())
        .unary(ListService, request)
        .await
        .map(axum::body::Body::new)
}

fn error(status: StatusCode) -> Response {
    (
        status,
        Json(json!({"error":{"code":status.as_u16(),"message":status.as_str()}})),
    )
        .into_response()
}

struct ListService;

impl tonic::server::UnaryService<ListRequest> for ListService {
    type Response = ListResponse;
    type Future = std::future::Ready<Result<tonic::Response<ListResponse>, tonic::Status>>;

    fn call(&mut self, request: tonic::Request<ListRequest>) -> Self::Future {
        let request = request.into_inner();
        let result =
            if request.parent != "projects/_/buckets/test-bucket" || request.prefix != "runtime/" {
                Err(tonic::Status::invalid_argument(
                    "unexpected bucket or prefix",
                ))
            } else {
                match request.page_token.as_str() {
                    "" => Ok(("runtime/one.json", "next")),
                    "next" => Ok(("runtime/two.json", "")),
                    _ => Err(tonic::Status::invalid_argument("unexpected page token")),
                }
            };
        std::future::ready(result.map(|(name, token)| {
            tonic::Response::new(ListResponse {
                objects: vec![ListedObject { name: name.into() }],
                next_page_token: token.into(),
            })
        }))
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct ListRequest {
    #[prost(string, tag = "1")]
    parent: String,
    #[prost(string, tag = "3")]
    page_token: String,
    #[prost(string, tag = "6")]
    prefix: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ListResponse {
    #[prost(message, repeated, tag = "1")]
    objects: Vec<ListedObject>,
    #[prost(string, tag = "3")]
    next_page_token: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct ListedObject {
    #[prost(string, tag = "1")]
    name: String,
}
