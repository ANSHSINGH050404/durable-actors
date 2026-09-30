use super::*;
use axum::{Router, routing::post};
use google_cloud_auth::credentials::anonymous;
use google_cloud_storage::client::{Storage, StorageControl};
use std::{future::Future, pin::Pin};

#[tokio::test]
async fn rapid_reads_stream_content_and_distinguish_absence_from_denial() -> Result<()> {
    let routes = Router::new().route("/google.storage.v2.Storage/BidiReadObject", post(read));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let clients = GcsClients {
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
    };
    let store = GcsSnapshots::new("rapid-test", clients, StorageClass::Rapid)?;
    let prefix = "durable-actors/v3/snapshots/aa/cHJvamVjdA/Q291bnRlcg/b25l/epoch/";
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        assert_eq!(
            store.get(&format!("{prefix}1.json")).await?,
            Some(Bytes::from_static(b"state"))
        );
        assert_eq!(store.get(&format!("{prefix}2.json")).await?, None);
        assert!(store.get(&format!("{prefix}3.json")).await.is_err());
        Ok::<_, anyhow::Error>(())
    })
    .await;
    server.abort();
    result??;
    Ok(())
}

async fn read(request: axum::extract::Request) -> axum::response::Response {
    tonic::server::Grpc::new(tonic_prost::ProstCodec::<ReadResponse, ReadRequest>::default())
        .streaming(ReadService, request)
        .await
        .map(axum::body::Body::new)
}

struct ReadService;
impl tonic::server::StreamingService<ReadRequest> for ReadService {
    type Response = ReadResponse;
    type ResponseStream =
        futures_util::stream::Iter<std::vec::IntoIter<Result<ReadResponse, tonic::Status>>>;
    type Future = Pin<
        Box<
            dyn Future<Output = Result<tonic::Response<Self::ResponseStream>, tonic::Status>>
                + Send,
        >,
    >;
    fn call(&mut self, request: tonic::Request<tonic::Streaming<ReadRequest>>) -> Self::Future {
        Box::pin(async move {
            let request = request.into_inner().message().await?.unwrap();
            let spec = request.spec.unwrap();
            assert_eq!(spec.bucket, "projects/_/buckets/rapid-test");
            assert!(!spec.object.contains('/'));
            if spec.object.ends_with("2.json") {
                return Err(tonic::Status::not_found("missing"));
            }
            if spec.object.ends_with("3.json") {
                return Err(tonic::Status::permission_denied("denied"));
            }
            let range = request.ranges.into_iter().next().unwrap();
            let response = ReadResponse {
                metadata: Some(Metadata {
                    generation: 1,
                    size: 5,
                }),
                ranges: vec![RangeData {
                    data: Some(Data {
                        content: b"state".to_vec(),
                    }),
                    range: Some(range),
                    end: true,
                }],
            };
            Ok(tonic::Response::new(futures_util::stream::iter(vec![Ok(
                response,
            )])))
        })
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct ReadRequest {
    #[prost(message, optional, tag = "1")]
    spec: Option<Spec>,
    #[prost(message, repeated, tag = "8")]
    ranges: Vec<Range>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct Spec {
    #[prost(string, tag = "1")]
    bucket: String,
    #[prost(string, tag = "2")]
    object: String,
}
#[derive(Clone, PartialEq, prost::Message)]
struct Range {
    #[prost(int64, tag = "1")]
    offset: i64,
    #[prost(int64, tag = "2")]
    length: i64,
    #[prost(int64, tag = "3")]
    id: i64,
}
#[derive(Clone, PartialEq, prost::Message)]
struct ReadResponse {
    #[prost(message, optional, tag = "4")]
    metadata: Option<Metadata>,
    #[prost(message, repeated, tag = "6")]
    ranges: Vec<RangeData>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct Metadata {
    #[prost(int64, tag = "3")]
    generation: i64,
    #[prost(int64, tag = "6")]
    size: i64,
}
#[derive(Clone, PartialEq, prost::Message)]
struct RangeData {
    #[prost(message, optional, tag = "1")]
    data: Option<Data>,
    #[prost(message, optional, tag = "2")]
    range: Option<Range>,
    #[prost(bool, tag = "3")]
    end: bool,
}
#[derive(Clone, PartialEq, prost::Message)]
struct Data {
    #[prost(bytes, tag = "1")]
    content: Vec<u8>,
}
