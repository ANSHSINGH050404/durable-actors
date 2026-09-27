use super::*;
use crate::grpc::proto::{
    ControlPlaneReply, ControlPlaneRequest,
    actor_control_plane_service_server::{
        ActorControlPlaneService, ActorControlPlaneServiceServer,
    },
};

struct LocalCredentials;
#[tonic::async_trait]
impl ActorControlPlaneService for LocalCredentials {
    async fn execute(
        &self,
        _: Request<ControlPlaneRequest>,
    ) -> Result<tonic::Response<ControlPlaneReply>, tonic::Status> {
        Ok(tonic::Response::new(
            super::super::protocol::encode_reply(ControlPlaneCommandReply::StorageAccess {
                token: None,
                replacement_token: "renewed-local-token".into(),
            })
            .unwrap(),
        ))
    }
}

#[tokio::test]
async fn local_hosts_refresh_socket_credentials_without_a_cloud_token() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let client = ControlPlaneClient::connect(
        format!("http://{}", listener.local_addr()?),
        "initial-token",
    )
    .await?;
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ActorControlPlaneServiceServer::new(LocalCredentials))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let result = client.refresh_storage_access().await;
    server.abort();
    result?;
    assert_eq!(
        client.authorization.read().unwrap().to_str()?,
        "Bearer renewed-local-token"
    );
    Ok(())
}

#[tokio::test]
async fn idle_spare_preconnects_and_reuses_the_connection_with_its_assigned_token() -> Result<()> {
    use futures_util::StreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let connections = Arc::new(AtomicUsize::new(0));
    let accepted = connections.clone();
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).inspect(move |_| {
        accepted.fetch_add(1, Ordering::SeqCst);
    });
    let (send, mut received) = tokio::sync::mpsc::unbounded_channel();
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(ActorControlPlaneServiceServer::new(AssignedCredentials(
                send,
            )))
            .serve_with_incoming(incoming),
    );
    let client = ControlPlaneClient::prewarm(&url).await?;
    tokio::time::timeout(Duration::from_secs(1), async {
        while connections.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    assert!(
        received.try_recv().is_err(),
        "idle spares must not execute host commands"
    );
    client
        .with_host_token("assigned-token")?
        .refresh_storage_access()
        .await?;
    assert_eq!(
        received.recv().await.as_deref(),
        Some("Bearer assigned-token")
    );
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    server.abort();
    Ok(())
}

struct AssignedCredentials(tokio::sync::mpsc::UnboundedSender<String>);

#[tonic::async_trait]
impl ActorControlPlaneService for AssignedCredentials {
    async fn execute(
        &self,
        request: Request<ControlPlaneRequest>,
    ) -> Result<tonic::Response<ControlPlaneReply>, tonic::Status> {
        self.0
            .send(
                request
                    .metadata()
                    .get("authorization")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned(),
            )
            .unwrap();
        LocalCredentials.execute(request).await
    }
}
