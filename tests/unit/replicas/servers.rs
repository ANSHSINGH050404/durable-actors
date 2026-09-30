use super::*;
use crate::bucket::{Bucket, ReplicaPlacement};

pub(crate) const SECRET: &str = "0123456789abcdef0123456789abcdef";

pub(crate) struct TestReplica {
    pub placement: ReplicaPlacement,
    task: tokio::task::JoinHandle<()>,
    _directory: tempfile::TempDir,
}

impl TestReplica {
    pub async fn start(zone: &str, archive: Arc<dyn Bucket>) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let disk = ReplicaDisk::open(directory.path().join("replica.sqlite")).await?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let placement = ReplicaPlacement {
            id: disk.identity().await?,
            address: format!("http://{}", listener.local_addr()?),
            zone: zone.into(),
        };
        let server = Arc::new(ReplicaServer {
            id: placement.id.clone(),
            disk,
            archive: Archive(archive),
            access: Access::new(SECRET.into())?,
            assignment: tokio::sync::OnceCell::new(),
            clock: Arc::new(SystemClock),
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, routes(server)).await.unwrap();
        });
        Ok(Self {
            placement,
            task,
            _directory: directory,
        })
    }
}

impl Drop for TestReplica {
    fn drop(&mut self) {
        self.task.abort();
    }
}
