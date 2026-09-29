use super::{
    access::Access,
    archive::Archive,
    client::{Command, ReplicaClient, Reply, Request, position},
    store::ReplicaDisk,
};
use crate::{
    bucket::GcsBucket,
    clock::{Clock, SystemClock},
};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

const BATCH_BYTES: usize = 16 * 1024 * 1024;
const BATCH_AGE_MS: u64 = 10_000;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Assignment {
    pub prefix: String,
    pub replicas: Vec<crate::bucket::ReplicaPlacement>,
}

pub async fn serve_replica(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    let access = Access::new(std::env::var("DURABLE_ACTORS_REPLICA_SECRET")?)?;
    let bucket = GcsBucket::new(&std::env::var("DURABLE_ACTORS_ARCHIVE_BUCKET")?).await?;
    bucket.require_standard().await?;
    let disk = ReplicaDisk::open(PathBuf::from("/tmp/replica/replica.sqlite")).await?;
    let id = disk.identity().await?;
    let assignment = disk.assignment().await?;
    let server = Arc::new(ReplicaServer {
        id,
        disk,
        archive: Archive(Arc::new(bucket)),
        access,
        assignment: tokio::sync::OnceCell::new_with(assignment),
        clock: Arc::new(SystemClock),
    });
    let listener = tokio::net::TcpListener::bind("0.0.0.0:7200").await?;
    let stop = CancellationToken::new();
    let _guard = stop.clone().drop_guard();
    let uploader = server.clone();
    let upload_stop = stop.clone();
    let task = tokio::spawn(async move { uploader.upload_loop(upload_stop).await });
    tracing::info!(replica = %server.id, "dedicated replica spare ready");
    axum::serve(listener, routes(server))
        .with_graceful_shutdown(shutdown)
        .await?;
    stop.cancel();
    task.await?;
    Ok(())
}

struct ReplicaServer {
    id: String,
    disk: ReplicaDisk,
    archive: Archive,
    access: Access,
    assignment: tokio::sync::OnceCell<Assignment>,
    clock: Arc<dyn Clock>,
}

async fn assign(
    State(server): State<Arc<ReplicaServer>>,
    headers: HeaderMap,
    Json(assignment): Json<Assignment>,
) -> Result<StatusCode, StatusCode> {
    server
        .access
        .authorize(bearer(&headers)?, None)
        .map_err(|_| StatusCode::FORBIDDEN)?;
    server
        .assign(assignment)
        .await
        .map_err(|_| StatusCode::CONFLICT)?;
    Ok(StatusCode::NO_CONTENT)
}

fn bearer(headers: &HeaderMap) -> Result<&str, StatusCode> {
    headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)
}

fn routes(server: Arc<ReplicaServer>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/identity", get(identity))
        .route("/assign", post(assign))
        .route("/storage", post(storage))
        .layer(DefaultBodyLimit::disable())
        .with_state(server)
}
async fn identity(
    State(server): State<Arc<ReplicaServer>>,
    headers: HeaderMap,
) -> Result<Json<String>, StatusCode> {
    server
        .access
        .authorize(bearer(&headers)?, None)
        .map_err(|_| StatusCode::FORBIDDEN)?;
    Ok(Json(server.id.clone()))
}
async fn health(State(server): State<Arc<ReplicaServer>>) -> StatusCode {
    match server.disk.identity().await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}
async fn storage(
    State(server): State<Arc<ReplicaServer>>,
    headers: HeaderMap,
    Json(request): Json<Request>,
) -> Result<Json<std::result::Result<Reply, String>>, StatusCode> {
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .ok_or(StatusCode::UNAUTHORIZED)?;
    server
        .access
        .authorize(token, request.command.scope())
        .map_err(|_| StatusCode::FORBIDDEN)?;
    if request.replica != server.id {
        return Err(StatusCode::CONFLICT);
    }
    Ok(Json(
        server
            .execute(request.command)
            .await
            .map_err(|e| format!("{e:#}")),
    ))
}

impl ReplicaServer {
    async fn assign(&self, assignment: Assignment) -> Result<()> {
        crate::storage_paths::actor_from_snapshot(&format!("{}1.json", assignment.prefix))?;
        ensure!(
            assignment.replicas.iter().any(|r| r.id == self.id),
            "replica is not a group member"
        );
        let bound = self
            .assignment
            .get_or_try_init(|| async {
                self.disk.bind_assignment(&assignment).await?;
                anyhow::Ok(assignment.clone())
            })
            .await?;
        ensure!(*bound == assignment, "replica already assigned");
        Ok(())
    }

    async fn execute(&self, command: Command) -> Result<Reply> {
        let assignment = self.assignment.get().context("replica is unassigned")?;
        if let Some(scope) = command.scope() {
            let expected = match &command {
                Command::Get { object } => position(object)?.0,
                _ => scope.to_owned(),
            };
            ensure!(
                expected == assignment.prefix,
                "replica belongs to another activation"
            );
        }

        let mut reply = Reply::default();
        match command {
            Command::Prepare { prefix } => self.disk.prepare(&prefix).await?,
            Command::Seal { prefix } => self.disk.seal(&prefix).await?,
            Command::Append { prefix, record } => {
                self.disk
                    .append(&prefix, record, self.clock.now_ms()?)
                    .await?
            }
            Command::Latest { prefix } => {
                if let Some((object, bytes)) = self.disk.latest(&prefix).await? {
                    reply.object = Some(object);
                    reply.data = Some(STANDARD.encode(bytes));
                } else if let Some(object) = self.disk.latest_object(&prefix).await? {
                    let (_, version) = position(&object)?;
                    let bytes = self
                        .archive
                        .get(&prefix, version)
                        .await?
                        .context("archived replica head missing")?;
                    reply.object = Some(object);
                    reply.data = Some(STANDARD.encode(bytes));
                }
            }
            Command::Get { object } => {
                let (prefix, version) = position(&object)?;
                let bytes = match self.disk.get(&prefix, version).await? {
                    Some(bytes) => Some(bytes),
                    None => self.archive.get(&prefix, version).await?,
                };
                reply.data = bytes.map(|bytes| STANDARD.encode(bytes));
            }
            Command::List { prefix } => {
                let (local, archived) =
                    tokio::try_join!(self.disk.list(&prefix), self.archive.list(&prefix))?;
                reply.keys = local
                    .into_iter()
                    .chain(archived)
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect();
            }
            Command::Flush { prefix } => {
                self.disk.require_sealed(&prefix).await?;
                while self.disk.batch(&prefix, BATCH_BYTES).await?.is_some() {
                    self.upload_prefix(&prefix).await?;
                }
                reply.object = self.disk.latest_object(&prefix).await?;
            }
            Command::Archived { key } => {
                let batch = self.archive.read(&key).await?;
                ensure!(
                    batch.prefix == assignment.prefix,
                    "archive belongs to another activation"
                );
                self.disk.archived(&key, &batch).await?;
            }
        }
        Ok(reply)
    }

    async fn upload_loop(&self, stop: CancellationToken) {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! { _ = stop.cancelled() => return, _ = interval.tick() => {} }
            tokio::select! {
                _ = stop.cancelled() => return,
                result = self.upload_due() => if let Err(error) = result { tracing::warn!(%error, "archive upload deferred; replica log retained"); }
            }
        }
    }

    async fn upload_due(&self) -> Result<()> {
        let Some(assignment) = self.assignment.get() else {
            return Ok(());
        };
        let primary = assignment
            .replicas
            .first()
            .is_some_and(|replica| replica.id == self.id);
        let age = if primary {
            BATCH_AGE_MS
        } else {
            BATCH_AGE_MS * 3
        };
        let size = if primary { BATCH_BYTES } else { usize::MAX / 2 };
        let prefixes = self.disk.due(self.clock.now_ms()?, age, size).await?;
        let results = futures_util::future::join_all(
            prefixes.iter().map(|prefix| self.upload_prefix(prefix)),
        )
        .await;
        for result in results {
            result?;
        }
        Ok(())
    }

    async fn upload_prefix(&self, prefix: &str) -> Result<()> {
        let Some(batch) = self.disk.batch(prefix, BATCH_BYTES).await? else {
            return Ok(());
        };
        let key = self.archive.write(&batch).await?;
        self.disk.archived(&key, &batch).await?;
        for result in futures_util::future::join_all(
            self.assignment
                .get()
                .context("replica is unassigned")?
                .replicas
                .iter()
                .filter(|peer| peer.id != self.id)
                .map(|peer| async {
                    ReplicaClient::new(peer.clone(), self.access.admin().into())?
                        .archived(&key)
                        .await
                }),
        )
        .await
        {
            if let Err(error) = result {
                tracing::warn!(%error, "replica archive notification deferred");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/replicas/server.rs"]
mod tests;
