use std::{future::Future, path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use tokio::net::TcpListener;

use super::process::{ActorHostConfig, serve_assigned_host, spawn_executor_process};
use crate::actor::{ActorExecutorListener, WarmExecutor};
use crate::bucket::WarmGcs;

pub(super) struct WarmHost {
    pub readiness: Option<tokio::sync::oneshot::Sender<super::process::HostReadiness>>,
    pub listener: TcpListener,
    pub executor: WarmExecutor,
    pub javascript: tokio::process::Child,
    pub entrypoint: String,
    pub storage: WarmGcs,
    pub control_plane: Option<WarmControlPlane>,
}

pub(super) struct WarmControlPlane {
    pub url: String,
    pub client: crate::control_plane::ControlPlaneClient,
}

pub async fn serve_spare(shutdown: impl Future<Output = ()> + Send + 'static) -> Result<()> {
    super::protect_runtime_credentials()?;
    let socket = std::env::var("DURABLE_ACTORS_EXECUTOR_SOCKET")
        .unwrap_or_else(|_| "/tmp/durable-actors-executor.sock".into());
    let token = std::env::var("DURABLE_ACTORS_SPARE_TOKEN").context("spare token missing")?;
    ensure!(token.len() >= 32, "spare token is too short");
    let control_bind =
        std::env::var("DURABLE_ACTORS_SPARE_BIND").unwrap_or_else(|_| "0.0.0.0:7102".into());
    let control = TcpListener::bind(control_bind).await?;
    let (send, receive) = tokio::sync::oneshot::channel();
    let stop = tokio_util::sync::CancellationToken::new();
    let _stop_guard = stop.clone().drop_guard();
    let mut server = tokio::spawn(async move {
        axum::serve(control, super::assignment::router(token, send))
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
    });
    let ready = std::env::var("DURABLE_ACTORS_SPARE_READY_FILE")
        .unwrap_or_else(|_| "/tmp/durable-actors-spare-ready".into());
    let bind = std::env::var("DURABLE_ACTORS_HOST_BIND").unwrap_or_else(|_| "0.0.0.0:7101".into());
    let listener = TcpListener::bind(bind).await?;
    let ipc = ActorExecutorListener::bind(&socket).await?;
    let mut javascript = spawn_executor_process(true, &socket, None)?;
    let (warmed, control_plane) = tokio::join!(
        async {
            tokio::try_join!(
                async { tokio::time::timeout(Duration::from_secs(30), ipc.accept_warm()).await? },
                async {
                    let storage = WarmGcs::new().await?;
                    storage.preconnect().await;
                    anyhow::Ok(storage)
                },
            )
        },
        prewarm_control_plane(std::env::var("DURABLE_ACTORS_CONTROL_PLANE_URL").ok()),
    );
    let (mut executor, storage) = warmed?;
    let control_plane = Some(control_plane?);
    tokio::fs::write(&ready, b"ready\n").await?;
    tokio::pin!(shutdown);
    let assigned = tokio::select! {
        value = receive => value?,
        result = &mut server => anyhow::bail!("spare assignment server stopped: {result:?}"),
        status = javascript.wait() => anyhow::bail!("generic Bun executor exited: {}", status?),
        () = &mut shutdown => return Ok(()),
        () = storage.keep_warm() => unreachable!("storage warmup stopped"),
    };
    tokio::fs::remove_file(&ready).await?;
    let environment = assigned.environment;
    let config = ActorHostConfig::from_lookup(|name| environment.get(name).cloned())?;
    ensure!(
        config.actor.is_some(),
        "spare assignment must name exactly one actor"
    );
    let entrypoint = environment
        .get("DURABLE_ACTORS_ENTRYPOINT")
        .context("missing customer entrypoint")?
        .clone();
    ensure!(
        entrypoint.starts_with("/customer/")
            && (entrypoint.ends_with(".mjs") || entrypoint.ends_with(".pyz"))
            && !Path::new(&entrypoint)
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir)),
        "customer entrypoint must be a compiled module under /customer"
    );
    if entrypoint.ends_with(".pyz") {
        javascript.kill().await?;
        drop(executor);
        let ipc = ActorExecutorListener::bind(&socket).await?;
        javascript = spawn_executor_process(true, &socket, Some(&entrypoint))?;
        executor = tokio::time::timeout(Duration::from_secs(30), ipc.accept_warm()).await??;
    }
    let warm = WarmHost {
        readiness: Some(assigned.ready),
        listener,
        executor,
        javascript,
        entrypoint,
        storage,
        control_plane,
    };
    serve_assigned_host(config, Some(warm), shutdown).await
}

async fn prewarm_control_plane(url: Option<String>) -> Result<WarmControlPlane> {
    let url = url.context("spare control-plane URL required")?;
    let client = crate::control_plane::ControlPlaneClient::prewarm(&url).await?;
    Ok(WarmControlPlane { url, client })
}

#[cfg(test)]
#[path = "../../tests/unit/host/spare.rs"]
mod tests;
