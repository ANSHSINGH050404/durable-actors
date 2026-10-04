use std::{
    env,
    future::Future,
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use tokio::{net::TcpListener, process::Command};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{error, info};

use crate::{
    actor::{ActorExecutorConnection, ActorExecutorListener},
    clock::SystemClock,
    control_plane::{ActorJwtVerifier, ActorTokenPurpose, ControlPlaneClient},
    host::http::ActorHostHttpService,
    host_leases::MAX_HOST_LEASE_DURATION_MS,
};

use super::{
    ActorHost, HostEndpoint, HostLeaseMaintainer, LeaseRenewalTask, actor_host::ActorActivity,
};

const HOST_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const HOST_ACTOR_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_HOST_IDLE_TIMEOUT_MS: u64 = 10_000;
const MAX_IDLE_TIMEOUT_MS: u64 = 86_400_000;

pub struct ActorHostConfig {
    runtime_config: crate::bucket::access::HostStorageConfig,
    artifact: Option<crate::artifacts::ArtifactManifest>,
    customer_environment: std::collections::HashMap<String, String>,
    pub(super) actor: Option<crate::actor::ActorKey>,
    new_actor: bool,
    owner_hint: Option<crate::bucket::OwnershipHint>,
    ready_file: Option<PathBuf>,
    pub control_plane_url: String,
    pub host_token: String,
    pub jwt_public_keys: String,

    pub host_id: super::HostId,
    pub session_id: String,
    pub executor_socket: PathBuf,
    pub host_bind: SocketAddr,
    pub host_route: Option<String>,
    pub jwt_issuer: String,
    pub invocation_jwt_audience: String,
    pub jwt_max_lifetime: Duration,
    pub lease_duration: Duration,
    pub renew_every: Duration,
    pub host_idle_timeout: Duration,
    metadata: Option<HostMetadataFile>,
    startup_started_at: Instant,
    configuration_loaded_at_ms: f64,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct HostReadiness {
    pub host_id: super::HostId,
    pub session_id: String,
    pub route: String,
    pub canonical_region: String,
    pub owner_epoch: u64,
    pub lease: crate::host_leases::HostLease,
}

struct HostMetadataFile {
    path: PathBuf,
    canonical_region: String,
}

impl ActorHostConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }
}

pub async fn serve_actor_host<F>(config: ActorHostConfig, shutdown: F) -> Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    super::protect_runtime_credentials()?;
    serve_assigned_host(config, None, shutdown).await
}

pub(super) async fn serve_assigned_host(
    config: ActorHostConfig,
    mut warm: Option<super::spare::WarmHost>,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<()> {
    let readiness = warm.as_mut().and_then(|warm| warm.readiness.take());
    let mut timings = HostStartupTimings::new(&config);
    let prepared = match prepare_actor_host(&config, &mut timings, warm).await {
        Ok(prepared) => prepared,
        Err(error) => {
            log_startup(&config, &timings, "failed", Some(&error));
            return Err(error);
        }
    };
    let PreparedActorHost {
        invocation_auth,
        listener,
        route,
        executor_connection,
        mut javascript,
        host,
        lease,
        renewal,
        sockets,
        credentials: _credentials,
        storage,
        replication,
    } = prepared;
    let mut lease_lost = renewal.lease_lost();
    let mut activity = host.activity();
    let mut actor_stopped = host.stopped();

    let service = ActorHostHttpService::new(
        host.clone(),
        config.session_id.clone(),
        invocation_auth,
        sockets.clone(),
    )
    .router();
    let initialized = async {
        let owner_epoch =
            initialize_executor(&config, &executor_connection, &host, &sockets, replication)
                .await?;
        let ready = HostReadiness {
            host_id: config.host_id.clone(),
            session_id: config.session_id.clone(),
            route: route.clone(),
            canonical_region: config.runtime_config.region.clone(),
            owner_epoch,
            lease: storage.current_lease()?,
        };
        if let Some(path) = &config.ready_file {
            let temporary = path.with_extension("tmp");
            tokio::fs::write(&temporary, serde_json::to_vec(&ready)?).await?;
            tokio::fs::rename(temporary, path).await?;
        }
        anyhow::Ok(ready)
    }
    .await;
    let ready = match initialized {
        Ok(ready) => ready,
        Err(error) => {
            log_startup(&config, &timings, "failed", Some(&error));
            let _ = lease.unregister().await;
            let _ = renewal.shutdown().await;
            return Err(error);
        }
    };
    lease.observe(executor_connection.executor(), host.queues());
    if let Some(readiness) = readiness {
        let _ = readiness.send(ready);
    }
    timings.executor_notified_at_ms = Some(timings.elapsed_ms());
    log_startup(&config, &timings, "ready", None);
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let routes = service;
    let mut server = Box::pin(async move {
        axum::serve(listener, routes)
            .with_graceful_shutdown(async move { server_stop.cancelled().await })
            .await
            .context("serve actor host endpoints")
    });
    let mut executor_task = Box::pin(executor_connection.run(stop.clone()));
    let shutdown = async {
        tokio::select! { () = shutdown => {}, () = storage.stop.cancelled() => {} }
    };
    tokio::pin!(shutdown);

    info!(host_id = %config.host_id, route, "durable-actors host is ready");
    let stop_result = wait_for_host_stop(
        server.as_mut(),
        executor_task.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease_lost,
        (&mut activity, &mut actor_stopped),
        config.host_idle_timeout,
    )
    .await;
    let shutdown_started = Instant::now();
    let shutdown_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    stop_host_tasks(&host, &stop, server, executor_task).await;
    drop(javascript);
    let unregister_result = storage
        .finish(shutdown_deadline - Duration::from_secs(2))
        .await;
    let renewal_result = renewal.shutdown().await;
    info!(
        event = "host_shutdown",
        duration_ms = shutdown_started.elapsed().as_secs_f64() * 1000.0,
        storage_released = unregister_result.is_ok(),
        error = unregister_result
            .as_ref()
            .err()
            .map(|error| format!("{error:#}"))
    );
    info!(host_id = %config.host_id, "durable-actors host stopped");
    stop_result?;
    renewal_result?;
    unregister_result
}

async fn initialize_executor(
    config: &ActorHostConfig,
    connection: &ActorExecutorConnection,
    host: &ActorHost,
    sockets: &Arc<super::sockets::HostSockets>,
    replication: Arc<dyn crate::litestream::Replicator>,
) -> Result<u64> {
    connection
        .mark_ready(Some(sockets.clone()), Some(sockets.clone()), replication)
        .await?;
    let owner_epoch = match &config.actor {
        Some(actor) => host.activate_actor(actor.clone()).await?.owner_epoch,
        None => 0,
    };
    Ok(owner_epoch)
}

impl ActorHostConfig {
    pub(super) fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let startup_started_at = Instant::now();
        let control_plane_url = required(&mut get, "DURABLE_ACTORS_CONTROL_PLANE_URL")?;
        let host_token = required(&mut get, "DURABLE_ACTORS_HOST_TOKEN")?;
        let jwt_public_keys = required(&mut get, "DURABLE_ACTORS_JWT_PUBLIC_KEYS")?;
        let host_id = super::HostId::new(required(&mut get, "DURABLE_ACTORS_HOST_ID")?);
        ensure!(
            host_id.as_str().starts_with("host.v3."),
            "DURABLE_ACTORS_HOST_ID is invalid"
        );
        let session_id = required(&mut get, "DURABLE_ACTORS_SESSION_ID")?;
        uuid::Uuid::parse_str(&session_id).context("DURABLE_ACTORS_SESSION_ID must be a UUID")?;
        let executor_socket = get("DURABLE_ACTORS_EXECUTOR_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/durable-actors-executor.sock"));
        let host_route = get("DURABLE_ACTORS_HOST_ROUTE");
        if let Some(route) = &host_route {
            tonic::transport::Endpoint::from_shared(route.clone())
                .context("DURABLE_ACTORS_HOST_ROUTE must be a valid HTTP or HTTPS URI")?;
        }
        let metadata = HostMetadataFile::from_lookup(&mut get)?;
        let host_bind = get("DURABLE_ACTORS_HOST_BIND")
            .unwrap_or_else(|| {
                if host_route.is_some() {
                    "0.0.0.0:7101"
                } else {
                    "127.0.0.1:0"
                }
                .into()
            })
            .parse()
            .context("DURABLE_ACTORS_HOST_BIND must be a socket address")?;
        let jwt_issuer = get("DURABLE_ACTORS_JWT_ISSUER")
            .unwrap_or_else(|| "durable-actors-control-plane".into());
        let invocation_jwt_audience = get("DURABLE_ACTORS_INVOKE_JWT_AUDIENCE")
            .unwrap_or_else(|| "durable-actors-invoke".into());
        let jwt_max_lifetime =
            duration_seconds(&mut get, "DURABLE_ACTORS_JWT_MAX_TTL_SECONDS", 86_400)?;
        let lease_duration = duration_ms(&mut get, "DURABLE_ACTORS_LEASE_MS", 30_000)?;
        let renew_every = duration_ms(&mut get, "DURABLE_ACTORS_RENEW_MS", 10_000)?;
        let host_idle_timeout = Duration::from_millis(host_idle_timeout_ms(&mut get)?);
        ensure!(
            lease_duration.as_millis() <= u128::from(MAX_HOST_LEASE_DURATION_MS),
            "DURABLE_ACTORS_LEASE_MS is too large"
        );
        ensure!(
            renew_every < lease_duration,
            "DURABLE_ACTORS_RENEW_MS must be shorter than DURABLE_ACTORS_LEASE_MS"
        );
        let runtime_config = serde_json::from_str(
            &get("DURABLE_ACTORS_RUNTIME_CONFIG").context("host storage configuration missing")?,
        )
        .context("parse host runtime configuration")?;
        let actor: Option<crate::actor::ActorKey> = get("DURABLE_ACTORS_ACTOR")
            .map(|value| serde_json::from_str(&value))
            .transpose()?;
        if let Some(actor) = &actor {
            actor.validate()?;
        }
        Ok(Self {
            new_actor: get("DURABLE_ACTORS_ACTOR_IS_NEW")
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(false),
            actor,
            owner_hint: get("DURABLE_ACTORS_OWNER_HINT")
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
            ready_file: get("DURABLE_ACTORS_HOST_READY_FILE").map(PathBuf::from),
            customer_environment: get("DURABLE_ACTORS_CUSTOMER_ENV")
                .map(|value| serde_json::from_str(&value))
                .transpose()?
                .unwrap_or_default(),
            artifact: get("DURABLE_ACTORS_CODE_ARTIFACT")
                .map(|value| crate::artifacts::ArtifactManifest::decode(&value))
                .transpose()?,
            runtime_config,
            control_plane_url,
            host_token,
            jwt_public_keys,
            host_id,
            session_id,
            executor_socket,
            host_bind,
            host_route,
            jwt_issuer,
            invocation_jwt_audience,
            jwt_max_lifetime,
            lease_duration,
            renew_every,
            host_idle_timeout,
            metadata,
            configuration_loaded_at_ms: startup_started_at.elapsed().as_secs_f64() * 1_000.0,
            startup_started_at,
        })
    }
}

impl HostMetadataFile {
    fn from_lookup(get: &mut impl FnMut(&str) -> Option<String>) -> Result<Option<Self>> {
        let Some(path) = get("DURABLE_ACTORS_HOST_METADATA_FILE") else {
            return Ok(None);
        };
        ensure!(
            !path.is_empty(),
            "DURABLE_ACTORS_HOST_METADATA_FILE must not be empty"
        );
        let canonical_region = required(get, "DURABLE_ACTORS_REGION")?;
        crate::placement::validate_region(&canonical_region)?;
        Ok(Some(Self {
            path: path.into(),
            canonical_region,
        }))
    }
}

struct PreparedActorHost {
    replication: Arc<dyn crate::litestream::Replicator>,
    storage: Arc<super::storage::HostStorage>,
    credentials: DropGuard,
    sockets: Arc<super::sockets::HostSockets>,
    invocation_auth: ActorJwtVerifier,
    listener: TcpListener,
    route: String,
    executor_connection: ActorExecutorConnection,
    javascript: tokio::process::Child,
    host: Arc<ActorHost>,
    lease: Arc<HostLeaseMaintainer>,
    renewal: LeaseRenewalTask,
}

async fn prepare_actor_host(
    config: &ActorHostConfig,
    timings: &mut HostStartupTimings,
    warm: Option<super::spare::WarmHost>,
) -> Result<PreparedActorHost> {
    let replication = warm
        .as_ref()
        .map(|warm| warm.replication.clone())
        .unwrap_or_else(|| Arc::new(crate::litestream::Litestream::default()));
    timings.replication_ready_at_ms = Some(timings.elapsed_ms());
    let invocation_auth = invocation_auth(config)?;
    timings.authentication_ready_at_ms = Some(timings.elapsed_ms());
    let (warm_listener, warm_executor, warm_storage, warm_control_plane) = match warm {
        Some(warm) => (
            Some(warm.listener),
            Some((warm.executor, warm.javascript, warm.entrypoint)),
            Some(warm.storage),
            warm.control_plane,
        ),
        None => (None, None, None, None),
    };
    let (control_plane, (listener, route, endpoint)) = tokio::try_join!(
        async {
            match warm_control_plane.filter(|warm| warm.url == config.control_plane_url) {
                Some(warm) => warm.client.with_host_token(&config.host_token),
                None => {
                    ControlPlaneClient::connect(&config.control_plane_url, &config.host_token).await
                }
            }
        },
        bind_host_listener(config, warm_listener),
    )?;
    timings.control_plane_ready_at_ms = Some(timings.elapsed_ms());
    let control_plane = Arc::new(control_plane);
    let stop = CancellationToken::new();
    let credentials = stop.clone().drop_guard();
    let storage = Arc::new(
        super::storage::HostStorage::new(
            config.runtime_config.clone(),
            config.host_id.clone(),
            config.session_id.clone(),
            control_plane.clone(),
            stop,
            warm_storage,
            config.actor.clone(),
        )
        .await?
        .with_activation(config.new_actor, config.owner_hint.clone()),
    );
    let started = timings.started_at;
    let storage_ready = async {
        let result = prepare_storage(config, &endpoint, storage.clone()).await;
        timings.storage_ready_at_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
        result
    };
    let executor_ready = async {
        if let Some(artifact) = &config.artifact {
            storage.install_code(artifact).await?;
        }
        let result = if let Some((executor, javascript, entrypoint)) = warm_executor {
            let connection = tokio::time::timeout(
                Duration::from_secs(60),
                executor.load(&entrypoint, &config.customer_environment),
            )
            .await??;
            Ok((connection, javascript))
        } else {
            connect_executor(
                &config.executor_socket,
                timings.started_at,
                &mut timings.javascript_spawned_at_ms,
            )
            .await
        };
        timings.executor_ready_at_ms = Some(started.elapsed().as_secs_f64() * 1_000.0);
        result
    };
    let (lease, executor) = tokio::join!(storage_ready, executor_ready);
    let (lease, renewal) = lease?;
    let (executor_connection, javascript) = match executor {
        Ok(executor) => executor,
        Err(error) => {
            let _ = lease.unregister().await;
            let _ = renewal.shutdown().await;
            return Err(error);
        }
    };
    let sockets = Arc::new(super::sockets::HostSockets::new(
        storage.clone(),
        control_plane.clone(),
    ));
    let host = Arc::new(
        ActorHost::new(
            endpoint,
            executor_connection.executor(),
            storage.clone(),
            super::persistence::ActorPersistence::new(storage.clone()),
            sockets.clone(),
            replication.clone(),
        )
        .with_traces(crate::request_traces::TraceSender::start(
            control_plane.clone(),
            storage.stop.child_token(),
        )),
    );
    timings.lease_registered_at_ms = Some(timings.elapsed_ms());
    Ok(PreparedActorHost {
        replication,
        storage,
        credentials,
        sockets,
        invocation_auth,
        listener,
        route,
        executor_connection,
        javascript,
        host,
        lease,
        renewal,
    })
}

async fn prepare_storage(
    config: &ActorHostConfig,
    endpoint: &HostEndpoint,
    storage: Arc<super::storage::HostStorage>,
) -> Result<(Arc<HostLeaseMaintainer>, LeaseRenewalTask)> {
    let lease = Arc::new(HostLeaseMaintainer::new(
        endpoint.clone(),
        config.session_id.clone(),
        storage.clone(),
        Arc::new(SystemClock),
        config.lease_duration,
        config.renew_every,
    )?);
    let renewal = lease.clone().start().await?;
    Ok((lease, renewal))
}

async fn bind_host_listener(
    config: &ActorHostConfig,
    listener: Option<TcpListener>,
) -> Result<(TcpListener, String, HostEndpoint)> {
    let listener = match listener {
        Some(listener) => listener,
        None => TcpListener::bind(config.host_bind).await?,
    };
    let bound = listener.local_addr()?;
    let route = config
        .host_route
        .clone()
        .unwrap_or_else(|| format!("http://{bound}"));
    write_host_metadata(config, &route).await?;
    let endpoint = HostEndpoint {
        id: config.host_id.clone(),
        route: route.clone(),
    };
    Ok((listener, route, endpoint))
}

fn invocation_auth(config: &ActorHostConfig) -> Result<ActorJwtVerifier> {
    ActorJwtVerifier::for_scope(
        &config.jwt_public_keys,
        config.jwt_issuer.clone(),
        config.invocation_jwt_audience.clone(),
        ActorTokenPurpose::Invocation,
        config.jwt_max_lifetime,
    )
}

async fn write_host_metadata(config: &ActorHostConfig, route: &str) -> Result<()> {
    let Some(metadata) = &config.metadata else {
        return Ok(());
    };
    let document = serde_json::to_vec(&serde_json::json!({
        "hostId": config.host_id,
        "sessionId": config.session_id,
        "route": route,
        "canonicalRegion": metadata.canonical_region,
    }))?;
    let temporary = metadata
        .path
        .with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    tokio::fs::write(&temporary, document)
        .await
        .context("write actor host metadata")?;
    tokio::fs::rename(&temporary, &metadata.path)
        .await
        .context("publish actor host metadata")
}

struct HostStartupTimings {
    started_at: Instant,
    configuration_loaded_at_ms: f64,
    authentication_ready_at_ms: Option<f64>,
    control_plane_ready_at_ms: Option<f64>,
    storage_ready_at_ms: Option<f64>,
    executor_ready_at_ms: Option<f64>,
    replication_ready_at_ms: Option<f64>,
    javascript_spawned_at_ms: Option<f64>,
    lease_registered_at_ms: Option<f64>,
    executor_notified_at_ms: Option<f64>,
}

impl HostStartupTimings {
    fn new(config: &ActorHostConfig) -> Self {
        Self {
            started_at: config.startup_started_at,
            configuration_loaded_at_ms: config.configuration_loaded_at_ms,
            authentication_ready_at_ms: None,
            control_plane_ready_at_ms: None,
            storage_ready_at_ms: None,
            executor_ready_at_ms: None,
            replication_ready_at_ms: None,
            javascript_spawned_at_ms: None,
            lease_registered_at_ms: None,
            executor_notified_at_ms: None,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64() * 1_000.0
    }
}

fn log_startup(
    config: &ActorHostConfig,
    timings: &HostStartupTimings,
    outcome: &str,
    error: Option<&anyhow::Error>,
) {
    info!(
        event = "actor_host_startup",

        host_id = %config.host_id,
        started_at_ms = 0,
        configuration_loaded_at_ms = timings.configuration_loaded_at_ms,
        authentication_ready_at_ms = timings.authentication_ready_at_ms,
        control_plane_ready_at_ms = timings.control_plane_ready_at_ms,
        storage_ready_at_ms = timings.storage_ready_at_ms,
        executor_ready_at_ms = timings.executor_ready_at_ms,
        replication_ready_at_ms = timings.replication_ready_at_ms,
        javascript_spawned_at_ms = timings.javascript_spawned_at_ms,
        lease_registered_at_ms = timings.lease_registered_at_ms,
        executor_notified_at_ms = timings.executor_notified_at_ms,
        completed_at_ms = timings.elapsed_ms(),
        outcome,
        error = error.map(|error| format!("{error:#}")),
        "actor host startup completed"
    );
}

async fn connect_executor(
    socket: &std::path::Path,
    started_at: Instant,
    javascript_spawned_at_ms: &mut Option<f64>,
) -> Result<(ActorExecutorConnection, tokio::process::Child)> {
    let listener = ActorExecutorListener::bind(socket).await?;
    let entrypoint = std::env::var("DURABLE_ACTORS_ENTRYPOINT").ok();
    let javascript =
        spawn_executor_process(false, &socket.display().to_string(), entrypoint.as_deref())?;
    *javascript_spawned_at_ms = Some(started_at.elapsed().as_secs_f64() * 1_000.0);
    Ok((listener.accept().await?, javascript))
}

async fn wait_for_host_stop<ServerFuture, ExecutorFuture, ShutdownFuture>(
    mut server: std::pin::Pin<&mut ServerFuture>,
    mut executor: std::pin::Pin<&mut ExecutorFuture>,
    javascript: &mut tokio::process::Child,
    mut shutdown: std::pin::Pin<&mut ShutdownFuture>,
    lease_lost: &mut tokio::sync::watch::Receiver<bool>,
    activity: (
        &mut tokio::sync::watch::Receiver<ActorActivity>,
        &mut tokio::sync::watch::Receiver<bool>,
    ),
    idle_timeout: Duration,
) -> Result<()>
where
    ServerFuture: Future<Output = Result<()>> + ?Sized,
    ExecutorFuture: Future<Output = Result<()>> + ?Sized,
    ShutdownFuture: Future<Output = ()> + ?Sized,
{
    let (activity, actor_stopped) = activity;
    loop {
        if *actor_stopped.borrow() {
            break Err(anyhow::anyhow!(
                "actor activation stopped; host self-fenced"
            ));
        }
        let current = *activity.borrow_and_update();
        tokio::select! {
            changed = actor_stopped.changed() => {
                if changed.is_err() { break Err(anyhow::anyhow!("actor lifecycle tracker stopped")); }
            }
            result = server.as_mut() => break result.context("serve actor host network endpoints"),
            result = executor.as_mut() => break result.context("run JavaScript actor executor"),
            result = javascript.wait() => break Err(anyhow::anyhow!("JavaScript actor executor exited with {}", result?)),
            () = shutdown.as_mut() => break Ok(()),
            changed = lease_lost.changed() => {
                if changed.is_err() || *lease_lost.borrow() {
                    break Err(anyhow::anyhow!("host lease expired; host self-fenced"));
                }
            }
            changed = activity.changed() => {
                if changed.is_err() { break Err(anyhow::anyhow!("actor activity tracker stopped")); }
            }
            () = tokio::time::sleep_until(current.last_active + idle_timeout), if current.active == 0 => {
                if activity.has_changed()? { continue; }
                break Ok(());
            },
        }
    }
}

async fn stop_host_tasks(
    host: &ActorHost,
    stop: &CancellationToken,
    server: impl Future,
    executor: impl Future,
) {
    if let Err(error) = host.drain(HOST_ACTOR_DRAIN_TIMEOUT).await {
        error!(error = %format!("{error:#}"), "actor invocations did not drain cleanly");
    }
    stop.cancel();
    let _ = tokio::time::timeout(HOST_TASK_SHUTDOWN_TIMEOUT, async {
        let _ = tokio::join!(server, executor);
    })
    .await;
}

pub(super) fn spawn_executor_process(
    generic: bool,
    socket: &str,
    entrypoint: Option<&str>,
) -> Result<tokio::process::Child> {
    let mut command = if entrypoint.is_some_and(|path| path.ends_with(".pyz")) {
        let mut command = Command::new(
            std::env::var("DURABLE_ACTORS_PYTHON").unwrap_or_else(|_| "python3".into()),
        );
        command.args(["-m", "durable_actors.host"]);
        if generic {
            command.arg("--generic");
        }
        command
    } else {
        let mut command = Command::new("bun");
        command.args([
            "--eval",
            "import(process.env.DURABLE_ACTORS_SDK_HOST ?? \"durable-actors/host\").then(module => module[process.env.DURABLE_ACTORS_GENERIC_EXECUTOR === \"1\" ? \"runGenericHost\" : \"runActorHost\"]())",
        ]);
        command
    };
    command
        .env(
            "DURABLE_ACTORS_GENERIC_EXECUTOR",
            if generic { "1" } else { "0" },
        )
        .env("DURABLE_ACTORS_EXECUTOR_SOCKET", socket)
        .env_remove("DURABLE_ACTORS_SPARE_TOKEN")
        .env_remove("DURABLE_ACTORS_RUNTIME_CONFIG")
        .env_remove("DURABLE_ACTORS_HOST_TOKEN")
        .env_remove("GOOGLE_APPLICATION_CREDENTIALS")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .context("start actor executor")
}

fn required(get: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    let value = get(name).with_context(|| format!("{name} is required"))?;
    ensure!(!value.is_empty(), "{name} must not be empty");
    Ok(value)
}

pub(crate) fn host_idle_timeout_ms(get: &mut impl FnMut(&str) -> Option<String>) -> Result<u64> {
    let timeout = duration_ms(
        get,
        "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS",
        DEFAULT_HOST_IDLE_TIMEOUT_MS,
    )?;
    ensure!(
        timeout.as_millis() <= u128::from(MAX_IDLE_TIMEOUT_MS),
        "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS is too large"
    );
    Ok(timeout.as_millis() as u64)
}

fn duration_ms(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of milliseconds"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_millis(value))
}

fn duration_seconds(
    get: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    let value = get(name)
        .map(|value| value.parse::<u64>())
        .transpose()
        .with_context(|| format!("{name} must be an integer number of seconds"))?
        .unwrap_or(default);
    ensure!(value > 0, "{name} must be positive");
    Ok(Duration::from_secs(value))
}

#[cfg(test)]
#[path = "../../tests/unit/host/process.rs"]
mod tests;
