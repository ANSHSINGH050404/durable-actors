use std::{
    fs::{File, OpenOptions},
    future::Future,
    io::Write,
    os::fd::FromRawFd,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anstyle::{AnsiColor, Style};
use anyhow::{Context, Result, ensure};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use axum::{Router, extract::Request, response::Response};
use base64::{Engine, engine::general_purpose::STANDARD};
use clap::{Args, ValueEnum};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{Span, info, info_span};

use crate::{
    bucket::{
        Bucket, FileBucket, GcsBucket, RuntimeStorage,
        access::{BucketLocation, RuntimeAccess},
    },
    clock::SystemClock,
    sandbox::{HostSandboxRuntimeConfig, LocalSandboxProvider},
};

use super::{
    ActorJwtIssuer, ActorJwtVerifier, ActorTokenPurpose, ControlPlaneService,
    admin::{AdminService, HostLaunchSpec, LocalAdminRegistry},
    public_api,
    service::SandboxHostProvisioner,
};

#[derive(Args)]
pub struct DevOptions {
    #[arg(long, env = "DURABLE_ACTORS_PROJECT_ID", default_value = "local")]
    pub project_id: String,
    #[arg(long, env = "DURABLE_ACTORS_SECRET")]
    pub api_key: Option<String>,
    #[arg(long, env = "DURABLE_ACTORS_PROJECT", default_value = ".")]
    pub project: PathBuf,
    #[arg(long, env = "DURABLE_ACTORS_PORT", default_value_t = 7100)]
    pub port: u16,
    #[arg(long, env = "DURABLE_ACTORS_DATA_DIR")]
    pub data_dir: Option<PathBuf>,
    #[arg(
        long,
        env = "DURABLE_ACTORS_ENTRYPOINT",
        default_value = "src/actors.ts"
    )]
    pub entrypoint: String,
    #[arg(
        long,
        env = "DURABLE_ACTORS_STORAGE",
        value_enum,
        default_value = "local"
    )]
    pub storage: DevStorage,
    #[arg(long, hide = true, value_parser = clap::value_parser!(i32).range(3..))]
    pub ready_fd: Option<i32>,
    #[arg(long, hide = true)]
    pub sdk_host: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum DevStorage {
    Local,
    Gcs,
}

pub async fn serve_local(
    options: DevOptions,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    super::admin::validate_component("project ID", &options.project_id, 64)?;
    let project = options
        .project
        .canonicalize()
        .context("find actor project directory")?;
    ensure!(
        project.join(&options.entrypoint).is_file(),
        "actor file {} is missing; create it before starting the demo",
        options.entrypoint
    );
    let directory = options
        .data_dir
        .clone()
        .unwrap_or_else(|| project.join(".durable-actors"));
    let _lock = prepare_directory(&directory)?;
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, options.port))
        .await
        .context("bind local runtime; use --port to select another port")?;
    let origin = format!("http://{}", listener.local_addr()?);
    let storage = local_storage(&options, &directory).await?;
    let provider = Arc::new(
        LocalSandboxProvider::new(
            std::env::current_exe()?,
            project.clone(),
            storage.runtime.clone(),
            options.sdk_host.clone(),
            directory.join("local-hosts.sqlite3"),
        )
        .await?,
    );
    let routes = local_routes(
        &options,
        &project,
        &origin,
        &storage,
        provider.clone(),
        &directory,
    )
    .await?;
    let server = LocalServer::start(listener, routes, provider);
    let ready = notify_launcher(
        &origin,
        options.api_key.as_deref(),
        &storage.region,
        &options.project_id,
        options.ready_fd,
    );
    if ready.is_ok() {
        anstream::println!(
            "{}",
            styled_local_ready_message(
                &origin,
                &directory,
                &options.project_id,
                options.api_key.as_deref()
            )
        );
    }
    server.run_until(shutdown, ready).await
}

struct LocalServer {
    provider: Arc<LocalSandboxProvider>,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl LocalServer {
    fn start(
        listener: TcpListener,
        routes: tonic::service::Routes,
        provider: Arc<LocalSandboxProvider>,
    ) -> Self {
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, logged_routes(routes))
                .with_graceful_shutdown(stopped.cancelled_owned())
                .await
        });
        Self {
            provider,
            stop,
            server,
        }
    }

    async fn run_until(
        mut self,
        shutdown: impl Future<Output = ()>,
        ready: Result<()>,
    ) -> Result<()> {
        let result = match ready {
            Ok(()) => {
                tokio::select! {
                    _ = shutdown => Ok(()),
                    result = &mut self.server => result.context("local server task failed").and_then(|result| result.context("local server failed")),
                }
            }
            Err(error) => Err(error),
        };
        // Hosts unregister their leases through this server while draining.
        self.provider.shutdown().await;
        self.stop.cancel();
        if !self.server.is_finished()
            && tokio::time::timeout(Duration::from_secs(5), &mut self.server)
                .await
                .is_err()
        {
            self.server.abort();
        }
        result
    }
}

fn logged_routes(routes: tonic::service::Routes) -> Router {
    routes.into_axum_router().layer(
        TraceLayer::new_for_http()
            .make_span_with(|request: &Request| {
                info_span!(
                    target: "durable_actors::dev",
                    "control_plane_request",
                    method = %request.method(),
                    path = %request.uri().path(),
                )
            })
            .on_response(|response: &Response, latency: Duration, span: &Span| {
                info!(
                    target: "durable_actors::dev",
                    parent: span,
                    status = response.status().as_u16(),
                    latency_ms = %format_args!("{:.1}", latency.as_secs_f64() * 1_000.0),
                    "request completed"
                );
            })
            .on_failure(()),
    )
}

fn prepare_directory(directory: &Path) -> Result<File> {
    std::fs::create_dir_all(directory)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("runtime.lock"))?;
    lock.try_lock()
        .context("another local runtime is already using this data directory")?;
    std::fs::write(directory.join(".gitignore"), "*\n")?;
    Ok(lock)
}

struct LocalState {
    traces: crate::request_traces::TraceStore,
    runtime: Arc<RuntimeStorage>,
    access: Arc<RuntimeAccess>,
    region: String,
}

async fn local_storage(options: &DevOptions, directory: &Path) -> Result<LocalState> {
    let location = match options.storage {
        DevStorage::Local => BucketLocation::File {
            directory: directory.canonicalize()?.join("objects"),
        },
        DevStorage::Gcs => BucketLocation::Gcs {
            artifact_bucket: std::env::var("DURABLE_ACTORS_BUCKET")
                .context("GCS bucket required")?,
            bucket: std::env::var("DURABLE_ACTORS_BUCKET")
                .context("DURABLE_ACTORS_STORAGE=gcs requires DURABLE_ACTORS_BUCKET")?,
        },
    };
    let bucket: Arc<dyn Bucket> = match &location {
        BucketLocation::File { directory } => Arc::new(FileBucket::new(directory.clone())?),
        BucketLocation::Gcs { bucket, .. } => Arc::new(GcsBucket::new(bucket).await?),
    };
    let runtime = Arc::new(RuntimeStorage::new(bucket, Arc::new(SystemClock))?);
    let bootstrap = Arc::new(RuntimeAccess::new(location, runtime.persistence.clone())?);
    Ok(LocalState {
        traces: crate::request_traces::TraceStore::open(Arc::new(
            crate::request_traces::persistence::sqlite::SqliteTracePersistence::new(
                directory.join("request-traces.sqlite3"),
            ),
        ))
        .await?,
        runtime,
        access: bootstrap,
        region: "north-america-east".into(),
    })
}

async fn local_routes(
    options: &DevOptions,
    project: &Path,
    origin: &str,
    storage: &LocalState,
    provider: Arc<LocalSandboxProvider>,
    directory: &Path,
) -> Result<tonic::service::Routes> {
    let issuer = local_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "durable-actors-control-plane",
        "durable-actors-authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(86_400),
    )?;
    let spec = HostLaunchSpec {
        sandboxes: Default::default(),
        project_id: options.project_id.clone(),
        source: None,
        code_snapshot: None,
        image_ref: "local".into(),
        working_directory: project.display().to_string(),
        actor_entrypoint: Some(options.entrypoint.clone()),
        secret_refs: vec![],
    };
    let registry = Arc::new(LocalAdminRegistry::default());
    let runtime = HostSandboxRuntimeConfig {
        control_plane_url: origin.to_owned(),
        jwt_issuer: "durable-actors-control-plane".into(),
        invocation_jwt_audience: "durable-actors-invoke".into(),
        host_idle_timeout_ms: crate::host::host_idle_timeout_ms(&mut |name| {
            std::env::var(name).ok()
        })?,
    };
    let max_socket_connections =
        crate::sockets::max_connections(&mut |name| std::env::var(name).ok())?;
    let provisioner = Arc::new(
        SandboxHostProvisioner::new(provider, runtime, issuer.clone(), None)
            .with_runtime_access(storage.access.clone()),
    );
    let socket_gateway = super::socket_gateway::SocketGateway::start(
        origin.to_owned(),
        Arc::new(super::socket_directory::MemorySocketDirectory::default()),
        max_socket_connections,
        true,
        CancellationToken::new(),
    )
    .await?;
    let gateway = super::gateway::Gateway::new(&issuer, origin.to_owned(), socket_gateway)?;
    let mut service = ControlPlaneService::new(
        storage.runtime.clone(),
        auth,
        registry.clone(),
        issuer.clone(),
        provisioner,
    )
    .with_runtime_access(storage.access.clone())
    .with_local_builds(Arc::new(super::local_build::LocalBuilds::new(
        project.to_owned(),
        directory.canonicalize()?.join("code"),
        Arc::new(super::local_build::ActorCodeCompiler::new(
            options.sdk_host.clone(),
        )),
    )))
    .with_traces(storage.traces.clone());
    service.gateway = Some(gateway);
    let admin = AdminService::new(options.api_key.clone(), registry, issuer)?;
    service.deploy_source(&admin, &spec, None).await?;
    let inventory = Arc::new(super::socket_inventory::GatewayInventoryReader::new(
        storage.runtime.clone(),
        service.gateway.as_ref().unwrap().connections.clone(),
        options.api_key.clone(),
    ));
    let inspector = super::inspection::ActorInspector::new(
        inventory,
        storage.runtime.clone(),
        service.changes.clone(),
    )
    .with_traces(service.traces.clone());
    let public = public_api::router(service.clone(), admin.clone())
        .merge(super::inspection::local_router(inspector, admin));
    Ok(tonic::service::Routes::from(public).add_service(service.into_internal_service()))
}

fn local_issuer() -> Result<ActorJwtIssuer> {
    let key = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| anyhow::anyhow!("generate local signing key"))?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(key.as_ref()),
        "local",
        "durable-actors-control-plane",
        "durable-actors-authority",
        "durable-actors-invoke",
        Duration::from_secs(86_400),
    )
}

fn notify_launcher(
    origin: &str,
    api_key: Option<&str>,
    region: &str,
    project_id: &str,
    ready_fd: Option<i32>,
) -> Result<()> {
    if let Some(fd) = ready_fd {
        // The launcher transfers ownership of this inherited readiness descriptor.
        let mut ready = unsafe { File::from_raw_fd(fd) };
        let connection = serde_json::json!({ "projectId": project_id, "pid": std::process::id(), "controlPlaneUrl": origin, "apiKey": api_key, "storageRegion": region });
        serde_json::to_writer(&mut ready, &connection)?;
        ready.flush()?;
    }
    Ok(())
}

fn styled_local_ready_message(
    origin: &str,
    directory: &Path,
    project_id: &str,
    secret: Option<&str>,
) -> String {
    let styles = LocalReadyStyles {
        title: Style::new().bold().fg_color(Some(AnsiColor::Cyan.into())),
        context: Style::new().fg_color(Some(AnsiColor::BrightBlack.into())),
        ready: Style::new().bold().fg_color(Some(AnsiColor::Green.into())),
        label: Style::new().bold(),
        command: Style::new().fg_color(Some(AnsiColor::Cyan.into())),
    };
    format_local_ready_message(origin, directory, project_id, secret, styles)
}

#[cfg(test)]
fn local_ready_message(origin: &str, directory: &Path, project_id: &str) -> String {
    format_local_ready_message(
        origin,
        directory,
        project_id,
        None,
        LocalReadyStyles::default(),
    )
}

fn format_local_ready_message(
    origin: &str,
    directory: &Path,
    project_id: &str,
    secret: Option<&str>,
    styles: LocalReadyStyles,
) -> String {
    let LocalReadyStyles {
        title,
        context,
        ready,
        label,
        command,
    } = styles;
    let credentials = local_authentication_instructions(secret);
    let project = if project_id == "local" {
        String::new()
    } else {
        format!("     {command}DURABLE_ACTORS_PROJECT_ID={project_id}{command:#}\n")
    };
    format!(
        "{title}durable actors{title:#} {context}/ local{context:#}\n\n  {ready}Ready{ready:#}    {origin}\n  {label}Project{label:#}  {project_id}\n  {label}State{label:#}    {}\n\n  {label}Connect your application{label:#}\n  Keep this server running. In your application project:\n\n  {label}1. Configure your client{label:#}\n     Paste the following into your client application's .env file.\n     This is the application that connects to this actor server.\n\n{project}     {command}DURABLE_ACTORS_CONTROL_PLANE_URL={origin}{command:#}\n{credentials}\n\n  {label}2. Generate your client{label:#}\n     {command}durable-actors generate{command:#}\n\n  Start your application backend with this .env loaded.\n",
        directory.display(),
    )
}

fn local_authentication_instructions(secret: Option<&str>) -> &'static str {
    if secret.is_some() {
        "     Authentication is enabled. Set DURABLE_ACTORS_SECRET in your backend to the same value."
    } else {
        "     Authentication is disabled. No secret is needed for local development."
    }
}

#[derive(Clone, Copy, Default)]
struct LocalReadyStyles {
    title: Style,
    context: Style,
    ready: Style,
    label: Style,
    command: Style,
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/local.rs"]
mod tests;
