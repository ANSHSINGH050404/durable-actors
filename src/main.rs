use std::io::IsTerminal;

use anyhow::Result;
use clap::{Parser, Subcommand};
use durable_actors::{
    control_plane::{ControlPlaneProcessConfig, DevOptions, serve_control_plane, serve_local},
    host::{ActorHostConfig, serve_actor_host},
};
use tokio::sync::oneshot;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let development_logs = cli.command.is_some()
        || std::env::var("DURABLE_ACTORS_LOG_MODE").as_deref() == Ok("development");
    init_logging(development_logs);
    if let Err(error) = run(cli).await {
        if development_logs {
            error!(error = %format!("{error:#}"), "local actor runtime failed");
        } else {
            error!(error = %format!("{error:#}"), "durable-actors process failed");
        }
        std::process::exit(1);
    }
}

fn init_logging(development: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(if development {
            "durable_actors=error,durable_actors::dev=info"
        } else {
            "info"
        })
    });
    if development {
        tracing_subscriber::fmt()
            .compact()
            .without_time()
            .with_target(false)
            .with_ansi(std::io::stdout().is_terminal())
            .with_env_filter(filter)
            .init();
    } else {
        tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_env_filter(filter)
            .init();
    }
}

async fn run(cli: Cli) -> Result<()> {
    if let Some(Commands::Dev(options)) = cli.command {
        return serve_local(options, shutdown_signal()).await;
    }
    let shutdown = shutdown_signal();
    match std::env::var("DURABLE_ACTORS_PROCESS_ROLE")
        .as_deref()
        .unwrap_or("host")
    {
        "control_plane" => {
            serve_control_plane(ControlPlaneProcessConfig::from_env()?, shutdown).await
        }
        "spare" => durable_actors::host::serve_spare(shutdown).await,
        "host" => serve_actor_host(ActorHostConfig::from_env()?, shutdown).await,
        "replica" => durable_actors::replication::serve_replica_host(shutdown).await,
        role => anyhow::bail!("unsupported DURABLE_ACTORS_PROCESS_ROLE {role:?}"),
    }
}

#[derive(Parser)]
#[command(
    version,
    about = "Run durable TypeScript and Python actors locally or in the cloud"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Start local actors with persistent file storage")]
    Dev(DevOptions),
}

async fn shutdown_signal() {
    if std::env::var_os("DURABLE_ACTORS_PARENT_LIFETIME_STDIN").is_none() {
        wait_for_signal().await;
        info!("shutdown signal received");
        return;
    }
    tokio::select! {
        _ = wait_for_signal() => info!("shutdown signal received"),
        _ = wait_for_parent_stdin_close() => info!("parent process exited"),
    }
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn wait_for_parent_stdin_close() {
    let (closed, receiver) = oneshot::channel();
    // Tokio's stdin read cannot be cancelled and would block runtime shutdown after a signal.
    std::thread::spawn(move || {
        let _ = std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink());
        let _ = closed.send(());
    });
    let _ = receiver.await;
}
