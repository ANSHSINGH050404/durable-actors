use axum::{Router, extract::WebSocketUpgrade, response::Response, routing::get};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::oneshot;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::*;
use std::collections::HashMap;

#[tokio::test]
async fn server_carries_websocket_upgrades() -> Result<()> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let routes = tonic::service::Routes::from(Router::new().route("/socket", get(echo_websocket)));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(serve_routes(listener, routes, async {
        let _ = shutdown_rx.await;
    }));

    let (mut socket, _) = connect_async(format!("ws://{address}/socket")).await?;
    socket.send(Message::Text("hello".into())).await?;
    assert_eq!(
        socket.next().await.transpose()?,
        Some(Message::Text("hello".into()))
    );
    socket.close(None).await?;
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

#[test]
fn parses_the_minimal_storage_configuration() -> Result<()> {
    let values = process_environment();
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    assert_eq!(config.storage.bucket, "actor-state-test");
    let resources = &config.sandbox_provider.pool.resources;
    assert_eq!((resources.cpu_millis, resources.memory_mib), (250, 256));
    assert_eq!(resources, &crate::sandbox::ResourceLimits::default());
    assert_eq!(config.sandbox_provider.runtime.host_idle_timeout_ms, 10_000);
    assert_eq!(config.jwt_max_lifetime, Duration::from_secs(86_400));
    assert_eq!(config.api_key.as_deref(), Some("api-key"));
    Ok(())
}

#[test]
fn pool_capacity_is_configurable_and_validated() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|v| (*v).into()))
    };
    let defaults = parse(&values)?.sandbox_provider.pool;
    assert_eq!(
        (defaults.idle, defaults.fleet_maximum, defaults.max_starting),
        (64, 256, 32)
    );
    values.extend([
        ("DURABLE_ACTORS_SPARE_IDLE", "128"),
        ("DURABLE_ACTORS_SPARE_FLEET_MAX", "512"),
        ("DURABLE_ACTORS_SPARE_MAX_STARTING", "4"),
    ]);
    let configured = parse(&values)?.sandbox_provider.pool;
    assert_eq!(
        (
            configured.idle,
            configured.fleet_maximum,
            configured.max_starting
        ),
        (128, 512, 4)
    );
    values.insert("DURABLE_ACTORS_SPARE_FLEET_MAX", "1");
    assert!(parse(&values).is_err());
    values.insert("DURABLE_ACTORS_SPARE_FLEET_MAX", "512");
    values.insert("DURABLE_ACTORS_SPARE_MAX_STARTING", "0");
    assert!(parse(&values).is_err());
    Ok(())
}

#[test]
fn host_idle_timeout_is_configurable_and_bounded() -> Result<()> {
    for value in ["1", "120000", "86400000"] {
        let mut values = process_environment();
        values.insert("DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS", value);
        let config = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        })?;
        assert_eq!(
            config.sandbox_provider.runtime.host_idle_timeout_ms,
            value.parse::<u64>()?
        );
    }
    for value in ["0", "-1", "1.5", "86400001", "not-a-number"] {
        let mut values = process_environment();
        values.insert("DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS", value);
        assert!(
            ControlPlaneProcessConfig::from_lookup(|name| {
                values.get(name).map(|value| (*value).into())
            })
            .is_err()
        );
    }
    Ok(())
}

#[test]
fn server_configuration_allows_an_unset_secret_on_any_address() -> Result<()> {
    for bind in [
        "127.0.0.1:7100",
        "[::1]:7100",
        "0.0.0.0:7100",
        "[::]:7100",
        "192.168.1.1:7100",
    ] {
        let mut values = process_environment();
        values.remove("DURABLE_ACTORS_SECRET");
        values.insert("DURABLE_ACTORS_CONTROL_PLANE_BIND", bind);
        let config = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        })?;
        assert!(config.api_key.is_none());
        assert_eq!(config.bind, bind.parse::<SocketAddr>()?);
    }
    Ok(())
}

#[test]
fn server_rejects_an_empty_or_untrimmed_shared_secret() {
    for secret in ["", " ", " key", "key "] {
        let mut values = process_environment();
        values.insert("DURABLE_ACTORS_SECRET", secret);
        let result = ControlPlaneProcessConfig::from_lookup(|name| {
            values.get(name).map(|value| (*value).into())
        });
        assert!(result.is_err(), "server accepted an invalid secret");
    }
}

#[test]
fn authentication_warning_depends_on_the_listening_address_and_secret() -> Result<()> {
    for (bind, exposed) in [
        ("127.0.0.1:7100", false),
        ("127.0.0.2:7100", false),
        ("[::1]:7100", false),
        ("0.0.0.0:7100", true),
        ("[::]:7100", true),
        ("192.168.1.1:7100", true),
    ] {
        for secret in [None, Some("configured-secret")] {
            let output = tempfile::NamedTempFile::new()?;
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::WARN)
                .with_writer(output.reopen()?)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                warn_if_authentication_disabled(bind.parse().unwrap(), secret);
            });
            let logs = std::fs::read_to_string(output.path())?;
            if exposed && secret.is_none() {
                assert!(
                    logs.contains("Authentication is disabled"),
                    "{bind}: {logs}"
                );
                assert!(logs.contains(bind));
                assert!(logs.contains("DURABLE_ACTORS_SECRET"));
            } else {
                assert!(logs.is_empty(), "unexpected warning at {bind}: {logs}");
            }
        }
    }
    Ok(())
}

#[test]
fn production_defaults_to_zonal_replicas_and_gke() -> Result<()> {
    let values = process_environment();
    let config = ControlPlaneProcessConfig::from_lookup(|name| {
        values.get(name).map(|value| (*value).into())
    })?;
    assert!(matches!(
        config.storage.persistence,
        crate::bucket::PersistenceConfig::Replicated {
            durability: crate::bucket::Durability::Zonal,
            ..
        }
    ));
    assert_eq!(
        config.sandbox_provider.gke.zones["north-america-west"],
        vec!["us-west4-a"]
    );
    Ok(())
}

#[test]
fn compute_region_accepts_multiple_zones_and_rejects_empty_or_mismatched_sets() -> Result<()> {
    let mut values = process_environment();
    values.insert(
        "DURABLE_ACTORS_GKE_ZONES",
        r#"{"north-america-west":["us-west4-a","us-west4-b","us-west4-c"]}"#,
    );
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    parse(&values)?;
    for zones in [
        r#"{"north-america-west":[]}"#,
        r#"{"north-america-west":["us-west4-a","us-east4-b"]}"#,
        r#"{"north-america-west":["us-west4-a","us-west4-a"]}"#,
    ] {
        values.insert("DURABLE_ACTORS_GKE_ZONES", zones);
        assert!(parse(&values).is_err(), "{zones}");
    }
    Ok(())
}

#[test]
fn configures_socket_events_without_a_separate_key() -> Result<()> {
    let mut complete = HashMap::from([(
        "DURABLE_ACTORS_SOCKET_EVENT_URL",
        "https://api.example.com/events",
    )]);
    let sink =
        socket_event_sink_config(&mut |name| complete.get(name).map(|value| (*value).into()))?
            .context("socket event sink was not configured")?;
    assert_eq!(sink.url, "https://api.example.com/events");
    complete.remove("DURABLE_ACTORS_SOCKET_EVENT_URL");
    assert!(
        socket_event_sink_config(&mut |name| complete.get(name).map(|value| (*value).into()))?
            .is_none()
    );
    Ok(())
}

async fn echo_websocket(upgrade: WebSocketUpgrade) -> Response {
    upgrade.on_upgrade(async |mut socket| {
        if let Some(Ok(message)) = socket.recv().await {
            let _ = socket.send(message).await;
        }
    })
}

fn process_environment() -> HashMap<&'static str, &'static str> {
    HashMap::from([
        (
            "DURABLE_ACTORS_GOOGLE_SERVICE_ACCOUNT",
            "test@project.iam.gserviceaccount.com",
        ),
        ("DURABLE_ACTORS_JWT_SIGNING_KEY", "c2lnbmluZw=="),
        ("DURABLE_ACTORS_SECRET", "api-key"),
        ("DURABLE_ACTORS_BUCKET", "actor-state-test"),
        (
            "DURABLE_ACTORS_RUNTIME_IMAGE",
            "registry.example/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        ("DURABLE_ACTORS_ARTIFACT_BUCKET", "customer-code"),
        ("DURABLE_ACTORS_ARCHIVE_BUCKET", "actor-archive"),
        (
            "DURABLE_ACTORS_REPLICA_SECRET",
            "0123456789abcdef0123456789abcdef",
        ),
        (
            "DURABLE_ACTORS_REPLICA_PLACEMENTS",
            r#"["us-west4-a","us-west4-a","us-west4-a"]"#,
        ),
        (
            "DURABLE_ACTORS_GKE_ZONES",
            r#"{"north-america-west":"us-west4-a"}"#,
        ),
        ("DURABLE_ACTORS_PUBLIC_URL", "https://actors.example.com"),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL",
            "https://objects.example.com",
        ),
        (
            "DURABLE_ACTORS_POSTGRES_URL",
            "postgresql://localhost/actors",
        ),
    ])
}

#[test]
fn analytics_retention_is_configurable_and_bounded() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert_eq!(
        parse(&values)?.storage.trace_retention,
        Duration::from_secs(30 * 86400)
    );
    values.insert("DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS", "7");
    assert_eq!(
        parse(&values)?.storage.trace_retention,
        Duration::from_secs(7 * 86400)
    );
    for invalid in ["0", "-1", "1.5", "3651", ""] {
        values.insert("DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS", invalid);
        assert!(parse(&values).is_err());
    }
    Ok(())
}

#[test]
fn default_region_requires_a_configured_compute_zone() -> Result<()> {
    let mut values = process_environment();
    let parse = |values: &HashMap<&str, &str>| {
        ControlPlaneProcessConfig::from_lookup(|name| values.get(name).map(|value| (*value).into()))
    };
    assert_eq!(
        parse(&values)?.region.as_deref(),
        Some("north-america-west")
    );
    values.insert("DURABLE_ACTORS_REGION", "north-america-east");
    assert!(parse(&values).is_err());
    values.remove("DURABLE_ACTORS_REGION");
    values.insert(
        "DURABLE_ACTORS_GKE_ZONES",
        r#"{"north-america-west":"us-west4-b"}"#,
    );
    assert!(parse(&values).is_ok());
    Ok(())
}
