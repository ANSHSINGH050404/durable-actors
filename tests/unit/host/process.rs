use super::*;
use std::collections::HashMap;

#[test]
fn host_needs_no_local_state_directory() -> Result<()> {
    let values = values();
    let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
    assert_eq!(
        config.executor_socket,
        PathBuf::from("/tmp/durable-actors-executor.sock")
    );
    assert_eq!(config.host_idle_timeout, Duration::from_secs(10));
    assert_eq!(config.jwt_max_lifetime, Duration::from_secs(86_400));
    Ok(())
}

#[tokio::test]
async fn host_publishes_complete_metadata_before_dependencies_are_ready() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("host.json");
    let mut values = values();
    values.insert("DURABLE_ACTORS_HOST_BIND".into(), "127.0.0.1:0".into());
    values.insert(
        "DURABLE_ACTORS_HOST_METADATA_FILE".into(),
        path.display().to_string(),
    );
    values.insert("DURABLE_ACTORS_REGION".into(), "north-america-east".into());
    values.insert(
        "DURABLE_ACTORS_HOST_ROUTE".into(),
        "https://host.example.com".into(),
    );
    let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
    let (_, route, _) = bind_host_listener(&config, None).await?;
    let metadata: serde_json::Value = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
    assert_eq!(
        metadata,
        serde_json::json!({
                "hostId": config.host_id,
        "sessionId": config.session_id,
                "route": route,
                "canonicalRegion": "north-america-east",
            })
    );
    let handle: crate::sandbox::ActorHostHandle = serde_json::from_value(metadata)?;
    assert_eq!(handle.host_id, config.host_id);
    assert_eq!(std::fs::read_dir(directory.path())?.count(), 1);
    Ok(())
}

#[tokio::test]
async fn metadata_publication_failure_prevents_host_readiness() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut values = values();
    values.insert(
        "DURABLE_ACTORS_HOST_METADATA_FILE".into(),
        directory
            .path()
            .join("missing/host.json")
            .display()
            .to_string(),
    );
    values.insert("DURABLE_ACTORS_REGION".into(), "north-america-east".into());
    let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
    assert!(bind_host_listener(&config, None).await.is_err());
    Ok(())
}

#[test]
fn host_metadata_requires_a_valid_region() {
    for region in [None, Some(""), Some("bad/region")] {
        let mut values = values();
        values.insert(
            "DURABLE_ACTORS_HOST_METADATA_FILE".into(),
            "/tmp/host.json".into(),
        );
        if let Some(region) = region {
            values.insert("DURABLE_ACTORS_REGION".into(), region.into());
        }
        assert!(ActorHostConfig::from_lookup(|name| values.get(name).cloned()).is_err());
    }
}

#[test]
fn startup_timings_begin_with_only_configuration_loaded() {
    let values = values();
    let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned()).unwrap();
    let timings = HostStartupTimings::new(&config);

    assert!(timings.configuration_loaded_at_ms <= timings.elapsed_ms());
    assert!(timings.javascript_spawned_at_ms.is_none());
    assert!(timings.executor_notified_at_ms.is_none());
}

#[tokio::test]
async fn closing_the_last_socket_after_idle_does_not_restart_the_timer() -> Result<()> {
    assert_activity_prevents_idle_shutdown(true).await
}

#[tokio::test]
async fn admitted_requests_prevent_idle_host_shutdown() -> Result<()> {
    assert_activity_prevents_idle_shutdown(false).await
}

async fn assert_activity_prevents_idle_shutdown(socket: bool) -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(std::future::pending::<Result<()>>());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (requests, mut activity) = tokio::sync::watch::channel(ActorActivity {
        active: usize::from(!socket),
        resident: true,
        ..Default::default()
    });
    let evictions = std::cell::Cell::new(0);
    let (_stopped_sender, mut actor_stopped) = tokio::sync::watch::channel(false);
    let (sockets, mut socket_activity) = tokio::sync::watch::channel(usize::from(socket));
    let mut stopped = Box::pin(wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        (&mut activity, &mut socket_activity, &mut actor_stopped),
        Duration::from_millis(100),
        |_| {
            evictions.set(evictions.get() + 1);
            requests.send_modify(|activity| activity.resident = false);
            std::future::ready(Ok(()))
        },
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(150), stopped.as_mut())
            .await
            .is_err()
    );
    assert_eq!(evictions.get(), usize::from(socket));
    if socket {
        sockets.send_replace(0);
    } else {
        requests.send_modify(|activity| {
            activity.active = 0;
            activity.last_active = tokio::time::Instant::now();
        });
    }
    let remaining = if socket {
        Duration::from_millis(50)
    } else {
        Duration::from_secs(1)
    };
    tokio::time::timeout(remaining, stopped.as_mut()).await??;
    drop(stopped);
    javascript.kill().await?;
    Ok(())
}

#[tokio::test]
async fn failed_activation_stops_host_during_idle_eviction() -> Result<()> {
    let mut server = Box::pin(std::future::pending::<Result<()>>());
    let mut executor = Box::pin(std::future::pending::<Result<()>>());
    let mut shutdown = Box::pin(std::future::pending::<()>());
    let mut javascript = tokio::process::Command::new("sleep")
        .arg("60")
        .kill_on_drop(true)
        .spawn()?;
    let (_lease_sender, mut lease) = tokio::sync::watch::channel(false);
    let (_activity_sender, mut activity) = tokio::sync::watch::channel(ActorActivity {
        resident: true,
        ..Default::default()
    });
    let (stopped_sender, mut actor_stopped) = tokio::sync::watch::channel(false);
    let (_sockets, mut socket_activity) = tokio::sync::watch::channel(1);
    let evicting = std::cell::Cell::new(false);
    let mut stopped = Box::pin(wait_for_host_stop(
        server.as_mut(),
        executor.as_mut(),
        &mut javascript,
        shutdown.as_mut(),
        &mut lease,
        (&mut activity, &mut socket_activity, &mut actor_stopped),
        Duration::from_millis(10),
        |_| {
            evicting.set(true);
            std::future::pending::<Result<()>>()
        },
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), stopped.as_mut())
            .await
            .is_err()
    );
    assert!(evicting.get());
    stopped_sender.send_replace(true);
    let error = tokio::time::timeout(Duration::from_secs(1), stopped.as_mut())
        .await?
        .unwrap_err();
    assert!(error.to_string().contains("activation stopped"));
    drop(stopped);
    javascript.kill().await?;
    Ok(())
}

fn values() -> HashMap<String, String> {
    HashMap::from([
        (
            "DURABLE_ACTORS_RUNTIME_CONFIG".into(),
            serde_json::json!({
                "bucket": {"type":"file", "directory":"/tmp/actor-test-bucket"},
                "persistence":{"type":"local"}, "region":"north-america-east",
                "token":null
            })
            .to_string(),
        ),
        (
            "DURABLE_ACTORS_CONTROL_PLANE_URL".into(),
            "http://127.0.0.1:7100".into(),
        ),
        ("DURABLE_ACTORS_HOST_TOKEN".into(), "host-jwt".into()),
        ("DURABLE_ACTORS_JWT_PUBLIC_KEYS".into(), "{}".into()),
        (
            "DURABLE_ACTORS_HOST_ID".into(),
            "host.v3.revision-1.host-1".into(),
        ),
        (
            "DURABLE_ACTORS_SESSION_ID".into(),
            "00000000-0000-4000-8000-000000000001".into(),
        ),
    ])
}

#[test]
fn host_decodes_pinned_code_artifact_before_assignment() -> Result<()> {
    let manifest = crate::artifacts::ArtifactManifest {
        bucket: "customer-code".into(),
        files: vec![crate::artifacts::ArtifactFile {
            path: "actors.mjs".into(),
            object: "artifacts/test".into(),
            generation: 7,
            sha256: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
        }],
    };
    let mut values = values();
    values.insert("DURABLE_ACTORS_CODE_ARTIFACT".into(), manifest.encode()?);
    let config = ActorHostConfig::from_lookup(|name| values.get(name).cloned())?;
    assert_eq!(config.artifact.unwrap().files[0].generation, 7);
    values.insert("DURABLE_ACTORS_CODE_ARTIFACT".into(), "invalid".into());
    assert!(ActorHostConfig::from_lookup(|name| values.get(name).cloned()).is_err());
    Ok(())
}
