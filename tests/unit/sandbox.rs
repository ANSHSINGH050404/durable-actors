use super::*;

const PROCESS_DEADLINE: Duration = Duration::from_secs(30);

#[test]
fn decodes_provider_provisioning_timings() {
    let handle: ActorHostHandle = serde_json::from_value(serde_json::json!({
        "hostId": "host.v3.revision.session",
        "route": "https://host.example.com",
        "canonicalRegion": "north-america-east",
        "provisioning": {
            "provider": "modal",
            "resourceId": "sb-actor",
            "reused": false,
            "startedAtMs": 0,
            "resourcesResolvedAtMs": 12,
            "existingHostCheckedAtMs": 34,
            "sandboxScheduledAtMs": 56,
            "hostReadyObservedAtMs": 123,
            "routeReadAtMs": 125,
            "metadataWrittenAtMs": 129,
            "completedAtMs": 130
        }
    }))
    .expect("actor host handle");

    let provisioning = handle.provisioning.expect("provisioning timings");
    assert_eq!(provisioning.resource_id, "sb-actor");
    assert_eq!(provisioning.sandbox_scheduled_at_ms, Some(56));
    assert_eq!(provisioning.completed_at_ms, 130);
}

#[test]
fn rejects_ambiguous_command_configuration() {
    assert!(CommandSandboxProvider::new("".into(), "modal".into(), HashMap::new()).is_err());
    assert!(CommandSandboxProvider::new("modal".into(), "".into(), HashMap::new()).is_err());
}

#[tokio::test]
async fn provider_calls_share_a_process_without_serializing() -> Result<()> {
    let (directory, provider) = test_provider()?;
    let requests = (0..5)
        .map(|index| {
            serde_json::json!({
                "index": index,
                "barrier": directory.path(),
            })
        })
        .collect::<Vec<_>>();
    let replies = tokio::time::timeout(
        PROCESS_DEADLINE,
        futures_util::future::try_join_all(
            requests
                .iter()
                .map(|request| provider.execute::<_, serde_json::Value>("test", request)),
        ),
    )
    .await
    .expect("all five commands must start before any replies")?;
    let pids = replies
        .iter()
        .map(|reply| reply["pid"].as_u64().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(pids.len(), 1);
    for (index, reply) in replies.iter().enumerate() {
        assert_eq!(reply["index"], index);
    }
    Ok(())
}

#[tokio::test]
async fn provider_failures_do_not_affect_other_calls() -> Result<()> {
    let (_directory, provider) = test_provider()?;
    for (request, message) in [
        (serde_json::json!({"fail": true}), "test failure"),
        (serde_json::json!({"oversized": true}), "response exceeds"),
        (
            serde_json::json!({"malformed": true}),
            "decode provider response",
        ),
    ] {
        let healthy_request = serde_json::json!({"index": 42});
        let (failed, healthy) = tokio::join!(
            provider.execute::<_, serde_json::Value>("test", &request),
            provider.execute::<_, serde_json::Value>("test", &healthy_request),
        );
        assert!(failed.unwrap_err().to_string().contains(message));
        assert_eq!(healthy?["index"], 42);
    }
    Ok(())
}

#[tokio::test]
async fn cancelling_one_provider_call_preserves_the_worker() -> Result<()> {
    let (directory, provider) = test_provider()?;
    let marker = directory.path().join("cancelled.pid");
    let request = serde_json::json!({"marker": marker});
    let mut cancelled = Box::pin(provider.execute::<_, serde_json::Value>("test", &request));
    let wait_for_start = async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        result = &mut cancelled => panic!("provider should still be waiting: {result:?}"),
        started = tokio::time::timeout(PROCESS_DEADLINE, wait_for_start) => started?,
    }
    let pid: u32 = std::fs::read_to_string(marker)?.parse()?;
    drop(cancelled);
    let healthy: serde_json::Value = provider
        .execute("test", &serde_json::json!({"index": 42}))
        .await?;
    assert_eq!(healthy["index"], 42);
    assert_eq!(healthy["pid"], pid);
    tokio::time::timeout(PROCESS_DEADLINE, async {
        while !directory.path().join("cancelled.pid.cancelled").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn failed_worker_is_replaced_without_replaying_the_command() -> Result<()> {
    let (directory, provider) = test_provider()?;
    let warm: serde_json::Value = provider.execute("test", &serde_json::json!({})).await?;
    let attempts = directory.path().join("attempts");
    let failed = provider
        .execute::<_, serde_json::Value>(
            "test",
            &serde_json::json!({"disconnect": true, "attempts": attempts}),
        )
        .await;
    assert!(
        failed
            .unwrap_err()
            .to_string()
            .contains("outcome may be unknown")
    );
    let next: serde_json::Value = provider
        .execute("test", &serde_json::json!({"index": 42}))
        .await?;
    assert_eq!(next["index"], 42);
    assert_ne!(next["pid"], warm["pid"]);
    assert_eq!(std::fs::read_to_string(attempts)?, "attempt\n");
    Ok(())
}

#[tokio::test]
async fn dropping_provider_stops_its_worker() -> Result<()> {
    let (_directory, provider) = test_provider()?;
    let reply: serde_json::Value = provider.execute("test", &serde_json::json!({})).await?;
    drop(provider);
    wait_for_exit(reply["pid"].as_u64().unwrap() as u32).await
}

async fn wait_for_exit(pid: u32) -> Result<()> {
    tokio::time::timeout(PROCESS_DEADLINE, async {
        while tokio::process::Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status()
            .await?
            .success()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    Ok(())
}

fn test_provider() -> Result<(tempfile::TempDir, CommandSandboxProvider)> {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("custom-provider");
    std::fs::write(&path, include_str!("../fixtures/sandbox-provider.cjs"))?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))?;
    let provider =
        CommandSandboxProvider::new("modal".into(), path.display().to_string(), HashMap::new())?;
    Ok((directory, provider))
}

#[tokio::test]
async fn actor_builds_outlive_ordinary_provider_requests_but_remain_bounded() -> Result<()> {
    let (directory, provider) = test_provider()?;
    let marker = directory.path().join("building.pid");
    let request = serde_json::json!({"marker": marker});
    let mut build = Box::pin(provider.execute::<_, serde_json::Value>("build_code", &request));
    let started = async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        result = &mut build => panic!("build should still be running: {result:?}"),
        result = tokio::time::timeout(PROCESS_DEADLINE, started) => result?,
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(180)).await;
    assert!(futures_util::poll!(&mut build).is_pending());
    tokio::time::advance(Duration::from_secs(600)).await;
    assert!(build.await.unwrap_err().to_string().contains("timed out"));
    Ok(())
}
