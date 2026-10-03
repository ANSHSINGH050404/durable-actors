use super::*;
use crate::{
    actor::ActorKey, host_leases::HostLease, placement::testing::LocalObjectPlacementStore,
};
use crate::{bucket::testing::RuntimeFixture, host::HostId};
use tokio_util::task::AbortOnDropHandle;

const DEADLINE: Duration = Duration::from_secs(5);
// Process startup can be delayed by other tests on shared CI runners.
const STARTUP_DEADLINE: Duration = Duration::from_secs(30);

#[tokio::test]
async fn independent_actors_start_before_either_becomes_ready() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let first = fixture.request("first");
    let second = fixture.request("second");
    let a = fixture.start(&first);
    let b = fixture.start(&second);
    fixture.started(&first).await?;
    fixture.started(&second).await?;
    fixture.release(&first)?;
    fixture.release(&second)?;
    assert_eq!(a.await??.host_id, first.host_id);
    assert_eq!(b.await??.host_id, second.host_id);
    fixture.provider.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn warm_host_lookups_do_not_wait_for_another_startup() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let warm = fixture.request("warm");
    fixture.release(&warm)?;
    fixture.start(&warm).await??;
    let cold = fixture.request("cold");
    let starting = fixture.start(&cold);
    fixture.started(&cold).await?;
    tokio::time::timeout(DEADLINE, fixture.provider.wait_ready(&warm.host_id)).await??;
    fixture.release(&cold)?;
    starting.await??;
    fixture.provider.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn shutdown_cancels_a_host_that_has_not_reported_ready() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let request = fixture.request("pending");
    let starting = fixture.start(&request);
    fixture.started(&request).await?;
    tokio::time::timeout(DEADLINE, fixture.provider.shutdown()).await?;
    assert!(starting.await?.is_err());
    fixture.stopped(&request).await?;
    Ok(())
}

#[tokio::test]
async fn duplicate_requests_share_startup_even_when_the_first_caller_disconnects() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let first = fixture.request("same");
    let a = fixture.start(&first);
    fixture.started(&first).await?;
    let mut second = first.clone();
    second.host_id = HostId::new("host-duplicate");
    second.session_id = "session-duplicate".into();
    let b = fixture.start(&second);
    a.abort();
    assert!(a.await.unwrap_err().is_cancelled());
    fixture.release(&first)?;
    let host = tokio::time::timeout(DEADLINE, b).await???;
    assert_eq!(host.host_id, first.host_id);
    assert!(!fixture.marker(&second, "started").exists());
    fixture.provider.shutdown().await;
    fixture.stopped(&first).await?;
    Ok(())
}

#[tokio::test]
async fn pending_actor_starts_do_not_block_warm_host_reuse() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let warm = fixture.request("warm");
    fixture.release(&warm)?;
    fixture.start(&warm).await??;
    let first = fixture.request("first");
    let second = fixture.request("second");
    let a = fixture.start(&first);
    let b = fixture.start(&second);
    fixture.started(&first).await?;
    fixture.started(&second).await?;
    let reused = tokio::time::timeout(DEADLINE, fixture.start(&warm)).await???;
    assert_eq!(reused.host_id, warm.host_id);
    fixture.release(&first)?;
    fixture.release(&second)?;
    assert_eq!(a.await??.host_id, first.host_id);
    assert_eq!(b.await??.host_id, second.host_id);
    fixture.provider.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn failed_startups_release_the_reservation_for_retry() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let request = fixture.request("failure");
    std::fs::write(fixture.marker(&request, "fail"), "")?;
    let error = fixture.start(&request).await?.unwrap_err();
    assert!(error.to_string().contains("exited"), "{error:#}");
    fixture.stopped(&request).await?;
    std::fs::remove_file(fixture.marker(&request, "fail"))?;
    fixture.release(&request)?;
    assert_eq!(fixture.start(&request).await??.host_id, request.host_id);
    fixture.provider.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn a_host_with_an_expired_ownership_lease_is_stopped_before_replacement() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let old = fixture.request("expired");
    fixture.release(&old)?;
    let mut lease = fixture.start(&old).await??.lease.unwrap();
    lease.expires_at_ms = 0;
    fixture.placements.set_owner(
        &old.actor.as_ref().unwrap().storage_key(),
        lease,
        &old.canonical_region,
    )?;
    let mut replacement = old.clone();
    replacement.host_id = HostId::new("host-fresh");
    replacement.session_id = "session-fresh".into();
    let starting = fixture.start(&replacement);
    fixture.started(&replacement).await?;
    fixture.stopped(&old).await?;
    fixture.release(&replacement)?;
    assert_eq!(starting.await??.host_id, replacement.host_id);
    fixture.provider.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn an_exited_ready_host_can_be_started_again() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let old = fixture.request("exiting");
    fixture.release(&old)?;
    fixture.start(&old).await??;
    std::fs::write(fixture.marker(&old, "fail"), "")?;
    fixture.stopped(&old).await?;
    tokio::time::timeout(DEADLINE, async {
        while fixture
            .provider
            .runtime
            .store
            .host(old.host_id.as_str())
            .await?
            .unwrap()
            .status
            != "failed"
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        anyhow::Ok(())
    })
    .await??;
    let mut replacement = old.clone();
    replacement.host_id = HostId::new("host-restarted");
    replacement.session_id = "session-restarted".into();
    fixture.release(&replacement)?;
    assert_eq!(
        fixture.start(&replacement).await??.host_id,
        replacement.host_id
    );
    fixture.provider.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn deployment_retirement_cancels_pending_hosts_and_allows_a_replacement() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let pending = fixture.request("pending");
    let starting = fixture.start(&pending);
    fixture.started(&pending).await?;
    let mut other = fixture.request("other");
    other.host_config_key = "other-config".into();
    fixture.release(&other)?;
    fixture.start(&other).await??;
    let retired = tokio::time::timeout(
        DEADLINE,
        fixture.provider.terminate_hosts(&TerminateHostsRequest {
            host_config_key: pending.host_config_key.clone(),
            canonical_regions: vec![pending.canonical_region.clone()],
        }),
    )
    .await??;
    assert_eq!(retired.resource_ids, vec![pending.host_id.as_str()]);
    assert!(starting.await?.is_err());
    fixture.stopped(&pending).await?;
    fixture.provider.wait_ready(&other.host_id).await?;
    let mut replacement = pending.clone();
    assert!(
        tokio::time::timeout(DEADLINE, fixture.provider.ensure_host(&pending))
            .await?
            .is_err()
    );
    replacement.host_config_key = "replacement-config".into();
    replacement.host_id = HostId::new("host-replacement");
    replacement.session_id = "replacement".into();
    fixture.release(&replacement)?;
    assert_eq!(
        fixture.start(&replacement).await??.host_id,
        replacement.host_id
    );
    fixture.provider.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn shutdown_reaps_every_host_in_a_pending_burst() -> Result<()> {
    let fixture = LocalFixture::new().await?;
    let requests: Vec<_> = (0..6)
        .map(|i| fixture.request(&format!("host-{i}")))
        .collect();
    let starting: Vec<_> = requests
        .iter()
        .map(|request| fixture.start(request))
        .collect();
    for request in &requests {
        fixture.started(request).await?;
    }
    tokio::time::timeout(DEADLINE, fixture.provider.shutdown()).await?;
    for task in starting {
        assert!(task.await?.is_err());
    }
    for request in &requests {
        fixture.stopped(request).await?;
    }
    assert!(
        fixture
            .provider
            .ensure_host(&fixture.request("late"))
            .await
            .is_err()
    );
    Ok(())
}

struct LocalFixture {
    provider: Arc<LocalSandboxProvider>,
    placements: Arc<LocalObjectPlacementStore>,
    directory: TempDir,
}

impl LocalFixture {
    async fn new() -> Result<Self> {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir()?;
        let executable = directory.path().join("host");
        std::fs::write(&executable, include_str!("fixtures/local-host.cjs"))?;
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o755))?;
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let provider = Arc::new(
            LocalSandboxProvider::new(
                executable,
                directory.path().into(),
                placements.clone(),
                None,
                directory.path().join("hosts.sqlite3"),
            )
            .await?,
        );
        Ok(Self {
            provider,
            placements,
            directory,
        })
    }

    fn request(&self, id: &str) -> EnsureHostRequest {
        EnsureHostRequest {
            actor_is_new: true,
            owner_hint: None,
            actor: Some(ActorKey {
                project_id: "test".into(),
                actor_name: "Counter".into(),
                actor_id: id.into(),
            }),
            code_snapshot: None,
            spare: None,
            resources: Default::default(),
            runtime_config: None,
            host_config_key: "local".into(),
            canonical_region: "north-america-east".into(),
            host_id: HostId::new(format!("host-{id}")),
            session_id: format!("session-{id}"),
            host_token: "unused".into(),
            jwt_public_keys: "unused".into(),
            control_plane_url: "http://127.0.0.1:7100".into(),
            jwt_issuer: "local".into(),
            invocation_jwt_audience: "local".into(),
            image_ref: "local".into(),
            working_directory: self.directory.path().display().to_string(),
            actor_entrypoint: None,
            secret_refs: vec![],
            host_idle_timeout_ms: 300_000,
        }
    }

    fn start(&self, request: &EnsureHostRequest) -> AbortOnDropHandle<Result<ActorHostHandle>> {
        let provider = self.provider.clone();
        let request = request.clone();
        AbortOnDropHandle::new(tokio::spawn(
            async move { provider.ensure_host(&request).await },
        ))
    }

    fn release(&self, request: &EnsureHostRequest) -> Result<()> {
        let lease = HostLease {
            id: request.host_id.clone(),
            session_id: request.session_id.clone(),
            route: "http://127.0.0.1:7101".into(),
            expires_at_ms: SystemClock.now_ms()? + 60_000,
        };
        self.placements.set_owner(
            &request.actor.as_ref().unwrap().storage_key(),
            lease.clone(),
            &request.canonical_region,
        )?;
        let mut release =
            atomic_write_file::AtomicWriteFile::open(self.marker(request, "release"))?;
        serde_json::to_writer(
            &mut release,
            &serde_json::json!({ "ownerEpoch": 1, "lease": lease }),
        )?;
        release.commit()?;
        Ok(())
    }

    async fn started(&self, request: &EnsureHostRequest) -> Result<()> {
        self.wait_marker(request, "started", STARTUP_DEADLINE).await
    }

    async fn stopped(&self, request: &EnsureHostRequest) -> Result<()> {
        self.wait_marker(request, "stopped", DEADLINE).await
    }

    async fn wait_marker(
        &self,
        request: &EnsureHostRequest,
        suffix: &str,
        deadline: Duration,
    ) -> Result<()> {
        tokio::time::timeout(deadline, async {
            while !self.marker(request, suffix).exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .with_context(|| format!("{} never {suffix}", request.host_id))
    }

    fn marker(&self, request: &EnsureHostRequest, suffix: &str) -> PathBuf {
        self.directory
            .path()
            .join(format!("{}.{suffix}", request.host_id))
    }
}

#[tokio::test]
async fn shutdown_rejects_new_hosts_before_starting_a_process() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let project = directory.path().to_path_buf();
    let fixture = RuntimeFixture::new()?;
    let provider = LocalSandboxProvider::new(
        project.join("unused-executable"),
        project.clone(),
        fixture.runtime,
        None,
        project.join("hosts.sqlite3"),
    )
    .await?;
    provider.shutdown().await;
    let request = EnsureHostRequest {
        actor_is_new: true,
        owner_hint: Some("{\"generation\":17,\"record\":{\"epoch\":3}}".into()),
        actor: None,
        code_snapshot: None,
        spare: None,
        resources: Default::default(),
        runtime_config: None,

        host_config_key: "local".into(),
        canonical_region: "north-america-east".into(),
        host_id: HostId::new("host"),
        session_id: "session".into(),
        host_token: "unused".into(),
        jwt_public_keys: "unused".into(),
        control_plane_url: "http://127.0.0.1:7100".into(),
        jwt_issuer: "local".into(),
        invocation_jwt_audience: "local".into(),
        image_ref: "local".into(),
        working_directory: project.display().to_string(),
        actor_entrypoint: None,
        secret_refs: vec![],
        host_idle_timeout_ms: 300_000,
    };
    assert_eq!(
        host_environment(&request, &directory).get("DURABLE_ACTORS_LOG_MODE"),
        Some(&"development".to_owned())
    );
    assert_eq!(
        host_environment(&request, &directory).get("DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS"),
        Some(&"300000".to_owned())
    );
    assert_eq!(
        host_environment(&request, &directory).get("DURABLE_ACTORS_OWNER_HINT"),
        request.owner_hint.as_ref()
    );
    let error = provider.ensure_host(&request).await.unwrap_err();
    assert!(error.to_string().contains("shutting down"), "{error:#}");
    Ok(())
}
