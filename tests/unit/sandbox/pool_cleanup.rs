use super::*;
use crate::{postgres::testing::with_postgres, sandbox::*};

struct Provider {
    fail: std::sync::atomic::AtomicBool,
    pause: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

#[async_trait::async_trait]
impl SandboxProvider for Provider {
    async fn stopped_spares(&self, spares: &[SpareHandle]) -> Result<Vec<String>> {
        anyhow::ensure!(
            !self.fail.load(std::sync::atomic::Ordering::SeqCst),
            "API unavailable"
        );
        let pause = self.pause.lock().unwrap().take();
        if let Some((observed, resume)) = pause {
            let _ = observed.send(());
            resume.await?;
        }
        Ok(spares
            .iter()
            .filter(|spare| spare.name == "do-actor-done" || spare.name == "do-actor-evicted")
            .map(|spare| spare.resource_id.clone())
            .collect())
    }
    async fn build_code(&self, _: &BuildCodeRequest) -> Result<BuiltActorCode> {
        anyhow::bail!("unexpected build")
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("unexpected assignment")
    }
    async fn socket_credentials(&self, _: &SocketCredentialsRequest) -> Result<SocketCredentials> {
        anyhow::bail!("unexpected credentials")
    }
    async fn terminate_hosts(&self, _: &TerminateHostsRequest) -> Result<HostTermination> {
        anyhow::bail!("unexpected termination")
    }
}

#[tokio::test]
async fn completed_hosts_are_removed_but_live_hosts_and_uncertain_observations_are_retained()
-> Result<()> {
    with_postgres(async |fixture| {
        let provider = Arc::new(Provider {
            fail: true.into(),
            pause: None.into(),
        });
        let pool = SparePool::new(
            PostgresDatabase::connect(&fixture.url).await?,
            provider.clone(),
            PoolConfig {
                control_plane_url: None,
                kind: SpareKind::Actor,
                idle: 0,
                fleet_maximum: 0,
                max_starting: 1,
                idle_ttl_seconds: 600,
                regions: vec![],
                resources: ResourceLimits::default(),
            },
        );
        for name in ["done", "live"] {
            pool.reserve_host(name, name, "revision").await?;
            let spare = SpareHandle {
                name: format!("do-actor-{name}"),
                resource_id: format!("sandboxes/do-actor-{name}/{name}-uid"),
                route: "http://host:7101".into(),
                canonical_region: "region".into(),
                control_route: "http://host:7102".into(),
                control_token: "token".into(),
            };
            pool.remember(name, "revision", &spare).await?;
        }
        pool.reserve_host("starting", "starting", "revision")
            .await?;
        assert!(pool.forget_stopped().await.is_err());
        assert!(pool.host("done").await?.is_some());
        assert!(pool.host("live").await?.is_some());
        provider
            .fail
            .store(false, std::sync::atomic::Ordering::SeqCst);
        let stop = CancellationToken::new();
        pool.start(
            Arc::new(crate::control_plane::admin::LocalAdminRegistry::default()),
            stop.clone(),
        );
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            while pool.host("done").await?.is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            anyhow::Ok(())
        })
        .await;
        stop.cancel();
        result??;
        pool.forget_stopped().await?;
        assert!(pool.host("done").await?.is_none());
        assert!(pool.host("live").await?.is_some());
        assert_eq!(
            pool.store
                .0
                .query_one("SELECT count(*) FROM durable_actors_spares", &[])
                .await?
                .get::<_, i64>(0),
            2
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn stale_cleanup_observation_cannot_remove_a_replacement_identity() -> Result<()> {
    with_postgres(async |fixture| {
        let (observed, captured) = tokio::sync::oneshot::channel();
        let (resume, paused) = tokio::sync::oneshot::channel();
        let provider = Arc::new(Provider {
            fail: false.into(),
            pause: Some((observed, paused)).into(),
        });
        let pool = SparePool::new(
            PostgresDatabase::connect(&fixture.url).await?,
            provider,
            PoolConfig {
                control_plane_url: None,
                kind: SpareKind::Actor,
                idle: 0,
                fleet_maximum: 0,
                max_starting: 1,
                idle_ttl_seconds: 600,
                regions: vec![],
                resources: ResourceLimits::default(),
            },
        );
        let mut spare = SpareHandle {
            name: "do-actor-done".into(),
            resource_id: "sandboxes/do-actor-done/old-uid".into(),
            route: "http://host:7101".into(),
            canonical_region: "region".into(),
            control_route: "http://host:7102".into(),
            control_token: "token".into(),
        };
        pool.reserve_host("done", "old", "revision").await?;
        pool.remember("old", "revision", &spare).await?;
        let cleanup = pool.forget_stopped();
        tokio::pin!(cleanup);
        tokio::select! {
            result = &mut cleanup => panic!("cleanup did not wait for observation: {result:?}"),
            result = captured => result?,
        }
        pool.store
            .0
            .execute(
                "DELETE FROM durable_actors_spares WHERE host_id = 'old'",
                &[],
            )
            .await?;
        pool.reserve_host("done", "new", "revision").await?;
        spare.resource_id = "sandboxes/do-actor-done/new-uid".into();
        pool.remember("new", "revision", &spare).await?;
        let _ = resume.send(());
        cleanup.await?;
        assert_eq!(pool.host("new").await?, Some(spare));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn reconciliation_excludes_evicted_spares_from_subsequent_claims() -> Result<()> {
    with_postgres(async |fixture| {
        let provider = Arc::new(Provider { fail: false.into(), pause: None.into() });
        let pool = SparePool::new(PostgresDatabase::connect(&fixture.url).await?, provider, PoolConfig {
            control_plane_url: None, kind: SpareKind::Actor, idle: 2, fleet_maximum: 4,
            max_starting: 2, idle_ttl_seconds: 600, regions: vec![], resources: ResourceLimits::default(),
        });
        for name in ["evicted", "warm"] {
            let spare = SpareHandle {
                name: format!("do-actor-{name}"),
                resource_id: format!("sandboxes/do-actor-{name}/{name}-uid"),
                route: "http://host:7101".into(),
                canonical_region: "region".into(),
                control_route: "http://host:7102".into(),
                control_token: "token".into(),
            };
            pool.store.0.execute("INSERT INTO durable_actors_spares (name, pool_key, status, handle, expires_at) VALUES ($1, 'warm', 'ready', $2, clock_timestamp() + interval '10 minutes')", &[&spare.name, &serde_json::to_string(&spare)?]).await?;
        }
        pool.forget_stopped().await?;
        assert_eq!(pool.store.claim("warm", "next", "revision").await?.unwrap().name, "do-actor-warm");
        assert!(pool.store.claim("warm", "another", "revision").await?.is_none());
        Ok(())
    }).await
}
