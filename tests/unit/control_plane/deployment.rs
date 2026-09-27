use super::*;
use crate::{
    control_plane::{ActorTokenPurpose, admin::LocalAdminRegistry},
    placement::testing::LocalObjectPlacementStore,
    sandbox::{
        ActorHostHandle, BuildCodeRequest, BuiltActorCode, SocketCredentials,
        SocketCredentialsRequest,
    },
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

#[tokio::test]
async fn openapi_is_available_without_credentials_or_a_deployment() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}/openapi.yaml", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let response = reqwest::get(url).await?;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/yaml");
    let expected = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/docs/reference/openapi.yaml"
    ))?;
    assert_eq!(response.text().await?, expected);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn source_deployment_builds_once_and_preserves_code_on_secret_updates_and_failures()
-> Result<()> {
    let (service, admin, provider) = fixture()?;
    let routes = super::super::public_api::router(service, admin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/v1/projects/default/deployment",
        listener.local_addr()?
    );
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let source = serde_json::json!({"imageRef":"im-customer", "workingDirectory":"/project", "actorEntrypoint":"src/actors.ts", "secretRefs":[]});
    for changed in [true, true] {
        let reply: serde_json::Value = client
            .put(&url)
            .bearer_auth("api-key")
            .json(&source)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply["changed"], changed);
    }
    assert_eq!(provider.builds.lock().unwrap().len(), 1);
    let compiled = admin.current_deployment("default").await?.unwrap();
    assert_eq!(compiled.image_ref, "im-runtime");
    assert_eq!(compiled.code_snapshot.as_deref(), Some("im-code-1"));
    assert_eq!(compiled.actor_entrypoint.as_deref(), Some("actors.mjs"));
    assert_eq!(compiled.working_directory, "/customer");
    assert_eq!(compiled.source.as_ref().unwrap().image_ref, "im-customer");
    let mut roundtrip: serde_json::Value = client
        .get(&url)
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(roundtrip, source);
    roundtrip["secretRefs"] = serde_json::json!(["replacement-secrets"]);
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&roundtrip)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(provider.builds.lock().unwrap().len(), 1);
    let active = admin.current_deployment("default").await?.unwrap();
    assert_eq!(active.code_snapshot, compiled.code_snapshot);
    assert_ne!(active.host_config_key(), compiled.host_config_key());
    assert_eq!(
        admin
            .deployment_contract("default")
            .await?
            .unwrap()
            .contract,
        contract()
    );
    assert_eq!(
        provider.retired.lock().unwrap().as_slice(),
        &[compiled.host_config_key(), compiled.host_config_key()]
    );

    provider.fail.store(true, Ordering::SeqCst);
    roundtrip["imageRef"] = "im-broken".into();
    let failed = client
        .put(&url)
        .bearer_auth("api-key")
        .json(&roundtrip)
        .send()
        .await?;
    assert_eq!(failed.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(
        admin.current_deployment("default").await?,
        Some(active.clone())
    );
    assert_eq!(provider.retired.lock().unwrap().len(), 2);

    provider.fail.store(false, Ordering::SeqCst);
    roundtrip["imageRef"] = "im-updated".into();
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&roundtrip)
        .send()
        .await?
        .error_for_status()?;
    let updated = admin.current_deployment("default").await?.unwrap();
    assert_ne!(updated.code_snapshot, active.code_snapshot);
    assert_eq!(provider.builds.lock().unwrap().len(), 3);
    assert_eq!(
        provider.retired.lock().unwrap().as_slice(),
        &[
            compiled.host_config_key(),
            compiled.host_config_key(),
            active.host_config_key()
        ]
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn cached_code_cannot_be_registered_with_an_unrelated_contract() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let source = source();
    service.deploy_source(&admin, &source, None).await?;
    let active = admin.current_deployment("default").await?;

    let unrelated = super::super::contracts::PublicActorContract::new(serde_json::from_str(
        include_str!("../../../sdk/tests/fixtures/public-contract.json"),
    )?)?;
    let result = service
        .deploy_source(&admin, &source, Some(&unrelated))
        .await;
    assert!(
        result.is_err(),
        "a supplied contract must match the cached code"
    );
    assert_eq!(admin.current_deployment("default").await?, active);
    assert_eq!(provider.builds.lock().unwrap().len(), 1);
    assert!(provider.retired.lock().unwrap().is_empty());
    service.deploy_source(&admin, &source, None).await?;
    assert_eq!(
        admin
            .deployment_contract("default")
            .await?
            .unwrap()
            .contract,
        contract()
    );
    assert_eq!(provider.builds.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn compiled_deployments_and_cached_contracts_are_scoped_to_the_project() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let mut first = source();
    first.project_id = "team-a".into();
    let mut second = first.clone();
    second.project_id = "team-b".into();
    service.deploy_source(&admin, &first, None).await?;
    service.deploy_source(&admin, &second, None).await?;
    let first_compiled = admin.current_deployment("team-a").await?.unwrap();
    let second_compiled = admin.current_deployment("team-b").await?.unwrap();
    assert_eq!(first_compiled.project_id, "team-a");
    assert_eq!(second_compiled.project_id, "team-b");
    assert_ne!(
        first_compiled.host_config_key(),
        second_compiled.host_config_key()
    );
    assert_eq!(provider.builds.lock().unwrap().len(), 2);

    service.deploy_source(&admin, &first, None).await?;
    assert_eq!(provider.builds.lock().unwrap().len(), 2);
    assert_eq!(
        admin.deployment_contract("team-a").await?.unwrap().contract,
        contract()
    );
    assert_eq!(
        admin.deployment_contract("team-b").await?.unwrap().contract,
        contract()
    );
    service.delete_deployment(&admin, "team-a").await?;
    assert_eq!(
        admin.current_deployment("team-b").await?,
        Some(second_compiled.clone())
    );
    assert!(
        !provider
            .retired
            .lock()
            .unwrap()
            .contains(&second_compiled.host_config_key())
    );
    Ok(())
}

#[tokio::test]
async fn resolved_targets_expire_within_host_idle_lease_and_authorization_limits() -> Result<()> {
    use crate::clock::{Clock, SystemClock};

    for (idle_ms, lease_ms, grant_ms) in [
        (10_000, 30_000, None),
        (25_000, 30_000, Some(60_000)),
        (1, 30_000, Some(60_000)),
        (120_000, 8_000, Some(60_000)),
        (120_000, 60_000, Some(120_000)),
        (30_000, 30_000, Some(7_000)),
    ] {
        let (mut service, admin, _) = fixture_with_idle_timeout(idle_ms)?;
        let spec = source();
        admin.register_deployment(&spec, None).await?;
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        let before = SystemClock.now_ms()?;
        let lease = HostLease {
            id: HostId::new(format!("host.v3.{}.one", spec.host_config_key())),
            session_id: uuid::Uuid::new_v4().to_string(),
            route: "https://host.example.com".into(),
            expires_at_ms: before + lease_ms,
        };
        let placements = Arc::new(LocalObjectPlacementStore::default());
        placements.set_owner(&actor.storage_key(), lease.clone(), "north-america-west")?;
        service.placements = placements;
        let grant = grant_ms.map(|grant_ms| super::super::session::InvocationGrant {
            subject: "subject".into(),
            grant_id: "grant".into(),
            expires_at: ((before + grant_ms) / 1_000) as i64,
            methods: vec!["increment".into()],
        });
        let target = service
            .resolve_actor_route(&actor, None, None, grant.clone())
            .await?;
        let after = SystemClock.now_ms()?;
        let expiry = u64::try_from(target.expires_at_ms)?;
        let authorization_expiry = grant.map_or(u64::MAX, |grant| grant.expires_at as u64 * 1_000);
        assert!(
            expiry <= after + idle_ms,
            "target outlives host idle timeout: {idle_ms}ms"
        );
        assert!(expiry <= lease.expires_at_ms, "target outlives host lease");
        assert!(
            expiry <= authorization_expiry,
            "target outlives authorization"
        );
        assert!(
            expiry <= (after / 1_000 + 60) * 1_000,
            "target outlives credential"
        );
        assert!(
            expiry
                >= (before + idle_ms)
                    .min(lease.expires_at_ms)
                    .min(authorization_expiry)
                    .min((before / 1_000 + 60) * 1_000)
        );
    }
    Ok(())
}

fn fixture() -> Result<(ControlPlaneService, AdminService, Arc<BuildProvider>)> {
    fixture_with_idle_timeout(60_000)
}

fn fixture_with_idle_timeout(
    host_idle_timeout_ms: u64,
) -> Result<(ControlPlaneService, AdminService, Arc<BuildProvider>)> {
    let issuer = super::tests::test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        std::time::Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    let provider = Arc::new(BuildProvider::default());
    let provisioner = Arc::new(SandboxHostProvisioner::new(
        provider.clone(),
        HostSandboxRuntimeConfig {
            control_plane_url: "http://control".into(),
            jwt_issuer: "issuer".into(),
            invocation_jwt_audience: "invocation".into(),
            host_idle_timeout_ms,
        },
        issuer.clone(),
        Some("im-runtime".into()),
    ));
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        provisioner,
    );
    Ok((service, admin, provider))
}

fn source() -> HostLaunchSpec {
    HostLaunchSpec {
        project_id: "default".into(),
        source: None,
        image_ref: "im-customer".into(),
        code_snapshot: None,
        working_directory: "/project".into(),
        actor_entrypoint: Some("src/actors.ts".into()),
        secret_refs: vec![],
    }
}

fn contract() -> serde_json::Value {
    serde_json::json!({"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}})
}

#[derive(Default)]
struct BuildProvider {
    builds: Mutex<Vec<serde_json::Value>>,
    retired: Mutex<Vec<String>>,
    fail: AtomicBool,
}

#[async_trait]
impl SandboxProvider for BuildProvider {
    async fn build_code(&self, request: &BuildCodeRequest) -> Result<BuiltActorCode> {
        let mut builds = self.builds.lock().unwrap();
        builds.push(serde_json::to_value(request)?);
        ensure!(!self.fail.load(Ordering::SeqCst), "compilation failed");
        Ok(BuiltActorCode {
            code_snapshot: format!("im-code-{}", builds.len()),
            contract: contract(),
        })
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("no actor invocation in deployment test")
    }
    async fn socket_credentials(&self, _: &SocketCredentialsRequest) -> Result<SocketCredentials> {
        anyhow::bail!("no socket in deployment test")
    }
    async fn terminate_hosts(&self, request: &TerminateHostsRequest) -> Result<HostTermination> {
        self.retired
            .lock()
            .unwrap()
            .push(request.host_config_key.clone());
        Ok(HostTermination {
            provider: "test".into(),
            resource_ids: vec![],
        })
    }
}
