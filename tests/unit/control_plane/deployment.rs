use super::*;
use crate::{
    control_plane::{ActorTokenPurpose, admin::LocalAdminRegistry},
    placement::testing::LocalObjectPlacementStore,
    sandbox::ActorHostHandle,
};
use std::sync::Mutex;

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
async fn bundle_registration_preserves_code_and_contract_during_secret_rotation() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let routes = super::super::public_api::router(service, admin.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "http://{}/v1/projects/default/deployment",
        listener.local_addr()?
    );
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let bundle =
        crate::artifacts::ArtifactManifest::decode(&crate::sandbox::testing::code_artifact(1))?;
    let mut document: serde_json::Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    document["actors"][0]["sandbox"] = serde_json::json!({"cpu":2,"memoryMiB":4096});
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&serde_json::json!({"bundle":bundle,"contract":document}))
        .send()
        .await?
        .error_for_status()?;
    let active = admin.current_deployment("default").await?.unwrap();
    assert_eq!(active.image_ref, "im-runtime");
    assert_eq!(active.actor_entrypoint.as_deref(), Some("actors.mjs"));
    assert_eq!(active.code_snapshot, Some(bundle.encode()?));
    assert!(active.sandboxes.contains_key("ChatRoom"));
    let mut current: serde_json::Value = client
        .get(&url)
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(current["bundle"], serde_json::to_value(bundle)?);
    current["secretRefs"] = serde_json::json!(["rotated"]);
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&current)
        .send()
        .await?
        .error_for_status()?;
    let rotated = admin.current_deployment("default").await?.unwrap();
    assert_eq!(rotated.code_snapshot, active.code_snapshot);
    assert_eq!(rotated.sandboxes, active.sandboxes);
    assert_ne!(rotated.host_config_key(), active.host_config_key());
    assert_eq!(
        admin
            .deployment_contract("default")
            .await?
            .unwrap()
            .contract,
        document
    );
    assert_eq!(
        provider.retired.lock().unwrap().as_slice(),
        &[active.host_config_key()]
    );
    // A new bundle must carry a contract before replacing the running deployment.
    current["bundle"]["files"][0]["generation"] = 2.into();
    assert_eq!(
        client
            .put(&url)
            .bearer_auth("api-key")
            .json(&current)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    assert_eq!(admin.current_deployment("default").await?, Some(rotated));
    current["contract"] = contract();
    client
        .put(&url)
        .bearer_auth("api-key")
        .json(&current)
        .send()
        .await?
        .error_for_status()?;
    assert!(
        admin
            .current_deployment("default")
            .await?
            .unwrap()
            .sandboxes
            .is_empty()
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn bundle_registration_validates_immutable_artifacts_before_retiring_hosts() -> Result<()> {
    let (service, admin, provider) = fixture()?;
    let source = source();
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    service
        .deploy_source(&admin, &source, Some(&contract))
        .await?;
    let active = admin.current_deployment("default").await?;
    for mutation in ["bucket", "object", "generation", "sha256", "path"] {
        let mut bundle =
            crate::artifacts::ArtifactManifest::decode(source.code_snapshot.as_ref().unwrap())?;
        match mutation {
            "bucket" => bundle.bucket = "another-bucket".into(),
            "object" => bundle.files[0].object = "outside/actors.mjs".into(),
            "generation" => bundle.files[0].generation = 0,
            "sha256" => bundle.files[0].sha256 = "bad".into(),
            _ => bundle.files[0].path = "../actors.mjs".into(),
        }
        if let Ok(snapshot) = bundle.encode() {
            let mut invalid = source.clone();
            invalid.code_snapshot = Some(snapshot);
            assert!(
                service
                    .deploy_source(&admin, &invalid, Some(&contract))
                    .await
                    .is_err()
            );
        }
        assert_eq!(admin.current_deployment("default").await?, active);
    }
    assert!(provider.retired.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn bundle_contracts_are_scoped_to_the_project() -> Result<()> {
    let (service, admin, _) = fixture()?;
    let first = source();
    let mut second = first.clone();
    second.project_id = "team-b".into();
    let contract = super::super::contracts::PublicActorContract::new(contract())?;
    service
        .deploy_source(&admin, &first, Some(&contract))
        .await?;
    assert!(service.deploy_source(&admin, &second, None).await.is_err());
    service
        .deploy_source(&admin, &second, Some(&contract))
        .await?;
    service.delete_deployment(&admin, "default").await?;
    assert!(admin.current_deployment("team-b").await?.is_some());
    Ok(())
}

#[tokio::test]
async fn resolved_targets_expire_within_host_idle_lease_and_authorization_limits() -> Result<()> {
    use crate::clock::{Clock, SystemClock};

    for (default_idle_ms, override_idle_ms, lease_ms, grant_ms) in [
        (10_000, None, 30_000, None),
        (25_000, None, 30_000, Some(60_000)),
        (1, None, 30_000, Some(60_000)),
        (120_000, None, 8_000, Some(60_000)),
        (120_000, None, 60_000, Some(120_000)),
        (30_000, None, 30_000, Some(7_000)),
        (10_000, Some(60_000), 120_000, None),
        (60_000, Some(1_000), 120_000, None),
    ] {
        let (mut service, admin, _) = fixture_with_idle_timeout(default_idle_ms)?;
        let mut spec = source();
        let idle_ms = override_idle_ms.unwrap_or(default_idle_ms);
        if let Some(timeout) = override_idle_ms {
            spec.sandboxes.insert(
                "Counter".into(),
                serde_json::from_value(serde_json::json!({"idleTimeoutMs":timeout}))?,
            );
        }
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

fn fixture() -> Result<(ControlPlaneService, AdminService, Arc<Provider>)> {
    fixture_with_idle_timeout(60_000)
}

fn fixture_with_idle_timeout(
    host_idle_timeout_ms: u64,
) -> Result<(ControlPlaneService, AdminService, Arc<Provider>)> {
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
    let provider = Arc::new(Provider::default());
    let provisioner = Arc::new(
        SandboxHostProvisioner::new(
            provider.clone(),
            HostSandboxRuntimeConfig {
                control_plane_url: "http://control".into(),
                jwt_issuer: "issuer".into(),
                invocation_jwt_audience: "invocation".into(),
                host_idle_timeout_ms,
            },
            issuer.clone(),
            Some("im-runtime".into()),
        )
        .with_runtime_access(Arc::new(crate::bucket::access::RuntimeAccess::new(
            crate::bucket::access::BucketLocation::Gcs {
                bucket: "owner-bucket".into(),
                artifact_bucket: "test-artifacts".into(),
            },
            crate::bucket::PersistenceConfig::Local,
        )?)),
    );
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
        sandboxes: Default::default(),
        project_id: "default".into(),
        source: None,
        image_ref: "bundle".into(),
        code_snapshot: Some(crate::sandbox::testing::code_artifact(1)),
        working_directory: "/customer".into(),
        actor_entrypoint: Some("actors.mjs".into()),
        secret_refs: vec![],
    }
}

fn contract() -> serde_json::Value {
    serde_json::json!({"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}})
}

#[derive(Default)]
struct Provider {
    retired: Mutex<Vec<String>>,
}

#[async_trait]
impl SandboxProvider for Provider {
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        anyhow::bail!("unexpected spare inspection")
    }
    async fn ensure_host(&self, _: &EnsureHostRequest) -> Result<ActorHostHandle> {
        anyhow::bail!("no actor invocation in deployment test")
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
