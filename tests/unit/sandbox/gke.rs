use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Cluster {
    creates: AtomicUsize,
}
#[async_trait]
impl SandboxCluster for Cluster {
    async fn stopped_spares(&self, _: &[crate::sandbox::SpareHandle]) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    async fn create_spare(&self, _: &CreateSpareRequest) -> Result<SpareHandle> {
        self.creates.fetch_add(1, Ordering::SeqCst);
        anyhow::bail!("unexpected pod creation")
    }
    async fn retire_spare(&self, _: &SpareHandle) -> Result<()> {
        Ok(())
    }
    async fn secrets(&self, _: &[String]) -> Result<HashMap<String, String>> {
        Ok(HashMap::from([(
            "CUSTOMER_SECRET".into(),
            "assigned-secret".into(),
        )]))
    }
}
struct Assign;
#[async_trait]
impl HostAssignment for Assign {
    async fn assign(
        &self,
        spare: &SpareHandle,
        environment: HashMap<String, String>,
    ) -> Result<ActorHostHandle> {
        assert_eq!(
            environment["DURABLE_ACTORS_ENTRYPOINT"],
            "/customer/actors.mjs"
        );
        assert!(environment["DURABLE_ACTORS_CODE_ARTIFACT"].starts_with("gcs:"));
        let secrets: HashMap<String, String> =
            serde_json::from_str(&environment["DURABLE_ACTORS_CUSTOMER_ENV"])?;
        assert_eq!(secrets["CUSTOMER_SECRET"], "assigned-secret");
        let host = HostId::new(environment["DURABLE_ACTORS_HOST_ID"].clone());
        Ok(ActorHostHandle {
            lease: Some(crate::host_leases::HostLease {
                id: host.clone(),
                session_id: environment["DURABLE_ACTORS_SESSION_ID"].clone(),
                route: spare.route.clone(),
                expires_at_ms: 9999999999999,
            }),
            owner_epoch: 3,
            host_id: host,
            route: spare.route.clone(),
            canonical_region: spare.canonical_region.clone(),
            provisioning: None,
        })
    }
}

#[tokio::test]
async fn prewarmed_assignment_uses_ready_host_without_creating_a_pod() -> Result<()> {
    let cluster = Arc::new(Cluster {
        creates: AtomicUsize::new(0),
    });
    let provider = GkeSandboxProvider {
        cluster: cluster.clone(),
        assignment: Arc::new(Assign),
    };
    let request = request()?;
    let result = provider.ensure_host(&request).await?;
    assert_eq!(result.owner_epoch, 3);
    assert!(result.provisioning.unwrap().reused);
    assert_eq!(cluster.creates.load(Ordering::SeqCst), 0);
    Ok(())
}

fn request() -> Result<EnsureHostRequest> {
    let manifest = crate::artifacts::ArtifactManifest {
        bucket: "code-bucket".into(),
        files: vec![crate::artifacts::ArtifactFile {
            path: "actors.mjs".into(),
            object: "code/actors.mjs".into(),
            generation: 1,
            sha256: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
        }],
    };
    Ok(EnsureHostRequest {
        actor_is_new: true,
        owner_hint: None,
        actor: Some(crate::actor::ActorKey {
            project_id: "project".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        }),
        code_snapshot: Some(manifest.encode()?),
        spare: Some(SpareHandle {
            control_route: "http://10.0.0.1:7102".into(),
            control_token: "token".into(),
            name: "warm".into(),
            resource_id: "sandboxes/warm/uid".into(),
            route: "http://10.0.0.1:7101".into(),
            canonical_region: "north-america-west".into(),
        }),
        resources: ResourceLimits::default(),
        runtime_config: Some("{}".into()),
        host_config_key: "cfg.test".into(),
        canonical_region: "north-america-west".into(),
        host_id: HostId::new("host.v3.test.one"),
        session_id: uuid::Uuid::new_v4().to_string(),
        host_token: "host-jwt".into(),
        jwt_public_keys: "{}".into(),
        control_plane_url: "http://control:7100".into(),
        jwt_issuer: "issuer".into(),
        invocation_jwt_audience: "invoke".into(),
        image_ref: "registry/runtime@sha256:abc".into(),
        working_directory: "/customer".into(),
        actor_entrypoint: Some("/customer/actors.mjs".into()),
        secret_refs: vec!["customer".into()],
        host_idle_timeout_ms: 10000,
    })
}

#[test]
fn python_assignment_uses_its_manifest_entrypoint() -> Result<()> {
    let mut request = request()?;
    let mut manifest =
        crate::artifacts::ArtifactManifest::decode(request.code_snapshot.as_ref().unwrap())?;
    manifest.files[0].path = "actors.pyz".into();
    request.code_snapshot = Some(manifest.encode()?);
    request.actor_entrypoint = Some("actors.pyz".into());
    let environment =
        assignment_environment(&request, request.spare.as_ref().unwrap(), HashMap::new())?;
    assert_eq!(
        environment["DURABLE_ACTORS_ENTRYPOINT"],
        "/customer/actors.pyz"
    );
    Ok(())
}

struct ExpiredAssignment;
#[async_trait]
impl HostAssignment for ExpiredAssignment {
    async fn assign(
        &self,
        spare: &SpareHandle,
        environment: HashMap<String, String>,
    ) -> Result<ActorHostHandle> {
        let mut handle = Assign.assign(spare, environment).await?;
        handle.lease.as_mut().unwrap().expires_at_ms = 0;
        Ok(handle)
    }
}

#[tokio::test]
async fn assignment_rejects_a_host_whose_lease_expired_before_its_reply() -> Result<()> {
    let provider = GkeSandboxProvider {
        cluster: Arc::new(Cluster {
            creates: AtomicUsize::new(0),
        }),
        assignment: Arc::new(ExpiredAssignment),
    };
    assert!(provider.ensure_host(&request()?).await.is_err());
    Ok(())
}
