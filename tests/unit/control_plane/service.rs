use std::{sync::Mutex, time::Duration};

use super::super::{ActorTokenPurpose, admin::LocalAdminRegistry};
use super::*;
use crate::{actor_state::ActorStorageKey, placement::testing::LocalObjectPlacementStore};
use aws_lc_rs::{rand::SystemRandom, signature::Ed25519KeyPair};
use base64::{Engine, engine::general_purpose::STANDARD};

#[path = "delegated_access.rs"]
mod delegated_access;

struct FakeSocketEventSink {
    delivered: tokio::sync::mpsc::UnboundedSender<serde_json::Value>,
}

#[async_trait]
impl super::super::event_sink::SocketMessageEventSink for FakeSocketEventSink {
    async fn deliver(&self, event: super::super::event_sink::SocketMessageEvent) -> Result<()> {
        self.delivered.send(serde_json::to_value(event)?)?;
        Ok(())
    }
}

fn test_lease(host: &HostId) -> HostLease {
    HostLease {
        id: host.clone(),
        session_id: "00000000-0000-4000-8000-000000000001".into(),
        route: "https://host.example.com".into(),
        expires_at_ms: u64::MAX,
    }
}

struct UnavailableProvisioner;

#[async_trait]
impl HostProvisioner for UnavailableProvisioner {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _previous: Option<&HostLaunchSpec>,
        _region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<crate::control_plane::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }

    async fn ensure_actor_host(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
    ) -> Result<(HostLease, u64)> {
        anyhow::bail!("host creation is outside this test")
    }

    async fn terminate_hosts(
        &self,
        _spec: &HostLaunchSpec,
        _regions: &[String],
    ) -> Result<HostTermination> {
        anyhow::bail!("host termination is outside this test")
    }
}

struct FakeRetiringProvisioner {
    retired: tokio::sync::mpsc::UnboundedSender<(HostLaunchSpec, Vec<String>)>,
    fail: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl HostProvisioner for FakeRetiringProvisioner {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _previous: Option<&HostLaunchSpec>,
        _region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<crate::control_plane::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }

    async fn ensure_actor_host(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
    ) -> Result<(HostLease, u64)> {
        anyhow::bail!("host creation is outside this test")
    }

    async fn terminate_hosts(
        &self,
        spec: &HostLaunchSpec,
        regions: &[String],
    ) -> Result<HostTermination> {
        self.retired.send((spec.clone(), regions.to_vec()))?;
        ensure!(
            !self.fail.load(std::sync::atomic::Ordering::Relaxed),
            "termination failed"
        );
        Ok(HostTermination {
            provider: "test".into(),
            resource_ids: vec!["sandbox-1".into()],
        })
    }
}

#[tokio::test]
async fn gcs_routes_use_the_hosts_epoch_without_claiming_or_preparing_in_the_control_plane()
-> Result<()> {
    struct Provisioner(HostLease);
    #[async_trait]
    impl HostProvisioner for Provisioner {
        async fn prepare_deployment(
            &self,
            source: &HostLaunchSpec,
            _previous: Option<&HostLaunchSpec>,
            _region: &str,
        ) -> Result<(
            HostLaunchSpec,
            Option<crate::control_plane::contracts::PublicActorContract>,
        )> {
            Ok((source.clone(), None))
        }

        async fn ensure_actor_host(
            &self,
            _: &HostLaunchSpec,
            _: &str,
            _actor: &ActorKey,
            _new_actor: bool,
        ) -> Result<(HostLease, u64)> {
            Ok((self.0.clone(), 42))
        }
        async fn terminate_hosts(
            &self,
            _: &HostLaunchSpec,
            _: &[String],
        ) -> Result<HostTermination> {
            anyhow::bail!("unused")
        }
    }
    let issuer = test_issuer()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let route = format!("http://{}", listener.local_addr()?);
    let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let host_requests = requests.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().fallback(move || {
                host_requests.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }
            }),
        )
        .await
    });
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            project_id: "default".into(),
            source: None,
            code_snapshot: None,
            image_ref: "image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    let service = ControlPlaneService::new(
        placements.clone(),
        ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "authority",
            ActorTokenPurpose::ControlPlane,
            Duration::from_secs(60),
        )?,
        registry,
        issuer.clone(),
        Arc::new(Provisioner(HostLease {
            id: HostId::new("host.v3.revision.host"),
            session_id: uuid::Uuid::new_v4().to_string(),
            route: route.clone(),
            expires_at_ms: u64::MAX,
        })),
    );
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let target = service
        .resolve_actor_route(&actor, None, None, None)
        .await?;
    assert_eq!(target.route, route);
    assert_eq!(target.owner_epoch, 42);
    assert!(placements.get(&actor.storage_key()).await?.is_none());
    service
        .resolve_actor_route(&actor, None, None, None)
        .await?;
    assert_eq!(
        requests.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "readiness must eliminate the extra activation RPC"
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn deploying_replaces_running_hosts_even_when_configuration_is_unchanged() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = super::super::admin::AdminService::new(
        Some("admin-token".into()),
        registry.clone(),
        issuer.clone(),
    )?;
    let (retired_tx, mut retired_rx) = tokio::sync::mpsc::unbounded_channel();
    let provisioner = Arc::new(FakeRetiringProvisioner {
        retired: retired_tx,
        fail: std::sync::atomic::AtomicBool::new(true),
    });
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        provisioner.clone(),
    );
    let first = HostLaunchSpec {
        project_id: "default".into(),
        source: None,
        code_snapshot: None,
        image_ref: "image-1".into(),
        working_directory: "/workspace".into(),
        actor_entrypoint: None,
        secret_refs: vec![],
    };
    let mut replacement = first.clone();

    replacement.image_ref = "image-2".into();

    assert!(service.register_deployment(&admin, &first, None).await?);
    assert!(retired_rx.try_recv().is_err());
    assert!(
        service
            .register_deployment(&admin, &replacement, None)
            .await
            .is_err()
    );
    assert_eq!(
        admin.current_deployment("default").await?,
        Some(first.clone())
    );
    assert_eq!(
        retired_rx.recv().await,
        Some((
            first.clone(),
            super::super::regions::ALL
                .iter()
                .map(|r| (*r).into())
                .collect()
        ))
    );
    provisioner
        .fail
        .store(false, std::sync::atomic::Ordering::Relaxed);
    assert!(
        service
            .register_deployment(&admin, &replacement, None)
            .await?
    );
    assert_eq!(
        retired_rx.recv().await,
        Some((
            first,
            super::super::regions::ALL
                .iter()
                .map(|r| (*r).into())
                .collect()
        ))
    );
    assert_eq!(
        admin.current_deployment("default").await?,
        Some(replacement.clone())
    );
    assert!(
        service
            .register_deployment(&admin, &replacement, None)
            .await?
    );
    assert_eq!(retired_rx.try_recv()?.0, replacement);
    let mut secret_update = replacement.clone();
    secret_update.secret_refs = vec!["project-secrets-updated".into()];
    assert!(
        service
            .register_deployment(&admin, &secret_update, None)
            .await?
    );
    assert_eq!(
        retired_rx.recv().await,
        Some((
            replacement,
            super::super::regions::ALL
                .iter()
                .map(|r| (*r).into())
                .collect()
        ))
    );
    provisioner
        .fail
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(service.delete_deployment(&admin, "default").await.is_err());
    assert_eq!(
        admin.current_deployment("default").await?,
        Some(secret_update)
    );
    provisioner
        .fail
        .store(false, std::sync::atomic::Ordering::Relaxed);
    assert!(service.delete_deployment(&admin, "default").await?);
    assert_eq!(admin.current_deployment("default").await?, None);
    assert!(!service.delete_deployment(&admin, "default").await?);
    Ok(())
}

#[tokio::test]
async fn accepted_socket_messages_are_delivered_to_the_configured_event_sink() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let (delivered_tx, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        Arc::new(LocalAdminRegistry::default()),
        issuer,
        Arc::new(UnavailableProvisioner),
    )
    .with_socket_event_sink(Some(Arc::new(FakeSocketEventSink {
        delivered: delivered_tx,
    })));
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "ChatRoom".into(),
        actor_id: "room-1".into(),
    };

    service.deliver_socket_message_event(
        &actor,
        Some("trigger-1".into()),
        &ActorSocketEvent::Message {
            connection_id: "socket-1".into(),
            message: crate::actor::ActorSocketMessage::Text {
                data: "hello".into(),
            },
        },
    );

    let delivered = tokio::time::timeout(Duration::from_secs(1), delivered_rx.recv())
        .await?
        .context("socket event delivery task stopped")?;
    assert_eq!(delivered["actorName"], "ChatRoom");
    assert_eq!(delivered["actorId"], "room-1");
    assert_eq!(delivered["triggerId"], "trigger-1");
    assert_eq!(delivered["connectionId"], "socket-1");
    assert_eq!(delivered["message"]["type"], "text");
    assert_eq!(delivered["message"]["data"], "hello");
    Ok(())
}

#[test]
fn existing_actors_stay_pinned_to_the_assigned_region() -> Result<()> {
    let actor = ActorStorageKey::new("object.v1.project.Counter.one");
    let current = ObjectPlacement {
        lease: test_lease(&HostId::new("host.v3.revision.host")),
        object: actor,
        owner: HostId::new("host.v3.revision.host"),
        owner_epoch: 1,
        home_region: "north-america-east".into(),
        state_version: 0,
        state_object: None,
        last_request_id: None,
    };

    assert_eq!(
        select_target_region(None, "north-america-central")?,
        "north-america-central"
    );
    assert_eq!(
        select_target_region(Some(&current), "north-america-west")?,
        "north-america-east"
    );
    for (reported, expected) in [
        ("us-east-1", "north-america-east"),
        ("us-west-2", "north-america-west"),
        ("us-central1", "north-america-central"),
        ("us-central1-a", "north-america-central"),
        ("us-ashburn-1", "north-america-east"),
        ("westus3", "north-america-west"),
    ] {
        assert_eq!(select_target_region(None, reported)?, expected);
        assert_eq!(
            select_target_region(Some(&current), reported)?,
            "north-america-east"
        );
    }
    for unsupported in ["", "unknown", "us-east-999", "us-central1-unknown"] {
        assert_eq!(
            select_target_region(None, unsupported)?,
            "north-america-central",
            "{unsupported}"
        );
        assert_eq!(
            select_target_region(Some(&current), unsupported)?,
            "north-america-east"
        );
    }
    Ok(())
}

#[test]
fn execution_regions_do_not_require_separate_buckets() -> Result<()> {
    assert_eq!(
        select_target_region(None, "southcentralus")?,
        "north-america-south"
    );
    assert_eq!(select_target_region(None, "europe-west")?, "europe-west");
    Ok(())
}

struct LosingActivation {
    placements: Arc<LocalObjectPlacementStore>,
    actor: ActorKey,
    waited: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl HostProvisioner for LosingActivation {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _previous: Option<&HostLaunchSpec>,
        _region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<crate::control_plane::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }

    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
    ) -> Result<(HostLease, u64)> {
        let host = HostId::new(format!("host.v3.{}.winner", spec.host_config_key()));
        let mut lease = test_lease(&host);
        lease.route = "https://winner.example.com".into();
        self.placements
            .set_owner(&self.actor.storage_key(), lease, region)?;
        anyhow::bail!("another activation won ownership")
    }
    async fn wait_ready(&self, _: &HostId) -> Result<()> {
        self.waited.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn terminate_hosts(&self, _: &HostLaunchSpec, _: &[String]) -> Result<HostTermination> {
        unreachable!()
    }
}

#[tokio::test]
async fn a_losing_activation_routes_to_the_ready_winner() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "invocation",
        ActorTokenPurpose::Invocation,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            project_id: "default".into(),
            source: None,
            image_ref: "im-runtime".into(),
            code_snapshot: Some("im-code".into()),
            working_directory: "/customer".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let provisioner = Arc::new(LosingActivation {
        placements: placements.clone(),
        actor: actor.clone(),
        waited: false.into(),
    });
    let service = ControlPlaneService::new(placements, auth, registry, issuer, provisioner.clone());
    let target = service
        .route_actor(&actor, "north-america-east", None, None)
        .await?;
    assert_eq!(target.lease.route, "https://winner.example.com");
    assert!(provisioner.waited.load(std::sync::atomic::Ordering::SeqCst));
    Ok(())
}

struct FakeRoutingProvisioner {
    failed_regions: Vec<&'static str>,
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl HostProvisioner for FakeRoutingProvisioner {
    async fn prepare_deployment(
        &self,
        source: &HostLaunchSpec,
        _previous: Option<&HostLaunchSpec>,
        _region: &str,
    ) -> Result<(
        HostLaunchSpec,
        Option<crate::control_plane::contracts::PublicActorContract>,
    )> {
        Ok((source.clone(), None))
    }

    async fn socket_credentials(
        &self,
        _spec: &HostLaunchSpec,
        _region: &str,
        lease: &HostLease,
    ) -> Result<crate::sandbox::SocketCredentials> {
        Ok(crate::sandbox::SocketCredentials {
            url: lease.route.clone(),
            token: String::new(),
        })
    }

    async fn ensure_actor_host(
        &self,
        spec: &HostLaunchSpec,
        region: &str,
        _actor: &ActorKey,
        _new_actor: bool,
    ) -> Result<(HostLease, u64)> {
        self.calls.lock().unwrap().push(region.to_owned());
        ensure!(
            !self.failed_regions.contains(&region),
            "host unavailable in {region}"
        );
        Ok((
            HostLease {
                id: HostId::new(format!("host.v3.{}.{region}", spec.host_config_key())),
                session_id: uuid::Uuid::new_v4().to_string(),
                route: "https://host.example.com".into(),
                expires_at_ms: u64::MAX,
            },
            1,
        ))
    }

    async fn terminate_hosts(
        &self,
        _spec: &HostLaunchSpec,
        _regions: &[String],
    ) -> Result<HostTermination> {
        unreachable!()
    }
}

#[tokio::test]
async fn provisioning_never_changes_the_assigned_region() -> Result<()> {
    let central = "north-america-central";
    let south = "north-america-south";
    for (failed_regions, existing, reported, expected_region, expected_calls) in [
        (vec![], false, "southcentralus", Some(south), vec![south]),
        (
            vec![],
            false,
            "eu-west-1",
            Some("europe-west"),
            vec!["europe-west"],
        ),
        (
            vec![],
            false,
            "unmapped-region",
            Some(central),
            vec![central],
        ),
        (vec![south], false, "southcentralus", None, vec![south]),
        (vec![south], true, "eastus", None, vec![south]),
        (
            vec![south, central],
            false,
            "southcentralus",
            None,
            vec![south],
        ),
        (vec![central], false, "unmapped-region", None, vec![central]),
    ] {
        let issuer = test_issuer()?;
        let auth = ActorJwtVerifier::for_scope(
            issuer.verifier_keys_json()?,
            "issuer",
            "invocation",
            ActorTokenPurpose::Invocation,
            Duration::from_secs(60),
        )?;
        let registry = Arc::new(LocalAdminRegistry::default());
        registry
            .register_test_deployment(&HostLaunchSpec {
                project_id: "default".into(),
                source: None,
                code_snapshot: None,
                image_ref: "image".into(),
                working_directory: "/app".into(),
                actor_entrypoint: None,
                secret_refs: vec![],
            })
            .await?;
        let placements = Arc::new(LocalObjectPlacementStore::default());
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        if existing {
            placements.set_owner(
                &actor.storage_key(),
                test_lease(&HostId::new("old-host")),
                south,
            )?;
        }
        let before = placements.get(&actor.storage_key()).await?;
        let provisioner = Arc::new(FakeRoutingProvisioner {
            failed_regions,
            calls: Mutex::new(vec![]),
        });
        let service = ControlPlaneService::new(
            placements.clone(),
            auth,
            registry.clone(),
            issuer,
            provisioner.clone(),
        );
        let spec = registry.launch_spec("default").await?.unwrap();
        let result = service
            .ensure_actor_host(
                &ActorKey {
                    project_id: "default".into(),
                    actor_name: "Counter".into(),
                    actor_id: "one".into(),
                },
                &spec,
                before.as_ref(),
                &select_target_region(before.as_ref(), reported)?,
            )
            .await;
        assert_eq!(*provisioner.calls.lock().unwrap(), expected_calls);
        if let Some(region) = expected_region {
            let (selected, _, _) = result?;
            assert_eq!(selected, region);
            assert_eq!(placements.get(&actor.storage_key()).await?, before);
        } else {
            assert!(result.is_err());
            if expected_calls.len() == 2 {
                let error = format!("{:#}", result.err().unwrap());
                assert!(error.contains(south) && error.contains(central), "{error}");
            }
            assert_eq!(placements.get(&actor.storage_key()).await?, before);
        }
    }
    Ok(())
}

#[tokio::test]
async fn application_credentials_work_without_postgres() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    registry
        .register_test_deployment(&HostLaunchSpec {
            project_id: "default".into(),
            source: None,
            code_snapshot: None,
            image_ref: "image".into(),
            working_directory: "/app".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let database = crate::postgres::PostgresDatabase::lazy(
        "postgresql://localhost:1/unavailable?sslmode=disable&connect_timeout=1",
    )?;
    let admin = AdminService::new(
        Some("api-key".into()),
        Arc::new(super::super::admin::PostgresAdminRegistry::from_database(
            database,
        )),
        issuer.clone(),
    )?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    let host_id = fixture_host("fixture");
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "lobby".into(),
    };
    placements.set_owner(
        &actor.storage_key(),
        test_lease(&host_id),
        "north-america-east",
    )?;
    let service = ControlPlaneService::new(
        placements,
        auth,
        registry,
        issuer.clone(),
        Arc::new(FakeRoutingProvisioner {
            failed_regions: vec![],
            calls: Mutex::new(vec![]),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let requests = [(
        "actors/Room/lobby/find-websocket",
        serde_json::json!({"metadata":{"userId":"trusted"},"authorizationLifetimeMs":30000}),
    )];
    for (path, body) in requests {
        let response = client
            .post(format!("{origin}/v1/projects/default/{path}"))
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?;
        let status = response.status();
        let issued: serde_json::Value = response.json().await?;
        assert!(status.is_success(), "{path}: {status} {issued}");
        let mut url = reqwest::Url::parse(issued["websocketUrl"].as_str().unwrap())?;
        let key = url
            .query_pairs()
            .find(|(name, _)| name == "key")
            .unwrap()
            .1
            .into_owned();
        url.set_query(None);
        assert_eq!(url.as_str(), "wss://host.example.com/v1/socket");
        let ticket = issuer.verify_socket(&key)?;
        assert_eq!(ticket.actor.actor_id, "lobby");
        assert_eq!(ticket.metadata, body["metadata"]);
        assert_eq!(issued["homeRegion"], "north-america-east");
        assert!(issued.get("key").is_none());
    }
    assert_eq!(
        client
            .get(format!("{origin}/v1/projects/default/deployment"))
            .bearer_auth("api-key")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn socket_ticket_issuance_requires_api_key_and_cannot_delegate_backend_access() -> Result<()>
{
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    let host_id = HostId::new(format!(
        "host.v3.{}.fixture",
        HostLaunchSpec {
            project_id: "default".into(),
            source: None,
            image_ref: "im-runtime".into(),
            code_snapshot: Some("im-code".into()),
            working_directory: "/customer".into(),
            actor_entrypoint: None,
            secret_refs: vec![]
        }
        .host_config_key()
    ));
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "lobby".into(),
    };
    placements.set_owner(
        &actor.storage_key(),
        test_lease(&host_id),
        "north-america-east",
    )?;
    let service = ControlPlaneService::new(
        placements,
        auth,
        registry,
        issuer.clone(),
        Arc::new(FakeRoutingProvisioner {
            failed_regions: vec![],
            calls: Mutex::new(vec![]),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let url = format!("{origin}/v1/projects/default/actors/Room/lobby/find-websocket");
    let body = serde_json::json!({"metadata":{"userId":"trusted"},"authorizationLifetimeMs":30000,"homeRegion":"north-america-east"});
    let host_token = issuer
        .issue_host(
            &fixture_host("fixture"),
            &uuid::Uuid::new_v4().to_string(),
            "v1",
            "north-america-east",
            &actor,
        )?
        .token;
    for credential in ["", "wrong", &host_token] {
        assert_eq!(
            client
                .post(&url)
                .bearer_auth(credential)
                .json(&body)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }
    client
        .put(format!("{origin}/v1/projects/default/deployment"))
        .bearer_auth("api-key")
        .json(&serde_json::json!({"imageRef":"im-runtime","workingDirectory":"/customer"}))
        .send()
        .await?
        .error_for_status()?;
    for operation in ["find-websocket", "find-actor"] {
        let response = client
            .post(format!(
                "{origin}/v1/projects/default/actors/Room/lobby/{operation}"
            ))
            .bearer_auth("api-key")
            .json(&if operation == "find-websocket" {
                serde_json::json!({"metadata":{},"homeRegion":"north-america-west"})
            } else {
                serde_json::json!({"homeRegion":"north-america-west"})
            })
            .send()
            .await?;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::CONFLICT,
            "{operation}"
        );
    }
    let issued = client
        .post(&url)
        .bearer_auth("api-key")
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(issued.headers().get("cache-control").unwrap(), "no-store");
    let issued: serde_json::Value = issued.json().await?;
    let socket_url = reqwest::Url::parse(issued["websocketUrl"].as_str().unwrap())?;
    let key = socket_url
        .query_pairs()
        .find(|(name, _)| name == "key")
        .unwrap()
        .1
        .into_owned();
    assert_eq!(socket_url.scheme(), "wss");
    assert_eq!(socket_url.host_str(), Some("host.example.com"));
    assert_eq!(socket_url.path(), "/v1/socket");
    assert_eq!(
        socket_url.query_pairs().collect::<Vec<_>>(),
        vec![("key".into(), key.as_str().into())]
    );
    assert_ne!(key, "api-key");
    assert_eq!(
        client
            .post(&url)
            .bearer_auth(&key)
            .json(&body)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .post(format!(
                "{origin}/v1/projects/default/actors/Room/lobby/find-actor"
            ))
            .bearer_auth(&key)
            .json(&serde_json::json!({}))
            .send()
            .await?
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn actor_discovery_authenticates_and_validates_each_request_contract() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    let placements = Arc::new(LocalObjectPlacementStore::default());
    {
        let host = HostId::new(format!(
            "host.v3.{}.fixture",
            HostLaunchSpec {
                project_id: "default".into(),
                source: None,
                image_ref: "im-runtime".into(),
                code_snapshot: Some("im-code".into()),
                working_directory: "/customer".into(),
                actor_entrypoint: None,
                secret_refs: vec![]
            }
            .host_config_key()
        ));
        let actor = ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "one".into(),
        };
        placements.set_owner(
            &actor.storage_key(),
            test_lease(&host),
            "north-america-east",
        )?;
    }
    let service = ControlPlaneService::new(
        placements,
        auth,
        registry,
        issuer.clone(),
        Arc::new(FakeRoutingProvisioner {
            failed_regions: vec![],
            calls: Mutex::new(vec![]),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let deployment =
        serde_json::json!({ "imageRef": "im-runtime", "workingDirectory": "/customer" });
    let registered = client
        .put(format!("{origin}/v1/projects/default/deployment"))
        .bearer_auth("api-key")
        .json(&deployment)
        .send()
        .await?;
    assert_eq!(registered.status(), reqwest::StatusCode::OK);
    for suffix in ["find-actor", "find-websocket"] {
        let url = format!("{origin}/v1/projects/default/actors/Counter/one/{suffix}");
        for key in ["", "wrong-key"] {
            assert_eq!(
                client
                    .post(&url)
                    .bearer_auth(key)
                    .json(&serde_json::json!({"effects":[]}))
                    .send()
                    .await?
                    .status(),
                reqwest::StatusCode::UNAUTHORIZED
            );
        }
    }
    let response = client
        .post(format!(
            "{origin}/v1/projects/default/actors/Counter/one/find-actor"
        ))
        .bearer_auth("api-key")
        .json(&serde_json::json!({}))
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
    let target: serde_json::Value = response.json().await?;
    assert_eq!(target["homeRegion"], "north-america-east");
    assert_eq!(target["route"], "https://host.example.com");
    assert!(target["ownerEpoch"].is_u64());
    assert!(target["expiresAtMs"].is_i64());
    assert!(
        target["token"]
            .as_str()
            .is_some_and(|token| !token.is_empty())
    );
    for (suffix, body) in [
        ("find-actor", serde_json::json!({"metadata":{}})),
        ("find-actor", serde_json::json!({"homeRegion":42})),
        ("find-websocket", serde_json::json!({})),
        (
            "find-websocket",
            serde_json::json!({"metadata":{},"unknown":true}),
        ),
        (
            "find-websocket",
            serde_json::json!({"metadata":null,"authorizationLifetimeMs":999}),
        ),
        (
            "find-websocket",
            serde_json::json!({"metadata":null,"authorizationLifetimeMs":86400001}),
        ),
    ] {
        let reply = client
            .post(format!(
                "{origin}/v1/projects/default/actors/Counter/one/{suffix}"
            ))
            .bearer_auth("api-key")
            .json(&body)
            .send()
            .await?;
        assert_eq!(reply.status(), reqwest::StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            reply.json::<serde_json::Value>().await?["error"]["code"],
            "invalid_request"
        );
    }
    assert_ne!(target["token"], "api-key");
    server.abort();
    Ok(())
}

#[tokio::test]
async fn deployment_reads_and_deletion_require_the_api_key() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    admin
        .register_test_deployment(&HostLaunchSpec {
            project_id: "default".into(),
            source: None,
            code_snapshot: None,
            image_ref: "image-1".into(),
            working_directory: "/workspace".into(),
            actor_entrypoint: None,
            secret_refs: vec![],
        })
        .await?;
    let (retired, _retired_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        Arc::new(FakeRetiringProvisioner {
            retired,
            fail: std::sync::atomic::AtomicBool::new(false),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let deployment_url = format!("{origin}/v1/projects/default/deployment");
    for method in [reqwest::Method::GET, reqwest::Method::DELETE] {
        assert_eq!(
            client
                .request(method, &deployment_url)
                .send()
                .await?
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
    }
    let deployment: serde_json::Value = client
        .get(&deployment_url)
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(deployment["imageRef"], "image-1");
    assert_eq!(deployment["secretRefs"], serde_json::json!([]));
    for changed in [true, false] {
        let reply: serde_json::Value = client
            .delete(&deployment_url)
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(reply["changed"], changed);
    }
    assert_eq!(
        client
            .get(&deployment_url)
            .bearer_auth("api-key")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn contract_api_returns_the_current_deployments_contract() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    let host_token = issuer
        .issue_host(
            &HostId::new("host.v3.r1.one"),
            &uuid::Uuid::new_v4().to_string(),
            "r1",
            "us-east",
            &ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "one".into(),
            },
        )?
        .token;
    let (retired, _retired_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        Arc::new(FakeRetiringProvisioner {
            retired,
            fail: std::sync::atomic::AtomicBool::new(false),
        }),
    );
    let routes = super::super::public_api::router(service, admin);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    for path in ["/v1/projects/default/deployment/contract"] {
        for credential in ["", "wrong", &host_token] {
            assert_eq!(
                client
                    .get(format!("{origin}{path}"))
                    .bearer_auth(credential)
                    .send()
                    .await?
                    .status(),
                reqwest::StatusCode::UNAUTHORIZED
            );
        }
        let response = client
            .get(format!("{origin}{path}"))
            .bearer_auth("api-key")
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        assert_eq!(
            response.json::<serde_json::Value>().await?["error"]["code"],
            "not_found"
        );
    }
    let document: serde_json::Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    let mut deployment = serde_json::json!({"imageRef":"im-runtime", "workingDirectory":"/customer", "contract":document});
    for scope in ["/v1/projects/default"] {
        for changed in [true, true] {
            let reply: serde_json::Value = client
                .put(format!("{origin}{scope}/deployment"))
                .bearer_auth("api-key")
                .json(&deployment)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            assert_eq!(reply["changed"], changed);
        }
        let response = client
            .get(format!("{origin}{scope}/deployment/contract"))
            .bearer_auth("api-key")
            .send()
            .await?
            .error_for_status()?;
        assert_eq!(response.headers()["cache-control"], "no-store");
        let reply: serde_json::Value = response.json().await?;
        assert_eq!(reply["contract"], document);
        assert!(
            reply["contractHash"]
                .as_str()
                .unwrap()
                .starts_with("sha256:")
        );
    }
    deployment["contract"] = serde_json::json!({"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}});
    client
        .put(format!("{origin}/v1/projects/default/deployment"))
        .bearer_auth("api-key")
        .json(&deployment)
        .send()
        .await?
        .error_for_status()?;
    let active: serde_json::Value = client
        .get(format!("{origin}/v1/projects/default/deployment/contract"))
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(active["contract"]["actors"], serde_json::json!([]));
    deployment["contract"] = serde_json::json!({"version":2,"actors":[]});
    assert_eq!(
        client
            .put(format!("{origin}/v1/projects/default/deployment"))
            .bearer_auth("api-key")
            .json(&deployment)
            .send()
            .await?
            .status(),
        reqwest::StatusCode::BAD_REQUEST
    );
    let unchanged: serde_json::Value = client
        .get(format!("{origin}/v1/projects/default/deployment/contract"))
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(unchanged, active);
    server.abort();
    Ok(())
}

pub(super) fn test_issuer() -> Result<ActorJwtIssuer> {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())?;
    ActorJwtIssuer::from_base64_pkcs8(
        &STANDARD.encode(pkcs8.as_ref()),
        "test-key",
        "issuer",
        "authority",
        "invocation",
        Duration::from_secs(60),
    )
}

#[tokio::test]
async fn initial_replica_registration_is_authenticated_and_does_not_require_an_active_primary()
-> Result<()> {
    use crate::{
        bucket::{
            FileBucket, GrpcReplicaPeers, RuntimeStorage,
            access::{BucketLocation, RuntimeAccess},
        },
        clock::SystemClock,
        replication::{ReplicaAccess, ReplicaScope, ReplicaSet, ReplicaTarget},
    };
    let directory = tempfile::tempdir()?;
    let scope = ReplicaScope {
        actor: ActorKey {
            project_id: "default".into(),
            actor_name: "Counter".into(),
            actor_id: "starting".into(),
        },
        host: HostId::new("host.v3.revision.primary"),
        session: uuid::Uuid::new_v4().to_string(),
        region: "us-east".into(),
    };
    let targets = vec![ReplicaTarget {
        host_id: "assigned".into(),
        url: "http://127.0.0.1:1".into(),
        region: scope.region.clone(),
    }];
    let fleet = Arc::new(ReplicaSet(targets.clone()));
    let access = ReplicaAccess::new("secret", Arc::new(SystemClock));
    let runtime = Arc::new(RuntimeStorage::new(
        Arc::new(FileBucket::new(directory.path().into())?),
        fleet.clone(),
        Arc::new(GrpcReplicaPeers::new(access.clone())?),
        access.clone(),
        "http://control".into(),
        Arc::new(SystemClock),
    )?);
    let initial = Arc::new(RuntimeAccess::new(
        BucketLocation::File {
            directory: directory.path().into(),
        },
        fleet,
        access,
        runtime.clone(),
    )?);
    let issuer = test_issuer()?;
    let token = issuer.issue_host(
        &scope.host,
        &scope.session,
        "revision",
        &scope.region,
        &scope.actor,
    )?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let service = ControlPlaneService::new(
        runtime.clone(),
        auth,
        Arc::new(LocalAdminRegistry::default()),
        issuer,
        Arc::new(UnavailableProvisioner),
    )
    .with_runtime_access(initial.clone());
    for command in [
        ControlPlaneCommand::PrepareInitialReplicas,
        ControlPlaneCommand::PrepareReplicaConnections,
    ] {
        let request = super::super::protocol::encode_command(command)?;
        assert_eq!(
            service
                .execute(Request::new(request))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::Unauthenticated
        );
    }
    let request = |command| -> Result<_> {
        let mut request = Request::new(super::super::protocol::encode_command(command)?);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {}", token.token).parse()?);
        Ok(request)
    };
    let reply = service
        .execute(request(ControlPlaneCommand::PrepareReplicaConnections)?)
        .await?
        .into_inner();
    let ControlPlaneCommandReply::Replicas { targets: assigned } =
        super::super::protocol::decode_reply(reply)?
    else {
        anyhow::bail!("expected replica connection targets");
    };
    assert_eq!(assigned, targets);
    assert!(runtime.replica_members(&scope).await?.is_empty());
    initial.prewarm(scope.clone());
    let reply = service
        .execute(request(ControlPlaneCommand::PrepareInitialReplicas)?)
        .await?
        .into_inner();
    assert!(matches!(
        super::super::protocol::decode_reply(reply)?,
        ControlPlaneCommandReply::InitialReplicas { .. }
    ));
    assert_eq!(runtime.replica_members(&scope).await?, targets);
    assert!(
        runtime
            .get_owner(&scope.actor.storage_key())
            .await?
            .is_none()
    );
    assert_eq!(
        service
            .execute(request(ControlPlaneCommand::EnsureReplicas {
                failed: vec![]
            })?)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    Ok(())
}

#[tokio::test]
async fn project_http_deployments_only_replace_and_retire_their_own_hosts() -> Result<()> {
    let issuer = test_issuer()?;
    let auth = ActorJwtVerifier::for_scope(
        issuer.verifier_keys_json()?,
        "issuer",
        "authority",
        ActorTokenPurpose::ControlPlane,
        Duration::from_secs(60),
    )?;
    let registry = Arc::new(LocalAdminRegistry::default());
    let admin = AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
    let (retired, mut retired_rx) = tokio::sync::mpsc::unbounded_channel();
    let service = ControlPlaneService::new(
        Arc::new(LocalObjectPlacementStore::default()),
        auth,
        registry,
        issuer,
        Arc::new(FakeRetiringProvisioner {
            retired,
            fail: std::sync::atomic::AtomicBool::new(false),
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let origin = format!("http://{}", listener.local_addr()?);
    let routes = super::super::public_api::router(service, admin);
    let server = tokio::spawn(async move { axum::serve(listener, routes).await });
    let client = reqwest::Client::new();
    let document: serde_json::Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    for project in ["team-a", "team-b"] {
        client.put(format!("{origin}/v1/projects/{project}/deployment")).bearer_auth("api-key")
            .json(&serde_json::json!({"imageRef":project, "workingDirectory":"/app", "contract":document}))
            .send().await?.error_for_status()?;
    }
    assert!(retired_rx.try_recv().is_err());
    client.put(format!("{origin}/v1/projects/team-a/deployment")).bearer_auth("api-key")
        .json(&serde_json::json!({"imageRef":"new-a", "workingDirectory":"/app", "contract":document}))
        .send().await?.error_for_status()?;
    assert_eq!(retired_rx.try_recv()?.0.project_id, "team-a");
    client
        .delete(format!("{origin}/v1/projects/team-a/deployment"))
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?;
    assert_eq!(retired_rx.try_recv()?.0.project_id, "team-a");
    assert!(retired_rx.try_recv().is_err());
    let remaining: serde_json::Value = client
        .get(format!("{origin}/v1/projects/team-b/deployment"))
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(remaining["imageRef"], "team-b");
    let contract: serde_json::Value = client
        .get(format!("{origin}/v1/projects/team-b/deployment/contract"))
        .bearer_auth("api-key")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(contract["contract"], document);
    assert_eq!(
        client
            .get(format!("{origin}/v1/projects/team-a/deployment/contract"))
            .bearer_auth("api-key")
            .send()
            .await?
            .status(),
        reqwest::StatusCode::NOT_FOUND
    );
    server.abort();
    Ok(())
}

fn fixture_host(suffix: &str) -> HostId {
    let spec = HostLaunchSpec {
        source: None,
        code_snapshot: None,
        project_id: "default".into(),

        image_ref: "image".into(),
        working_directory: "/app".into(),
        actor_entrypoint: None,
        secret_refs: vec![],
    };
    HostId::new(format!("host.v3.{}.{suffix}", spec.host_config_key()))
}

#[tokio::test]
async fn regional_discovery_allows_omitted_home_region() -> Result<()> {
    for project in ["local", "team-a"] {
        for operation in ["find-actor", "find-websocket"] {
            for existing in [false, true] {
                let issuer = test_issuer()?;
                let auth = ActorJwtVerifier::for_scope(
                    issuer.verifier_keys_json()?,
                    "issuer",
                    "authority",
                    ActorTokenPurpose::ControlPlane,
                    Duration::from_secs(60),
                )?;
                let registry = Arc::new(LocalAdminRegistry::default());
                registry
                    .register_test_deployment(&HostLaunchSpec {
                        project_id: project.into(),
                        source: None,
                        code_snapshot: None,
                        image_ref: "image".into(),
                        working_directory: "/app".into(),
                        actor_entrypoint: None,
                        secret_refs: vec![],
                    })
                    .await?;
                let admin =
                    AdminService::new(Some("api-key".into()), registry.clone(), issuer.clone())?;
                let placements = Arc::new(LocalObjectPlacementStore::default());
                let actor = ActorKey {
                    project_id: project.into(),
                    actor_name: "Room".into(),
                    actor_id: "lobby".into(),
                };
                if existing {
                    placements.set_owner(
                        &actor.storage_key(),
                        test_lease(&HostId::new("old-host")),
                        "north-america-east",
                    )?;
                }
                let provisioner = Arc::new(FakeRoutingProvisioner {
                    failed_regions: vec![],
                    calls: Mutex::new(vec![]),
                });
                let mut service = ControlPlaneService::new(
                    placements,
                    auth,
                    registry,
                    issuer.clone(),
                    provisioner.clone(),
                );
                service.region = Some("north-america-west".into());
                let routes = super::super::public_api::router(service, admin);
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                let url = format!(
                    "http://{}/v1/projects/{project}/actors/Room/lobby/{operation}",
                    listener.local_addr()?
                );
                let server = tokio::spawn(async { axum::serve(listener, routes).await });
                let client = reqwest::Client::new();
                let mut body = serde_json::json!({});
                if operation == "find-websocket" {
                    body["metadata"] = serde_json::json!({"userId":"trusted"});
                }
                let response = client
                    .post(&url)
                    .bearer_auth("api-key")
                    .json(&body)
                    .send()
                    .await?;
                assert_eq!(response.headers().get("cache-control").unwrap(), "no-store");
                let status = response.status();
                let grant: serde_json::Value = response.json().await?;
                assert_eq!(
                    status,
                    reqwest::StatusCode::OK,
                    "{operation}, existing={existing}: {grant}"
                );
                let expected_region = if existing {
                    "north-america-east"
                } else {
                    "north-america-west"
                };
                assert_eq!(grant["homeRegion"], expected_region);
                assert_eq!(*provisioner.calls.lock().unwrap(), vec![expected_region]);
                if operation == "find-websocket" {
                    let url = reqwest::Url::parse(grant["websocketUrl"].as_str().unwrap())?;
                    assert_eq!(url.scheme(), "wss");
                    let key = url
                        .query_pairs()
                        .find(|(name, _)| name == "key")
                        .unwrap()
                        .1
                        .into_owned();
                    let ticket = issuer.verify_socket(&key)?;
                    assert_eq!(ticket.region, expected_region);
                    assert_eq!(ticket.actor, actor);
                    assert_eq!(ticket.metadata, body["metadata"]);
                }
                body["homeRegion"] = "north-america-east".into();
                assert_eq!(
                    client
                        .post(&url)
                        .bearer_auth("api-key")
                        .json(&body)
                        .send()
                        .await?
                        .status(),
                    reqwest::StatusCode::CONFLICT
                );
                if existing {
                    body["homeRegion"] = "north-america-west".into();
                    assert_eq!(
                        client
                            .post(&url)
                            .bearer_auth("api-key")
                            .json(&body)
                            .send()
                            .await?
                            .status(),
                        reqwest::StatusCode::CONFLICT
                    );
                }
                server.abort();
            }
        }
    }
    Ok(())
}
