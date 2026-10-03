use super::*;

#[tokio::test]
async fn reservations_coalesce_by_actor_and_publish_only_the_current_token() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let store = LocalHostStore::open(directory.path().join("hosts.db")).await?;
    let request = request("one");
    let claims =
        futures_util::future::try_join_all((0..8).map(|_| store.reserve(&request))).await?;
    assert_eq!(claims.iter().filter(|claim| claim.created).count(), 1);
    assert!(claims.iter().all(|claim| claim.token == claims[0].token));
    assert!(store.reserve(&self::request("two")).await?.created);
    let token = &claims[0].token;
    let lease = lease(&request);
    store.retire(token).await?;
    assert!(!store.publish(token, &lease, 1).await?);
    store.finish(token, "retired").await?;
    let replacement = store.reserve(&request).await?;
    assert_ne!(replacement.token, *token);
    assert!(!store.publish(token, &lease, 1).await?);
    assert!(store.publish(&replacement.token, &lease, 2).await?);
    assert_eq!(
        store.host(request.host_id.as_str()).await?.unwrap().epoch,
        2
    );
    Ok(())
}

#[tokio::test]
async fn retirement_and_shutdown_reject_reservations_until_their_lifecycle_allows_them()
-> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("hosts.db");
    let store = LocalHostStore::open(path.clone()).await?;
    let request = request("one");
    let claim = store.reserve(&request).await?;
    let retired = store.retire_config("config").await?;
    assert_eq!(
        retired,
        vec![(claim.token.clone(), request.host_id.as_str().into())]
    );
    assert!(store.reserve(&self::request("two")).await.is_err());
    store.finish(&claim.token, "retired").await?;
    assert!(store.reserve(&request).await.is_err());
    let mut replacement = request.clone();
    replacement.host_config_key = "new-config".into();
    assert!(store.reserve(&replacement).await?.created);
    store.shutdown().await?;
    assert!(store.reserve(&self::request("two")).await.is_err());
    drop(store);
    let reopened = LocalHostStore::open(path).await?;
    assert!(reopened.host(request.host_id.as_str()).await?.is_none());
    assert!(reopened.reserve(&request).await?.created);
    Ok(())
}

fn request(id: &str) -> EnsureHostRequest {
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
        host_config_key: "config".into(),
        canonical_region: "north-america-east".into(),
        host_id: crate::host::HostId::new(format!("host-{id}")),
        session_id: format!("session-{id}"),
        host_token: "unused".into(),
        jwt_public_keys: "unused".into(),
        control_plane_url: "http://127.0.0.1:7100".into(),
        jwt_issuer: "local".into(),
        invocation_jwt_audience: "local".into(),
        image_ref: "local".into(),
        working_directory: "/project".into(),
        actor_entrypoint: None,
        secret_refs: vec![],
        host_idle_timeout_ms: 300_000,
    }
}

fn lease(request: &EnsureHostRequest) -> HostLease {
    HostLease {
        id: request.host_id.clone(),
        session_id: request.session_id.clone(),
        route: "http://127.0.0.1:7101".into(),
        expires_at_ms: 60_000,
    }
}
