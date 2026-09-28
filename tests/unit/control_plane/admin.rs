use super::*;

#[test]
fn secret_changes_update_host_configuration() {
    let mut deployment = spec("image-1");
    let initial = deployment.host_config_key();
    deployment.secret_refs = vec!["secrets-first".into()];
    let first = deployment.host_config_key();
    assert_ne!(initial, first);
    deployment.secret_refs = vec!["secrets-second".into()];
    assert_ne!(first, deployment.host_config_key());
    deployment.secret_refs.clear();
    assert_eq!(initial, deployment.host_config_key());
}

#[tokio::test]
async fn deployment_retains_secret_references() -> Result<()> {
    let registry = LocalAdminRegistry::default();
    let mut deployment = spec("im-1");
    deployment.secret_refs = vec!["project-secrets".into()];
    assert!(registry.register_test_deployment(&deployment).await?);
    assert_eq!(
        registry.launch_spec("default").await?,
        Some(deployment.clone())
    );
    deployment.secret_refs = vec!["replacement-secrets".into()];
    assert!(registry.register_test_deployment(&deployment).await?);
    Ok(())
}

#[tokio::test]
async fn registration_replaces_the_projects_deployment() -> Result<()> {
    let registry = LocalAdminRegistry::default();
    assert!(registry.register_test_deployment(&spec("im-1")).await?);
    assert!(!registry.register_test_deployment(&spec("im-1")).await?);
    let replacement = spec("im-2");

    assert!(registry.register_test_deployment(&replacement).await?);
    assert_eq!(registry.launch_spec("default").await?, Some(replacement));
    Ok(())
}

#[tokio::test]
async fn postgres_registration_replaces_the_single_deployment_atomically() -> Result<()> {
    crate::postgres::testing::with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let registry = PostgresAdminRegistry::from_database(database.clone());
        let mut deployment = spec("image-1");
        let initial_key = deployment.host_config_key();
        deployment.sandboxes.insert(
            "Counter".into(),
            serde_json::from_value(serde_json::json!({"cpu":2,"regions":["canada"]}))?,
        );
        assert_ne!(initial_key, deployment.host_config_key());
        deployment.secret_refs = vec!["project-secrets".into()];
        deployment.source = Some(DeploymentSource {
            image_ref: "im-source".into(),
            working_directory: "/project".into(),
            actor_entrypoint: Some("src/actors.ts".into()),
        });

        assert!(registry.register_test_deployment(&deployment).await?);
        assert_eq!(
            registry.launch_spec("default").await?,
            Some(deployment.clone())
        );
        assert!(!registry.register_test_deployment(&deployment).await?);
        deployment.source.as_mut().unwrap().image_ref = "im-updated".into();
        assert!(registry.register_test_deployment(&deployment).await?);
        assert_eq!(registry.launch_spec("default").await?, Some(deployment));
        registry.remove_deployment("default").await?;
        assert_eq!(registry.launch_spec("default").await?, None);
        registry.remove_deployment("default").await?;
        Ok(())
    })
    .await
}

fn spec(image: &str) -> HostLaunchSpec {
    HostLaunchSpec {
        sandboxes: Default::default(),
        project_id: "default".into(),
        source: None,
        code_snapshot: None,
        image_ref: image.into(),
        working_directory: "/workspace".into(),
        actor_entrypoint: Some("src/actors.ts".into()),
        secret_refs: vec![],
    }
}

#[tokio::test]
async fn projects_keep_independent_deployments_and_host_identities() -> Result<()> {
    let registry = LocalAdminRegistry::default();
    let deployment = |project: &str| -> HostLaunchSpec {
        let mut value = serde_json::to_value(spec("same-image")).unwrap();
        value["projectId"] = project.into();
        serde_json::from_value(value).unwrap()
    };
    let first = deployment("team-a");
    let second = deployment("team-b");
    assert_ne!(first.host_config_key(), second.host_config_key());
    registry.register_test_deployment(&first).await?;
    registry.register_test_deployment(&second).await?;
    let mut deployments = registry.launch_specs().await?;
    deployments.sort_by(|a, b| a.project_id.cmp(&b.project_id));
    assert_eq!(deployments, vec![first.clone(), second.clone()]);
    assert_eq!(registry.launch_spec("team-a").await?, Some(first));
    assert_eq!(registry.launch_spec("team-b").await?, Some(second.clone()));
    registry.remove_deployment("team-a").await?;
    assert_eq!(registry.launch_spec("team-a").await?, None);
    assert_eq!(registry.launch_spec("team-b").await?, Some(second));
    Ok(())
}

#[tokio::test]
async fn postgres_projects_keep_deployments_contracts_and_deletions_separate() -> Result<()> {
    crate::postgres::testing::with_postgres(async |fixture| {
        let registry =
            PostgresAdminRegistry::from_database(PostgresDatabase::connect(&fixture.url).await?);
        let contract = PublicActorContract::new(serde_json::from_str(include_str!(
            "../../../sdk/tests/fixtures/public-contract.json"
        ))?)?;
        let mut first = spec("first-image");
        first.project_id = "team-a".into();
        let mut second = spec("second-image");
        second.project_id = "team-b".into();
        registry
            .register_deployment(&first, Some(&contract))
            .await?;
        registry
            .register_deployment(&second, Some(&contract))
            .await?;
        let mut deployments = registry.launch_specs().await?;
        deployments.sort_by(|a, b| a.project_id.cmp(&b.project_id));
        assert_eq!(deployments, vec![first.clone(), second.clone()]);
        assert_eq!(registry.launch_spec("team-a").await?, Some(first.clone()));
        assert_eq!(registry.launch_spec("team-b").await?, Some(second.clone()));

        first.secret_refs = vec!["team-a-secret".into()];
        registry
            .register_deployment(&first, Some(&contract))
            .await?;
        assert_eq!(registry.launch_spec("team-b").await?, Some(second.clone()));
        assert!(registry.deployment_contract("team-b").await?.is_some());
        registry.remove_deployment("team-a").await?;
        assert!(registry.deployment_contract("team-a").await?.is_none());
        assert!(registry.deployment_contract("team-b").await?.is_some());
        assert_eq!(registry.launch_spec("team-b").await?, Some(second));
        Ok(())
    })
    .await
}
