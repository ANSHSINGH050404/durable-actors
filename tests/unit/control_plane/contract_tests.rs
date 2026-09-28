use std::sync::Arc;

use anyhow::Result;
use serde_json::{Value, json};

use super::PublicActorContract;
use crate::{
    control_plane::admin::{
        AdminRegistry, HostLaunchSpec, LocalAdminRegistry, PostgresAdminRegistry,
    },
    postgres::{PostgresDatabase, testing::with_postgres},
};

#[test]
fn published_declarations_and_dependencies_are_preserved_and_hashed() -> Result<()> {
    let mut document: Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    let original = PublicActorContract::new(document.clone())?;
    document["typescript"]["dependencies"] = json!({"ai": "7.0.97"});
    document["actors"][0]["description"] = json!("A documented actor.");
    document["actors"][0]["rpc"]["methods"][0]["description"] = json!("A documented RPC.");
    let changed = PublicActorContract::new(document.clone())?;
    assert_ne!(original.hash(), changed.hash());
    assert_eq!(changed.document(), &document);
    document["typescript"]["declarations"] = json!("");
    assert!(PublicActorContract::new(document).is_err());
    Ok(())
}

#[test]
fn compiler_contract_is_preserved_and_hashes_ignore_json_object_key_order() -> Result<()> {
    let value: Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    let contract = PublicActorContract::new(value.clone())?;
    assert_eq!(contract.document(), &value);
    let first = PublicActorContract::new(serde_json::from_str(
        r#"{"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}}"#,
    )?)?;
    let second = PublicActorContract::new(serde_json::from_str(
        r#"{"typescript":{"dependencies":{},"declarations":"export interface ActorTypes {}"},"actors":[],"version":1}"#,
    )?)?;
    assert_eq!(first.hash(), second.hash());
    assert_ne!(contract.hash(), first.hash());
    Ok(())
}

#[test]
fn malformed_or_nonportable_contracts_are_rejected() -> Result<()> {
    let valid: Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    for invalid in [
        json!({}),
        json!({"version":2,"actors":[]}),
        json!({"version":1,"actors":{}}),
    ] {
        assert!(PublicActorContract::new(invalid).is_err());
    }
    for (pointer, replacement) in [
        ("/actors/0/actorName", json!("different")),
        ("/actors/0/rpc/methods/0/name", json!("then")),
        ("/actors/0/rpc/methods/0/result/kind", json!("anything")),
        (
            "/actors/0/rpc/methods/1/parameters/0/type/$ref",
            json!("https://example.com/types.json"),
        ),
        (
            "/actors/0/rpc/methods/1/result/type/$ref",
            json!("#/definitions/Missing"),
        ),
        (
            "/actors/0/rpc/methods/1/parameters/0/optional",
            json!("yes"),
        ),
    ] {
        let mut malformed = valid.clone();
        *malformed.pointer_mut(pointer).expect(pointer) = replacement;
        assert!(PublicActorContract::new(malformed).is_err(), "{pointer}");
    }
    for sandbox in [
        json!({"cpu":0}),
        json!({"memoryMiB":127}),
        json!({"idleTimeoutMs":0}),
        json!({"regions":["unknown"]}),
        json!({"regions":["canada","canada"]}),
    ] {
        let mut malformed = valid.clone();
        malformed["actors"][0]["sandbox"] = sandbox;
        assert!(PublicActorContract::new(malformed).is_err());
    }
    let mut duplicate = valid.clone();
    duplicate["actors"]
        .as_array_mut()
        .unwrap()
        .push(valid["actors"][0].clone());
    assert!(PublicActorContract::new(duplicate).is_err());
    let mut external = valid;
    external["actors"][0]["rpc"]["schema"]["definitions"]["External"] =
        json!({"$ref":"file:///private/example"});
    assert!(PublicActorContract::new(external).is_err());
    Ok(())
}

#[test]
fn actor_names_must_be_typescript_identifiers() -> Result<()> {
    let valid: Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    for name in [
        "chat-room",
        "chat.room",
        "1ChatRoom",
        "123",
        "-",
        ".",
        "class",
        "interface",
        "constructor",
        "type",
        "async",
        "await",
        "using",
        "satisfies",
        "undefined",
        "defer",
    ] {
        let mut document = valid.clone();
        document["actors"][0]["actorName"] = json!(name);
        document["actors"][0]["socket"]["actorName"] = json!(name);
        let error = PublicActorContract::new(document).unwrap_err();
        assert!(
            error.to_string().contains("TypeScript identifier"),
            "{name}: {error}"
        );
    }
    for name in [
        "ChatRoom",
        "_",
        "_chat2",
        "chat_room",
        "Class",
        "class1",
        "asyncActor",
    ] {
        let mut document = valid.clone();
        document["actors"][0]["actorName"] = json!(name);
        document["actors"][0]["socket"]["actorName"] = json!(name);
        PublicActorContract::new(document)?;
    }
    Ok(())
}

#[test]
fn rest_parameters_require_array_schemas_without_tuple_items() -> Result<()> {
    let valid: Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    for (schema, accepted) in [
        (json!(true), false),
        (json!(false), false),
        (json!({"type": "string"}), false),
        (json!({"items": {"type": "string"}}), false),
        (json!({"type": ["array", "null"]}), false),
        (json!({"type": "array", "items": []}), false),
        (
            json!({"type": "array", "items": [{"type": "string"}]}),
            false,
        ),
        (json!({"type": "array", "items": {"type": "string"}}), true),
        (json!({"type": "array", "items": true}), true),
        (json!({"type": "array"}), true),
    ] {
        let mut document = valid.clone();
        document["actors"][0]["rpc"]["schema"]["definitions"]["Method_sendMessage_Parameter_0"] =
            schema.clone();
        PublicActorContract::new(document.clone())?;
        document["actors"][0]["rpc"]["methods"][1]["parameters"][0]["rest"] = json!(true);
        let result = PublicActorContract::new(document);
        assert_eq!(result.is_ok(), accepted, "{schema}: {result:?}");
    }
    Ok(())
}

#[tokio::test]
async fn in_memory_contract_replaces_the_current_deployment_atomically() -> Result<()> {
    registry_behavior(Arc::new(LocalAdminRegistry::default())).await
}

#[tokio::test]
async fn postgres_latest_contract_is_atomic_and_survives_reconnection() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        registry_behavior(Arc::new(PostgresAdminRegistry::from_database(database))).await?;
        let registry =
            PostgresAdminRegistry::from_database(PostgresDatabase::connect(&fixture.url).await?);
        let contract = PublicActorContract::new(json!({"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}}))?;
        let deployment = spec();
        registry
            .register_deployment(&deployment, Some(&contract))
            .await?;
        drop(registry);
        let reopened =
            PostgresAdminRegistry::from_database(PostgresDatabase::connect(&fixture.url).await?);
        assert_eq!(
            reopened
                .deployment_contract("default")
                .await?
                .unwrap()
                .contract,
            *contract.document()
        );
        Ok(())
    })
    .await
}

async fn registry_behavior(registry: Arc<dyn AdminRegistry>) -> Result<()> {
    let mut deployment = spec();
    let empty = PublicActorContract::new(
        json!({"version":1,"actors":[],"typescript":{"declarations":"export interface ActorTypes {}","dependencies":{}}}),
    )?;
    let full = PublicActorContract::new(serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?)?;
    assert!(registry.deployment_contract("default").await?.is_none());
    assert!(
        registry
            .register_deployment(&deployment, Some(&empty))
            .await?
    );
    assert!(
        !registry
            .register_deployment(&deployment, Some(&empty))
            .await?
    );
    let first = registry.deployment_contract("default").await?.unwrap();
    assert_eq!(first.contract_hash, empty.hash());
    assert_eq!(first.contract, *empty.document());

    deployment.image_ref = "updated-image".into();
    assert!(
        registry
            .register_deployment(&deployment, Some(&full))
            .await?
    );
    assert_eq!(
        registry.launch_spec("default").await?,
        Some(deployment.clone())
    );
    assert_eq!(
        registry
            .deployment_contract("default")
            .await?
            .unwrap()
            .contract,
        *full.document()
    );
    assert!(
        registry
            .register_deployment(&deployment, Some(&empty))
            .await?
    );
    assert_eq!(
        registry
            .deployment_contract("default")
            .await?
            .unwrap()
            .contract,
        *empty.document()
    );
    assert!(registry.register_deployment(&deployment, None).await?);
    assert!(registry.deployment_contract("default").await?.is_none());

    let mut other = deployment.clone();
    other.image_ref = "other-image".into();
    let (left, right) = tokio::join!(
        registry.register_deployment(&deployment, Some(&empty)),
        registry.register_deployment(&other, Some(&full))
    );
    left?;
    right?;
    let active = registry.launch_spec("default").await?.unwrap();
    let expected = if active == deployment { empty } else { full };
    assert_eq!(
        registry
            .deployment_contract("default")
            .await?
            .unwrap()
            .contract,
        *expected.document()
    );
    registry.remove_deployment("default").await?;
    assert!(registry.launch_spec("default").await?.is_none());
    assert!(registry.deployment_contract("default").await?.is_none());
    Ok(())
}

fn spec() -> HostLaunchSpec {
    HostLaunchSpec {
        sandboxes: Default::default(),
        project_id: "default".into(),
        source: None,
        code_snapshot: None,
        image_ref: "image".into(),
        working_directory: "/app".into(),
        actor_entrypoint: None,
        secret_refs: vec![],
    }
}

#[test]
fn python_contracts_publish_without_typescript_declarations() -> Result<()> {
    let mut document: Value = serde_json::from_str(include_str!(
        "../../../sdk/tests/fixtures/public-contract.json"
    ))?;
    document.as_object_mut().unwrap().remove("typescript");
    let contract = PublicActorContract::new(document.clone())?;
    assert_eq!(contract.document(), &document);
    Ok(())
}
