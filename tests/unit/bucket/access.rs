use super::*;

#[test]
fn credentials_are_scoped_to_one_actor_and_its_deployed_code() -> Result<()> {
    let actor = crate::actor::ActorKey {
        project_id: "tenant".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let mut code = crate::artifacts::ArtifactManifest {
        bucket: "code".into(),
        files: vec![crate::artifacts::ArtifactFile {
            path: "actors.mjs".into(),
            object: "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/actors.mjs"
                .into(),
            generation: 1,
            sha256: "unused".into(),
        }],
    };
    let value = boundary(
        "authority",
        &super::super::PersistenceConfig::Local,
        Some("code"),
        &actor,
        Some(&code),
    )?;
    let rules = value["accessBoundary"]["accessBoundaryRules"]
        .as_array()
        .unwrap();
    assert_eq!(rules.len(), 3);
    assert_eq!(
        rules[0]["availabilityCondition"]["expression"],
        format!(
            "resource.name == {}",
            serde_json::to_string(&format!(
                "projects/_/buckets/authority/objects/{}",
                crate::storage_paths::owner(&actor.storage_key())?
            ))?
        )
    );
    assert_eq!(
        rules[2]["availabilityCondition"]["expression"],
        format!(
            "resource.name.startsWith({})",
            serde_json::to_string(
                "projects/_/buckets/code/objects/durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/"
            )?
        )
    );
    assert_eq!(
        rules[2]["availablePermissions"],
        json!(["inRole:roles/storage.objectViewer"])
    );
    for index in 0..1000 {
        let path = format!("modules/{index}.mjs");
        code.files.push(crate::artifacts::ArtifactFile {
            object: format!(
                "durable-actors/v3/artifacts/12345678-1234-4234-8234-123456789012/{path}"
            ),
            path,
            generation: 1,
            sha256: "unused".into(),
        });
    }
    let large = boundary(
        "authority",
        &super::super::PersistenceConfig::Local,
        Some("code"),
        &actor,
        Some(&code),
    )?;
    assert!(serde_json::to_vec(&large)?.len() < 2048);
    code.files[1].object =
        "durable-actors/v3/artifacts/87654321-1234-4234-8234-123456789012/modules/0.mjs".into();
    assert!(
        boundary(
            "authority",
            &super::super::PersistenceConfig::Local,
            Some("code"),
            &actor,
            Some(&code)
        )
        .is_err()
    );
    code.files.truncate(1);
    let mut other = actor.clone();
    other.project_id = "other".into();
    assert_ne!(
        value,
        boundary(
            "authority",
            &super::super::PersistenceConfig::Local,
            Some("code"),
            &other,
            Some(&code)
        )?
    );
    assert!(
        boundary(
            "authority",
            &super::super::PersistenceConfig::Local,
            Some("different-bucket"),
            &actor,
            Some(&code)
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn rapid_credentials_cover_only_the_actor_in_each_configured_bucket() -> Result<()> {
    let actor = crate::actor::ActorKey {
        project_id: "tenant".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let config: super::super::PersistenceConfig = serde_json::from_value(
        json!({"type":"rapid", "archive_bucket":"archive-test", "buckets":[
            {"bucket":"rapid-test-a", "zone":"us-west4-a"}, {"bucket":"rapid-test-b", "zone":"us-west4-b"}
        ]}),
    )?;
    let value = boundary("authority", &config, None, &actor, None)?;
    let rules = value["accessBoundary"]["accessBoundaryRules"]
        .as_array()
        .unwrap();
    assert_eq!(rules.len(), 4);
    let prefix = super::super::rapid::object_name(&crate::storage_paths::snapshots(&actor)?)?;
    for (rule, bucket) in rules[1..]
        .iter()
        .zip(["archive-test", "rapid-test-a", "rapid-test-b"])
    {
        assert_eq!(
            rule["availableResource"],
            format!("//storage.googleapis.com/projects/_/buckets/{bucket}")
        );
        assert!(
            rule["availabilityCondition"]["expression"]
                .as_str()
                .unwrap()
                .contains(&prefix)
        );
    }
    for rule in &rules[2..] {
        assert!(
            rule["availabilityCondition"]["expression"]
                .as_str()
                .unwrap()
                .contains(&prefix.replacen("snapshots-", "uploads-", 1))
        );
    }
    Ok(())
}
