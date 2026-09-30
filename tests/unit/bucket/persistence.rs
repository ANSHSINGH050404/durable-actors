use super::*;
use serde_json::json;

#[test]
fn rapid_requires_exactly_two_distinct_zones_and_separate_archive() -> Result<()> {
    let good = json!({"type":"rapid", "archive_bucket":"archive-test", "buckets":[
        {"bucket":"rapid-test-a", "zone":"us-west4-a"},
        {"bucket":"rapid-test-b", "zone":"us-west4-b"}
    ]});
    serde_json::from_value::<PersistenceConfig>(good.clone())?.validate()?;
    let mut one = good.clone();
    one["buckets"].as_array_mut().unwrap().pop();
    assert!(
        serde_json::from_value::<PersistenceConfig>(one)?
            .validate()
            .is_err()
    );
    let mut three = good.clone();
    three["buckets"]
        .as_array_mut()
        .unwrap()
        .push(json!({"bucket":"rapid-test-c", "zone":"us-west4-c"}));
    assert!(
        serde_json::from_value::<PersistenceConfig>(three)?
            .validate()
            .is_err()
    );
    let mut same_archive = good.clone();
    same_archive["archive_bucket"] = json!("rapid-test-a");
    assert!(
        serde_json::from_value::<PersistenceConfig>(same_archive)?
            .validate()
            .is_err()
    );
    for field in ["bucket", "zone"] {
        let mut config = good.clone();
        config["buckets"][1][field] = config["buckets"][0][field].clone();
        assert!(
            serde_json::from_value::<PersistenceConfig>(config)?
                .validate()
                .is_err()
        );
    }
    Ok(())
}
