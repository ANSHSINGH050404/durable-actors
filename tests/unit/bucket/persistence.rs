use super::*;
use serde_json::json;

#[test]
fn rapid_defaults_to_two_acknowledged_zones() -> Result<()> {
    let config: PersistenceConfig = serde_json::from_value(json!({
        "type": "rapid", "archive_bucket": "archive-test",
        "buckets": [
            {"bucket": "rapid-test-a", "zone": "us-west4-a"},
            {"bucket": "rapid-test-b", "zone": "us-west4-b"}
        ]
    }))?;
    config.validate()?;
    assert_eq!(serde_json::to_value(config)?["ack_zones"], 2);
    Ok(())
}

#[test]
fn rapid_requires_distinct_buckets_and_acknowledged_zones() {
    let good = json!({"type":"rapid", "archive_bucket":"archive-test", "buckets":[
        {"bucket":"rapid-test-a", "zone":"us-west4-a"},
        {"bucket":"rapid-test-b", "zone":"us-west4-b"}
    ]});
    for (path, value) in [
        (vec!["ack_zones"], json!(0)),
        (vec!["ack_zones"], json!(3)),
        (vec!["archive_bucket"], json!("rapid-test-a")),
    ] {
        let mut config = good.clone();
        config[path[0]] = value;
        assert!(
            serde_json::from_value::<PersistenceConfig>(config)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    for field in ["bucket", "zone"] {
        let mut config = good.clone();
        config["buckets"][1][field] = config["buckets"][0][field].clone();
        assert!(
            serde_json::from_value::<PersistenceConfig>(config)
                .unwrap()
                .validate()
                .is_err()
        );
    }
}
