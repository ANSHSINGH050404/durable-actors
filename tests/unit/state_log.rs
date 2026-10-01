use super::*;
use serde_json::json;

#[test]
fn round_trips_sqlite_commit_and_result() -> Result<()> {
    let sqlite = crate::test_sqlite::snapshot(json!({"count": 3, "nested": [true, null]}))?;
    let snapshot = StateSnapshot::new(1, 2, "request".into(), sqlite, json!(3))?;
    let restored = StateSnapshot::decode(&snapshot.encode()?)?;
    assert_eq!(
        serde_json::to_value(restored)?,
        serde_json::to_value(snapshot)?
    );
    Ok(())
}

#[test]
fn rejects_invalid_commit_metadata_and_transaction_ranges() -> Result<()> {
    let sqlite = crate::test_sqlite::snapshot(json!({}))?;
    let valid = serde_json::to_value(StateSnapshot::new(
        1,
        1,
        "request".into(),
        sqlite,
        Value::Null,
    )?)?;
    for (field, value) in [
        ("stateVersion", json!(0)),
        ("ownerEpoch", json!(0)),
        ("requestId", json!("")),
        ("requestId", json!("x".repeat(256))),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(StateSnapshot::decode(&serde_json::to_vec(&invalid)?).is_err());
    }
    for (field, value) in [("txid", json!(0)), ("files", json!([]))] {
        let mut invalid = valid.clone();
        invalid["sqlite"][field] = value;
        assert!(StateSnapshot::decode(&serde_json::to_vec(&invalid)?).is_err());
    }
    for (field, value) in [
        ("first", json!(2)),
        ("last", json!(0)),
        ("level", json!(7)),
        ("data", json!("invalid")),
    ] {
        let mut invalid = valid.clone();
        invalid["sqlite"]["files"][0][field] = value;
        assert!(StateSnapshot::decode(&serde_json::to_vec(&invalid)?).is_err());
    }
    Ok(())
}
