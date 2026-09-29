use super::*;
use serde_json::json;

#[test]
fn preserves_encoded_state_when_forwarding_a_snapshot() -> Result<()> {
    let bytes = br#"{"stateVersion":1,"ownerEpoch":2,"requestId":"request-1","state":{ "value": "\u0061", "nested": [1, true, null] },"result":null}"#;
    let snapshot = StateSnapshot::decode(bytes)?;
    assert_eq!(snapshot.encode()?, bytes);
    Ok(())
}

#[test]
fn rejects_invalid_snapshots_at_decode() {
    let valid = json!({
        "stateVersion": 1, "ownerEpoch": 1, "requestId": "request-1",
        "state": {}, "result": null,
    });
    for (field, value) in [
        ("stateVersion", json!(0)),
        ("ownerEpoch", json!(0)),
        ("requestId", json!("")),
        ("requestId", json!("x".repeat(256))),
        ("state", json!([])),
        ("state", Value::Null),
        ("state", json!("{}")),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(StateSnapshot::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
    }
    assert!(StateSnapshot::decode(br#"{"stateVersion":1,"ownerEpoch":1,"requestId":"r","state":{"value":},"result":null}"#).is_err());
}
