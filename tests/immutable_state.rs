#[path = "fixtures/sqlite.rs"]
mod sqlite;
use anyhow::Result;
use durable_actors::state_log::StateSnapshot;
use serde_json::json;

#[test]
fn immutable_snapshot_round_trips_state_and_result() -> Result<()> {
    let snapshot = StateSnapshot::new(
        7,
        3,
        "request-7".into(),
        sqlite::snapshot(json!({ "count": 7 }))?,
        json!(7),
    )?;

    let decoded = StateSnapshot::decode(&snapshot.encode()?)?;

    assert_eq!(decoded.state_version, 7);
    assert_eq!(decoded.owner_epoch, 3);
    assert_eq!(decoded.request_id, "request-7");
    assert_eq!(
        serde_json::to_value(decoded.sqlite)?,
        serde_json::to_value(snapshot.sqlite)?
    );
    assert_eq!(decoded.result, json!(7));
    Ok(())
}
