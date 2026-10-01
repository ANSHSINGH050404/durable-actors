use crate::{
    actor::ActorState,
    litestream::{Litestream, Replicator, storage::read_fields},
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::sync::Arc;

pub async fn replication() -> Arc<dyn Replicator> {
    Arc::new(
        Litestream::start("litestream".into())
            .await
            .expect("test Litestream"),
    )
}

pub fn fields(state: Option<&ActorState>) -> Result<Value> {
    let state = state.context("executor hydration")?;
    let database = rusqlite::Connection::open(state.sqlite.path.as_ref().context("SQLite path")?)?;
    read_fields(&database)
}

pub async fn write_fields(state: Option<&ActorState>, fields: Value) -> Result<ActorState> {
    let state = state.context("executor hydration")?;
    commit_fields(state, fields)?;
    sync(state).await
}

fn commit_fields(state: &ActorState, fields: Value) -> Result<()> {
    let mut database =
        rusqlite::Connection::open(state.sqlite.path.as_ref().context("SQLite path")?)?;
    database.execute_batch("PRAGMA wal_autocheckpoint=0")?;
    let transaction = database.transaction()?;
    for (name, value) in fields.as_object().context("object fields")? {
        transaction.execute("INSERT INTO __terse_fields VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value=excluded.value WHERE value<>excluded.value", [name.as_str(), &value.to_string()])?;
    }
    transaction.commit()?;
    Ok(())
}

pub async fn sync(state: &ActorState) -> Result<ActorState> {
    let reply: Value = reqwest::Client::builder()
        .unix_socket(state.sqlite.socket.clone().context("replication socket")?)
        .build()?
        .post("http://localhost/sync")
        .json(&json!({"path": state.sqlite.path, "wait": true, "timeout": 10}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let mut state = state.clone();
    state.sqlite.txid = reply["txid"].as_u64().context("replicated transaction")?;
    Ok(state)
}
