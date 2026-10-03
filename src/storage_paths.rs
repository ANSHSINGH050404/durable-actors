use crate::{actor::ActorKey, actor_state::ActorStorageKey, host::HostId};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};

pub const ROOT: &str = "durable-actors/v3/";

pub fn snapshots(actor: &ActorKey) -> Result<String> {
    actor.validate()?;
    Ok(format!("{ROOT}snapshots/{}/", actor_path(actor)))
}

pub fn owner(object: &ActorStorageKey) -> Result<String> {
    let actor = actor_from_key(object)?;
    Ok(format!("{ROOT}owners/{}.json", actor_path(&actor)))
}

pub fn owner_in_project(key: &str, project: &str) -> bool {
    key.strip_prefix(&format!("{ROOT}owners/"))
        .and_then(|path| path.split('/').nth(1))
        .is_some_and(|encoded| encoded == component(project))
}

pub fn host(host: &HostId) -> String {
    format!("{ROOT}hosts/{}/", component(host.as_str()))
}

pub fn session(host_id: &HostId, session: &str) -> String {
    format!("{}sessions/{}", host(host_id), component(session))
}

pub fn actor_from_snapshot(object: &str) -> Result<ActorKey> {
    let parts: Vec<_> = object
        .strip_prefix(ROOT)
        .context("invalid snapshot root")?
        .split('/')
        .collect();
    ensure!(
        parts.len() == 7 && parts[0] == "snapshots",
        "invalid snapshot path"
    );
    let actor = ActorKey {
        project_id: decode(parts[2])?,
        actor_name: decode(parts[3])?,
        actor_id: decode(parts[4])?,
    };
    actor.validate()?;
    ensure!(
        object.starts_with(&snapshots(&actor)?),
        "snapshot shard mismatch"
    );
    Ok(actor)
}

pub fn actor_from_key(key: &ActorStorageKey) -> Result<ActorKey> {
    let parts: Vec<_> = key
        .as_str()
        .strip_prefix("object.v4.")
        .context("invalid actor key")?
        .split(':')
        .collect();
    ensure!(parts.len() == 3, "invalid actor identity");
    let actor = ActorKey {
        project_id: parts[0].into(),
        actor_name: parts[1].into(),
        actor_id: parts[2].into(),
    };
    actor.validate()?;
    Ok(actor)
}

fn actor_path(actor: &ActorKey) -> String {
    let hash = aws_lc_rs::digest::digest(
        &aws_lc_rs::digest::SHA256,
        actor.storage_key().as_str().as_bytes(),
    );
    format!(
        "{:02x}/{}/{}/{}",
        hash.as_ref()[0],
        component(&actor.project_id),
        component(&actor.actor_name),
        component(&actor.actor_id)
    )
}

fn component(value: &str) -> String {
    URL_SAFE_NO_PAD.encode(value)
}
fn decode(value: &str) -> Result<String> {
    Ok(String::from_utf8(URL_SAFE_NO_PAD.decode(value)?)?)
}
