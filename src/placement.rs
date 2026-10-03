#[cfg(test)]
#[path = "../tests/support/placement.rs"]
pub(crate) mod testing;

use anyhow::{Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{actor_state::ActorStorageKey, host::HostId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectPlacement {
    pub lease: crate::host_leases::HostLease,
    pub object: ActorStorageKey,
    pub owner: HostId,
    pub owner_epoch: u64,
    pub home_region: String,
    pub state_version: u64,
    pub state_object: Option<String>,
    pub last_request_id: Option<String>,
}

#[async_trait]
pub trait ObjectPlacementStore: Send + Sync {
    async fn get_owner(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        Ok(self.get_owner_with_hint(object).await?.0)
    }

    async fn get_owner_with_hint(
        &self,
        object: &ActorStorageKey,
    ) -> Result<(
        Option<ObjectPlacement>,
        Option<crate::bucket::OwnershipHint>,
    )>;

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>>;
}

pub fn validate_region(region: &str) -> Result<()> {
    ensure!(
        !region.is_empty()
            && region.len() <= 64
            && region.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            }),
        "sandbox region is invalid"
    );
    Ok(())
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ActorResidency {
    Live,
    Dormant,
    Unknown,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInstanceOverview {
    pub actor_id: String,
    pub status: ActorResidency,
    pub connections: Vec<ActorConnectionInventory>,
    pub waiting: Option<Vec<crate::host_leases::WaitingOperation>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorConnectionInventory {
    pub id: String,
    pub metadata: serde_json::Value,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInventory {
    pub actor_name: String,
    pub live: u64,
    pub dormant: u64,
    pub unknown: u64,
    pub instances: Vec<ActorInstanceOverview>,
}

#[async_trait]
pub trait ActorInventoryReader: Send + Sync {
    async fn actor_inventory(&self, project_id: &str) -> Result<ActorInventorySnapshot>;
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActorInventorySnapshot {
    pub actors: Vec<ActorInventory>,
    pub connections_complete: bool,
}
