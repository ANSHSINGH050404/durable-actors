use std::{collections::HashMap, sync::Mutex};

use anyhow::Result;
use async_trait::async_trait;

use super::{ObjectPlacement, ObjectPlacementStore, validate_region};
use crate::{actor_state::ActorStorageKey, host_leases::HostLease};

#[derive(Default)]
pub(crate) struct LocalObjectPlacementStore {
    placements: Mutex<HashMap<ActorStorageKey, ObjectPlacement>>,
}

#[async_trait]
impl ObjectPlacementStore for LocalObjectPlacementStore {
    async fn get_owner_with_hint(
        &self,
        object: &ActorStorageKey,
    ) -> Result<(
        Option<ObjectPlacement>,
        Option<crate::bucket::OwnershipHint>,
    )> {
        Ok((self.get(object).await?, None))
    }

    async fn get(&self, object: &ActorStorageKey) -> Result<Option<ObjectPlacement>> {
        Ok(self
            .placements
            .lock()
            .map_err(|_| anyhow::anyhow!("object placement lock poisoned"))?
            .get(object)
            .cloned())
    }
}

impl LocalObjectPlacementStore {
    pub fn set_owner(
        &self,
        object: &ActorStorageKey,
        lease: HostLease,
        home_region: &str,
    ) -> Result<()> {
        validate_region(home_region)?;
        self.placements.lock().unwrap().insert(
            object.clone(),
            ObjectPlacement {
                owner: lease.id.clone(),
                lease,
                object: object.clone(),
                owner_epoch: 1,
                home_region: home_region.into(),
                state_version: 0,
                state_object: None,
                last_request_id: None,
            },
        );
        Ok(())
    }
}
