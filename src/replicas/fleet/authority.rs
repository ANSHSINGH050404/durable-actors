use crate::{actor::ActorKey, bucket::Bucket, clock::Clock, host::HostId, host_leases::HostLease};
use anyhow::{Context, Result, ensure};
use serde::Deserialize;
use std::sync::Arc;

pub(super) struct Authority {
    bucket: Arc<dyn Bucket>,
    clock: Arc<dyn Clock>,
}
#[derive(Deserialize)]
struct Owner {
    actor: ActorKey,
    epoch: u64,
    lease: HostLease,
    sealed: bool,
}
impl Authority {
    pub fn new(bucket: Arc<dyn Bucket>, clock: Arc<dyn Clock>) -> Self {
        Self { bucket, clock }
    }

    pub async fn require_live(&self, prefix: &str) -> Result<()> {
        let (_, _, owner, epoch) = self.owner(prefix).await?;
        ensure!(
            owner.epoch == epoch
                && !owner.sealed
                && owner.lease.expires_at_ms > self.clock.now_ms()?,
            "replica group has no live owner"
        );
        Ok(())
    }

    pub async fn authorize_writer(&self, prefix: &str, host: &HostId, session: &str) -> Result<()> {
        let (_, _, owner, epoch) = self.owner(prefix).await?;
        ensure!(
            owner.epoch == epoch
                && !owner.sealed
                && owner.lease.id == *host
                && owner.lease.session_id == session
                && owner.lease.expires_at_ms > self.clock.now_ms()?,
            "replica assignment does not belong to this live activation"
        );
        Ok(())
    }

    pub async fn authorize_finish(&self, prefix: &str, host: &HostId, session: &str) -> Result<()> {
        let (_, _, owner, epoch) = self.owner(prefix).await?;
        ensure!(
            epoch <= owner.epoch
                && (epoch < owner.epoch
                    || owner.lease.expires_at_ms <= self.clock.now_ms()?
                    || (owner.lease.id == *host && owner.lease.session_id == session)),
            "cannot retire another live activation"
        );
        Ok(())
    }

    pub async fn retire_if_inactive(&self, prefix: &str, disrupted: bool) -> Result<bool> {
        let (key, object, owner, epoch) = self.owner(prefix).await?;
        ensure!(epoch <= owner.epoch, "replica group is ahead of ownership");
        if epoch < owner.epoch || owner.sealed {
            return Ok(true);
        }
        if !disrupted && owner.lease.expires_at_ms > self.clock.now_ms()? {
            return Ok(false);
        }
        let mut value: serde_json::Value = serde_json::from_slice(&object.bytes)?;
        value["lease"]["expires_at_ms"] = 0.into();
        value["mutation"] = uuid::Uuid::new_v4().to_string().into();
        self.bucket
            .compare_and_swap(&key, Some(object.generation), serde_json::to_vec(&value)?)
            .await
    }

    async fn owner(
        &self,
        prefix: &str,
    ) -> Result<(String, crate::bucket::BucketObject, Owner, u64)> {
        let actor = crate::storage_paths::actor_from_snapshot(&format!("{prefix}1.json"))?;
        let epoch = u64::from_str_radix(
            prefix
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .context("epoch missing")?,
            16,
        )?;
        ensure!(
            prefix
                == format!(
                    "{}{:032x}/",
                    crate::storage_paths::snapshots(&actor)?,
                    epoch
                ),
            "invalid epoch prefix"
        );
        let key = crate::storage_paths::owner(&actor.storage_key())?;
        let object = self
            .bucket
            .get(&key)
            .await?
            .context("replica owner is missing")?;
        let owner: Owner = serde_json::from_slice(&object.bytes)?;
        ensure!(owner.actor == actor, "replica owner scope mismatch");
        Ok((key, object, owner, epoch))
    }
}
