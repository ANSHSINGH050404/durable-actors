use super::*;
use crate::replicas::record::{Batch, Record, checksum};

impl ReplicaFleet {
    pub(super) async fn write(&self, object: &str, bytes: Bytes) -> Result<Group> {
        let (prefix, version) = position(object)?;
        let snapshot = crate::state_log::StateSnapshot::decode(&bytes)?;
        ensure!(
            snapshot.state_version == version,
            "snapshot version mismatch"
        );
        ensure!(
            prefix.ends_with(&format!("/{:032x}/", snapshot.owner_epoch)),
            "snapshot epoch mismatch"
        );
        let mut update = self.registry.lock(&prefix).await?;
        self.authority.require_live(&prefix).await?;
        let group = self.registry.lookup(&prefix).await?;
        self.to_bucket(&group, &mut update).await?;
        self.write_bucket(&prefix, object, bytes, &update).await?;
        self.recruit(&prefix, &mut update).await?;
        self.authority.require_live(&prefix).await?;
        self.group(&prefix).await
    }

    pub(super) async fn to_bucket(
        &self,
        group: &GroupRecord,
        update: &mut GroupUpdate,
    ) -> Result<()> {
        if group.state == "bucket" {
            return Ok(());
        }
        update.switching().await?;
        let checkpoint = if group.ever_ready {
            self.archive_final(group).await?
        } else {
            group.checkpoint.clone()
        };
        update.bucket(checkpoint.as_ref()).await?;
        tracing::info!(event = "replica_bucket_fallback", prefix = %group.prefix);
        Ok(())
    }

    async fn write_bucket(
        &self,
        prefix: &str,
        object: &str,
        bytes: Bytes,
        update: &GroupUpdate,
    ) -> Result<()> {
        let (_, version) = position(object)?;
        let group = self.registry.lookup(prefix).await?;
        if let Some(checkpoint) = &group.checkpoint {
            let (_, previous) = position(&checkpoint.object)?;
            ensure!(
                version == previous || previous.checked_add(1) == Some(version),
                "bucket log has a gap or regressed"
            );
        }
        if let Some(previous) = self.archive.get(prefix, version).await? {
            ensure!(
                previous.as_slice() == bytes.as_ref(),
                "conflicting bucket retry"
            );
        } else {
            self.archive
                .write(&Batch {
                    prefix: prefix.into(),
                    records: vec![Record::encode(version, &bytes, None)?],
                })
                .await?;
        }
        update
            .checkpoint(&Checkpoint {
                object: object.into(),
                digest: checksum(&bytes),
            })
            .await
    }

    pub(super) async fn recruit(&self, prefix: &str, update: &mut GroupUpdate) -> Result<()> {
        update.ensure().await?;
        let group = self.registry.lookup(prefix).await?;
        if group.state == "ready" {
            return self.authority.require_live(prefix).await;
        }
        if group.state != "bucket" {
            self.to_bucket(&group, update).await?;
        }
        let eligible = match self.available().await {
            Ok(pods) => pods,
            Err(error) => {
                tracing::warn!(%error, %prefix, "replica inventory unavailable; retaining bucket durability");
                return self.authority.require_live(prefix).await;
            }
        };
        if self.validate_members(&eligible).is_err() {
            return self.authority.require_live(prefix).await;
        }
        let names = eligible.into_iter().map(|pod| pod.name).collect::<Vec<_>>();
        update.claim(&self.zones, &names).await?;
        let group = self.registry.lookup(prefix).await?;
        let result = self.initialize(&group).await;
        self.authority.require_live(prefix).await?;
        match result {
            Ok(()) => {
                update.ready().await?;
                tracing::info!(event = "replica_fleet_ready", %prefix, replicas = group.pods.len());
                Ok(())
            }
            Err(error) => {
                tracing::debug!(%error, %prefix, "replica capacity unavailable; using bucket durability");
                update.bucket(group.checkpoint.as_ref()).await
            }
        }
    }

    async fn initialize(&self, group: &GroupRecord) -> Result<()> {
        self.validate_members(&group.pods)?;
        let copies = placements(&group.pods)?;
        let assignment = Assignment {
            prefix: group.prefix.clone(),
            replicas: copies.clone(),
        };
        let seed = match &group.checkpoint {
            Some(checkpoint) => {
                let (_, version) = position(&checkpoint.object)?;
                let bytes = self
                    .archive
                    .get(&group.prefix, version)
                    .await?
                    .context("replica seed is missing")?;
                ensure!(
                    checksum(&bytes) == checkpoint.digest,
                    "replica seed checksum mismatch"
                );
                Some((checkpoint.object.clone(), Bytes::from(bytes)))
            }
            None => None,
        };
        futures_util::future::try_join_all(group.pods.iter().zip(&copies).map(
            |(pod, copy)| async {
                self.pods.protect(pod, &group.prefix).await?;
                self.peers.assign(copy, &assignment).await?;
                if let Some((object, bytes)) = &seed {
                    self.peers.seed(copy, object, bytes.clone()).await?;
                }
                Ok::<_, anyhow::Error>(())
            },
        ))
        .await?;
        Ok(())
    }

    pub(super) async fn available(&self) -> Result<Vec<PodRecord>> {
        let inventory = self.pods.observed().await?;
        let mut nodes = HashSet::new();
        let mut desired = self.zones.clone();
        let mut available = Vec::new();
        for pod in self.registry.unassigned().await? {
            if pod.placement.is_none() {
                continue;
            }
            let Some(index) = desired.iter().position(|zone| *zone == pod.zone) else {
                continue;
            };
            let health = self.pods.health(&pod, &inventory).await?;
            if health.live
                && !health.draining
                && health.current_image
                && let Some(node) = &pod.node
                && nodes.insert(node.clone())
            {
                desired.remove(index);
                available.push(pod);
            }
        }
        Ok(available)
    }

    fn validate_members(&self, pods: &[PodRecord]) -> Result<()> {
        let copies = placements(pods)?;
        ensure!(
            copies.len() >= self.zones.len().min(2),
            "insufficient replica capacity"
        );
        ensure!(
            pods.iter()
                .filter_map(|pod| pod.node.as_ref())
                .collect::<HashSet<_>>()
                .len()
                == copies.len(),
            "replicas must occupy distinct nodes"
        );
        for regional in [false, true] {
            let domain = |zone: &str| {
                if regional {
                    zone.rsplit_once('-').unwrap().0.to_owned()
                } else {
                    zone.to_owned()
                }
            };
            let desired = self
                .zones
                .iter()
                .map(|zone| domain(zone))
                .collect::<HashSet<_>>()
                .len();
            let available = copies
                .iter()
                .map(|copy| domain(&copy.zone))
                .collect::<HashSet<_>>()
                .len();
            ensure!(
                available >= desired.min(2),
                "replicas do not span the required failure domains"
            );
        }
        Ok(())
    }
}
