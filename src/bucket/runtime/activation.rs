use std::time::Instant;

use super::*;
use crate::host_leases::HostLeaseRequest;

#[derive(Default)]
struct ActivationRecovery {
    snapshot: Option<LoadedSnapshot>,
    session_ms: Option<f64>,
    snapshot_ms: Option<f64>,
}

impl RuntimeStorage {
    pub async fn register_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        region: &str,
        new_actor: bool,
        owner_hint: Option<&OwnershipHint>,
    ) -> Result<LoadedActor> {
        actor.validate()?;
        crate::placement::validate_region(region)?;
        request.validate_duration()?;
        let started = Instant::now();
        let now = self.clock.now_ms()?;
        // Unsealed recovery can fence a live session if the hinted lease was renewed.
        let owner_hint = owner_hint.filter(|hint| {
            hint.record.sealed
                && hint.generation > 0
                && hint.record.actor == *actor
                && hint.record.region == region
                && hint.record.lease.expires_at_ms <= now
                && (hint.record.lease.id != request.id
                    || hint.record.lease.session_id != request.session_id)
        });
        for hint in [owner_hint, None] {
            let read_started = Instant::now();
            let current = if let Some(hint) = hint {
                Some((hint.generation, hint.record.clone()))
            } else if new_actor && owner_hint.is_none() {
                None
            } else {
                self.load(&actor.storage_key()).await?
            };
            let ownership_read_ms = read_started.elapsed().as_secs_f64() * 1_000.0;
            let mut recovery = self
                .recover_activation(
                    actor,
                    request,
                    region,
                    current.as_ref().map(|(_, record)| record),
                )
                .await?;
            let mut record = Ownership {
                persistence: self.persistence.clone(),
                sealed: false,
                inventory: ActivationInventory::default(),
                actor: actor.clone(),
                epoch: current
                    .as_ref()
                    .map_or(Some(1), |(_, record)| record.epoch.checked_add(1))
                    .context("owner epoch overflow")?,
                region: region.into(),
                base: recovery
                    .snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.reference.clone()),
                lease: self.new_lease(request)?,
                mutation: String::new(),
            };
            let write_started = Instant::now();
            if !self
                .replace_activation(&mut record, current.map(|(generation, _)| generation))
                .await?
            {
                ensure!(hint.is_some(), "actor activation changed concurrently");
                continue;
            }
            self.snapshots.prepare(&record.stream()?.prefix).await?;
            self.seed_replicas(&record, &mut recovery).await?;
            tracing::info!(
                event = "actor_activation_storage",
                project_id = %actor.project_id,
                actor_name = %actor.actor_name,
                actor_id = %actor.actor_id,
                host_id = %request.id,
                session_id = %request.session_id,
                new_actor,
                ownership_read_ms,
                session_recovery_ms = recovery.session_ms,
                snapshot_load_ms = recovery.snapshot_ms,
                ownership_write_ms = write_started.elapsed().as_secs_f64() * 1_000.0,
                duration_ms = started.elapsed().as_secs_f64() * 1_000.0,
            );
            return Ok(self.remember(record, recovery.snapshot));
        }
        anyhow::bail!("actor activation changed concurrently")
    }

    async fn seed_replicas(
        &self,
        record: &Ownership,
        recovery: &mut ActivationRecovery,
    ) -> Result<()> {
        use crate::state_transport::SnapshotWriter;
        if !matches!(
            self.persistence,
            super::super::PersistenceConfig::Replicated { .. }
        ) {
            return Ok(());
        }
        let Some(previous) = &recovery.snapshot else {
            return Ok(());
        };
        let mut snapshot = crate::state_log::StateSnapshot::decode(&previous.bytes)?;
        snapshot.owner_epoch = record.epoch;
        let bytes = snapshot.encode()?;
        let mut plan = self.write_plan(record, snapshot.state_version)?;
        // The recovered baseline keeps its version while entering a fresh replica stream.
        plan.stream.base_version = 0;
        self.write_snapshot(&plan, bytes.clone()).await?;
        recovery.snapshot = Some(LoadedSnapshot {
            reference: SnapshotRef::new(plan.object_name, &snapshot, &bytes),
            bytes: bytes.into(),
        });
        Ok(())
    }

    pub async fn renew_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        inventory: ActivationInventory,
    ) -> Result<HostLease> {
        let (generation, mut record) = self
            .load(&actor.storage_key())
            .await?
            .context("actor ownership missing")?;
        ensure!(
            record.actor == *actor
                && record.lease.id == request.id
                && record.lease.session_id == request.session_id,
            "actor ownership changed"
        );
        let current = &record.lease;
        ensure!(
            current.expires_at_ms > self.clock.now_ms()?,
            "expired activation cannot renew"
        );
        ensure!(
            current.route == request.route,
            "activation route cannot change"
        );
        let lease = self.new_lease(request)?;
        record.lease = lease.clone();
        record.inventory = inventory;
        self.save_activation(&mut record, Some(generation)).await?;
        Ok(lease)
    }

    pub async fn release_activation(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
    ) -> Result<()> {
        self.release_with_checkpoint(actor, host, session, None)
            .await
    }

    pub(super) async fn release_with_checkpoint(
        &self,
        actor: &ActorKey,
        host: &HostId,
        session: &str,
        checkpoint: Option<SessionCheckpoint>,
    ) -> Result<()> {
        for _ in 0..3 {
            let Some((generation, mut record)) = self.load(&actor.storage_key()).await? else {
                return Ok(());
            };
            if record.lease.id != *host
                || record.lease.session_id != session
                || record.lease.expires_at_ms == 0
            {
                return Ok(());
            }
            record.lease.expires_at_ms = 0;
            if let Some(checkpoint) = &checkpoint {
                record.base = checkpoint.snapshot.clone();
                record.sealed = true;
            }
            if self
                .replace_activation(&mut record, Some(generation))
                .await?
            {
                return Ok(());
            }
        }
        anyhow::bail!("actor activation kept changing during release")
    }

    async fn recover_activation(
        &self,
        actor: &ActorKey,
        request: &HostLeaseRequest,
        region: &str,
        current: Option<&Ownership>,
    ) -> Result<ActivationRecovery> {
        let Some(record) = current else {
            return Ok(ActivationRecovery::default());
        };
        ensure!(
            record.persistence == self.persistence,
            "actor persistence configuration changed; an explicit state migration is required"
        );
        ensure!(
            record.actor == *actor && record.region == region,
            "ownership scope cannot change"
        );
        ensure!(
            record.lease.id != request.id || record.lease.session_id != request.session_id,
            "activation session cannot be reused"
        );
        ensure!(
            record.lease.expires_at_ms <= self.clock.now_ms()?,
            "previous owner lease is still active"
        );
        if record.sealed {
            let started = Instant::now();
            return Ok(ActivationRecovery {
                snapshot: self.load_latest(record, None).await?,
                snapshot_ms: Some(started.elapsed().as_secs_f64() * 1_000.0),
                session_ms: None,
            });
        }
        let started = Instant::now();
        self.snapshots.seal(&record.stream()?.prefix).await?;
        let snapshot = self.latest(record, None).await?;
        Ok(ActivationRecovery {
            snapshot,
            session_ms: Some(started.elapsed().as_secs_f64() * 1_000.0),
            snapshot_ms: None,
        })
    }

    fn new_lease(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        request.validate_duration()?;
        ensure!(
            !request.id.as_str().is_empty() && !request.session_id.is_empty(),
            "activation identity missing"
        );
        Ok(HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: self
                .clock
                .now_ms()?
                .checked_add(request.duration_ms)
                .context("lease expiration overflow")?,
        })
    }

    async fn save_activation(&self, record: &mut Ownership, generation: Option<i64>) -> Result<()> {
        ensure!(
            self.replace_activation(record, generation).await?,
            "actor activation changed concurrently"
        );
        Ok(())
    }

    async fn replace_activation(
        &self,
        record: &mut Ownership,
        generation: Option<i64>,
    ) -> Result<bool> {
        record.mutation = uuid::Uuid::new_v4().to_string();
        replace(
            self.authority.as_ref(),
            &ownership_key(&record.actor.storage_key())?,
            generation,
            serde_json::to_vec(record)?,
        )
        .await
    }
}
