use std::{sync::Arc, time::Instant};

use crate::{
    litestream::{Replicator, storage::SqliteCapture},
    state_log::SqliteSnapshot,
    storage::SnapshotRef,
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde_json::Value;
use tracing::{info, warn};

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure,
        ActorMethodEviction, ActorMethodInvocation, ActorMethodOutcome, ActorSocketEffect,
        ActorSocketInvocation, ActorSocketOutcome, ActorSocketPublisher, ActorState,
        validate_socket_effects,
    },
    state_log::StateSnapshot,
    storage::WritePlan,
};

use super::HostEndpoint;

#[async_trait]
pub(crate) trait ActorStorage: Send + Sync {
    async fn acquire_actor(
        &self,
        _actor: &crate::actor::ActorKey,
        _host: &super::HostId,
    ) -> Result<ActorActivation>;

    async fn load_actor_state(
        &self,
        _actor: &crate::actor::ActorKey,
        _host: &super::HostId,
        _epoch: u64,
    ) -> Result<(u64, bytes::Bytes)>;
    async fn read_snapshot(
        &self,
        actor: &crate::actor::ActorKey,
        snapshot: &SnapshotRef,
    ) -> Result<bytes::Bytes>;
    fn ensure_authority(&self) -> Result<()>;
    async fn prepare_state_write(
        &self,
        actor: &crate::actor::ActorKey,
        host_id: &super::HostId,
        owner_epoch: u64,
        expected_version: u64,
    ) -> Result<WritePlan>;
}

#[derive(Clone, Debug)]
pub(crate) struct ActorActivation {
    pub owner_epoch: u64,
    pub state_version: u64,
    pub state: Option<bytes::Bytes>,
}

#[derive(Default)]
pub(super) struct CommitOrigin {
    pub connection_id: Option<String>,
    pub interleaved: bool,
}

impl CommitOrigin {
    pub(super) fn socket(event: &crate::actor::ActorSocketEvent) -> Self {
        use crate::actor::ActorSocketEvent;
        let connection = match event {
            ActorSocketEvent::Connect { connection }
            | ActorSocketEvent::Disconnect { connection, .. } => &connection.id,
            ActorSocketEvent::Message { connection_id, .. } => connection_id,
        };
        Self {
            connection_id: Some(connection.clone()),
            interleaved: false,
        }
    }
}

pub(super) struct ActorRuntime {
    endpoint: HostEndpoint,
    executor: Arc<dyn ActorExecutor>,
    storage: Arc<dyn ActorStorage>,
    state: Arc<dyn crate::state_transport::SnapshotWriter>,
    publisher: Arc<dyn ActorSocketPublisher>,
    replication: Arc<dyn Replicator>,
    cached_state: Option<CachedActorState>,
    activation: Option<ActorActivation>,
}

impl ActorRuntime {
    pub(super) fn new(
        endpoint: HostEndpoint,
        executor: Arc<dyn ActorExecutor>,
        storage: Arc<dyn ActorStorage>,
        state: Arc<dyn crate::state_transport::SnapshotWriter>,
        publisher: Arc<dyn ActorSocketPublisher>,
        replication: Arc<dyn Replicator>,
    ) -> Self {
        Self {
            endpoint,
            executor,
            storage,
            state,
            publisher,
            replication,
            cached_state: None,
            activation: None,
        }
    }

    pub(super) fn state_version(&self) -> Option<u64> {
        self.cached_state
            .as_ref()
            .map(|state| state.state_version)
            .filter(|version| *version > 0)
    }

    pub(super) fn endpoint(&self) -> &HostEndpoint {
        &self.endpoint
    }

    pub(super) fn executor(&self) -> Arc<dyn ActorExecutor> {
        self.executor.clone()
    }

    pub(super) async fn prepare_invocation(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        replay: bool,
        timings: &mut InvocationTimings,
    ) -> Result<PreparedInvocation> {
        self.storage.ensure_authority()?;
        timings.queue_admitted_at_ms = Some(timings.elapsed_ms());
        let mut cached = self
            .take_or_load_state(&invocation.actor, owner_epoch, timings)
            .await?;
        if let Err(error) = self.finish_pending_commit(invocation, &mut cached).await {
            self.cached_state = Some(cached);
            warn!(actor = %invocation.actor.storage_key(), %error, "pending actor state commit remains unresolved");
            return Ok(PreparedInvocation::Completed(
                ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                },
            ));
        }
        timings.pending_commit_resolved_at_ms = Some(timings.elapsed_ms());
        let result = if replay {
            cached.replay(&invocation.request_id)
        } else {
            None
        };
        let state = cached.state();
        self.cached_state = Some(cached);
        Ok(match result {
            Some(result) => PreparedInvocation::Completed(ActorExecutionResult::Completed {
                result,
                effects: vec![],
            }),
            None => PreparedInvocation::Execute(state),
        })
    }

    pub(super) async fn finish_serial(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        outcome: Result<ActorMethodOutcome>,
        origin: CommitOrigin,
        timings: &mut InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        let executed = self.method_result(invocation, outcome).await;
        self.finish_method(invocation, owner_epoch, executed, origin, timings)
            .await
    }

    pub(super) async fn commit_interleaved(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        outcome: crate::actor::ActorInterleavedOutcome,
        mut origin: CommitOrigin,
        timings: &mut InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        self.storage.ensure_authority()?;
        validate_socket_effects(&outcome.effects)?;
        origin.interleaved = true;
        timings.actor_execution_completed_at_ms = Some(timings.elapsed_ms());
        let mut cached = self
            .cached_state
            .take()
            .context("interleaved activation has no state")?;
        ensure!(
            cached.owner_epoch == owner_epoch,
            "interleaved ownership changed"
        );
        let publication = if cached.state.as_deref() != Some(&outcome.state) {
            self.publish_result(
                invocation,
                owner_epoch,
                &mut cached,
                outcome.result.clone(),
                outcome.state,
                origin,
            )
            .await
        } else {
            Ok(ActorExecutionResult::Completed {
                result: outcome.result.clone(),
                effects: vec![],
            })
        };
        let version = cached.state_version;
        self.cached_state = if publication.is_err() && cached.pending.is_none() {
            None
        } else {
            Some(cached)
        };
        publication?;
        timings.state_publication_completed_at_ms = Some(timings.elapsed_ms());
        self.complete_with_state(&invocation.actor, version, outcome.result, outcome.effects)
            .await
    }

    pub(super) async fn activate_actor(
        &mut self,
        actor: &crate::actor::ActorKey,
    ) -> Result<ActorActivation> {
        self.storage.ensure_authority()?;
        if let Some(activation) = &self.activation {
            return Ok(activation.clone());
        }
        let mut activation = self.storage.acquire_actor(actor, &self.endpoint.id).await?;
        if self.cached_state.is_none() {
            let cached = if activation.state_version == 0 {
                CachedActorState::new(
                    activation.owner_epoch,
                    SqliteCapture::new(self.replication.clone()).await?,
                )
            } else {
                let bytes = activation
                    .state
                    .take()
                    .context("activation has no recovered state")?;
                self.load_cached(
                    actor,
                    activation.owner_epoch,
                    activation.state_version,
                    &bytes,
                )
                .await?
            };
            self.cached_state = Some(cached);
        }
        self.executor
            .hydrate(
                actor.clone(),
                self.cached_state.as_ref().and_then(CachedActorState::state),
            )
            .await?;
        self.storage.ensure_authority()?;
        self.activation = Some(activation.clone());
        Ok(activation)
    }

    pub(super) async fn invoke_actor(
        &mut self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        mut timings: InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        self.storage.ensure_authority()?;
        let outcome = self
            .invoke_actor_once(&invocation, owner_epoch, &mut timings)
            .await;
        Self::log_invocation(&self.endpoint, &invocation, &timings, &outcome);
        self.storage.ensure_authority()?;
        outcome
    }

    pub(super) async fn handle_socket_event(
        &mut self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
        mut timings: InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        self.storage.ensure_authority()?;
        let persistence = ActorInvocation {
            request_id: invocation.request_id.clone(),
            actor: invocation.actor.clone(),
            method: socket_event_name(&invocation.event).into(),
            args: Vec::new(),
        };
        let outcome = self
            .handle_socket_event_once(invocation, owner_epoch, &persistence, &mut timings)
            .await;
        Self::log_invocation(&self.endpoint, &persistence, &timings, &outcome);
        self.storage.ensure_authority()?;
        outcome
    }

    async fn handle_socket_event_once(
        &mut self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
        persistence: &ActorInvocation,
        timings: &mut InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        timings.queue_admitted_at_ms = Some(timings.elapsed_ms());
        let mut cached = self
            .take_or_load_state(&invocation.actor, owner_epoch, timings)
            .await?;
        if self
            .finish_pending_commit(persistence, &mut cached)
            .await
            .is_err()
        {
            self.cached_state = Some(cached);
            return Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            });
        }
        timings.pending_commit_resolved_at_ms = Some(timings.elapsed_ms());
        let origin = CommitOrigin::socket(&invocation.event);
        let state = cached.state();
        self.cached_state = Some(cached);
        let outcome = self.execute_socket_event(invocation, state).await;
        timings.actor_execution_completed_at_ms = Some(timings.elapsed_ms());
        let (next_state, effects) = match outcome {
            Ok(outcome) => outcome,
            Err(result) => return Ok(result),
        };
        let mut cached = self
            .cached_state
            .take()
            .context("actor socket event has no state")?;
        if cached.state.as_deref() == Some(&next_state) {
            let version = cached.state_version;
            self.cached_state = Some(cached);
            return self
                .complete_with_state(&persistence.actor, version, Value::Null, effects)
                .await;
        }
        let published = self
            .publish_result(
                persistence,
                owner_epoch,
                &mut cached,
                Value::Null,
                next_state,
                origin,
            )
            .await;
        timings.state_publication_completed_at_ms = Some(timings.elapsed_ms());
        if published.is_err() {
            self.evict(&persistence.actor).await;
        }
        let version = cached.state_version;
        self.cached_state = if published.is_err() && cached.pending.is_none() {
            None
        } else {
            Some(cached)
        };
        match published {
            Ok(ActorExecutionResult::Completed { .. }) => {
                self.complete_with_state(&persistence.actor, version, Value::Null, effects)
                    .await
            }
            Ok(result) => Ok(result),
            Err(_) => Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            }),
        }
    }

    async fn invoke_actor_once(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        timings: &mut InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        let state = match self
            .prepare_invocation(invocation, owner_epoch, true, timings)
            .await?
        {
            PreparedInvocation::Execute(state) => state,
            PreparedInvocation::Completed(result) => return Ok(result),
        };
        let executed = self.execute_method(invocation, state).await;
        self.finish_method(
            invocation,
            owner_epoch,
            executed,
            CommitOrigin::default(),
            timings,
        )
        .await
    }

    async fn finish_method(
        &mut self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        executed: std::result::Result<
            (Value, ActorState, Vec<ActorSocketEffect>),
            ActorExecutionResult,
        >,
        origin: CommitOrigin,
        timings: &mut InvocationTimings,
    ) -> Result<ActorExecutionResult> {
        timings.actor_execution_completed_at_ms = Some(timings.elapsed_ms());
        let (result, next_state, effects) = match executed {
            Ok(outcome) => outcome,
            Err(failure) => return Ok(failure),
        };
        let mut cached = self
            .cached_state
            .take()
            .context("actor invocation has no state")?;
        if cached.state.as_deref() == Some(&next_state) {
            let version = cached.state_version;
            self.cached_state = Some(cached);
            return self
                .complete_with_state(&invocation.actor, version, result, effects)
                .await;
        }

        let published = self
            .publish_result(
                invocation,
                owner_epoch,
                &mut cached,
                result,
                next_state,
                origin,
            )
            .await;
        timings.state_publication_completed_at_ms = Some(timings.elapsed_ms());
        if published.is_err() {
            self.evict(&invocation.actor).await;
        }
        let version = cached.state_version;
        self.cached_state = if published.is_err() && cached.pending.is_none() {
            None
        } else {
            Some(cached)
        };
        match published {
            Ok(ActorExecutionResult::Completed { result, .. }) => {
                self.complete_with_state(&invocation.actor, version, result, effects)
                    .await
            }
            Ok(result) => Ok(result),
            Err(error) => {
                warn!(
                    actor = %invocation.actor.storage_key(),
                    error = %format!("{error:#}"),
                    "actor completed but state publication could not be confirmed"
                );
                Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                })
            }
        }
    }

    async fn complete_with_state(
        &self,
        actor: &crate::actor::ActorKey,
        state_version: u64,
        result: Value,
        effects: Vec<ActorSocketEffect>,
    ) -> Result<ActorExecutionResult> {
        self.storage.ensure_authority()?;
        let (mut automatic, effects): (Vec<_>, Vec<_>) = effects.into_iter().partition(|effect| {
            matches!(
                effect,
                ActorSocketEffect::StateSnapshot { .. } | ActorSocketEffect::StateUpdate { .. }
            )
        });
        for effect in &mut automatic {
            match effect {
                ActorSocketEffect::StateSnapshot { version, .. }
                | ActorSocketEffect::StateUpdate { version, .. } => *version = Some(state_version),
                _ => unreachable!(),
            }
        }
        if !automatic.is_empty()
            && let Err(error) = self.publisher.publish(actor, automatic).await
        {
            warn!(actor = %actor.storage_key(), error = %error, "committed state notification failed");
            return Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            });
        }
        Ok(ActorExecutionResult::Completed { result, effects })
    }

    async fn take_or_load_state(
        &mut self,
        actor: &crate::actor::ActorKey,
        owner_epoch: u64,
        timings: &mut InvocationTimings,
    ) -> Result<CachedActorState> {
        let cached = self.cached_state.take();
        timings.state_cache_checked_at_ms = Some(timings.elapsed_ms());
        if let Some(cached) = cached
            && cached.owner_epoch == owner_epoch
        {
            return Ok(cached);
        }
        let (state_version, loaded) = self
            .storage
            .load_actor_state(actor, &self.endpoint.id, owner_epoch)
            .await?;
        if state_version == 0 {
            ensure!(loaded.is_empty(), "uninitialized actor has state");
            return Ok(CachedActorState::new(
                owner_epoch,
                SqliteCapture::new(self.replication.clone()).await?,
            ));
        }
        timings.state_downloaded_at_ms = Some(timings.elapsed_ms());
        let cached = self
            .load_cached(actor, owner_epoch, state_version, &loaded)
            .await?;
        timings.state_decoded_at_ms = Some(timings.elapsed_ms());
        Ok(cached)
    }

    async fn load_cached(
        &self,
        actor: &crate::actor::ActorKey,
        owner_epoch: u64,
        state_version: u64,
        loaded: &[u8],
    ) -> Result<CachedActorState> {
        let snapshot = StateSnapshot::decode(loaded)?;
        ensure!(
            snapshot.state_version == state_version,
            "actor snapshot version does not match its state head"
        );
        ensure!(
            snapshot.owner_epoch <= owner_epoch,
            "actor snapshot belongs to a newer owner epoch"
        );
        let mut segments = Vec::new();
        let mut current = snapshot.clone();
        loop {
            segments.push(current.sqlite.files.clone());
            let Some(parent) = current.sqlite.parent.clone() else {
                break;
            };
            let first = current.sqlite.files[0].first;
            let bytes = self.storage.read_snapshot(actor, &parent).await?;
            parent.verify(&bytes)?;
            current = StateSnapshot::decode(&bytes)?;
            current.validate_object(&parent.object)?;
            ensure!(
                current.state_version == parent.state_version,
                "SQLite dependency version mismatch"
            );
            ensure!(
                current.sqlite.txid.checked_add(1) == Some(first),
                "SQLite dependency transaction gap"
            );
        }
        let files = segments.into_iter().rev().flatten().collect::<Vec<_>>();
        let capture =
            SqliteCapture::restore(self.replication.clone(), &files, snapshot.sqlite.txid).await?;
        let mut cached = CachedActorState::new(owner_epoch, capture);
        cached.state_version = state_version;
        cached.state = cached.state();
        cached.last_request_id = Some(snapshot.request_id);
        cached.last_result = Some(snapshot.result);
        Ok(cached)
    }

    async fn execute_method(
        &mut self,
        invocation: &ActorInvocation,
        state: Option<Arc<ActorState>>,
    ) -> std::result::Result<(Value, ActorState, Vec<ActorSocketEffect>), ActorExecutionResult>
    {
        let outcome = self
            .executor
            .invoke_shared(
                ActorMethodInvocation {
                    request_id: invocation.request_id.clone(),
                    actor: invocation.actor.clone(),
                    method: invocation.method.clone(),
                    args: invocation.args.clone(),
                },
                state,
            )
            .await;
        self.method_result(invocation, outcome).await
    }

    async fn method_result(
        &mut self,
        invocation: &ActorInvocation,
        outcome: Result<ActorMethodOutcome>,
    ) -> std::result::Result<(Value, ActorState, Vec<ActorSocketEffect>), ActorExecutionResult>
    {
        match outcome {
            Ok(ActorMethodOutcome::Interleaved(_)) => {
                Err(failed("actor_error", "unexpected interleaved result"))
            }
            Ok(ActorMethodOutcome::Completed {
                result,
                state,
                effects,
            }) => match validate_socket_effects(&effects) {
                Ok(()) => Ok((result, state, effects)),
                Err(error) => {
                    self.evict(&invocation.actor).await;
                    Err(failed(
                        "actor_error",
                        format!("actor returned invalid socket effects: {error:#}"),
                    ))
                }
            },
            Ok(ActorMethodOutcome::Failed(failure)) => {
                if !matches!(
                    failure.code.as_str(),
                    "actor_method_failed" | "actor_socket_failed"
                ) {
                    self.evict(&invocation.actor).await;
                }
                let code = match failure.code.as_str() {
                    "resource_exhausted" => "resource_exhausted",
                    _ => "actor_error",
                };
                Err(failed(code, failure.message))
            }
            Err(error) => {
                self.evict(&invocation.actor).await;
                Err(failed(
                    "actor_error",
                    format!("actor executor failed: {error:#}"),
                ))
            }
        }
    }

    async fn execute_socket_event(
        &mut self,
        invocation: ActorSocketInvocation,
        state: Option<Arc<ActorState>>,
    ) -> std::result::Result<(ActorState, Vec<ActorSocketEffect>), ActorExecutionResult> {
        let actor = invocation.actor.clone();
        match self.executor.handle_socket_shared(invocation, state).await {
            Ok(ActorSocketOutcome::Interleaved(_)) => {
                Err(failed("actor_error", "unexpected interleaved result"))
            }
            Ok(ActorSocketOutcome::Handled { state, effects }) => {
                match validate_socket_effects(&effects) {
                    Ok(()) => Ok((state, effects)),
                    Err(error) => {
                        self.evict(&actor).await;
                        Err(failed(
                            "actor_error",
                            format!("actor returned invalid socket effects: {error:#}"),
                        ))
                    }
                }
            }
            Ok(ActorSocketOutcome::Failed(failure)) => {
                if failure.code != "actor_socket_failed" {
                    self.evict(&actor).await;
                }
                let code = match failure.code.as_str() {
                    "resource_exhausted" => "resource_exhausted",
                    _ => "actor_error",
                };
                Err(failed(code, failure.message))
            }
            Err(error) => Err(failed(
                "actor_error",
                format!("actor executor failed: {error:#}"),
            )),
        }
    }

    async fn publish_result(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut CachedActorState,
        result: Value,
        next_state: ActorState,
        origin: CommitOrigin,
    ) -> Result<ActorExecutionResult> {
        let mut timings = StateWriteTimings::new();
        let next_version = cached.state_version.checked_add(1);
        let outcome = self
            .publish_result_once(
                invocation,
                owner_epoch,
                cached,
                result,
                next_state,
                origin,
                &mut timings,
            )
            .await;
        self.log_state_write(invocation, owner_epoch, next_version, &timings, &outcome);
        outcome
    }

    async fn publish_result_once(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        cached: &mut CachedActorState,
        result: Value,
        mut next_state: ActorState,
        origin: CommitOrigin,
        timings: &mut StateWriteTimings,
    ) -> Result<ActorExecutionResult> {
        let next_version = cached
            .state_version
            .checked_add(1)
            .context("actor state version overflow")?;
        let ticket = match cached.next_write.take() {
            Some(ticket)
                if ticket.state_version == next_version
                    && ticket.stream.owner_epoch == owner_epoch =>
            {
                ticket
            }
            _ => {
                self.storage
                    .prepare_state_write(
                        &invocation.actor,
                        &self.endpoint.id,
                        owner_epoch,
                        cached.state_version,
                    )
                    .await?
            }
        };
        ensure!(
            ticket.state_version == next_version,
            "state write ticket has the wrong version"
        );
        ensure!(
            ticket.stream.owner_epoch == owner_epoch,
            "write capability belongs to another owner epoch"
        );
        timings.write_ticket_ready_at_ms = Some(timings.elapsed_ms());
        let mut snapshot = StateSnapshot::new(
            next_version,
            owner_epoch,
            invocation.request_id.clone(),
            cached.capture_sqlite(&mut next_state).await?,
            result.clone(),
        )?;
        snapshot.attribution = Some(crate::state_log::StateAttribution {
            operation: invocation.method.clone(),
            connection_id: origin.connection_id,
            committed_at_ms: unix_millis()? as u64,
            interleaved: origin.interleaved,
        });
        timings.snapshot_created_at_ms = Some(timings.elapsed_ms());
        let bytes = snapshot.encode()?;
        timings.snapshot_encoded_at_ms = Some(timings.elapsed_ms());
        cached.pending = Some(PendingStateCommit {
            snapshot,
            state: next_state,
            ticket,
            durable: false,
        });
        let pending = cached
            .pending
            .as_ref()
            .expect("pending write was installed");
        self.state.write_snapshot(&pending.ticket, bytes).await?;
        cached
            .pending
            .as_mut()
            .expect("pending write was installed")
            .durable = true;
        timings.snapshot_persisted_at_ms = Some(timings.elapsed_ms());
        timings.durability_proof = Some("object_storage");
        timings.snapshot_uploaded_at_ms = timings.snapshot_persisted_at_ms;
        self.finish_pending_commit(invocation, cached).await?;
        timings.state_finalized_at_ms = Some(timings.elapsed_ms());
        Ok(ActorExecutionResult::Completed {
            result,
            effects: Vec::new(),
        })
    }

    fn log_state_write(
        &self,
        invocation: &ActorInvocation,
        owner_epoch: u64,
        state_version: Option<u64>,
        timings: &StateWriteTimings,
        outcome: &Result<ActorExecutionResult>,
    ) {
        match outcome {
            Ok(_) => info!(
                event = "actor_state_write",
                request_id = %invocation.request_id,

                actor_name = %invocation.actor.actor_name,
                actor_id = %invocation.actor.actor_id,
                host_id = %self.endpoint.id,
                owner_epoch,
                state_version,
                started_at_ms = 0,
                write_ticket_ready_at_ms = timings.write_ticket_ready_at_ms,
                snapshot_created_at_ms = timings.snapshot_created_at_ms,
                snapshot_encoded_at_ms = timings.snapshot_encoded_at_ms,
                snapshot_uploaded_at_ms = timings.snapshot_uploaded_at_ms,
                snapshot_persisted_at_ms = timings.snapshot_persisted_at_ms,
                durability_proof = timings.durability_proof,
                state_finalized_at_ms = timings.state_finalized_at_ms,
                completed_at_ms = timings.elapsed_ms(),
                outcome = "committed",
                "immutable actor state committed"
            ),
            Err(error) => warn!(
                event = "actor_state_write",
                request_id = %invocation.request_id,

                actor_name = %invocation.actor.actor_name,
                actor_id = %invocation.actor.actor_id,
                host_id = %self.endpoint.id,
                owner_epoch,
                state_version,
                started_at_ms = 0,
                write_ticket_ready_at_ms = timings.write_ticket_ready_at_ms,
                snapshot_created_at_ms = timings.snapshot_created_at_ms,
                snapshot_encoded_at_ms = timings.snapshot_encoded_at_ms,
                snapshot_uploaded_at_ms = timings.snapshot_uploaded_at_ms,
                snapshot_persisted_at_ms = timings.snapshot_persisted_at_ms,
                durability_proof = timings.durability_proof,
                state_finalized_at_ms = timings.state_finalized_at_ms,
                completed_at_ms = timings.elapsed_ms(),
                outcome = "failed",
                error = %format!("{error:#}"),
                "immutable actor state commit failed"
            ),
        }
    }

    async fn finish_pending_commit(
        &self,
        invocation: &ActorInvocation,
        cached: &mut CachedActorState,
    ) -> Result<()> {
        let Some(pending) = &mut cached.pending else {
            return Ok(());
        };
        if !pending.durable {
            let renewed = self
                .storage
                .prepare_state_write(
                    &invocation.actor,
                    &self.endpoint.id,
                    cached.owner_epoch,
                    cached.state_version,
                )
                .await?;
            ensure!(
                renewed.stream == pending.ticket.stream
                    && renewed.object_name == pending.ticket.object_name,
                "pending writer has been fenced"
            );
            pending.ticket = renewed;
            self.state
                .write_snapshot(&pending.ticket, pending.snapshot.encode()?)
                .await?;
            pending.durable = true;
        }
        let stream = &pending.ticket.stream;
        let mut next_write = pending.ticket.clone();
        next_write.state_version = pending
            .snapshot
            .state_version
            .checked_add(1)
            .context("state version overflow")?;
        next_write.object_name = stream.object(next_write.state_version);
        let pending = cached.pending.take().expect("pending commit checked above");
        cached.sqlite_head = Some(SnapshotRef::new(
            pending.ticket.object_name.clone(),
            &pending.snapshot,
            &pending.snapshot.encode()?,
        ));
        cached.state_version = pending.snapshot.state_version;
        cached.state = Some(Arc::new(pending.state));
        cached.last_request_id = Some(pending.snapshot.request_id);
        cached.last_result = Some(pending.snapshot.result);
        cached.next_write = Some(next_write);
        Ok(())
    }

    pub(super) fn log_invocation(
        endpoint: &HostEndpoint,
        invocation: &ActorInvocation,
        timings: &InvocationTimings,
        outcome: &Result<ActorExecutionResult>,
    ) {
        match outcome {
            Ok(result) => Self::log_invocation_result(
                endpoint,
                invocation,
                timings,
                actor_execution_outcome(result),
                actor_execution_failure_code(result).unwrap_or(""),
                None,
            ),
            Err(error) => Self::log_invocation_result(
                endpoint,
                invocation,
                timings,
                "host_error",
                "",
                Some(format!("{error:#}")),
            ),
        }
    }

    fn log_invocation_result(
        endpoint: &HostEndpoint,
        invocation: &ActorInvocation,
        timings: &InvocationTimings,
        outcome: &str,
        failure_code: &str,
        error: Option<String>,
    ) {
        info!(
                event = "actor_host_invocation",
                request_id = %invocation.request_id,

                actor_name = %invocation.actor.actor_name,
                actor_id = %invocation.actor.actor_id,
                method = %invocation.method,
                host_id = %endpoint.id,
                started_at_ms = 0,
                queue_admitted_at_ms = timings.queue_admitted_at_ms,
                state_cache_checked_at_ms = timings.state_cache_checked_at_ms,
                state_downloaded_at_ms = timings.state_downloaded_at_ms,
                state_decoded_at_ms = timings.state_decoded_at_ms,
                pending_commit_resolved_at_ms = timings.pending_commit_resolved_at_ms,
                actor_execution_completed_at_ms = timings.actor_execution_completed_at_ms,
                state_publication_completed_at_ms = timings.state_publication_completed_at_ms,
                completed_at_ms = timings.elapsed_ms(),
                outcome,
                failure_code,
                error,
                "actor host invocation completed"
        );
    }

    pub(super) async fn evict(&mut self, actor: &crate::actor::ActorKey) {
        if let Err(error) = self
            .executor
            .evict(ActorMethodEviction {
                actor: actor.clone(),
            })
            .await
        {
            warn!(error = %format!("{error:#}"), "failed to evict actor after invocation failure");
        }
        self.cached_state = None;
        self.activation = None;
    }
}

pub(super) fn socket_event_name(event: &crate::actor::ActorSocketEvent) -> &'static str {
    match event {
        crate::actor::ActorSocketEvent::Connect { .. } => "onConnect",
        crate::actor::ActorSocketEvent::Message { .. } => "onMessage",
        crate::actor::ActorSocketEvent::Disconnect { .. } => "onDisconnect",
    }
}

pub(super) enum PreparedInvocation {
    Execute(Option<Arc<ActorState>>),
    Completed(ActorExecutionResult),
}

struct CachedActorState {
    sqlite: SqliteCapture,
    sqlite_head: Option<SnapshotRef>,
    owner_epoch: u64,
    state_version: u64,
    state: Option<Arc<ActorState>>,
    last_request_id: Option<String>,
    last_result: Option<Value>,
    next_write: Option<WritePlan>,
    pending: Option<PendingStateCommit>,
}

struct PendingStateCommit {
    snapshot: StateSnapshot,
    state: ActorState,
    ticket: WritePlan,
    durable: bool,
}

impl CachedActorState {
    fn new(owner_epoch: u64, sqlite: SqliteCapture) -> Self {
        Self {
            sqlite,
            sqlite_head: None,
            owner_epoch,
            state_version: 0,
            state: None,
            last_request_id: None,
            last_result: None,
            next_write: None,
            pending: None,
        }
    }

    async fn capture_sqlite(&mut self, state: &mut ActorState) -> Result<SqliteSnapshot> {
        let files = self.sqlite.capture(&state.sqlite).await?;
        let first = files
            .first()
            .context("actor commit has no replication files")?;
        let parent = if first.first == 1 {
            None
        } else {
            Some(
                self.sqlite_head
                    .clone()
                    .context("SQLite state has no durable head")?,
            )
        };
        state.sqlite = self.sqlite.state();
        Ok(SqliteSnapshot {
            txid: self.sqlite.txid(),
            parent,
            files,
        })
    }

    fn state(&self) -> Option<Arc<ActorState>> {
        Some(self.state.clone().unwrap_or_else(|| {
            Arc::new(ActorState {
                sqlite: self.sqlite.state(),
            })
        }))
    }

    fn replay(&self, request_id: &str) -> Option<Value> {
        (self.last_request_id.as_deref() == Some(request_id))
            .then(|| self.last_result.clone())
            .flatten()
    }
}

pub(super) struct InvocationTimings {
    started_at: Instant,
    queue_admitted_at_ms: Option<f64>,
    state_cache_checked_at_ms: Option<f64>,
    state_downloaded_at_ms: Option<f64>,
    state_decoded_at_ms: Option<f64>,
    pending_commit_resolved_at_ms: Option<f64>,
    actor_execution_completed_at_ms: Option<f64>,
    state_publication_completed_at_ms: Option<f64>,
}

impl InvocationTimings {
    pub(super) fn new() -> Self {
        Self {
            started_at: Instant::now(),
            queue_admitted_at_ms: None,
            state_cache_checked_at_ms: None,
            state_downloaded_at_ms: None,
            state_decoded_at_ms: None,
            pending_commit_resolved_at_ms: None,
            actor_execution_completed_at_ms: None,
            state_publication_completed_at_ms: None,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        elapsed_ms(self.started_at)
    }
}

struct StateWriteTimings {
    started_at: Instant,
    write_ticket_ready_at_ms: Option<f64>,
    snapshot_created_at_ms: Option<f64>,
    snapshot_encoded_at_ms: Option<f64>,
    snapshot_uploaded_at_ms: Option<f64>,
    snapshot_persisted_at_ms: Option<f64>,
    durability_proof: Option<&'static str>,
    state_finalized_at_ms: Option<f64>,
}

impl StateWriteTimings {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
            write_ticket_ready_at_ms: None,
            snapshot_created_at_ms: None,
            snapshot_encoded_at_ms: None,
            snapshot_uploaded_at_ms: None,
            snapshot_persisted_at_ms: None,
            durability_proof: None,
            state_finalized_at_ms: None,
        }
    }

    fn elapsed_ms(&self) -> f64 {
        elapsed_ms(self.started_at)
    }
}

fn actor_execution_outcome(result: &ActorExecutionResult) -> &'static str {
    match result {
        ActorExecutionResult::Completed { .. } => "completed",
        ActorExecutionResult::Failed { .. } => "failed",
        ActorExecutionResult::Reroute => "reroute",
        ActorExecutionResult::HostUnavailable => "host_unavailable",
    }
}

fn actor_execution_failure_code(result: &ActorExecutionResult) -> Option<&str> {
    match result {
        ActorExecutionResult::Failed { failure } => Some(&failure.code),
        _ => None,
    }
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

fn unix_millis() -> Result<i64> {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .context("system clock is before the Unix epoch")?
            .as_millis(),
    )
    .context("system clock exceeds supported state-write timestamp range")
}

fn failed(code: impl Into<String>, message: impl Into<String>) -> ActorExecutionResult {
    ActorExecutionResult::Failed {
        failure: ActorInvocationFailure {
            code: code.into(),
            message: message.into(),
        },
    }
}
