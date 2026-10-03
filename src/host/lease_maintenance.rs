use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use tokio::{sync::watch, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::{
    clock::Clock,
    host_leases::{HostLease, HostLeaseRegistry, HostLeaseRequest},
};

use super::HostEndpoint;

const LEASE_RENEWAL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

pub(crate) struct HostLeaseMaintainer {
    observation: watch::Sender<HostObservation>,
    endpoint: HostEndpoint,
    session_id: String,
    store: Arc<dyn HostLeaseRegistry>,
    clock: Arc<dyn Clock>,
    lease_duration_ms: u64,
    renew_every: Duration,
}

#[derive(Clone, Default)]
struct HostObservation {
    queues: Option<super::queues::ActorQueues>,
    executor: Option<Arc<dyn crate::actor::ActorExecutor>>,
}

impl HostLeaseMaintainer {
    pub(crate) fn new(
        endpoint: HostEndpoint,
        session_id: String,
        store: Arc<dyn HostLeaseRegistry>,
        clock: Arc<dyn Clock>,
        lease_duration: Duration,
        renew_every: Duration,
    ) -> Result<Self> {
        let lease_duration_ms = u64::try_from(lease_duration.as_millis())?;
        ensure!(
            lease_duration_ms > 0,
            "host lease duration must be positive"
        );
        ensure!(
            !renew_every.is_zero(),
            "host lease renewal interval must be positive"
        );
        ensure!(
            renew_every < lease_duration,
            "host lease renewal interval must be shorter than its duration"
        );
        ensure!(!session_id.is_empty(), "host session ID must not be empty");

        Ok(Self {
            observation: watch::channel(HostObservation::default()).0,
            endpoint,
            session_id,
            store,
            clock,
            lease_duration_ms,
            renew_every,
        })
    }

    pub(crate) fn observe(
        &self,
        executor: Arc<dyn crate::actor::ActorExecutor>,
        queues: super::queues::ActorQueues,
    ) {
        self.observation.send_replace(HostObservation {
            executor: Some(executor),
            queues: Some(queues),
        });
    }

    pub(crate) async fn start(self: Arc<Self>) -> Result<LeaseRenewalTask> {
        let observation_changes = self.observation.subscribe();
        let observation = self.observation.borrow().clone();
        let changes = observation
            .executor
            .as_ref()
            .and_then(|executor| executor.residency_changes());
        let queue_changes = observation.queues.as_ref().map(|queues| queues.changes());
        let initial = self.renew_once_with_deadline().await?;
        info!(
            host_id = %initial.lease.id,
            route = %initial.lease.route,
            expires_at_ms = initial.lease.expires_at_ms,
            "host lease registered"
        );
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let (lease_lost_tx, lease_lost) = watch::channel(false);
        let manager = self.clone();
        let task = tokio::spawn(manager.renew_until_stopped(
            initial.local_deadline,
            task_shutdown,
            lease_lost_tx,
            changes,
            queue_changes,
            observation_changes,
        ));

        Ok(LeaseRenewalTask {
            shutdown,
            task,
            lease_lost,
        })
    }

    pub(crate) async fn unregister(&self) -> Result<()> {
        self.store
            .unregister(&self.endpoint.id, &self.session_id)
            .await?;
        info!(host_id = %self.endpoint.id, "host lease unregistered");
        Ok(())
    }

    async fn renew_until_stopped(
        self: Arc<Self>,
        mut local_deadline: Instant,
        shutdown: CancellationToken,
        lease_lost: watch::Sender<bool>,
        mut changes: Option<watch::Receiver<()>>,
        mut queue_changes: Option<watch::Receiver<Vec<crate::host_leases::ActorQueueInventory>>>,
        mut observation_changes: watch::Receiver<HostObservation>,
    ) {
        loop {
            if !self
                .wait_until_renewal(
                    local_deadline,
                    &shutdown,
                    &lease_lost,
                    &mut changes,
                    &mut queue_changes,
                    &mut observation_changes,
                )
                .await
            {
                return;
            }
            let observation = self.observation.borrow().clone();
            changes = observation
                .executor
                .as_ref()
                .and_then(|executor| executor.residency_changes());
            queue_changes = observation.queues.as_ref().map(|queues| queues.changes());
            let Some(deadline) = self
                .renew_before_deadline(local_deadline, &shutdown, &lease_lost)
                .await
            else {
                return;
            };
            local_deadline = deadline;
        }
    }

    async fn wait_until_renewal(
        &self,
        local_deadline: Instant,
        shutdown: &CancellationToken,
        lease_lost: &watch::Sender<bool>,
        changes: &mut Option<watch::Receiver<()>>,
        queue_changes: &mut Option<watch::Receiver<Vec<crate::host_leases::ActorQueueInventory>>>,
        observation_changes: &mut watch::Receiver<HostObservation>,
    ) -> bool {
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => false,
            _ = observation_changes.changed() => true,
            _ = tokio::time::sleep_until(local_deadline) => {
                warn!(
                    host_id = %self.endpoint.id,
                    "locally confirmed host lease expired; permanently self-fencing this process"
                );
                let _ = lease_lost.send(true);
                false
            }
            _ = tokio::time::sleep(self.renew_every) => true,
            _ = residency_changed(changes) => true,
            _ = async {
                residency_changed(queue_changes).await;
                tokio::time::sleep(Duration::from_millis(100)).await;
                if let Some(changes) = queue_changes { changes.borrow_and_update(); }
            } => true,
        }
    }

    async fn renew_before_deadline(
        &self,
        local_deadline: Instant,
        shutdown: &CancellationToken,
        lease_lost: &watch::Sender<bool>,
    ) -> Option<Instant> {
        let renewal = self.renew_once_with_deadline();
        tokio::pin!(renewal);
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => None,
            _ = tokio::time::sleep_until(local_deadline) => {
                warn!(
                    host_id = %self.endpoint.id,
                    "host lease expired while its renewal request was still pending; permanently self-fencing this process"
                );
                let _ = lease_lost.send(true);
                None
            }
            result = &mut renewal => Some(match result {
                Ok(confirmed) => confirmed.local_deadline,
                Err(error) => {
                    warn!(
                        host_id = %self.endpoint.id,
                        error = %format!("{error:#}"),
                        "host lease renewal failed; ownership checks will self-fence after expiry"
                    );
                    local_deadline
                }
            }),
        }
    }

    async fn renew_once_with_deadline(&self) -> Result<ConfirmedHostLease> {
        // The store stamps the durable expiration with its own clock. The locally
        // confirmed window is anchored to this host's clock, sampled before the
        // store round trip, so it always lapses at or before the stamped expiry
        // regardless of the absolute offset between the two clocks.
        let local_now_ms = self.clock.now_ms()?;
        let local_valid_until_ms = local_now_ms
            .checked_add(self.lease_duration_ms)
            .ok_or_else(|| anyhow::anyhow!("host lease expiration overflow"))?;
        let request = HostLeaseRequest {
            id: self.endpoint.id.clone(),
            session_id: self.session_id.clone(),
            route: self.endpoint.route.clone(),
            duration_ms: self.lease_duration_ms,
        };

        let observation = self.observation.borrow().clone();
        let residents = observation
            .executor
            .as_ref()
            .and_then(|executor| executor.resident_actors());
        let queues = observation.queues.as_ref().map(|queues| queues.inventory());
        let lease = self
            .store
            .register_with_inventory(&request, residents.as_deref(), queues.as_deref())
            .await?;
        debug!(
            host_id = %lease.id,
            route = %lease.route,
            expires_at_ms = lease.expires_at_ms,
            "host lease renewed"
        );

        // Anchor the Tokio timer before sampling the same monotonic clock used
        // above. This makes the process fence no later than the locally confirmed
        // lease window, even when a renewal RPC consumed most of that window.
        let deadline_anchor = Instant::now();
        let remaining_ms = local_valid_until_ms.saturating_sub(self.clock.now_ms()?);
        ensure!(
            remaining_ms > 0,
            "host lease expired before its registration response arrived"
        );
        let local_deadline = deadline_anchor
            .checked_add(Duration::from_millis(remaining_ms))
            .ok_or_else(|| anyhow::anyhow!("local host lease deadline overflow"))?;

        Ok(ConfirmedHostLease {
            lease,
            local_deadline,
        })
    }
}

async fn residency_changed<T>(changes: &mut Option<watch::Receiver<T>>) {
    if let Some(receiver) = changes {
        if receiver.changed().await.is_ok() {
            return;
        }
    }
    std::future::pending::<()>().await;
}

struct ConfirmedHostLease {
    lease: HostLease,
    local_deadline: Instant,
}

pub(crate) struct LeaseRenewalTask {
    shutdown: CancellationToken,
    task: JoinHandle<()>,
    lease_lost: watch::Receiver<bool>,
}

impl Drop for LeaseRenewalTask {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.task.abort();
    }
}

impl LeaseRenewalTask {
    pub(crate) fn lease_lost(&self) -> watch::Receiver<bool> {
        self.lease_lost.clone()
    }

    pub(crate) async fn shutdown(mut self) -> Result<()> {
        self.shutdown.cancel();
        match tokio::time::timeout(LEASE_RENEWAL_SHUTDOWN_TIMEOUT, &mut self.task).await {
            Ok(result) => result?,
            Err(_) => {
                self.task.abort();
                let _ = (&mut self.task).await;
                anyhow::bail!(
                    "host lease renewal did not stop within {}ms",
                    LEASE_RENEWAL_SHUTDOWN_TIMEOUT.as_millis()
                );
            }
        }
        info!("host lease renewal stopped");

        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/lease_maintenance.rs"]
mod tests;
