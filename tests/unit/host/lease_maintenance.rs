use super::*;
use crate::{
    clock::{Clock, SystemClock},
    host::HostId,
    host_leases::HostLease,
};
use async_trait::async_trait;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tokio::sync::Notify;

struct ManualClock(AtomicU64);

impl ManualClock {
    fn new(now_ms: u64) -> Self {
        Self(AtomicU64::new(now_ms))
    }

    fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> Result<u64> {
        Ok(self.0.load(Ordering::SeqCst))
    }
}

struct FlakyLeaseStore {
    calls: AtomicUsize,
    lease: Mutex<Option<HostLease>>,
    changed: Notify,
    clock: Arc<ManualClock>,
}

struct HangingLeaseRenewalStore {
    calls: AtomicUsize,
    lease: Mutex<Option<HostLease>>,
}

#[async_trait]
impl HostLeaseRegistry for HangingLeaseRenewalStore {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            return std::future::pending().await;
        }
        let lease = HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: SystemClock.now_ms()?.saturating_add(request.duration_ms),
        };
        *self.lease.lock().expect("test store lock") = Some(lease.clone());
        Ok(lease)
    }

    async fn unregister(&self, _id: &HostId, _session_id: &str) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl HostLeaseRegistry for FlakyLeaseStore {
    async fn register(&self, request: &HostLeaseRequest) -> Result<HostLease> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.changed.notify_one();
        if call == 2 {
            anyhow::bail!("temporary store failure");
        }
        let lease = HostLease {
            id: request.id.clone(),
            session_id: request.session_id.clone(),
            route: request.route.clone(),
            expires_at_ms: self.clock.now_ms()?.saturating_add(request.duration_ms),
        };
        *self.lease.lock().expect("test store lock") = Some(lease.clone());

        Ok(lease)
    }

    async fn unregister(&self, id: &HostId, session_id: &str) -> Result<()> {
        let mut lease = self.lease.lock().expect("test store lock");
        if lease
            .as_ref()
            .is_some_and(|lease| &lease.id == id && lease.session_id == session_id)
        {
            *lease = None;
        }
        Ok(())
    }
}

impl FlakyLeaseStore {
    async fn get(&self, id: &HostId) -> Result<Option<HostLease>> {
        Ok(self
            .lease
            .lock()
            .expect("test store lock")
            .clone()
            .filter(|lease| &lease.id == id))
    }
}

#[tokio::test]
async fn residency_change_renews_before_the_heartbeat() -> Result<()> {
    struct Executor(watch::Sender<()>);
    #[async_trait::async_trait]
    impl crate::actor::ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn residency_changes(&self) -> Option<watch::Receiver<()>> {
            Some(self.0.subscribe())
        }
        async fn invoke(
            &self,
            _: crate::actor::ActorMethodInvocation,
            _: Option<&crate::actor::ActorState>,
        ) -> Result<crate::actor::ActorMethodOutcome> {
            anyhow::bail!("unused")
        }
    }
    let clock = Arc::new(ManualClock::new(1_000));
    let store = Arc::new(FlakyLeaseStore {
        calls: AtomicUsize::new(0),
        lease: Mutex::new(None),
        changed: Notify::new(),
        clock: clock.clone(),
    });
    let (changed, _) = watch::channel(());
    let manager = Arc::new(HostLeaseMaintainer::new(
        HostEndpoint {
            id: HostId::new("events"),
            route: "http://host".into(),
        },
        "session".into(),
        store.clone(),
        clock,
        Duration::from_secs(30),
        Duration::from_secs(10),
    )?);
    let renewal = manager.clone().start().await?;
    manager
        .observation
        .send_modify(|sources| sources.executor = Some(Arc::new(Executor(changed.clone()))));
    changed.send_replace(());
    tokio::time::timeout(Duration::from_millis(500), wait_for_calls(&store, 2)).await??;
    renewal.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn queue_changes_publish_before_the_heartbeat() -> Result<()> {
    use crate::host_leases::ActorQueueInventory;
    struct Store(watch::Sender<Option<Vec<ActorQueueInventory>>>);
    #[async_trait]
    impl HostLeaseRegistry for Store {
        async fn register(&self, _: &HostLeaseRequest) -> Result<HostLease> {
            unreachable!()
        }
        async fn register_with_inventory(
            &self,
            request: &HostLeaseRequest,
            _: Option<&[crate::actor::ActorKey]>,
            _: &[crate::host_leases::ActorSocketInventory],
            queues: Option<&[ActorQueueInventory]>,
        ) -> Result<HostLease> {
            self.0.send_replace(queues.map(<[_]>::to_vec));
            Ok(HostLease {
                id: request.id.clone(),
                session_id: request.session_id.clone(),
                route: request.route.clone(),
                expires_at_ms: SystemClock.now_ms()? + request.duration_ms,
            })
        }
        async fn unregister(&self, _: &HostId, _: &str) -> Result<()> {
            Ok(())
        }
    }
    let (sender, mut reports) = watch::channel(None);
    let queues = super::super::queues::ActorQueues::new();
    let manager = Arc::new(HostLeaseMaintainer::new(
        HostEndpoint {
            id: HostId::new("queued"),
            route: "http://host".into(),
        },
        "session".into(),
        Arc::new(Store(sender)),
        Arc::new(SystemClock),
        Duration::from_secs(30),
        Duration::from_secs(10),
    )?);
    let renewal = manager.clone().start().await?;
    manager
        .observation
        .send_modify(|sources| sources.queues = Some(queues.clone()));
    let waiting = queues.enqueue(
        &crate::actor::ActorKey {
            project_id: "default".into(),
            actor_name: "Room".into(),
            actor_id: "one".into(),
        },
        "sendMessage".into(),
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        reports.wait_for(|report| report.as_ref().is_some_and(|queues| queues.len() == 1)),
    )
    .await??;
    assert_eq!(
        reports.borrow().as_ref().unwrap()[0].waiting[0].operation,
        "sendMessage"
    );
    drop(waiting);
    tokio::time::timeout(
        Duration::from_secs(2),
        reports.wait_for(|report| report.as_ref().is_some_and(|queues| queues.is_empty())),
    )
    .await??;
    renewal.shutdown().await?;
    Ok(())
}

#[test]
fn new_hosts_receive_unique_session_ids() {
    let first = HostEndpoint {
        id: HostId::new(uuid::Uuid::new_v4().to_string()),
        route: "sandbox-route".into(),
    };
    let second = HostEndpoint {
        id: HostId::new(uuid::Uuid::new_v4().to_string()),
        route: "sandbox-route".into(),
    };

    assert_ne!(first.id, second.id);
    assert_eq!(first.route, "sandbox-route");
}

#[tokio::test]
async fn registers_immediately_retries_failure_and_stops_cleanly() -> Result<()> {
    let clock = Arc::new(ManualClock::new(1_000));
    let store = Arc::new(FlakyLeaseStore {
        calls: AtomicUsize::new(0),
        lease: Mutex::new(None),
        changed: Notify::new(),
        clock: clock.clone(),
    });
    let node = HostEndpoint {
        id: HostId::new("node-a"),
        route: "sandbox-session-a".into(),
    };
    let manager = Arc::new(HostLeaseMaintainer::new(
        node.clone(),
        "session-a".into(),
        store.clone(),
        clock.clone(),
        Duration::from_millis(1_000),
        Duration::from_millis(10),
    )?);

    let renewal = manager.clone().start().await?;
    assert_eq!(store.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        store
            .get(&node.id)
            .await?
            .expect("initial lease")
            .expires_at_ms,
        2_000
    );

    clock.set(1_500);
    wait_for_calls(&store, 3).await?;
    assert_eq!(
        store
            .get(&node.id)
            .await?
            .expect("renewed lease")
            .expires_at_ms,
        2_500
    );

    renewal.shutdown().await?;
    let calls_after_shutdown = store.calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(store.calls.load(Ordering::SeqCst), calls_after_shutdown);

    Ok(())
}

#[tokio::test]
async fn pending_renewal_cannot_outlive_the_confirmed_lease_window() -> Result<()> {
    let store = Arc::new(HangingLeaseRenewalStore {
        calls: AtomicUsize::new(0),
        lease: Mutex::new(None),
    });
    let manager = Arc::new(HostLeaseMaintainer::new(
        HostEndpoint {
            id: HostId::new("node-a"),
            route: "sandbox-session-a".into(),
        },
        "session-a".into(),
        store.clone(),
        Arc::new(SystemClock),
        Duration::from_millis(100),
        Duration::from_millis(10),
    )?);

    let renewal = manager.start().await?;
    let mut lease_lost = renewal.lease_lost();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !*lease_lost.borrow() {
            lease_lost.changed().await?;
        }
        Ok::<(), watch::error::RecvError>(())
    })
    .await??;

    assert!(*lease_lost.borrow());
    assert_eq!(store.calls.load(Ordering::SeqCst), 2);
    renewal.shutdown().await?;
    Ok(())
}

async fn wait_for_calls(store: &FlakyLeaseStore, expected: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(1), async {
        while store.calls.load(Ordering::SeqCst) < expected {
            store.changed.notified().await;
        }
    })
    .await?;

    Ok(())
}
