use crate::{
    actor::{ActorMethodInvocation, ActorMethodOutcome, ActorSocketEffect, ActorSocketOutcome},
    state_log::StateSnapshot,
    state_transport::StateWrite,
    storage::WritePlan,
};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

use serde_json::json;

use super::*;
use crate::actor::ActorKey;

struct EmptySocketPublisher;

#[async_trait]
impl ActorSocketPublisher for EmptySocketPublisher {
    async fn publish(&self, _: &ActorKey, _: Vec<ActorSocketEffect>) -> Result<()> {
        Ok(())
    }
}

struct IncrementingExecutor {
    invocations: AtomicU64,
}

#[tokio::test]
async fn ordinary_methods_execute_and_commit_without_a_socket_gateway() -> Result<()> {
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );
    assert!(matches!(
        invoke(&host, "request-1").await?,
        ActorExecutionResult::Completed { result, .. } if result == json!(1)
    ));
    assert_eq!(executor.invocations.load(Ordering::Relaxed), 1);
    assert_eq!(state.writes.lock().unwrap().len(), 1);
    let snapshot = StateSnapshot::decode(&state.writes.lock().unwrap()[0])?;
    let attribution = snapshot.attribution.unwrap();
    assert_eq!(attribution.operation, "increment");
    assert!(attribution.connection_id.is_none());
    assert!(attribution.committed_at_ms > 0);
    Ok(())
}

struct SerialAdmissionExecutor {
    inner: IncrementingExecutor,
    admission: watch::Sender<()>,
}

impl SerialAdmissionExecutor {
    fn new() -> Self {
        Self {
            inner: IncrementingExecutor {
                invocations: AtomicU64::new(0),
            },
            admission: watch::channel(()).0,
        }
    }
}

#[async_trait]
impl ActorExecutor for SerialAdmissionExecutor {
    fn supports(&self, _: &str) -> bool {
        true
    }
    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        Some(self.admission.subscribe())
    }
    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<&Value>,
    ) -> Result<ActorMethodOutcome> {
        self.inner.invoke(invocation, state).await
    }
}

struct ExhaustedExecutor;

#[tokio::test]
async fn reentrant_completions_commit_in_snapshot_order_even_when_delivered_out_of_order()
-> Result<()> {
    assert_reentrant_commit_order(false).await
}

#[tokio::test]
async fn a_failed_reentrant_commit_stops_the_activation_without_acknowledging_later_writes()
-> Result<()> {
    assert_reentrant_commit_order(true).await
}

async fn assert_reentrant_commit_order(fail_write: bool) -> Result<()> {
    struct ReentrantExecutor {
        admission: watch::Sender<()>,
        first_started: tokio::sync::Notify,
        second_started: tokio::sync::Notify,
    }
    #[async_trait]
    impl ActorExecutor for ReentrantExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }
        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            _: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            let sequence = if invocation.request_id == "first" {
                self.admission.send_replace(());
                self.first_started.notify_one();
                self.second_started.notified().await;
                tokio::time::sleep(Duration::from_millis(30)).await;
                1
            } else {
                self.second_started.notify_one();
                2
            };
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence,
                    result: json!(sequence),
                    state: json!({"count": sequence}),
                    effects: vec![],
                },
            ))
        }
    }
    let executor = Arc::new(ReentrantExecutor {
        admission: watch::channel(()).0,
        first_started: Default::default(),
        second_started: Default::default(),
    });
    let state = Arc::new(FakeStateTransport::default());
    if fail_write {
        state.failures.store(1, Ordering::SeqCst);
    }
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );
    let operation = async {
        let first = invoke(&host, "first");
        let second = async {
            executor.first_started.notified().await;
            invoke(&host, "second").await
        };
        let (first, second) = tokio::join!(first, second);
        if fail_write {
            for result in [first?, second?] {
                assert!(
                    matches!(result, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
                );
            }
            assert_eq!(state.writes.lock().unwrap().len(), 1);
            assert_eq!(
                invoke(&host, "after-failure").await?,
                ActorExecutionResult::HostUnavailable
            );
            assert!(*host.stopped().borrow());
            return Ok(());
        }
        assert_eq!(first?, completed(1));
        assert_eq!(second?, completed(2));
        let writes = state.writes.lock().unwrap();
        assert_eq!(writes.len(), 2);
        assert_eq!(
            serde_json::from_str::<Value>(StateSnapshot::decode(&writes[0])?.state.get())?,
            json!({"count": 1})
        );
        assert_eq!(
            serde_json::from_str::<Value>(StateSnapshot::decode(&writes[1])?.state.get())?,
            json!({"count": 2})
        );
        Ok::<_, anyhow::Error>(())
    };
    tokio::time::timeout(Duration::from_secs(2), operation).await??;
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn a_reentrant_completion_cannot_open_an_ordinary_invocations_gate() -> Result<()> {
    struct Executor {
        admission: watch::Sender<()>,
        started: mpsc::UnboundedSender<String>,
        release_background: tokio::sync::Notify,
        release_ordinary: tokio::sync::Notify,
    }
    #[async_trait]
    impl ActorExecutor for Executor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
            Some(self.admission.subscribe())
        }
        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            _: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            self.started.send(invocation.request_id.clone())?;
            let sequence = match invocation.request_id.as_str() {
                "background" => {
                    self.admission.send_replace(());
                    self.release_background.notified().await;
                    1
                }
                "ordinary" => {
                    self.release_ordinary.notified().await;
                    2
                }
                "next" => 3,
                _ => unreachable!(),
            };
            Ok(ActorMethodOutcome::Interleaved(
                crate::actor::ActorInterleavedOutcome {
                    sequence,
                    result: json!(sequence),
                    state: json!({"count": sequence}),
                    effects: vec![],
                },
            ))
        }
    }
    let (started, mut starts) = mpsc::unbounded_channel();
    let executor = Arc::new(Executor {
        admission: watch::channel(()).0,
        started,
        release_background: Default::default(),
        release_ordinary: Default::default(),
    });
    let host = Arc::new(ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
    ));
    let operation = async {
        let caller = host.clone();
        let background = tokio::spawn(async move { invoke(&caller, "background").await });
        assert_eq!(starts.recv().await.as_deref(), Some("background"));
        let caller = host.clone();
        let ordinary = tokio::spawn(async move { invoke(&caller, "ordinary").await });
        assert_eq!(starts.recv().await.as_deref(), Some("ordinary"));
        let caller = host.clone();
        let next = tokio::spawn(async move { invoke(&caller, "next").await });
        executor.release_background.notify_one();
        assert_eq!(background.await??, completed(1));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), starts.recv())
                .await
                .is_err()
        );
        executor.release_ordinary.notify_one();
        assert_eq!(ordinary.await??, completed(2));
        assert_eq!(starts.recv().await.as_deref(), Some("next"));
        assert_eq!(next.await??, completed(3));
        Ok::<_, anyhow::Error>(())
    };
    tokio::time::timeout(Duration::from_secs(2), operation).await??;
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn application_errors_preserve_the_worker_and_committed_state() -> Result<()> {
    struct FailingExecutor {
        evictions: AtomicUsize,
    }

    #[async_trait]
    impl ActorExecutor for FailingExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }

        async fn invoke(
            &self,
            invocation: ActorMethodInvocation,
            state: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            if invocation.request_id.starts_with("fail") {
                return Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                    code: if invocation.request_id == "fail-application" {
                        "actor_method_failed"
                    } else {
                        "invalid_actor_state"
                    }
                    .into(),
                    message: "failed".into(),
                }));
            }
            let count = state.and_then(|state| state["count"].as_u64()).unwrap_or(0) + 1;
            Ok(ActorMethodOutcome::Completed {
                result: json!(count),
                state: json!({"count": count}),
                effects: vec![],
            })
        }

        async fn evict(&self, _: crate::actor::ActorMethodEviction) -> Result<()> {
            self.evictions.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    let executor = Arc::new(FailingExecutor {
        evictions: AtomicUsize::new(0),
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        Arc::new(FakeAuthority::default()),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );
    assert_eq!(invoke(&host, "first").await?, completed(1));
    for request in ["fail-application", "fail-fatal"] {
        assert!(
            matches!(invoke(&host, request).await?, ActorExecutionResult::Failed { failure } if failure.code == "actor_error")
        );
        assert_eq!(
            executor.evictions.load(Ordering::Relaxed),
            usize::from(request == "fail-fatal")
        );
        assert_eq!(state.writes.lock().unwrap().len(), 1);
    }
    assert_eq!(invoke(&host, "after-failure").await?, completed(2));
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

struct InvalidEffectsExecutor;

struct ControlledExecutor {
    started: tokio::sync::mpsc::UnboundedSender<String>,
    release: Arc<tokio::sync::Semaphore>,
}

#[async_trait]
impl ActorExecutor for ControlledExecutor {
    fn supports(&self, _: &str) -> bool {
        true
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<&Value>,
    ) -> Result<ActorMethodOutcome> {
        self.started.send(invocation.request_id.clone())?;
        if invocation.request_id == "panic" {
            panic!("actor executor panicked");
        }
        if invocation.request_id == "first" {
            self.release.acquire().await?.forget();
        }
        let count = state.and_then(|value| value["count"].as_u64()).unwrap_or(0) + 1;
        Ok(ActorMethodOutcome::Completed {
            result: json!(count),
            state: json!({"count": count}),
            effects: Vec::new(),
        })
    }

    async fn handle_socket(
        &self,
        invocation: ActorSocketInvocation,
        state: Option<&Value>,
    ) -> Result<ActorSocketOutcome> {
        let result = self
            .invoke(
                ActorMethodInvocation {
                    request_id: invocation.request_id,
                    actor: invocation.actor,
                    method: "onMessage".into(),
                    args: Vec::new(),
                },
                state,
            )
            .await?;
        match result {
            ActorMethodOutcome::Interleaved(outcome) => {
                Ok(ActorSocketOutcome::Interleaved(outcome))
            }
            ActorMethodOutcome::Completed { state, effects, .. } => {
                Ok(ActorSocketOutcome::Handled { state, effects })
            }
            ActorMethodOutcome::Failed(failure) => Ok(ActorSocketOutcome::Failed(failure)),
        }
    }
}

fn controlled_host() -> (
    Arc<ActorHost>,
    tokio::sync::mpsc::UnboundedReceiver<String>,
    Arc<tokio::sync::Semaphore>,
) {
    let (started, receiver) = tokio::sync::mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(ControlledExecutor {
            started,
            release: release.clone(),
        }),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
    );
    (Arc::new(host), receiver, release)
}

#[tokio::test]
async fn idle_eviction_rechecks_activity_before_unloading_the_actor() -> Result<()> {
    let (host, mut started, release) = controlled_host();
    let activity = host.activity();
    assert_eq!(invoke(&host, "warm").await?, completed(1));
    assert_eq!(started.recv().await.as_deref(), Some("warm"));
    let last_active = activity.borrow().last_active;

    let caller = host.clone();
    let running = tokio::spawn(async move { invoke(&caller, "first").await });
    assert_eq!(started.recv().await.as_deref(), Some("first"));
    host.evict_idle(last_active).await?;
    assert!(activity.borrow().resident);
    assert_eq!(activity.borrow().active, 1);

    release.add_permits(1);
    assert_eq!(running.await??, completed(2));
    host.evict_idle(last_active).await?;
    assert!(activity.borrow().resident);

    let last_active = activity.borrow().last_active;
    host.evict_idle(last_active).await?;
    assert!(!activity.borrow().resident);
    assert_eq!(invoke(&host, "after-eviction").await?, completed(3));
    assert!(activity.borrow().resident);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_actor_task_releases_admission_and_does_not_restart_unknown_state() -> Result<()> {
    for _ in 0..64 {
        let (host, mut started, release) = controlled_host();
        let mut activity = host.activity();
        let caller = host.clone();
        let first = tokio::spawn(async move { invoke(&caller, "first").await });
        assert_eq!(started.recv().await.as_deref(), Some("first"));
        let caller = host.clone();
        let panicking = tokio::spawn(async move { invoke(&caller, "panic").await });
        tokio::time::timeout(
            Duration::from_secs(2),
            activity.wait_for(|activity| activity.active == 2),
        )
        .await??;
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), first).await???,
            completed(1)
        );
        let result = tokio::time::timeout(Duration::from_secs(2), panicking).await???;
        assert!(
            matches!(result, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
        );
        assert_eq!(
            invoke(&host, "after-panic").await?,
            ActorExecutionResult::HostUnavailable
        );
        host.drain(Duration::from_secs(1)).await?;
        assert_eq!(activity.borrow().active, 0);
        assert!(host.queues().inventory().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn caller_cancellation_cannot_release_an_actor_during_its_commit() -> Result<()> {
    let (commit_started, mut committing) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport {
        paused_commit: Some((commit_started, release.clone())),
        ..Default::default()
    });
    let executor = Arc::new(SerialAdmissionExecutor::new());
    let host = Arc::new(ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    ));
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "first").await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), committing.recv()).await?,
        Some(())
    );
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let caller = host.clone();
    let mut second = tokio::spawn(async move { invoke(&caller, "second").await });
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut second)
            .await
            .is_err()
    );
    assert_eq!(executor.inner.invocations.load(Ordering::Relaxed), 1);
    release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), second).await???,
        completed(2)
    );
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn traces_measure_queue_wait_and_record_panics() -> Result<()> {
    let (host, mut started, release) = controlled_host();
    let (sender, mut traces) = crate::request_traces::TraceSender::channel(8);
    let host = Arc::new(Arc::try_unwrap(host).ok().unwrap().with_traces(sender));
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "first").await });
    assert_eq!(started.recv().await.as_deref(), Some("first"));
    let caller = host.clone();
    let second = tokio::spawn(async move { invoke(&caller, "second").await });
    let mut activity = host.activity();
    tokio::time::timeout(
        Duration::from_secs(2),
        activity.wait_for(|activity| activity.active == 2),
    )
    .await??;
    tokio::time::sleep(Duration::from_millis(25)).await;
    release.add_permits(1);
    first.await??;
    second.await??;
    let first_trace = traces.recv().await.unwrap();
    let second_trace = traces.recv().await.unwrap();
    assert_eq!(first_trace.request_id, "first");
    assert_eq!(second_trace.request_id, "second");
    assert_eq!(first_trace.state_version, Some(1));
    assert_eq!(second_trace.state_version, Some(2));
    assert!(second_trace.queue_wait_ms.unwrap() >= 25.0);
    assert!(second_trace.duration_ms >= second_trace.queue_wait_ms.unwrap());
    let _ = invoke(&host, "panic").await?;
    let interrupted = traces.recv().await.unwrap();
    assert!(matches!(
        interrupted.outcome,
        crate::request_traces::RequestOutcome::Interrupted
    ));
    host.drain(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_callers_do_not_interrupt_accepted_actor_operations() -> Result<()> {
    for socket in [false, true] {
        let (host, mut started, release) = controlled_host();
        let caller = host.clone();
        let first = tokio::spawn(async move {
            if socket {
                caller
                    .submit(
                        ActorOperation::Socket(ActorSocketInvocation {
                            request_id: "first".into(),
                            actor: ActorKey {
                                project_id: "default".into(),
                                actor_name: "Counter".into(),
                                actor_id: "counter-1".into(),
                            },
                            event: crate::actor::ActorSocketEvent::Message {
                                connection_id: "socket-1".into(),
                                message: crate::actor::ActorSocketMessage::Text {
                                    data: "increment".into(),
                                },
                            },
                            connections: Vec::new(),
                        }),
                        1,
                    )
                    .await
            } else {
                invoke(&caller, "first").await
            }
        });
        assert_eq!(started.recv().await.as_deref(), Some("first"));
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let caller = host.clone();
        let second = tokio::spawn(async move { invoke(&caller, "second").await });
        assert!(
            tokio::time::timeout(Duration::from_millis(30), started.recv())
                .await
                .is_err(),
            "the next call overtook an accepted operation"
        );
        release.add_permits(1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), second).await???,
            completed(2)
        );
        host.drain(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn actor_admission_is_bounded_and_other_identities_are_rejected() -> Result<()> {
    let (host, mut started, release) = controlled_host();
    let mut activity = host.activity();
    let caller = host.clone();
    let first = tokio::spawn(async move { invoke(&caller, "first").await });
    assert_eq!(started.recv().await.as_deref(), Some("first"));
    let mut queued = Vec::new();
    for index in 0..32 {
        let caller = host.clone();
        queued.push(tokio::spawn(async move {
            invoke(&caller, &format!("queued-{index}")).await
        }));
    }
    tokio::time::timeout(
        Duration::from_secs(2),
        activity.wait_for(|activity| activity.active == 33),
    )
    .await??;
    assert_eq!(
        invoke(&host, "overflow").await?,
        ActorExecutionResult::HostUnavailable
    );
    let other = host.invoke_actor(
        ActorInvocation {
            request_id: "other".into(),
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "other".into(),
            },
            method: "increment".into(),
            args: Vec::new(),
        },
        1,
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), other).await??,
        ActorExecutionResult::Reroute
    );
    assert!(host.drain(Duration::from_millis(30)).await.is_err());
    assert_eq!(
        invoke(&host, "draining").await?,
        ActorExecutionResult::HostUnavailable
    );
    release.add_permits(1);
    assert_eq!(first.await??, completed(1));
    for caller in queued {
        assert_eq!(caller.await??, ActorExecutionResult::HostUnavailable);
    }
    host.drain(Duration::from_secs(1)).await?;
    assert_eq!(activity.borrow().active, 0);
    assert!(host.queues().inventory().is_empty());
    Ok(())
}

#[async_trait]
impl ActorExecutor for IncrementingExecutor {
    fn supports(&self, actor_name: &str) -> bool {
        actor_name == "Counter"
    }

    async fn invoke(
        &self,
        _invocation: ActorMethodInvocation,
        state: Option<&Value>,
    ) -> Result<ActorMethodOutcome> {
        self.invocations.fetch_add(1, Ordering::Relaxed);
        let count = state
            .and_then(|state| state.get("count"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        Ok(ActorMethodOutcome::Completed {
            result: json!(count),
            state: json!({ "count": count }),
            effects: Vec::new(),
        })
    }

    async fn handle_socket(
        &self,
        _invocation: ActorSocketInvocation,
        state: Option<&Value>,
    ) -> Result<ActorSocketOutcome> {
        let count = state
            .and_then(|state| state.get("count"))
            .and_then(Value::as_u64)
            .unwrap_or(0)
            + 1;
        Ok(ActorSocketOutcome::Handled {
            state: json!({ "count": count }),
            effects: vec![ActorSocketEffect::Send {
                connection_id: "socket-1".into(),
                message: crate::actor::ActorSocketMessage::Text {
                    data: "ready".into(),
                },
            }],
        })
    }
}

#[async_trait]
impl ActorExecutor for ExhaustedExecutor {
    fn supports(&self, _actor_name: &str) -> bool {
        true
    }

    async fn invoke(
        &self,
        _invocation: ActorMethodInvocation,
        _state: Option<&Value>,
    ) -> Result<ActorMethodOutcome> {
        Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
            code: "resource_exhausted".into(),
            message: "actor session message is too large".into(),
        }))
    }
}

#[async_trait]
impl ActorExecutor for InvalidEffectsExecutor {
    fn supports(&self, _actor_name: &str) -> bool {
        true
    }

    async fn invoke(
        &self,
        _invocation: ActorMethodInvocation,
        _state: Option<&Value>,
    ) -> Result<ActorMethodOutcome> {
        Ok(ActorMethodOutcome::Completed {
            result: Value::Null,
            state: json!({ "count": 1 }),
            effects: vec![ActorSocketEffect::Close {
                connection_id: "socket-1".into(),
                code: 1001,
                reason: String::new(),
            }],
        })
    }
}

#[tokio::test]
async fn activation_acquires_on_host_without_preparing_a_write() -> Result<()> {
    let authority = Arc::new(FakeAuthority {
        ..Default::default()
    });
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
    );
    let activation = host.activate_actor(actor.clone()).await?;
    assert_eq!(activation.owner_epoch, 7);
    assert!(authority.preparations.lock().unwrap().is_empty());
    let invoke = |request_id: &str| ActorInvocation {
        request_id: request_id.into(),
        actor: actor.clone(),
        method: "increment".into(),
        args: vec![],
    };
    assert_eq!(host.invoke_actor(invoke("one"), 7).await?, completed(1));
    host.activate_actor(actor.clone()).await?;
    assert_eq!(host.invoke_actor(invoke("two"), 7).await?, completed(2));
    assert_eq!(*authority.preparations.lock().unwrap(), vec![0]);

    authority.fenced.store(true, Ordering::SeqCst);
    assert!(host.activate_actor(actor).await.is_err());
    Ok(())
}

#[tokio::test]
async fn activation_reuses_recovered_bytes_and_publishes_readiness_without_a_write_ticket()
-> Result<()> {
    let snapshot =
        StateSnapshot::new(3, 6, "previous".into(), json!({"count": 41}), json!(41))?.encode()?;
    let authority = Arc::new(FakeAuthority {
        activation_state: Some(snapshot),
        ..Default::default()
    });
    let transport = Arc::new(FakeStateTransport::default());
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "restored".into(),
    };
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host"),
            route: "http://host".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        transport.clone(),
        Arc::new(EmptySocketPublisher),
    );
    let activation = host.activate_actor(actor.clone()).await?;
    assert_eq!(activation.state_version, 3);
    assert!(authority.preparations.lock().unwrap().is_empty());
    let result = host
        .invoke_actor(
            ActorInvocation {
                actor,
                request_id: "next".into(),
                method: "increment".into(),
                args: vec![],
            },
            7,
        )
        .await?;
    assert_eq!(result, completed(42));
    assert_eq!(authority.loads.load(Ordering::SeqCst), 0);
    assert_eq!(*authority.preparations.lock().unwrap(), vec![3]);
    Ok(())
}

#[derive(Default)]
struct FakeAuthority {
    fenced: std::sync::atomic::AtomicBool,
    loads: AtomicUsize,
    initial_state: Option<(u64, bytes::Bytes)>,
    activation_state: Option<Vec<u8>>,
    preparations: Mutex<Vec<u64>>,
}

#[async_trait]
impl ActorStorage for FakeAuthority {
    async fn acquire_actor(
        &self,
        _actor: &ActorKey,
        _host: &super::super::HostId,
    ) -> Result<ActorActivation> {
        Ok(ActorActivation {
            owner_epoch: 7,
            state_version: self
                .activation_state
                .as_ref()
                .map(|bytes| StateSnapshot::decode(bytes).map(|s| s.state_version))
                .transpose()?
                .unwrap_or(0),
            state: self.activation_state.clone().map(Into::into),
        })
    }

    fn ensure_authority(&self) -> Result<()> {
        anyhow::ensure!(!self.fenced.load(Ordering::SeqCst), "host lease expired");
        Ok(())
    }
    async fn load_actor_state(
        &self,
        _: &ActorKey,
        _: &super::super::HostId,
        _: u64,
    ) -> Result<(u64, bytes::Bytes)> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        Ok(self.initial_state.clone().unwrap_or_default())
    }
    async fn prepare_state_write(
        &self,
        _actor: &ActorKey,
        _host_id: &super::super::HostId,
        _owner_epoch: u64,
        expected_version: u64,
    ) -> Result<WritePlan> {
        self.preparations.lock().unwrap().push(expected_version);
        let mut ticket = ticket(expected_version + 1);
        {
            let stream = crate::storage::StateStream {
                prefix: "snapshots/epoch/".into(),
                session: "snapshots/epoch/sessions/one/".into(),
                owner_epoch: _owner_epoch,
                base_version: 0,
            };
            ticket.object_name = stream.object(ticket.state_version);
            ticket.stream = stream;
        }
        Ok(ticket)
    }
}

#[derive(Default)]
struct FakeStateTransport {
    failures: AtomicUsize,
    writes: Mutex<Vec<Vec<u8>>>,
    paused_commit: Option<(mpsc::UnboundedSender<()>, Arc<tokio::sync::Semaphore>)>,
}

#[async_trait]
impl crate::state_transport::SnapshotWriter for FakeStateTransport {
    async fn write_snapshot(&self, _ticket: &WritePlan, bytes: Vec<u8>) -> Result<StateWrite> {
        if StateSnapshot::decode(&bytes)?.request_id == "first"
            && let Some((started, release)) = &self.paused_commit
        {
            started.send(())?;
            release.acquire().await?.forget();
        }
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            self.writes.lock().unwrap().push(bytes);
            anyhow::bail!("write response lost");
        }

        self.writes.lock().unwrap().push(bytes);
        Ok(StateWrite::Written)
    }
}

#[tokio::test]
async fn cold_actors_load_the_current_head_instead_of_a_cached_route_snapshot() -> Result<()> {
    let snapshot =
        StateSnapshot::new(8, 1, "previous".into(), json!({"count":8}), json!(8))?.encode()?;
    let authority = Arc::new(FakeAuthority {
        initial_state: Some((8, snapshot.into())),
        ..Default::default()
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );
    assert_eq!(invoke(&host, "request-1").await?, completed(9));
    assert_eq!(authority.loads.load(Ordering::Relaxed), 1);
    assert_eq!(*authority.preparations.lock().unwrap(), [8]);
    Ok(())
}

#[tokio::test]
async fn read_only_results_are_withheld_if_the_lease_expires_during_execution() -> Result<()> {
    struct ExpiringExecutor(Arc<FakeAuthority>);
    #[async_trait]
    impl ActorExecutor for ExpiringExecutor {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            state: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            self.0.fenced.store(true, Ordering::SeqCst);
            Ok(ActorMethodOutcome::Completed {
                result: json!(8),
                state: state.unwrap().clone(),
                effects: vec![],
            })
        }
    }
    let snapshot =
        StateSnapshot::new(8, 1, "previous".into(), json!({"count":8}), json!(8))?.encode()?;
    let authority = Arc::new(FakeAuthority {
        initial_state: Some((8, snapshot.into())),
        ..Default::default()
    });
    let state = Arc::new(FakeStateTransport::default());
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(ExpiringExecutor(authority.clone())),
        authority,
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );
    assert!(invoke(&host, "request-1").await.is_err());
    assert!(state.writes.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn write_results_are_withheld_if_the_lease_expires_during_persistence() -> Result<()> {
    {
        let (commit_started, mut committing) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let authority = Arc::new(FakeAuthority::default());
        let state = Arc::new(FakeStateTransport {
            paused_commit: Some((commit_started, release.clone())),
            ..Default::default()
        });
        let host = Arc::new(ActorHost::new(
            HostEndpoint {
                id: super::super::HostId::new("host-1"),
                route: "http://host.invalid/".into(),
            },
            Arc::new(IncrementingExecutor {
                invocations: AtomicU64::new(0),
            }),
            authority.clone(),
            state.clone(),
            Arc::new(EmptySocketPublisher),
        ));
        let caller = host.clone();
        let result = tokio::spawn(async move { invoke(&caller, "first").await });
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), committing.recv()).await?,
            Some(())
        );
        authority.fenced.store(true, Ordering::SeqCst);
        release.add_permits(1);
        let error = tokio::time::timeout(Duration::from_secs(2), result)
            .await??
            .unwrap_err();
        assert!(error.to_string().contains("host lease expired"));
        assert_eq!(state.writes.lock().unwrap().len(), 1);
        host.drain(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn epoch_snapshot_proofs_commit_locally_and_retry_an_ambiguous_write() -> Result<()> {
    let authority = Arc::new(FakeAuthority {
        ..Default::default()
    });
    let state = Arc::new(FakeStateTransport {
        failures: AtomicUsize::new(1),
        ..Default::default()
    });
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );
    assert!(matches!(
        invoke(&host, "request-1").await?,
        ActorExecutionResult::Failed { .. }
    ));
    assert_eq!(invoke(&host, "request-1").await?, completed(1));
    assert_eq!(invoke(&host, "request-2").await?, completed(2));
    assert_eq!(executor.invocations.load(Ordering::Relaxed), 2);

    assert_eq!(*authority.preparations.lock().unwrap(), [0, 0]);
    let writes = state.writes.lock().unwrap();
    assert_eq!(writes[0], writes[1]);
    assert_eq!(writes.len(), 3);
    Ok(())
}

#[tokio::test]
async fn durable_snapshot_commits_locally_before_success() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport {
        ..Default::default()
    });
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );
    assert_eq!(invoke(&host, "request-1").await?, completed(1));
    assert_eq!(state.writes.lock().unwrap().len(), 1);
    Ok(())
}

#[tokio::test]
async fn resident_actor_uses_immutable_snapshots_and_replays_the_last_request() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    let executor = Arc::new(IncrementingExecutor {
        invocations: AtomicU64::new(0),
    });
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );

    assert_eq!(invoke(&host, "request-1").await?, completed(1));
    assert_eq!(invoke(&host, "request-2").await?, completed(2));
    assert_eq!(invoke(&host, "request-2").await?, completed(2));

    assert_eq!(executor.invocations.load(Ordering::Relaxed), 2);
    assert_eq!(authority.loads.load(Ordering::Relaxed), 1);
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    assert_eq!(*authority.preparations.lock().unwrap(), [0]);
    let snapshots = state
        .writes
        .lock()
        .unwrap()
        .iter()
        .map(|bytes| StateSnapshot::decode(bytes))
        .collect::<Result<Vec<_>>>()?;
    assert_eq!(snapshots[0].state_version, 1);
    assert_eq!(snapshots[1].state_version, 2);
    Ok(())
}

#[tokio::test]
async fn retries_an_ambiguous_commit_without_executing_the_request_twice() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    state.failures.store(1, Ordering::SeqCst);
    let executor = Arc::new(SerialAdmissionExecutor::new());
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        executor.clone(),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );

    let first = invoke(&host, "request-1").await?;
    assert!(matches!(
        first,
        ActorExecutionResult::Failed { ref failure } if failure.code == "outcome_unknown"
    ));
    assert_eq!(invoke(&host, "request-1").await?, completed(1));

    assert_eq!(executor.inner.invocations.load(Ordering::Relaxed), 1);
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn preserves_executor_resource_exhaustion() -> Result<()> {
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(ExhaustedExecutor),
        Arc::new(FakeAuthority::default()),
        Arc::new(FakeStateTransport::default()),
        Arc::new(EmptySocketPublisher),
    );

    assert!(matches!(
        invoke(&host, "request-1").await?,
        ActorExecutionResult::Failed { ref failure } if failure.code == "resource_exhausted"
    ));
    Ok(())
}

#[tokio::test]
async fn invalid_socket_effects_do_not_commit_actor_state() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(InvalidEffectsExecutor),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );

    assert!(matches!(
        invoke(&host, "request-1").await?,
        ActorExecutionResult::Failed { ref failure }
            if failure.code == "actor_error" && failure.message.contains("invalid socket effects")
    ));
    assert!(state.writes.lock().unwrap().is_empty());

    Ok(())
}

#[tokio::test]
async fn publishes_automatic_state_after_commit_before_returning_to_rpc_callers() -> Result<()> {
    struct Emitter;
    #[async_trait]
    impl ActorExecutor for Emitter {
        fn supports(&self, _: &str) -> bool {
            true
        }
        async fn invoke(
            &self,
            _: ActorMethodInvocation,
            state: Option<&Value>,
        ) -> Result<ActorMethodOutcome> {
            let count = state.and_then(|state| state["count"].as_u64()).unwrap_or(0) + 1;
            Ok(ActorMethodOutcome::Completed {
                result: json!(count),
                state: json!({"count": count}),
                effects: serde_json::from_value(
                    json!([{ "type":"state_update", "changes":{"count":count}, "removed":[] }]),
                )?,
            })
        }
    }
    struct Publisher {
        state: Arc<FakeStateTransport>,
        values: Mutex<Vec<Value>>,
    }
    #[async_trait]
    impl crate::actor::ActorSocketPublisher for Publisher {
        async fn publish(&self, _: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()> {
            let persisted = self.state.writes.lock().unwrap();
            let version = StateSnapshot::decode(
                persisted
                    .last()
                    .expect("state must persist before publishing"),
            )?
            .state_version;
            let value = serde_json::to_value(effects)?;
            assert_eq!(value[0]["version"], version);
            self.values.lock().unwrap().push(value);
            Ok(())
        }
    }
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    let publisher = Arc::new(Publisher {
        state: state.clone(),
        values: Mutex::new(vec![]),
    });
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(Emitter),
        authority.clone(),
        state.clone(),
        publisher.clone(),
    );
    assert_eq!(invoke(&host, "one").await?, completed(1));
    assert_eq!(invoke(&host, "two").await?, completed(2));
    {
        let values = publisher.values.lock().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0][0]["changes"]["count"], 1);
        assert_eq!(values[1][0]["changes"]["count"], 2);
    }
    state.failures.store(1, Ordering::SeqCst);
    assert!(matches!(
        invoke(&host, "failed").await?,
        ActorExecutionResult::Failed { .. }
    ));
    assert_eq!(publisher.values.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn socket_events_return_effects_only_after_committing_state() -> Result<()> {
    let authority = Arc::new(FakeAuthority::default());
    let state = Arc::new(FakeStateTransport::default());
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Counter".into(),
        actor_id: "counter-1".into(),
    };
    let connection = crate::actor::ActorSocketConnection {
        id: "socket-1".into(),
        metadata: json!({ "userId": "user-1" }),
        tags: Vec::new(),
    };
    let host = ActorHost::new(
        HostEndpoint {
            id: super::super::HostId::new("host-1"),
            route: "http://host.invalid/".into(),
        },
        Arc::new(IncrementingExecutor {
            invocations: AtomicU64::new(0),
        }),
        authority.clone(),
        state.clone(),
        Arc::new(EmptySocketPublisher),
    );

    let invocation = |request_id: &str| ActorSocketInvocation {
        request_id: request_id.into(),
        actor: actor.clone(),
        event: crate::actor::ActorSocketEvent::Connect {
            connection: connection.clone(),
        },
        connections: Vec::new(),
    };
    let result = host
        .submit(ActorOperation::Socket(invocation("committed")), 1)
        .await?;

    assert!(matches!(
        result,
        ActorExecutionResult::Completed { result: Value::Null, ref effects } if effects.len() == 1
    ));
    assert_eq!(state.writes.lock().unwrap().len(), 1);
    let saved = state.writes.lock().unwrap()[0].clone();
    let snapshot = crate::state_log::StateSnapshot::decode(&saved)?;
    let attribution = snapshot.attribution.unwrap();
    assert_eq!(snapshot.request_id, "committed");
    assert_eq!(attribution.connection_id.as_deref(), Some("socket-1"));
    assert_eq!(attribution.operation, "onConnect");
    assert!(!attribution.interleaved);

    state.failures.store(1, Ordering::SeqCst);
    let failed = host
        .submit(ActorOperation::Socket(invocation("failed")), 1)
        .await?;
    assert!(
        matches!(failed, ActorExecutionResult::Failed { failure } if failure.code == "outcome_unknown")
    );

    host.drain(Duration::from_secs(1)).await?;
    assert_eq!(
        host.submit(ActorOperation::Socket(invocation("drained")), 1)
            .await?,
        ActorExecutionResult::HostUnavailable
    );
    assert_eq!(state.writes.lock().unwrap().len(), 2);
    Ok(())
}

async fn invoke(host: &ActorHost, request_id: &str) -> Result<ActorExecutionResult> {
    host.invoke_actor(
        ActorInvocation {
            request_id: request_id.into(),
            actor: ActorKey {
                project_id: "default".into(),
                actor_name: "Counter".into(),
                actor_id: "counter-1".into(),
            },
            method: "increment".into(),
            args: Vec::new(),
        },
        1,
    )
    .await
}

fn completed(count: u64) -> ActorExecutionResult {
    ActorExecutionResult::Completed {
        result: json!(count),
        effects: Vec::new(),
    }
}

fn ticket(state_version: u64) -> WritePlan {
    WritePlan {
        stream: crate::storage::StateStream {
            prefix: "snapshots/epoch/".into(),
            session: "session".into(),
            owner_epoch: 1,
            base_version: 0,
        },

        object_name: format!("snapshots/{state_version}.json"),
        state_version,
    }
}

#[path = "http_invocation.rs"]
mod http_invocation;
