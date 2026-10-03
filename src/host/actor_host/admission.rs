use std::collections::BTreeMap;

use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};

use super::*;
use crate::actor::{
    ActorInterleavedOutcome, ActorMethodInvocation, ActorMethodOutcome, ActorSocketOutcome,
};
use crate::host::actor_runtime::PreparedInvocation;

type Execution = Result<ActorMethodOutcome>;
type Completion = (u64, ActorRequest, Execution);

pub(super) async fn run(
    object: ActorStorageKey,
    runtime: ActorRuntime,
    mut requests: mpsc::UnboundedReceiver<ActorRequest>,
    completed: mpsc::Sender<ActorCompletion>,
    accepting: watch::Receiver<bool>,
    mut admission: watch::Receiver<()>,
) {
    let mut mailbox = Mailbox {
        object,
        runtime,
        completed,
        running: FuturesUnordered::new(),
        ready: BTreeMap::new(),
        next_sequence: 1,
        next_invocation: 0,
        blocked_on: None,
        interleaved: false,
    };
    let mut open = true;
    loop {
        tokio::select! {
            biased;
            Some((id, request, result)) = mailbox.running.next(), if !mailbox.running.is_empty() => {
                if !mailbox.executed(id, request, result).await { return; }
            }
            permission = admission.changed(), if mailbox.blocked_on.is_some() => {
                if permission.is_err() { return; }
                mailbox.interleaved = true;
                mailbox.blocked_on = None;
            }
            request = requests.recv(), if open && mailbox.blocked_on.is_none() => match request {
                Some(request) => {
                    let accepting = *accepting.borrow();
                    // A completed invocation may have left an unused permission.
                    admission.borrow_and_update();
                    if !mailbox.admit(request, accepting).await { return; }
                }
                None => open = false,
            },
            else => return,
        }
    }
}

struct Mailbox {
    object: ActorStorageKey,
    runtime: ActorRuntime,
    completed: mpsc::Sender<ActorCompletion>,
    running: FuturesUnordered<BoxFuture<'static, Completion>>,
    ready: BTreeMap<u64, (u64, ActorRequest, ActorInterleavedOutcome)>,
    next_invocation: u64,
    blocked_on: Option<u64>,
    interleaved: bool,
    next_sequence: u64,
}

impl Mailbox {
    async fn admit(&mut self, mut request: ActorRequest, accepting: bool) -> bool {
        if !accepting && !request.operation.is_disconnect() {
            self.finish(request, Ok(ActorExecutionResult::HostUnavailable))
                .await;
            return true;
        }
        drop(request.waiting.take());
        if let Some(trace) = &mut request.trace {
            trace.admitted();
        }
        if matches!(request.operation, ActorOperation::Activate { .. }) {
            return self.activate(request).await;
        }
        let invocation = request.operation.invocation().into_owned();
        let state = match self
            .runtime
            .prepare_invocation(
                &invocation,
                request.owner_epoch,
                matches!(request.operation, ActorOperation::Method(_)),
                &mut request.timings,
            )
            .await
        {
            Ok(PreparedInvocation::Execute(state)) => state,
            Ok(PreparedInvocation::Completed(result)) => {
                self.finish(request, Ok(result)).await;
                return true;
            }
            Err(error) => return self.stop(request, error).await,
        };
        let executor = self.runtime.executor();
        self.next_invocation += 1;
        let id = self.next_invocation;
        self.blocked_on = Some(id);
        self.running.push(
            async move {
                let result = execute(&*executor, &request.operation, state).await;
                (id, request, result)
            }
            .boxed(),
        );
        true
    }

    async fn activate(&mut self, mut request: ActorRequest) -> bool {
        let ActorOperation::Activate { actor, reply } = request.operation else {
            unreachable!()
        };
        let result = self.runtime.activate_actor(&actor).await;
        let valid = result.is_ok();
        let _ = reply.send(result);
        request.operation = ActorOperation::Method(ActorInvocation {
            actor,
            request_id: "activate".into(),
            method: "activate".into(),
            args: vec![],
        });
        self.finish(
            request,
            Ok(ActorExecutionResult::Completed {
                result: serde_json::Value::Null,
                effects: vec![],
            }),
        )
        .await;
        valid
    }

    async fn executed(&mut self, id: u64, mut request: ActorRequest, result: Execution) -> bool {
        if let Ok(ActorMethodOutcome::Interleaved(outcome)) = result {
            self.interleaved = true;
            if outcome.sequence < self.next_sequence || self.ready.contains_key(&outcome.sequence) {
                return self
                    .stop(request, anyhow::anyhow!("actor snapshot sequence repeated"))
                    .await;
            }
            self.ready.insert(outcome.sequence, (id, request, outcome));
            return self.commit_ready().await;
        }
        if self.interleaved {
            match &result {
                Ok(ActorMethodOutcome::Failed(failure))
                    if matches!(
                        failure.code.as_str(),
                        "actor_method_failed"
                            | "actor_socket_failed"
                            | "method_not_found"
                            | "method_not_callable"
                    ) => {}
                _ => {
                    return self
                        .stop(
                            request,
                            anyhow::anyhow!("interleaved actor execution failed: {result:?}"),
                        )
                        .await;
                }
            }
            let Ok(ActorMethodOutcome::Failed(failure)) = result else {
                unreachable!()
            };
            self.finish(
                request,
                Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure {
                        code: "actor_error".into(),
                        message: failure.message,
                    },
                }),
            )
            .await;
        } else {
            let invocation = request.operation.invocation().into_owned();
            let result = self
                .runtime
                .finish_serial(
                    &invocation,
                    request.owner_epoch,
                    result,
                    request.operation.commit_origin(),
                    &mut request.timings,
                )
                .await;
            self.finish(request, result).await;
        }
        self.release(id);
        true
    }

    async fn commit_ready(&mut self) -> bool {
        while let Some((id, mut request, outcome)) = self.ready.remove(&self.next_sequence) {
            let invocation = request.operation.invocation().into_owned();
            let result = self
                .runtime
                .commit_interleaved(
                    &invocation,
                    request.owner_epoch,
                    outcome,
                    request.operation.commit_origin(),
                    &mut request.timings,
                )
                .await;
            match result {
                Ok(result @ ActorExecutionResult::Completed { .. }) => {
                    self.next_sequence += 1;
                    self.finish(request, Ok(result)).await;
                    self.release(id);
                }
                Ok(_) => {
                    return self
                        .stop(request, anyhow::anyhow!("actor state publication failed"))
                        .await;
                }
                Err(error) => return self.stop(request, error).await,
            }
        }
        true
    }

    fn release(&mut self, id: u64) {
        if self.blocked_on == Some(id) {
            self.blocked_on = None;
        }
    }

    async fn stop(&mut self, request: ActorRequest, error: anyhow::Error) -> bool {
        tracing::warn!(%error, actor = %self.object, "actor admission stopped; publication outcome may be unknown");
        self.runtime.evict(request.operation.actor()).await;
        self.finish(
            request,
            Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure::outcome_unknown_after_execution(),
            }),
        )
        .await;
        false
    }

    async fn finish(&mut self, mut request: ActorRequest, result: Result<ActorExecutionResult>) {
        ActorRuntime::log_invocation(
            self.runtime.endpoint(),
            &request.operation.invocation(),
            &request.timings,
            &result,
        );
        if let Some(trace) = &mut request.trace {
            trace.state_version(self.runtime.state_version());
            trace.complete(&result);
        }
        let _ = self
            .completed
            .send(ActorCompletion {
                object: self.object.clone(),
                reply: request.reply,
                result,
            })
            .await;
    }
}

async fn execute(
    executor: &dyn ActorExecutor,
    operation: &ActorOperation,
    state: Option<Arc<crate::actor::ActorState>>,
) -> Execution {
    match operation {
        ActorOperation::Method(invocation) => {
            executor
                .invoke_shared(
                    ActorMethodInvocation {
                        request_id: invocation.request_id.clone(),
                        actor: invocation.actor.clone(),
                        method: invocation.method.clone(),
                        args: invocation.args.clone(),
                    },
                    state,
                )
                .await
        }
        ActorOperation::Socket(invocation) => Ok(
            match executor
                .handle_socket_shared(invocation.clone(), state)
                .await?
            {
                ActorSocketOutcome::Interleaved(outcome) => {
                    ActorMethodOutcome::Interleaved(outcome)
                }
                ActorSocketOutcome::Handled { state, effects } => ActorMethodOutcome::Completed {
                    result: serde_json::Value::Null,
                    state,
                    effects,
                },
                ActorSocketOutcome::Failed(failure) => ActorMethodOutcome::Failed(failure),
            },
        ),
        ActorOperation::Activate { .. } => unreachable!(),
    }
}
