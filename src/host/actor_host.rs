use crate::request_traces::{RequestKind, TraceRecorder, TraceSender};
use crate::request_tracking::HostState;
use crate::request_tracking::{RequestMetadata, RequestObserver, RequestTracker};
use std::{
    borrow::Cow,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{Id, JoinError, JoinSet},
};
use tracing::error;

use crate::{
    actor::{
        ActorExecutionResult, ActorExecutor, ActorInvocation, ActorInvocationFailure, ActorKey,
        ActorSocketEvent, ActorSocketInvocation, ActorSocketPublisher,
    },
    actor_state::ActorStorageKey,
};

use super::{
    HostEndpoint,
    actor_runtime::{ActorActivation, ActorRuntime, ActorStorage, socket_event_name},
    queues::{ActorQueues, WaitingRequest},
};

mod admission;

const HOST_COMMAND_CAPACITY: usize = 256;

pub(crate) struct ActorHost {
    traces: Option<TraceSender>,
    queues: ActorQueues,
    endpoint: HostEndpoint,
    commands: mpsc::Sender<HostCommand>,
    activity: watch::Receiver<ActorActivity>,
    stopped: watch::Receiver<bool>,
}

#[derive(Clone, Copy)]
pub(super) struct ActorActivity {
    pub active: usize,
    pub last_active: tokio::time::Instant,
}

impl Default for ActorActivity {
    fn default() -> Self {
        Self {
            active: 0,
            last_active: tokio::time::Instant::now(),
        }
    }
}

impl ActorHost {
    pub(crate) fn new(
        endpoint: HostEndpoint,

        executor: Arc<dyn ActorExecutor>,
        storage: Arc<dyn ActorStorage>,
        state: Arc<dyn crate::state_transport::SnapshotWriter>,
        publisher: Arc<dyn ActorSocketPublisher>,
        replication: Arc<dyn crate::litestream::Replicator>,
    ) -> Self {
        let (commands, incoming) = mpsc::channel(HOST_COMMAND_CAPACITY);
        let (activity_tx, activity) = watch::channel(ActorActivity::default());
        let (stopped_tx, stopped) = watch::channel(false);
        let queues = ActorQueues::new();
        let dispatcher = HostDispatcher::new(
            endpoint.clone(),
            executor,
            storage,
            state,
            publisher,
            replication,
            (activity_tx, stopped_tx),
            queues.clone(),
        );
        tokio::spawn(dispatcher.run(incoming));
        Self {
            traces: None,
            queues,
            endpoint,
            commands,
            activity,
            stopped,
        }
    }

    pub(crate) fn queues(&self) -> ActorQueues {
        self.queues.clone()
    }

    pub(super) fn activity(&self) -> watch::Receiver<ActorActivity> {
        self.activity.clone()
    }

    pub(crate) fn stopped(&self) -> watch::Receiver<bool> {
        self.stopped.clone()
    }

    pub(crate) fn with_traces(mut self, traces: TraceSender) -> Self {
        self.traces = Some(traces);
        self
    }

    pub(crate) async fn handle_socket_event_since(
        &self,
        invocation: ActorSocketInvocation,
        owner_epoch: u64,
        started: Instant,
        host_state: HostState,
        routing_ms: f64,
    ) -> Result<ActorExecutionResult> {
        self.submit_traced(
            ActorOperation::Socket(invocation),
            owner_epoch,
            started,
            host_state,
            routing_ms,
        )
        .await
        .0
    }

    pub(crate) fn id(&self) -> &super::HostId {
        &self.endpoint.id
    }

    pub(crate) async fn activate_actor(&self, actor: ActorKey) -> Result<ActorActivation> {
        let (reply, result) = oneshot::channel();
        self.submit(ActorOperation::Activate { actor, reply }, 0)
            .await?;
        result.await.context("actor activation was not completed")?
    }

    pub(crate) async fn invoke_actor(
        &self,
        invocation: ActorInvocation,
        owner_epoch: u64,
        host_state: HostState,
        routing_ms: f64,
    ) -> (Result<ActorExecutionResult>, Option<RequestMetadata>) {
        self.submit_traced(
            ActorOperation::Method(invocation),
            owner_epoch,
            Instant::now(),
            host_state,
            routing_ms,
        )
        .await
    }

    pub(crate) async fn drain(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            let (reply, done) = oneshot::channel();
            self.commands
                .send(HostCommand::Drain(reply))
                .await
                .context("actor dispatcher stopped")?;
            done.await
                .context("actor dispatcher stopped while draining")
        })
        .await
        .context("actor invocations did not drain before shutdown")?
    }

    async fn submit(
        &self,
        operation: ActorOperation,
        owner_epoch: u64,
    ) -> Result<ActorExecutionResult> {
        self.submit_traced(operation, owner_epoch, Instant::now(), HostState::Warm, 0.0)
            .await
            .0
    }

    async fn submit_traced(
        &self,
        operation: ActorOperation,
        owner_epoch: u64,
        started: Instant,
        host_state: HostState,
        routing_ms: f64,
    ) -> (Result<ActorExecutionResult>, Option<RequestMetadata>) {
        let (reply, result) = oneshot::channel();
        let (tracker, timing) =
            RequestTracker::new(started, host_state, routing_ms, self.observer(&operation));
        let request = ActorRequest {
            tracker,
            waiting: None,
            operation,
            owner_epoch,

            reply,
        };
        let outcome = match self
            .commands
            .send(HostCommand::Invoke(Box::new(request)))
            .await
        {
            Ok(()) => result.await.unwrap_or_else(|_| {
                Ok(ActorExecutionResult::Failed {
                    failure: ActorInvocationFailure::outcome_unknown_after_execution(),
                })
            }),
            Err(_) => Err(anyhow::anyhow!("actor dispatcher stopped")),
        };
        let timing = timing.await.ok();
        (outcome, timing)
    }

    fn observer(&self, operation: &ActorOperation) -> Option<Box<dyn RequestObserver>> {
        let sender = self.traces.as_ref()?.clone();
        let (kind, connection, metadata) = match operation {
            ActorOperation::Activate { .. } => return None,
            ActorOperation::Method(_) => (RequestKind::Method, None, None),
            ActorOperation::Socket(invocation) => match &invocation.event {
                ActorSocketEvent::Connect { connection } => (
                    RequestKind::Websocket,
                    Some(connection.id.clone()),
                    Some(connection.metadata.clone()),
                ),
                ActorSocketEvent::Disconnect { connection, .. } => {
                    (RequestKind::Websocket, Some(connection.id.clone()), None)
                }
                ActorSocketEvent::Message { connection_id, .. } => {
                    (RequestKind::Websocket, Some(connection_id.clone()), None)
                }
            },
        };
        Some(Box::new(TraceRecorder::new(
            sender,
            &operation.invocation(),
            kind,
            connection,
            metadata,
        )))
    }
}

struct HostDispatcher {
    last_active: tokio::time::Instant,
    endpoint: HostEndpoint,

    executor: Arc<dyn ActorExecutor>,
    storage: Arc<dyn ActorStorage>,
    state: Arc<dyn crate::state_transport::SnapshotWriter>,
    publisher: Arc<dyn ActorSocketPublisher>,
    replication: Arc<dyn crate::litestream::Replicator>,
    mailbox: Option<ActorMailbox>,
    identity: Option<ActorStorageKey>,
    tasks: JoinSet<()>,
    accepting: watch::Sender<bool>,
    activity: watch::Sender<ActorActivity>,
    stopped: watch::Sender<bool>,
    active: usize,
    queues: ActorQueues,
    drained: Vec<oneshot::Sender<()>>,
}

impl HostDispatcher {
    fn new(
        endpoint: HostEndpoint,

        executor: Arc<dyn ActorExecutor>,
        storage: Arc<dyn ActorStorage>,
        state: Arc<dyn crate::state_transport::SnapshotWriter>,
        publisher: Arc<dyn ActorSocketPublisher>,
        replication: Arc<dyn crate::litestream::Replicator>,
        status: (watch::Sender<ActorActivity>, watch::Sender<bool>),
        queues: ActorQueues,
    ) -> Self {
        let last_active = status.0.borrow().last_active;
        Self {
            last_active,
            endpoint,
            executor,
            storage,
            state,
            publisher,
            replication,
            mailbox: None,
            identity: None,
            tasks: JoinSet::new(),
            accepting: watch::channel(true).0,
            activity: status.0,
            stopped: status.1,
            active: 0,
            queues,
            drained: Vec::new(),
        }
    }

    async fn run(mut self, mut commands: mpsc::Receiver<HostCommand>) {
        let (completed, mut completions) = mpsc::channel(HOST_COMMAND_CAPACITY);
        loop {
            tokio::select! {
                biased;
                Some(completion) = completions.recv() => self.complete(completion),
                Some(result) = self.tasks.join_next_with_id(), if !self.tasks.is_empty() => self.task_stopped(result),
                command = commands.recv() => match command {
                    Some(HostCommand::Invoke(request)) => self.admit(*request, &completed),
                    Some(HostCommand::Drain(reply)) => {
                        self.accepting.send_replace(false);
                        self.drained.retain(|waiter| !waiter.is_closed());
                        self.drained.push(reply);
                        self.publish_activity();
                    }
                    None => return,
                },
            }
        }
    }

    fn admit(&mut self, mut request: ActorRequest, completed: &mpsc::Sender<ActorCompletion>) {
        if let Some(result) = self.validate(&request) {
            request.finish(&self.endpoint, result);
            return;
        }
        let object = request.operation.actor().storage_key();
        if self
            .identity
            .as_ref()
            .is_some_and(|identity| *identity != object)
        {
            request.finish(&self.endpoint, Ok(ActorExecutionResult::Reroute));
            return;
        }
        self.identity.get_or_insert_with(|| object.clone());
        if self.mailbox.is_none() {
            self.start_actor(request.operation.actor().clone(), completed.clone());
        }
        let mailbox = self.mailbox.as_mut().expect("actor mailbox created");
        if !matches!(&request.operation, ActorOperation::Activate { .. }) {
            request.waiting = Some(self.queues.enqueue(
                request.operation.actor(),
                request.operation.invocation().method.clone(),
            ));
        }
        match mailbox.sender.send(request) {
            Ok(()) => {
                mailbox.admitted += 1;
                self.active += 1;
                self.publish_activity();
            }
            Err(error) => {
                error
                    .0
                    .finish(&self.endpoint, Ok(ActorExecutionResult::HostUnavailable));
            }
        }
    }

    fn validate(&self, request: &ActorRequest) -> Option<Result<ActorExecutionResult>> {
        if !*self.accepting.borrow() && !request.operation.is_disconnect() {
            return Some(Ok(ActorExecutionResult::HostUnavailable));
        }
        if let Err(error) = request.operation.validate() {
            return Some(Err(error));
        }
        if !self
            .executor
            .supports(&request.operation.actor().actor_name)
        {
            return Some(Ok(ActorExecutionResult::Failed {
                failure: ActorInvocationFailure {
                    code: "actor_error".into(),
                    message: "actor name is not loaded by this host".into(),
                },
            }));
        }
        None
    }

    fn start_actor(&mut self, actor: ActorKey, completed: mpsc::Sender<ActorCompletion>) {
        let runtime = ActorRuntime::new(
            self.endpoint.clone(),
            self.executor.clone(),
            self.storage.clone(),
            self.state.clone(),
            self.publisher.clone(),
            self.replication.clone(),
        );
        let (sender, requests) = mpsc::unbounded_channel();
        let task = self.tasks.spawn(run_actor(
            actor.storage_key(),
            runtime,
            requests,
            completed,
            self.accepting.subscribe(),
        ));
        self.mailbox = Some(ActorMailbox {
            sender,
            admitted: 0,
            task_id: task.id(),
        });
    }

    fn complete(&mut self, completion: ActorCompletion) {
        debug_assert_eq!(self.identity.as_ref(), Some(&completion.object));
        let mailbox = self.mailbox.as_mut().expect("completed actor mailbox");
        // A stopped task's remaining admissions may already have been released.
        if mailbox.admitted > 0 {
            mailbox.admitted -= 1;
            self.active -= 1;
        }
        self.last_active = tokio::time::Instant::now();
        self.publish_activity();
        let _ = completion.reply.send(completion.result);
    }

    fn task_stopped(&mut self, result: Result<(Id, ()), JoinError>) {
        let id = match result {
            Ok((id, ())) => id,
            Err(error) => {
                error!(error = %error, "actor task stopped unexpectedly");
                error.id()
            }
        };
        if let Some(mailbox) = self
            .mailbox
            .as_mut()
            .filter(|mailbox| mailbox.task_id == id)
        {
            self.active -= mailbox.admitted;
            mailbox.admitted = 0;
        }
        self.stopped.send_replace(true);
        self.publish_activity();
    }

    fn publish_activity(&mut self) {
        self.activity.send_replace(ActorActivity {
            active: self.active,
            last_active: self.last_active,
        });
        if self.active == 0 {
            for waiter in self.drained.drain(..) {
                let _ = waiter.send(());
            }
        }
    }
}

async fn run_actor(
    object: ActorStorageKey,
    mut runtime: ActorRuntime,
    mut requests: mpsc::UnboundedReceiver<ActorRequest>,
    completed: mpsc::Sender<ActorCompletion>,
    accepting: watch::Receiver<bool>,
) {
    if let Some(admission) = runtime.executor().invocation_admission() {
        admission::run(object, runtime, requests, completed, accepting, admission).await;
        return;
    }
    while let Some(mut request) = requests.recv().await {
        drop(request.waiting.take());
        let invocation = request.operation.invocation().into_owned();
        let result = if !*accepting.borrow() && !request.operation.is_disconnect() {
            Ok(ActorExecutionResult::HostUnavailable)
        } else {
            request.tracker.admitted();
            match request.operation {
                ActorOperation::Activate { actor, reply } => {
                    let _ = reply.send(runtime.activate_actor(&actor).await);
                    Ok(ActorExecutionResult::Completed {
                        result: serde_json::Value::Null,
                        effects: vec![],
                    })
                }

                ActorOperation::Method(invocation) => {
                    runtime
                        .invoke_actor(invocation, request.owner_epoch, &mut request.tracker)
                        .await
                }
                ActorOperation::Socket(invocation) => {
                    runtime
                        .handle_socket_event(invocation, request.owner_epoch, &mut request.tracker)
                        .await
                }
            }
        };
        request.tracker.state_version(runtime.state_version());
        request.tracker.complete(&result);
        ActorRuntime::log_invocation(
            runtime.endpoint(),
            &invocation,
            request.tracker.completion(),
            &result,
        );
        if completed
            .send(ActorCompletion {
                object: object.clone(),
                reply: request.reply,
                result,
            })
            .await
            .is_err()
        {
            return;
        }
    }
}

enum HostCommand {
    Invoke(Box<ActorRequest>),
    Drain(oneshot::Sender<()>),
}

struct ActorMailbox {
    sender: mpsc::UnboundedSender<ActorRequest>,
    admitted: usize,
    task_id: Id,
}

struct ActorRequest {
    waiting: Option<WaitingRequest>,
    tracker: RequestTracker,
    operation: ActorOperation,
    owner_epoch: u64,

    reply: oneshot::Sender<Result<ActorExecutionResult>>,
}

struct ActorCompletion {
    object: ActorStorageKey,
    reply: oneshot::Sender<Result<ActorExecutionResult>>,
    result: Result<ActorExecutionResult>,
}

impl ActorRequest {
    fn finish(mut self, endpoint: &HostEndpoint, result: Result<ActorExecutionResult>) {
        self.tracker.complete(&result);
        ActorRuntime::log_invocation(
            endpoint,
            &self.operation.invocation(),
            self.tracker.completion(),
            &result,
        );
        let _ = self.reply.send(result);
    }
}

enum ActorOperation {
    Activate {
        actor: ActorKey,
        reply: oneshot::Sender<Result<ActorActivation>>,
    },
    Method(ActorInvocation),
    Socket(ActorSocketInvocation),
}

impl ActorOperation {
    fn commit_origin(&self) -> super::actor_runtime::CommitOrigin {
        match self {
            Self::Socket(invocation) => {
                super::actor_runtime::CommitOrigin::socket(&invocation.event)
            }
            _ => super::actor_runtime::CommitOrigin::default(),
        }
    }

    fn actor(&self) -> &ActorKey {
        match self {
            Self::Activate { actor, .. } => actor,
            Self::Method(invocation) => &invocation.actor,
            Self::Socket(invocation) => &invocation.actor,
        }
    }

    fn is_disconnect(&self) -> bool {
        matches!(
            self,
            Self::Socket(ActorSocketInvocation {
                event: ActorSocketEvent::Disconnect { .. },
                ..
            })
        )
    }

    fn validate(&self) -> Result<()> {
        self.invocation().validate()
    }

    fn invocation(&self) -> Cow<'_, ActorInvocation> {
        match self {
            Self::Activate { actor, .. } => Cow::Owned(ActorInvocation {
                request_id: "activate".into(),
                actor: actor.clone(),
                method: "activate".into(),
                args: vec![],
            }),
            Self::Method(invocation) => Cow::Borrowed(invocation),
            Self::Socket(invocation) => Cow::Owned(ActorInvocation {
                request_id: invocation.request_id.clone(),
                actor: invocation.actor.clone(),
                method: socket_event_name(&invocation.event).into(),
                args: Vec::new(),
            }),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/unit/host/actor_host.rs"]
mod tests;
