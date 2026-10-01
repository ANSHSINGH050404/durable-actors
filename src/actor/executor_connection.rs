use std::{
    collections::{HashMap, HashSet},
    os::unix::fs::FileTypeExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{
        UnixListener,
        unix::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::{ActorInvocationFailure, ActorKey, ActorSocketSource};

const ACTOR_EXECUTOR_PROTOCOL_VERSION: u32 = 21;
const MAX_PENDING_EXECUTOR_COMMANDS: usize = 64;

#[derive(Debug, Serialize)]
pub struct ActorMethodInvocation {
    pub request_id: String,
    pub actor: ActorKey,
    pub method: String,
    pub args: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct ActorMethodEviction {
    pub actor: ActorKey,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorSocketConnection {
    pub id: String,
    pub metadata: Value,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketMessage {
    Text { data: String },
    Binary { data: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketEvent {
    Connect {
        connection: ActorSocketConnection,
    },
    Message {
        connection_id: String,
        message: ActorSocketMessage,
    },
    Disconnect {
        connection: ActorSocketConnection,
        code: u16,
        reason: String,
        was_clean: bool,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct ActorSocketInvocation {
    pub request_id: String,
    pub actor: ActorKey,
    pub event: ActorSocketEvent,
    pub connections: Vec<ActorSocketConnection>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorSocketTagMatch {
    #[default]
    All,
    Any,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActorSocketEffect {
    StateSnapshot {
        connection_id: String,
        state: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
    },
    StateUpdate {
        changes: Value,
        removed: Vec<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        except_connection_ids: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        version: Option<u64>,
    },
    Send {
        connection_id: String,
        message: ActorSocketMessage,
    },
    Broadcast {
        message: ActorSocketMessage,
        except_connection_ids: Vec<String>,
        tags: Vec<String>,
        #[serde(default)]
        tag_match: ActorSocketTagMatch,
    },
    Close {
        connection_id: String,
        code: u16,
        reason: String,
    },
    Reject {
        connection_id: String,
        code: u16,
        reason: String,
    },
    SetMetadata {
        connection_id: String,
        metadata: Value,
    },
    SetTags {
        connection_id: String,
        tags: Vec<String>,
    },
}

#[derive(Debug, PartialEq)]
pub enum ActorMethodOutcome {
    Interleaved(ActorInterleavedOutcome),
    Completed {
        result: Value,
        state: ActorState,
        effects: Vec<ActorSocketEffect>,
    },
    Failed(ActorInvocationFailure),
}

#[derive(Debug, PartialEq)]
pub enum ActorSocketOutcome {
    Interleaved(ActorInterleavedOutcome),
    Handled {
        state: ActorState,
        effects: Vec<ActorSocketEffect>,
    },
    Failed(ActorInvocationFailure),
}

#[derive(Debug, PartialEq)]
pub struct ActorInterleavedOutcome {
    pub sequence: u64,
    pub result: Value,
    pub state: ActorState,
    pub effects: Vec<ActorSocketEffect>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActorState {
    pub sqlite: crate::litestream::storage::SqliteState,
}

#[async_trait]
pub trait ActorExecutor: Send + Sync {
    fn supports(&self, actor_name: &str) -> bool;

    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        None
    }

    async fn hydrate(&self, _actor: ActorKey, _state: Option<Arc<ActorState>>) -> Result<()> {
        Ok(())
    }

    fn resident_actors(&self) -> Option<Vec<ActorKey>> {
        None
    }

    fn residency_changes(&self) -> Option<watch::Receiver<()>> {
        None
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<&ActorState>,
    ) -> Result<ActorMethodOutcome>;

    async fn handle_socket(
        &self,
        _invocation: ActorSocketInvocation,
        _state: Option<&ActorState>,
    ) -> Result<ActorSocketOutcome> {
        Ok(ActorSocketOutcome::Failed(ActorInvocationFailure {
            code: "socket_not_supported".into(),
            message: "actor executor does not support sockets".into(),
        }))
    }

    // Queued executors can retain immutable snapshots; borrowed implementations keep their defaults.
    async fn invoke_shared(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<Arc<ActorState>>,
    ) -> Result<ActorMethodOutcome> {
        self.invoke(invocation, state.as_deref()).await
    }

    async fn handle_socket_shared(
        &self,
        invocation: ActorSocketInvocation,
        state: Option<Arc<ActorState>>,
    ) -> Result<ActorSocketOutcome> {
        self.handle_socket(invocation, state.as_deref()).await
    }

    async fn evict(&self, _eviction: ActorMethodEviction) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
pub(crate) trait ActorSocketPublisher: Send + Sync {
    async fn publish(&self, actor: &ActorKey, effects: Vec<ActorSocketEffect>) -> Result<()>;
}

pub(crate) struct ActorExecutorListener {
    listener: UnixListener,
    socket_path: PathBuf,
}

impl ActorExecutorListener {
    pub(crate) async fn bind(socket_path: impl Into<PathBuf>) -> Result<Self> {
        let socket_path = socket_path.into();
        prepare_socket_path(&socket_path).await?;
        if let Some(parent) = socket_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create actor executor directory {}", parent.display()))?;
        }
        let listener = UnixListener::bind(&socket_path)
            .with_context(|| format!("bind actor executor socket {}", socket_path.display()))?;
        Ok(Self {
            listener,
            socket_path,
        })
    }

    pub(crate) async fn accept(self) -> Result<ActorExecutorConnection> {
        let result = self.accept_connection().await;
        let cleanup = remove_socket(&self.socket_path).await;
        match (result, cleanup) {
            (Ok(connection), Ok(())) => Ok(connection),
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    async fn accept_connection(&self) -> Result<ActorExecutorConnection> {
        let (stream, _) =
            self.listener.accept().await.with_context(|| {
                format!("accept actor executor at {}", self.socket_path.display())
            })?;
        let (reader, writer) = stream.into_split();
        let reader = BufReader::new(reader);
        attach_executor(reader, writer).await
    }

    pub(crate) async fn accept_warm(self) -> Result<WarmExecutor> {
        let (stream, _) = self.listener.accept().await?;
        remove_socket(&self.socket_path).await?;
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        ensure!(
            matches!(
                read_client_message(&mut reader).await?,
                Some(ActorExecutorClientMessage::Warm {
                    protocol: ACTOR_EXECUTOR_PROTOCOL_VERSION
                })
            ),
            "generic executor did not finish warming"
        );
        Ok(WarmExecutor { reader, writer })
    }
}

pub(crate) struct WarmExecutor {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl WarmExecutor {
    pub(crate) async fn load(
        mut self,
        entrypoint: &str,
        environment: &HashMap<String, String>,
    ) -> Result<ActorExecutorConnection> {
        let mut bytes = serde_json::to_vec(&serde_json::json!({
            "type": "load", "entrypoint": entrypoint, "environment": environment
        }))?;
        bytes.push(b'\n');
        self.writer.write_all(&bytes).await?;
        attach_executor(self.reader, self.writer).await
    }
}

async fn attach_executor(
    mut reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
) -> Result<ActorExecutorConnection> {
    let actor_names = match read_client_message(&mut reader).await? {
        Some(ActorExecutorClientMessage::Attach {
            protocol,
            actor_names,
        }) => {
            ensure!(
                protocol == ACTOR_EXECUTOR_PROTOCOL_VERSION,
                "unsupported executor protocol {protocol}"
            );
            ensure!(
                !actor_names.is_empty(),
                "customer executor advertised no actor names"
            );
            actor_names
        }
        _ => anyhow::bail!("customer executor must attach after loading code"),
    };
    let (executor, task) = JsActorExecutor::start(reader, writer, actor_names);
    debug!(actor_names = ?executor.actor_names, "JavaScript executor connected");
    Ok(ActorExecutorConnection { executor, task })
}

pub(crate) struct ActorExecutorConnection {
    executor: Arc<JsActorExecutor>,
    task: JoinHandle<Result<()>>,
}

impl ActorExecutorConnection {
    pub(crate) fn executor(&self) -> Arc<dyn ActorExecutor> {
        self.executor.clone()
    }

    pub(crate) async fn mark_ready(
        &self,
        publisher: Option<Arc<dyn ActorSocketPublisher>>,
        sockets: Option<Arc<dyn ActorSocketSource>>,
    ) -> Result<()> {
        self.executor.mark_ready(publisher, sockets).await?;
        info!(
            actor_names = ?self.executor.actor_names,
            "customer JavaScript process attached to actor executor"
        );
        Ok(())
    }

    pub(crate) async fn run(mut self, shutdown: CancellationToken) -> Result<()> {
        tokio::select! {
            result = &mut self.task => {
                match result {
                    Ok(result) => result,
                    Err(error) => Err(error.into()),
                }
            }
            _ = shutdown.cancelled() => {
                self.task.abort();
                let _ = (&mut self.task).await;
                Ok(())
            }
        }
    }
}

impl Drop for ActorExecutorConnection {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type Residency = Arc<Mutex<Option<(Instant, Vec<ActorKey>)>>>;

struct JsActorExecutor {
    changes: watch::Sender<()>,
    residency: Residency,
    actor_names: HashSet<String>,
    admission: watch::Sender<()>,
    commands: mpsc::Sender<ExecutorRequest>,
}

#[async_trait]
impl ActorExecutor for JsActorExecutor {
    fn invocation_admission(&self) -> Option<watch::Receiver<()>> {
        Some(self.admission.subscribe())
    }

    async fn hydrate(&self, actor: ActorKey, state: Option<Arc<ActorState>>) -> Result<()> {
        match self
            .exchange(
                ExecutorCommand::Hydrate(ActorMethodEviction { actor }),
                state,
            )
            .await?
        {
            ExecutorReply::Hydrated => Ok(()),
            ExecutorReply::Failed { code, message } => {
                anyhow::bail!("actor hydration failed ({code}): {message}")
            }
            _ => anyhow::bail!("unexpected actor hydration reply"),
        }
    }

    fn residency_changes(&self) -> Option<watch::Receiver<()>> {
        Some(self.changes.subscribe())
    }
    fn resident_actors(&self) -> Option<Vec<ActorKey>> {
        self.residency
            .lock()
            .unwrap()
            .as_ref()
            .filter(|(at, _)| at.elapsed() < Duration::from_secs(5))
            .map(|(_, actors)| actors.clone())
    }

    fn supports(&self, actor_name: &str) -> bool {
        self.actor_names.contains(actor_name)
    }

    async fn invoke(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<&ActorState>,
    ) -> Result<ActorMethodOutcome> {
        self.invoke_shared(invocation, state.cloned().map(Arc::new))
            .await
    }

    async fn handle_socket(
        &self,
        invocation: ActorSocketInvocation,
        state: Option<&ActorState>,
    ) -> Result<ActorSocketOutcome> {
        self.handle_socket_shared(invocation, state.cloned().map(Arc::new))
            .await
    }

    async fn invoke_shared(
        &self,
        invocation: ActorMethodInvocation,
        state: Option<Arc<ActorState>>,
    ) -> Result<ActorMethodOutcome> {
        match self
            .exchange(ExecutorCommand::Invoke(invocation), state)
            .await?
        {
            ExecutorReply::Invoked {
                result,
                state,
                effects,
                sequence,
            } => Ok(match sequence {
                Some(sequence) => ActorMethodOutcome::Interleaved(ActorInterleavedOutcome {
                    sequence,
                    result,
                    state,
                    effects,
                }),
                None => ActorMethodOutcome::Completed {
                    result,
                    state,
                    effects,
                },
            }),
            ExecutorReply::Failed { code, message } => {
                Ok(ActorMethodOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            ExecutorReply::Hydrated | ExecutorReply::Evicted | ExecutorReply::StateRequired => {
                anyhow::bail!("actor executor returned eviction reply to invocation")
            }
            ExecutorReply::WebsocketHandled { .. } => {
                anyhow::bail!("actor executor returned socket reply to invocation")
            }
        }
    }

    async fn handle_socket_shared(
        &self,
        invocation: ActorSocketInvocation,
        state: Option<Arc<ActorState>>,
    ) -> Result<ActorSocketOutcome> {
        match self
            .exchange(ExecutorCommand::WebsocketEvent(invocation), state)
            .await?
        {
            ExecutorReply::WebsocketHandled {
                state,
                effects,
                sequence,
            } => Ok(match sequence {
                Some(sequence) => ActorSocketOutcome::Interleaved(ActorInterleavedOutcome {
                    sequence,
                    result: Value::Null,
                    state,
                    effects,
                }),
                None => ActorSocketOutcome::Handled { state, effects },
            }),
            ExecutorReply::Failed { code, message } => {
                Ok(ActorSocketOutcome::Failed(ActorInvocationFailure {
                    code,
                    message,
                }))
            }
            ExecutorReply::Hydrated
            | ExecutorReply::Invoked { .. }
            | ExecutorReply::Evicted
            | ExecutorReply::StateRequired => {
                anyhow::bail!("actor executor returned the wrong reply to socket event")
            }
        }
    }

    async fn evict(&self, eviction: ActorMethodEviction) -> Result<()> {
        match self
            .exchange(ExecutorCommand::Evict(eviction), None)
            .await?
        {
            ExecutorReply::Evicted => Ok(()),
            ExecutorReply::Failed { code, message } => {
                anyhow::bail!("actor executor rejected eviction ({code}): {message}")
            }
            ExecutorReply::Hydrated | ExecutorReply::Invoked { .. } => {
                anyhow::bail!("actor executor returned the wrong reply to eviction")
            }
            ExecutorReply::WebsocketHandled { .. } | ExecutorReply::StateRequired => {
                anyhow::bail!("actor executor returned socket reply to eviction")
            }
        }
    }
}

impl JsActorExecutor {
    fn start(
        reader: BufReader<OwnedReadHalf>,
        writer: OwnedWriteHalf,
        actor_names: Vec<String>,
    ) -> (Arc<Self>, JoinHandle<Result<()>>) {
        let (commands, incoming) = mpsc::channel(MAX_PENDING_EXECUTOR_COMMANDS);
        let residency = Arc::new(Mutex::new(None));
        let (changes, _) = watch::channel(());
        let (admission, _) = watch::channel(());
        let executor = Arc::new(Self {
            changes: changes.clone(),
            residency: residency.clone(),
            actor_names: actor_names.into_iter().collect(),
            admission: admission.clone(),
            commands,
        });
        let task = tokio::spawn(run_executor_connection(
            reader, writer, incoming, residency, changes, admission,
        ));
        (executor, task)
    }

    async fn mark_ready(
        &self,
        publisher: Option<Arc<dyn ActorSocketPublisher>>,
        sockets: Option<Arc<dyn ActorSocketSource>>,
    ) -> Result<()> {
        let (reply, ready) = oneshot::channel();
        self.commands
            .send(ExecutorRequest::Ready(reply, publisher, sockets))
            .await
            .context("actor executor stopped")?;
        ready
            .await
            .context("actor executor disconnected before readiness")?
    }

    async fn exchange(
        &self,
        command: ExecutorCommand,
        state: Option<Arc<ActorState>>,
    ) -> Result<ExecutorReply> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ExecutorRequest::Exchange(Box::new(PendingCommand {
                command,
                state,
                reply,
                resident_only: false,
                admission_granted: false,
            })))
            .await
            .context("actor executor stopped")?;
        response
            .await
            .context("customer actor executor disconnected before replying")?
    }
}

async fn run_executor_connection(
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    commands: mpsc::Receiver<ExecutorRequest>,
    residency: Residency,
    changes: watch::Sender<()>,
    admission: watch::Sender<()>,
) -> Result<()> {
    let (outbound, writes) = mpsc::channel(MAX_PENDING_EXECUTOR_COMMANDS + 1);
    let (inbound, replies) = mpsc::channel(MAX_PENDING_EXECUTOR_COMMANDS);
    let driver = ExecutorDriver {
        admission,
        changes,
        residency,
        pending: HashMap::new(),
        residents: HashSet::new(),
        next_message_id: 1,
        outbound,
        publisher: None,
        sockets: None,
        publishing: JoinSet::new(),
        publishing_ids: HashSet::new(),
        loading_connections: JoinSet::new(),
        connection_lookup_ids: HashSet::new(),
    };
    tokio::try_join!(
        driver.run(commands, replies),
        read_executor_messages(reader, inbound),
        write_executor_messages(writer, writes)
    )?;
    Ok(())
}

struct ExecutorDriver {
    admission: watch::Sender<()>,
    changes: watch::Sender<()>,
    residency: Residency,
    pending: HashMap<u64, PendingCommand>,
    residents: HashSet<ActorKey>,
    next_message_id: u64,
    outbound: mpsc::Sender<ExecutorWrite>,
    publisher: Option<Arc<dyn ActorSocketPublisher>>,
    sockets: Option<Arc<dyn ActorSocketSource>>,
    publishing: JoinSet<(u64, Result<()>)>,
    publishing_ids: HashSet<u64>,
    loading_connections: JoinSet<(u64, Result<Vec<ActorSocketConnection>>)>,
    connection_lookup_ids: HashSet<u64>,
}

impl ExecutorDriver {
    async fn run(
        mut self,
        mut commands: mpsc::Receiver<ExecutorRequest>,
        mut replies: mpsc::Receiver<Result<ActorExecutorClientMessage>>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                reply = replies.recv() => {
                    match reply.context("actor executor reader stopped")?? {
                        ActorExecutorClientMessage::Residency { actors } => {
                            ensure!(actors.len() <= 32, "too many resident actors");
                            for actor in &actors { actor.validate()?; }
                            let mut current = self.residency.lock().unwrap();
                            let changed = current.as_ref().is_none_or(|(_, previous)| previous != &actors);
                            *current = Some((Instant::now(), actors));
                            if changed { self.changes.send_replace(()); }
                        }
                        ActorExecutorClientMessage::ReadyForInvocation { message_id } => self.allow_next_invocation(message_id)?,
                        ActorExecutorClientMessage::Reply { message_id, reply } => self.deliver(message_id, reply)?,
                        ActorExecutorClientMessage::SocketEffects { message_id, effects } => self.publish(message_id, effects)?,
                        ActorExecutorClientMessage::GetConnections { message_id } => self.load_connections(message_id)?,
                        ActorExecutorClientMessage::Warm { .. } | ActorExecutorClientMessage::Attach { .. } => anyhow::bail!("customer actor executor attached more than once"),
                    }
                }
                published = self.publishing.join_next(), if !self.publishing.is_empty() => {
                    let (message_id, result) = published.context("socket publisher stopped")??;
                    self.publishing_ids.remove(&message_id);
                    self.outbound.send(ExecutorWrite {
                        bytes: encode_server_message(&ActorExecutorServerMessage::SocketEffectsPublished {
                            message_id,
                            error: result.err().map(|error| format!("{error:#}")),
                        })?,
                        written: None,
                    }).await.context("actor executor writer stopped")?;
                }
                loaded = self.loading_connections.join_next(), if !self.loading_connections.is_empty() => {
                    let (message_id, result) = loaded.context("connection lookup stopped")??;
                    self.connection_lookup_ids.remove(&message_id);
                    let (connections, error) = match result {
                        Ok(connections) => (connections, None),
                        Err(error) => (Vec::new(), Some(format!("{error:#}"))),
                    };
                    self.outbound.send(ExecutorWrite {
                        bytes: encode_server_message(&ActorExecutorServerMessage::SocketConnections {
                            message_id, connections, error,
                        })?,
                        written: None,
                    }).await.context("actor executor writer stopped")?;
                }
                command = commands.recv(), if self.pending.len() < MAX_PENDING_EXECUTOR_COMMANDS => match command {
                    Some(command) => self.handle_command(command)?,
                    None => return Ok(()),
                }
            }
        }
    }

    fn handle_command(&mut self, command: ExecutorRequest) -> Result<()> {
        match command {
            ExecutorRequest::Exchange(mut pending) => {
                let resident = self.residents.remove(pending.command.actor());
                pending.resident_only = resident
                    && !matches!(
                        pending.command,
                        ExecutorCommand::Evict(_) | ExecutorCommand::Hydrate(_)
                    );
                self.enqueue(*pending)
            }
            ExecutorRequest::Ready(written, publisher, sockets) => {
                self.publisher = publisher;
                self.sockets = sockets;
                self.outbound
                    .try_send(ExecutorWrite {
                        bytes: encode_server_message(&ActorExecutorServerMessage::Attached {
                            supports_residency: true,
                            protocol: ACTOR_EXECUTOR_PROTOCOL_VERSION,
                        })?,
                        written: Some(written),
                    })
                    .map_err(|_| {
                        anyhow::anyhow!("actor executor writer stopped or filled its queue")
                    })
            }
        }
    }

    fn allow_next_invocation(&mut self, message_id: u64) -> Result<()> {
        let pending = self
            .pending
            .get_mut(&message_id)
            .context("invocation admission has no active call")?;
        ensure!(
            matches!(
                pending.command,
                ExecutorCommand::Invoke(_) | ExecutorCommand::WebsocketEvent(_)
            ),
            "only invocations can admit another call"
        );
        ensure!(
            !pending.admission_granted,
            "invocation admission was already granted"
        );
        pending.admission_granted = true;
        self.admission.send_replace(());
        Ok(())
    }

    fn load_connections(&mut self, message_id: u64) -> Result<()> {
        let pending = self
            .pending
            .get(&message_id)
            .context("connection lookup has no active actor invocation")?;
        ensure!(
            !matches!(pending.command, ExecutorCommand::Evict(_)),
            "cannot load connections during eviction"
        );
        ensure!(
            self.connection_lookup_ids.insert(message_id),
            "actor sent concurrent connection lookups"
        );
        let actor = pending.command.actor().clone();
        let sockets = self.sockets.clone();
        self.loading_connections.spawn(async move {
            let result = async {
                sockets
                    .context("actor connection lookup is unavailable")?
                    .connections(&actor)
                    .await
            }
            .await;
            (message_id, result)
        });
        Ok(())
    }

    fn publish(&mut self, message_id: u64, effects: Vec<ActorSocketEffect>) -> Result<()> {
        let pending = self
            .pending
            .get(&message_id)
            .context("socket output has no active actor invocation")?;
        ensure!(
            self.publishing_ids.insert(message_id),
            "actor sent concurrent socket publications"
        );
        let actor = pending.command.actor().clone();
        let connecting = matches!(&pending.command, ExecutorCommand::WebsocketEvent(invocation) if matches!(invocation.event, ActorSocketEvent::Connect { .. }));
        let publisher = self.publisher.clone();
        self.publishing.spawn(async move {
            let result = async {
                ensure!(
                    !connecting,
                    "socket output cannot precede connection acceptance"
                );
                super::validate_socket_effects(&effects)?;
                publisher
                    .context("actor socket publishing is unavailable")?
                    .publish(&actor, effects)
                    .await
            }
            .await;
            (message_id, result)
        });
        Ok(())
    }

    fn enqueue(&mut self, pending: PendingCommand) -> Result<()> {
        let message_id = self.next_message_id;
        self.next_message_id = message_id
            .checked_add(1)
            .context("actor executor message ID overflow")?;
        let state = if pending.resident_only || matches!(pending.command, ExecutorCommand::Evict(_))
        {
            None
        } else {
            pending.state.as_deref()
        };
        let bytes = encode_server_message(&ActorExecutorServerMessage::Command {
            message_id,
            command: ExecutorCommandEnvelope {
                command: &pending.command,
                sqlite: state.map(|value| &value.sqlite),
                resident_only: pending.resident_only,
            },
        });
        match bytes {
            Ok(bytes) => {
                // Each pending command has at most one queued write; readiness has its own extra slot.
                self.outbound
                    .try_send(ExecutorWrite {
                        bytes,
                        written: None,
                    })
                    .map_err(|_| {
                        anyhow::anyhow!("actor executor writer stopped or filled its queue")
                    })?;
                self.pending.insert(message_id, pending);
            }
            Err(error) => {
                let _ = pending.reply.send(Err(error));
            }
        }
        Ok(())
    }

    fn deliver(&mut self, message_id: u64, reply: ExecutorReply) -> Result<()> {
        ensure!(
            !self.connection_lookup_ids.contains(&message_id),
            "actor completed before connection lookup finished"
        );
        ensure!(
            !self.publishing_ids.contains(&message_id),
            "actor completed before socket output was acknowledged"
        );
        let mut pending = self
            .pending
            .remove(&message_id)
            .with_context(|| format!("actor executor replied to unknown message {message_id}"))?;
        if matches!(reply, ExecutorReply::StateRequired) {
            if pending.resident_only {
                pending.resident_only = false;
                return self.enqueue(pending);
            }
            let _ = pending.reply.send(Err(anyhow::anyhow!(
                "actor executor refused explicit hydration"
            )));
            return Ok(());
        }
        if matches!(
            reply,
            ExecutorReply::Hydrated
                | ExecutorReply::Invoked { .. }
                | ExecutorReply::WebsocketHandled { .. }
        ) {
            if self.residents.len() >= 4096 {
                self.residents.clear();
            }
            self.residents.insert(pending.command.actor().clone());
        }
        let _ = pending.reply.send(Ok(reply));
        Ok(())
    }
}

async fn read_executor_messages(
    mut reader: BufReader<OwnedReadHalf>,
    inbound: mpsc::Sender<Result<ActorExecutorClientMessage>>,
) -> Result<()> {
    loop {
        let reply = match read_client_message(&mut reader).await {
            Ok(Some(message)) => Ok(message),
            Ok(None) => Err(anyhow::anyhow!(
                "customer JavaScript actor executor disconnected"
            )),
            Err(error) => Err(error),
        };
        let stopped = reply.is_err();
        inbound
            .send(reply)
            .await
            .context("actor executor driver stopped")?;
        if stopped {
            return Ok(());
        }
    }
}

async fn write_executor_messages(
    mut writer: OwnedWriteHalf,
    mut writes: mpsc::Receiver<ExecutorWrite>,
) -> Result<()> {
    while let Some(write) = writes.recv().await {
        let result = writer
            .write_all(&write.bytes)
            .await
            .context("write actor executor command");
        if let Some(written) = write.written {
            let _ = written.send(
                result
                    .as_ref()
                    .map(|_| ())
                    .map_err(|error| anyhow::anyhow!("{error:#}")),
            );
        }
        result?;
    }
    Ok(())
}

enum ExecutorRequest {
    Exchange(Box<PendingCommand>),
    Ready(
        oneshot::Sender<Result<()>>,
        Option<Arc<dyn ActorSocketPublisher>>,
        Option<Arc<dyn ActorSocketSource>>,
    ),
}

struct PendingCommand {
    command: ExecutorCommand,
    state: Option<Arc<ActorState>>,
    resident_only: bool,
    admission_granted: bool,
    reply: oneshot::Sender<Result<ExecutorReply>>,
}

struct ExecutorWrite {
    bytes: Vec<u8>,
    written: Option<oneshot::Sender<Result<()>>>,
}

impl ExecutorCommand {
    fn actor(&self) -> &ActorKey {
        match self {
            Self::Invoke(invocation) => &invocation.actor,
            Self::WebsocketEvent(invocation) => &invocation.actor,
            Self::Evict(eviction) | Self::Hydrate(eviction) => &eviction.actor,
        }
    }
}

async fn read_client_message(
    reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
) -> Result<Option<ActorExecutorClientMessage>> {
    let mut document = Vec::new();
    let bytes = reader.read_until(b'\n', &mut document).await?;
    if bytes == 0 {
        return Ok(None);
    }
    serde_json::from_slice(trim_ascii_end(&document))
        .map(Some)
        .context("decode customer actor executor message")
}

fn trim_ascii_end(mut document: &[u8]) -> &[u8] {
    while document.last().is_some_and(u8::is_ascii_whitespace) {
        document = &document[..document.len() - 1];
    }
    document
}

fn encode_server_message(message: &ActorExecutorServerMessage<'_>) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(message)?;
    bytes.push(b'\n');
    Ok(bytes)
}

async fn prepare_socket_path(path: &Path) -> Result<()> {
    match tokio::fs::symlink_metadata(path).await {
        Ok(metadata) => {
            ensure!(
                metadata.file_type().is_socket(),
                "refusing to replace non-socket actor executor path {}",
                path.display()
            );
            tokio::fs::remove_file(path).await?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

async fn remove_socket(path: &Path) -> Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {
            debug!(socket = %path.display(), "actor executor socket removed");
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorServerMessage<'a> {
    SocketConnections {
        message_id: u64,
        connections: Vec<ActorSocketConnection>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    SocketEffectsPublished {
        message_id: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Attached {
        supports_residency: bool,
        protocol: u32,
    },
    Command {
        message_id: u64,
        command: ExecutorCommandEnvelope<'a>,
    },
}

#[derive(Debug, Serialize)]
struct ExecutorCommandEnvelope<'a> {
    #[serde(flatten)]
    command: &'a ExecutorCommand,
    #[serde(skip_serializing_if = "Option::is_none")]
    sqlite: Option<&'a crate::litestream::storage::SqliteState>,
    resident_only: bool,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ActorExecutorClientMessage {
    Warm {
        protocol: u32,
    },
    Residency {
        actors: Vec<ActorKey>,
    },
    GetConnections {
        message_id: u64,
    },
    ReadyForInvocation {
        message_id: u64,
    },
    SocketEffects {
        message_id: u64,
        effects: Vec<ActorSocketEffect>,
    },
    Attach {
        protocol: u32,
        actor_names: Vec<String>,
    },
    Reply {
        message_id: u64,
        reply: ExecutorReply,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorCommand {
    Hydrate(ActorMethodEviction),
    Invoke(ActorMethodInvocation),
    WebsocketEvent(ActorSocketInvocation),
    Evict(ActorMethodEviction),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ExecutorReply {
    Hydrated,
    StateRequired,
    Invoked {
        result: Value,
        #[serde(flatten)]
        state: ActorState,
        #[serde(default)]
        sequence: Option<u64>,
        #[serde(default)]
        effects: Vec<ActorSocketEffect>,
    },
    WebsocketHandled {
        #[serde(flatten)]
        state: ActorState,
        #[serde(default)]
        sequence: Option<u64>,
        effects: Vec<ActorSocketEffect>,
    },
    Failed {
        code: String,
        message: String,
    },
    Evicted,
}

#[cfg(test)]
#[path = "../../tests/unit/actor/executor_connection.rs"]
mod tests;
