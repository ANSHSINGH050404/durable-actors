use crate::control_plane::admin::validate_component;
use crate::request_tracking::HostState;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::mpsc;

pub(crate) use crate::request_tracking::RequestOutcome;
use crate::request_tracking::{RequestCompletion, RequestObserver};

pub(crate) mod history;
pub(crate) mod metrics;
pub(crate) mod persistence;
pub(crate) mod replay;
use persistence::{TracePersistence, sqlite::SqliteTracePersistence};
use replay::ReplayQuery;

pub(crate) const TRACE_CAPACITY: usize = 500;
pub(crate) const TRACE_BATCH_SIZE: usize = 64;
pub(crate) const TRACE_METADATA_LIMIT: usize = 4096;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestTrace {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_version: Option<u64>,
    pub project_id: String,
    pub request_id: String,
    pub actor_name: String,
    pub actor_id: String,
    pub kind: RequestKind,
    pub operation: String,
    pub connection_id: Option<String>,
    pub started_at_ms: u64,
    pub routing_ms: f64,
    pub duration_ms: f64,
    pub queue_wait_ms: Option<f64>,
    #[serde(default)]
    pub host_state: HostState,
    pub outcome: RequestOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

impl RequestTrace {
    pub(crate) fn validate(&self) -> Result<()> {
        crate::actor::ActorKey {
            project_id: self.project_id.clone(),
            actor_name: self.actor_name.clone(),
            actor_id: self.actor_id.clone(),
        }
        .validate()?;
        ensure!(
            !self.request_id.is_empty() && self.request_id.len() <= 256,
            "invalid trace request ID"
        );
        ensure!(
            !self.operation.is_empty() && self.operation.len() <= 256,
            "invalid trace operation"
        );
        ensure!(
            self.connection_id.as_ref().is_none_or(|id| id.len() <= 256),
            "invalid trace connection ID"
        );
        ensure!(
            self.routing_ms.is_finite() && self.routing_ms >= 0.0,
            "invalid routing duration"
        );
        ensure!(
            self.duration_ms.is_finite() && self.duration_ms >= 0.0,
            "invalid trace duration"
        );
        ensure!(
            self.queue_wait_ms
                .is_none_or(|ms| ms.is_finite() && ms >= 0.0 && ms <= self.duration_ms),
            "invalid trace queue wait"
        );
        ensure!(
            self.metadata
                .as_ref()
                .is_none_or(|metadata| metadata.to_string().len() <= TRACE_METADATA_LIMIT),
            "trace metadata exceeds {TRACE_METADATA_LIMIT} bytes"
        );
        Ok(())
    }
}

// Connection metadata beyond the limit is omitted rather than failing the trace.
fn bounded_metadata(metadata: Option<serde_json::Value>) -> Option<serde_json::Value> {
    metadata.filter(|value| value.to_string().len() <= TRACE_METADATA_LIMIT)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RequestKind {
    Method,
    Websocket,
}

impl RequestKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Method => "method",
            Self::Websocket => "websocket",
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceEvent {
    #[serde(default = "new_event_id")]
    pub event_id: String,
    pub host_id: String,
    pub session_id: String,
    #[serde(flatten)]
    pub trace: RequestTrace,
}

fn new_event_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TraceRecord {
    pub sequence: u64,
    #[serde(flatten)]
    pub event: TraceEvent,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct TracePage {
    pub epoch: String,
    pub cursor: u64,
    pub capacity: usize,
    pub evicted: u64,
    pub dropped: u64,
    pub persistence_failed: bool,
    pub records: Vec<TraceRecord>,
    pub next_cursor: Option<String>,
    pub resume_cursor: String,
    pub reset: bool,
}

#[derive(Debug)]
pub(crate) struct InvalidTraceQuery;

impl std::fmt::Display for InvalidTraceQuery {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Invalid or incompatible request trace query")
    }
}
impl std::error::Error for InvalidTraceQuery {}

#[derive(Clone)]
pub(crate) struct TraceStore {
    pending: Arc<tokio::sync::Semaphore>,
    persistence: Arc<dyn TracePersistence>,
    pub changes: crate::postgres::notifications::ChangeFeed,
}

impl Default for TraceStore {
    fn default() -> Self {
        Self::new(Arc::new(SqliteTracePersistence::in_memory()))
    }
}

impl TraceStore {
    pub(crate) async fn open(persistence: Arc<dyn TracePersistence>) -> Result<Self> {
        persistence.initialize().await?;
        Ok(Self::new(persistence))
    }

    pub(crate) async fn record(
        &self,
        project_id: &str,
        host: &str,
        session: &str,
        traces: Vec<RequestTrace>,
        dropped: u64,
    ) -> Result<()> {
        validate_component("project ID", project_id, 64)?;
        ensure!(
            traces.iter().all(|trace| trace.project_id == project_id),
            "trace project does not match host"
        );
        let events = traces
            .into_iter()
            .map(|trace| TraceEvent {
                event_id: new_event_id(),
                host_id: host.into(),
                session_id: session.into(),
                trace,
            })
            .collect();
        self.persist_detached(project_id, events, dropped).await
    }

    pub(crate) async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        let mut page = self.persistence.replay(project_id, query).await?;
        let status = self.persistence.status(project_id).await?;
        page.dropped = status.dropped;
        page.persistence_failed = status.persistence_failed;
        Ok(page)
    }

    pub(crate) async fn metrics(
        &self,
        project_id: &str,
        query: &metrics::TimeRange,
    ) -> Result<metrics::OverviewMetrics> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.persistence.metrics(project_id, query).await
    }

    pub(crate) async fn queue_waits(
        &self,
        project_id: &str,
        query: &metrics::QueueWaitQuery,
    ) -> Result<Vec<metrics::QueueWaitRow>> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.persistence.queue_waits(project_id, query).await
    }

    pub(crate) async fn websockets(
        &self,
        project_id: &str,
        query: &metrics::TimeRange,
    ) -> Result<Vec<metrics::SocketSession>> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        self.persistence.websockets(project_id, query).await
    }

    pub(crate) async fn history(
        &self,
        project_id: &str,
        query: &history::HistoryQuery,
    ) -> Result<TracePage> {
        validate_component("project ID", project_id, 64).context(InvalidTraceQuery)?;
        query.validate().context(InvalidTraceQuery)?;
        let mut page = self.persistence.history(project_id, query).await?;
        let status = self.persistence.status(project_id).await?;
        page.dropped = status.dropped;
        page.persistence_failed = status.persistence_failed;
        Ok(page)
    }

    fn new(persistence: Arc<dyn TracePersistence>) -> Self {
        Self {
            pending: Arc::new(tokio::sync::Semaphore::new(64)),
            persistence,
            changes: Default::default(),
        }
    }

    async fn persist_detached(
        &self,
        project: &str,
        events: Vec<TraceEvent>,
        dropped: u64,
    ) -> Result<()> {
        if events.is_empty() {
            return self.publish_status(project, dropped).await;
        }
        let permit = match self.pending.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(error) => {
                self.report_persistence_failure(project, &error, events.len())
                    .await?;
                return Err(error.into());
            }
        };
        let store = self.clone();
        let project = project.to_owned();
        // Finish the commit even if the reporting connection is cancelled.
        tokio::spawn(async move {
            let _permit = permit;
            match store.persistence.append(&events).await {
                Ok(()) => store.publish_status(&project, dropped).await?,
                Err(error) => {
                    store
                        .report_persistence_failure(&project, &error, events.len())
                        .await?;
                    return Err(error);
                }
            }
            Ok(())
        })
        .await?
    }

    async fn publish_status(&self, project: &str, dropped: u64) -> Result<()> {
        if dropped > 0 {
            self.persistence
                .record_status(project, dropped, false)
                .await?;
        }
        self.changes.notify().await;
        Ok(())
    }

    async fn report_persistence_failure(
        &self,
        project: &str,
        error: &(impl std::fmt::Display + Sync),
        count: usize,
    ) -> Result<()> {
        tracing::error!(%error, count, "request trace persistence failed");
        self.persistence.record_status(project, 0, true).await?;
        self.changes.notify().await;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct TraceSender {
    sender: mpsc::Sender<RequestTrace>,
    dropped: Arc<AtomicU64>,
}

impl TraceSender {
    #[cfg(test)]
    pub(crate) fn channel(capacity: usize) -> (Self, mpsc::Receiver<RequestTrace>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            Self {
                sender,
                dropped: Arc::new(AtomicU64::new(0)),
            },
            receiver,
        )
    }
    pub(crate) fn start(
        client: Arc<crate::control_plane::ControlPlaneClient>,
        stop: tokio_util::sync::CancellationToken,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(1024);
        let dropped = Arc::new(AtomicU64::new(0));
        let lost = dropped.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(200));
            loop {
                tokio::select! { _ = stop.cancelled() => break, _ = interval.tick() => {} }
                let mut batch = Vec::new();
                while batch.len() < TRACE_BATCH_SIZE {
                    match receiver.try_recv() {
                        Ok(trace) => batch.push(trace),
                        Err(_) => break,
                    }
                }
                let missing = lost.swap(0, Ordering::Relaxed);
                if batch.is_empty() && missing == 0 {
                    continue;
                }
                let count = batch.len() as u64;
                if !matches!(
                    tokio::time::timeout(
                        Duration::from_secs(3),
                        client.report_traces(batch, missing)
                    )
                    .await,
                    Ok(Ok(()))
                ) {
                    lost.fetch_add(count.saturating_add(missing), Ordering::Relaxed);
                }
            }
        });
        Self { sender, dropped }
    }

    fn send(&self, trace: RequestTrace) {
        if self.sender.try_send(trace).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) struct TraceRecorder {
    sender: TraceSender,
    trace: RequestTrace,
}

impl TraceRecorder {
    pub(crate) fn new(
        sender: TraceSender,
        invocation: &crate::actor::ActorInvocation,
        kind: RequestKind,
        connection_id: Option<String>,
        metadata: Option<serde_json::Value>,
    ) -> Self {
        Self {
            sender,
            trace: RequestTrace {
                state_version: None,
                project_id: invocation.actor.project_id.clone(),
                request_id: invocation.request_id.clone(),
                actor_name: invocation.actor.actor_name.clone(),
                actor_id: invocation.actor.actor_id.clone(),
                kind,
                operation: invocation.method.clone(),
                connection_id,
                started_at_ms: 0,
                duration_ms: 0.0,
                queue_wait_ms: None,
                host_state: HostState::Warm,
                routing_ms: 0.0,
                outcome: RequestOutcome::Interrupted,
                metadata: bounded_metadata(metadata),
            },
        }
    }
}

impl RequestObserver for TraceRecorder {
    fn record(self: Box<Self>, completion: &RequestCompletion) {
        let mut trace = self.trace;
        trace.started_at_ms = completion.started_at_ms;
        trace.state_version = completion.state_version;
        trace.outcome = completion.outcome;
        trace.duration_ms = completion.timings.duration_ms;
        trace.queue_wait_ms = completion.timings.queue_wait_ms;
        trace.host_state = completion.timings.host_state;
        trace.routing_ms = completion.timings.routing_ms;
        self.sender.send(trace);
    }
}

#[cfg(test)]
#[path = "../tests/unit/request_traces/tests.rs"]
mod tests;
