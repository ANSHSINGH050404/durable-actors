use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::actor::ActorExecutionResult;

pub(crate) struct RequestTracker {
    started: Instant,
    started_at_ms: u64,
    queue_wait_ms: Option<f64>,
    host_state: HostState,
    state_version: Option<u64>,
    completion: Option<RequestCompletion>,
    response: Option<oneshot::Sender<RequestMetadata>>,
    observer: Option<Box<dyn RequestObserver>>,
    checkpoints: RequestCheckpoints,
}

impl RequestTracker {
    pub(crate) fn new(
        started: Instant,
        host_state: HostState,
        observer: Option<Box<dyn RequestObserver>>,
    ) -> (Self, oneshot::Receiver<RequestMetadata>) {
        let (response, timing) = oneshot::channel();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let tracker = Self {
            started,
            started_at_ms: now.saturating_sub(started.elapsed().as_millis() as u64),
            queue_wait_ms: None,
            host_state,
            state_version: None,
            completion: None,
            response: Some(response),
            observer,
            checkpoints: RequestCheckpoints::default(),
        };
        (tracker, timing)
    }

    pub(crate) fn admitted(&mut self) {
        if self.queue_wait_ms.is_none() && self.completion.is_none() {
            self.queue_wait_ms = Some(self.elapsed_ms());
        }
    }

    pub(crate) fn mark(&mut self, stage: RequestStage) {
        if self.completion.is_some() {
            return;
        }
        let elapsed = Some(self.elapsed_ms());
        match stage {
            RequestStage::StateCacheChecked => self.checkpoints.state_cache_checked_at_ms = elapsed,
            RequestStage::StateDownloaded => self.checkpoints.state_downloaded_at_ms = elapsed,
            RequestStage::StateDecoded => self.checkpoints.state_decoded_at_ms = elapsed,
            RequestStage::PendingCommitResolved => {
                self.checkpoints.pending_commit_resolved_at_ms = elapsed
            }
            RequestStage::ActorExecutionCompleted => {
                self.checkpoints.actor_execution_completed_at_ms = elapsed
            }
            RequestStage::StatePublicationCompleted => {
                self.checkpoints.state_publication_completed_at_ms = elapsed
            }
        }
    }

    pub(crate) fn state_version(&mut self, version: Option<u64>) {
        self.state_version = version;
    }

    pub(crate) fn complete(&mut self, result: &Result<ActorExecutionResult>) {
        self.finish(match result {
            Ok(ActorExecutionResult::Completed { .. }) => RequestOutcome::Completed,
            Ok(ActorExecutionResult::HostUnavailable) => RequestOutcome::Rejected,
            Ok(ActorExecutionResult::Reroute) => RequestOutcome::Rerouted,
            _ => RequestOutcome::Failed,
        });
    }

    pub(crate) fn finish(&mut self, outcome: RequestOutcome) {
        if self.completion.is_some() {
            return;
        }
        let completion = RequestCompletion {
            started_at_ms: self.started_at_ms,
            state_version: self.state_version,
            outcome,
            checkpoints: self.checkpoints.clone(),
            timings: RequestMetadata {
                duration_ms: self.elapsed_ms(),
                queue_wait_ms: self.queue_wait_ms,
                host_state: self.host_state,
            },
        };
        self.completion = Some(completion.clone());
        if let Some(observer) = self.observer.take() {
            observer.record(&completion);
        }
        if let Some(response) = self.response.take() {
            let _ = response.send(completion.timings);
        }
    }

    pub(crate) fn completion(&self) -> &RequestCompletion {
        self.completion.as_ref().expect("request has completed")
    }

    fn elapsed_ms(&self) -> f64 {
        self.started.elapsed().as_secs_f64() * 1000.0
    }
}

impl Drop for RequestTracker {
    fn drop(&mut self) {
        self.finish(RequestOutcome::Interrupted);
    }
}

pub(crate) trait RequestObserver: Send + Sync {
    fn record(self: Box<Self>, completion: &RequestCompletion);
}

#[derive(Clone, Debug)]
pub(crate) struct RequestCompletion {
    pub started_at_ms: u64,
    pub state_version: Option<u64>,
    pub outcome: RequestOutcome,
    pub timings: RequestMetadata,
    pub checkpoints: RequestCheckpoints,
}

pub(crate) enum RequestStage {
    StateCacheChecked,
    StateDownloaded,
    StateDecoded,
    PendingCommitResolved,
    ActorExecutionCompleted,
    StatePublicationCompleted,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RequestCheckpoints {
    pub state_cache_checked_at_ms: Option<f64>,
    pub state_downloaded_at_ms: Option<f64>,
    pub state_decoded_at_ms: Option<f64>,
    pub pending_commit_resolved_at_ms: Option<f64>,
    pub actor_execution_completed_at_ms: Option<f64>,
    pub state_publication_completed_at_ms: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RequestMetadata {
    pub duration_ms: f64,
    pub queue_wait_ms: Option<f64>,
    pub host_state: HostState,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RequestOutcome {
    Completed,
    Failed,
    Rejected,
    Rerouted,
    Interrupted,
}

impl RequestOutcome {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Rejected => "rejected",
            Self::Rerouted => "rerouted",
            Self::Interrupted => "interrupted",
        }
    }
}

#[cfg(test)]
#[path = "../tests/unit/request_tracking.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HostState {
    Cold,
    #[default]
    Warm,
}

impl HostState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold",
            Self::Warm => "warm",
        }
    }
}
