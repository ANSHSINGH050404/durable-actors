use super::*;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn completion_is_measured_once_and_shared_with_both_consumers() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let observer = RecordingObserver(records.clone());
    let started = Instant::now() - std::time::Duration::from_millis(25);
    let (mut tracker, response) =
        RequestTracker::new(started, HostState::Cold, 500.0, Some(Box::new(observer)));
    tracker.admitted();
    tracker.state_version(Some(7));
    tracker.mark(RequestStage::StateCacheChecked);
    tracker.mark(RequestStage::ActorExecutionCompleted);
    tracker.finish(RequestOutcome::Completed);
    tracker.finish(RequestOutcome::Failed);
    tracker.mark(RequestStage::StatePublicationCompleted);
    drop(tracker);

    let timing = response.await.unwrap();
    let records = records.lock().unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert!(matches!(record.outcome, RequestOutcome::Completed));
    assert_eq!(record.state_version, Some(7));
    assert!(
        record.checkpoints.actor_execution_completed_at_ms.unwrap()
            >= record.checkpoints.state_cache_checked_at_ms.unwrap()
    );
    assert!(record.checkpoints.actor_execution_completed_at_ms.unwrap() <= timing.duration_ms);
    assert!(
        record
            .checkpoints
            .state_publication_completed_at_ms
            .is_none()
    );
    assert_eq!(timing.routing_ms, 500.0);
    assert_eq!(timing.routing_ms, record.timings.routing_ms);
    assert!(timing.duration_ms < timing.routing_ms);
    assert_eq!(timing.host_state, HostState::Cold);
    assert_eq!(timing.host_state, record.timings.host_state);
    assert_eq!(timing.duration_ms, record.timings.duration_ms);
    assert_eq!(timing.queue_wait_ms, record.timings.queue_wait_ms);
    assert!(timing.queue_wait_ms.unwrap() >= 25.0);
    assert!(timing.duration_ms >= timing.queue_wait_ms.unwrap());
}

#[tokio::test]
async fn interrupted_requests_publish_the_same_completion_without_admission() {
    let records = Arc::new(Mutex::new(Vec::new()));
    let (tracker, response) = RequestTracker::new(
        Instant::now(),
        HostState::Warm,
        0.0,
        Some(Box::new(RecordingObserver(records.clone()))),
    );
    drop(tracker);
    let timing = response.await.unwrap();
    let records = records.lock().unwrap();
    assert!(matches!(records[0].outcome, RequestOutcome::Interrupted));
    assert_eq!(timing.duration_ms, records[0].timings.duration_ms);
    assert_eq!(timing.queue_wait_ms, None);
}

#[tokio::test]
async fn response_timing_is_available_without_an_observer() {
    let (mut tracker, response) = RequestTracker::new(Instant::now(), HostState::Warm, 0.0, None);
    tracker.admitted();
    tracker.finish(RequestOutcome::Completed);
    let timing = response.await.unwrap();
    assert!(timing.queue_wait_ms.is_some());
    assert!(timing.duration_ms >= timing.queue_wait_ms.unwrap());
}

struct RecordingObserver(Arc<Mutex<Vec<RequestCompletion>>>);

impl RequestObserver for RecordingObserver {
    fn record(self: Box<Self>, completion: &RequestCompletion) {
        self.0.lock().unwrap().push(completion.clone());
    }
}
