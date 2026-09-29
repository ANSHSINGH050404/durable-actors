use anyhow::Result;
use async_trait::async_trait;

use super::{
    TraceEvent, TracePage,
    history::HistoryQuery,
    metrics::{OverviewMetrics, QueueWaitQuery, QueueWaitRow, SocketSession, TimeRange},
    replay::ReplayQuery,
};

mod cursor;
pub(crate) mod postgres;
pub(crate) mod sqlite;

#[derive(Default)]
pub(crate) struct TraceStatus {
    pub dropped: u64,
    pub persistence_failed: bool,
}

// TraceStore validates project IDs and query bounds before calling the backends.
#[async_trait]
pub(crate) trait TracePersistence: Send + Sync {
    async fn initialize(&self) -> Result<()>;
    async fn record_status(&self, project: &str, dropped: u64, failed: bool) -> Result<()>;
    async fn status(&self, project: &str) -> Result<TraceStatus>;
    // Events have stable IDs; repeated appends must not duplicate them.
    async fn append(&self, events: &[TraceEvent]) -> Result<()>;
    async fn history(&self, project_id: &str, query: &HistoryQuery) -> Result<TracePage>;
    async fn metrics(&self, project_id: &str, query: &TimeRange) -> Result<OverviewMetrics>;
    async fn queue_waits(
        &self,
        project_id: &str,
        query: &QueueWaitQuery,
    ) -> Result<Vec<QueueWaitRow>>;
    async fn websockets(&self, project_id: &str, query: &TimeRange) -> Result<Vec<SocketSession>>;

    async fn replay(&self, project_id: &str, query: &ReplayQuery) -> Result<TracePage>;
}

#[cfg(test)]
#[path = "../../tests/unit/request_traces/persistence/contract_tests.rs"]
mod contract_tests;
