//! `GET /api/v1/pool/bindings` — native-bearer read of where each job
//! currently stands between the ready queue and a runner actually running
//! it: `ready` (no runner picked yet), `provisioning` (waiting on pool
//! capacity), `assigned` (a runner is picked but has not started it), or
//! `running` (a runner is executing it). Feeds the TUI's fleet view.

use super::*;
use crate::control::types::PoolBindingState;

/// One job's pool-dispatch binding.
#[derive(Debug, Clone, Serialize)]
pub struct PoolBindingEntry {
    pub run_id: String,
    pub job_id: String,
    pub state: &'static str,
    pub runner_id: Option<i64>,
    pub runner_name: Option<String>,
    pub since_seconds: u64,
    pub waitlist_position: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PoolBindingsResponse {
    pub bindings: Vec<PoolBindingEntry>,
    pub observed_at: chrono::DateTime<chrono::Utc>,
}

/// GET /api/v1/pool/bindings — every job either waiting in the ready queue
/// or currently held by a runner, with the pool-dispatch state the TUI's
/// fleet view needs to distinguish a stuck queue from a busy one.
pub async fn pool_bindings(
    State(shared): State<Arc<SharedState>>,
) -> Result<Json<PoolBindingsResponse>, ApiError> {
    let records = shared
        .state
        .backend
        .pool_bindings()
        .await
        .map_err(ApiError::from)?;
    let bindings = records
        .into_iter()
        .map(|record| PoolBindingEntry {
            run_id: record.run_id.0.to_string(),
            job_id: record.job_id.0,
            state: match record.state {
                PoolBindingState::Running => "running",
                PoolBindingState::Assigned => "assigned",
                PoolBindingState::Ready => "ready",
                PoolBindingState::Provisioning => "provisioning",
            },
            runner_id: record.runner_id,
            runner_name: record.runner_name,
            since_seconds: record.since_seconds,
            waitlist_position: record.waitlist_position,
        })
        .collect();
    Ok(Json(PoolBindingsResponse {
        bindings,
        observed_at: chrono::Utc::now(),
    }))
}
