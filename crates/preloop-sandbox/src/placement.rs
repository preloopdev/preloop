//! Placement: which provider and driver serve a request.
//!
//! Placement is split in two halves. The service does the hard part: it drops
//! providers that are offline (and, for snapshot templates, that do not hold
//! the snapshot) and every provider/driver pair that misses a required
//! capability or is at its slot cap, leaving a [`Candidate`] list. A
//! [`Placement`] then ranks those candidates by preference and the service
//! takes the first id it recognises. A placement is therefore an ordering, not
//! a filter: it can never make a candidate the service rejected eligible, and
//! it never sees a candidate that could not serve the request.
//!
//! [`LeastLoaded`] is the default and reproduces the service's historical
//! scan: fewest live sandboxes first, ties in the order the service produced
//! the candidates (provider id, then the provider's driver order).

use std::sync::Arc;

use crate::api::NetworkMode;

/// Identifies one candidate in the list handed to [`Placement::rank`].
///
/// Ids are meaningful only for the call that produced them; the service maps
/// the first recognised id back to its provider and driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CandidateId(pub usize);

/// A provider/driver pair that satisfies the request's hard constraints and
/// has room for one more sandbox.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Id a placement returns to select this pair.
    pub id: CandidateId,
    /// Provider (host agent) id.
    pub provider_id: String,
    /// Provider name, as registered.
    pub provider_name: String,
    /// Driver name.
    pub driver: String,
    /// Sandboxes this pair already holds that are not in a terminal state.
    pub live: usize,
    /// Concurrent sandbox cap the driver declared; 0 means no cap.
    pub slots: u32,
}

/// What was asked for, resolved (template requirements merged with the
/// request's), in the terms a placement ranks on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementRequest {
    /// Template being placed.
    pub template: String,
    /// Driver the request pins, if any: the acquire's requirement, else the
    /// driver the template names (a snapshot's holder).
    pub driver: String,
    /// Required guest OS.
    pub os: String,
    /// Required guest architecture, if any.
    pub arch: String,
    /// Required features.
    pub features: Vec<String>,
    /// Required optional operations.
    pub capabilities: Vec<String>,
    /// Whether the sandbox must be snapshot-capable.
    pub snapshot_capable: bool,
    /// Requested egress mode.
    pub network: NetworkMode,
    /// Whether host directories are mounted.
    pub host_mounts: bool,
    /// Whether host sockets are mounted.
    pub socket_mounts: bool,
}

/// Ranks the candidates the service already filtered.
///
/// `rank` returns candidate ids in preference order. The service takes the
/// first id that is in `candidates`; a placement that returns no id from the
/// list fails the request with an internal error. `rank` runs under the
/// service's state lock, so it must be cheap and must not call back into the
/// service.
pub trait Placement: Send + Sync {
    /// Name used by `PRELOOP_SANDBOX_PLACEMENT` and in diagnostics.
    fn name(&self) -> &'static str;
    /// Preference order over `candidates`.
    fn rank(&self, request: &PlacementRequest, candidates: &[Candidate]) -> Vec<CandidateId>;
}

impl std::fmt::Debug for dyn Placement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Placement({})", self.name())
    }
}

/// Fewest live sandboxes first; ties keep the order the service produced.
#[derive(Debug, Clone, Copy, Default)]
pub struct LeastLoaded;

impl LeastLoaded {
    /// Name in `PRELOOP_SANDBOX_PLACEMENT`.
    pub const NAME: &'static str = "least-loaded";
}

impl Placement for LeastLoaded {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn rank(&self, _request: &PlacementRequest, candidates: &[Candidate]) -> Vec<CandidateId> {
        // A stable sort by load reproduces the historical "first least-loaded
        // pair wins" scan: equal loads keep the service's candidate order.
        let mut ranked: Vec<&Candidate> = candidates.iter().collect();
        ranked.sort_by_key(|candidate| candidate.live);
        ranked.into_iter().map(|candidate| candidate.id).collect()
    }
}

/// Names of the built-in placements, in the order they are listed to users.
pub const NAMES: &[&str] = &[LeastLoaded::NAME];

/// Resolve a built-in placement by name.
pub fn by_name(name: &str) -> Option<Arc<dyn Placement>> {
    if name == LeastLoaded::NAME {
        return Some(Arc::new(LeastLoaded));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: usize, provider: &str, driver: &str, live: usize) -> Candidate {
        Candidate {
            id: CandidateId(id),
            provider_id: format!("prv-{provider}"),
            provider_name: provider.into(),
            driver: driver.into(),
            live,
            slots: 0,
        }
    }

    #[test]
    fn least_loaded_orders_by_live_count() {
        let candidates = vec![
            candidate(0, "host-a", "smolvm", 3),
            candidate(1, "host-b", "smolvm", 1),
            candidate(2, "host-c", "smolvm", 2),
        ];
        let ranked = LeastLoaded.rank(&PlacementRequest::default(), &candidates);
        let order: Vec<usize> = ranked.into_iter().map(|id| id.0).collect();
        assert_eq!(order, vec![1, 2, 0]);
    }

    #[test]
    fn least_loaded_keeps_candidate_order_for_ties() {
        let candidates = vec![
            candidate(0, "host-a", "smolvm", 2),
            candidate(1, "host-b", "smolvm", 1),
            candidate(2, "host-a", "fake", 2),
            candidate(3, "host-c", "smolvm", 1),
        ];
        let ranked = LeastLoaded.rank(&PlacementRequest::default(), &candidates);
        // Equal loads keep the service's order, driver order included.
        let order: Vec<usize> = ranked.into_iter().map(|id| id.0).collect();
        assert_eq!(order, vec![1, 3, 0, 2]);
    }

    #[test]
    fn least_loaded_ranks_every_candidate_once() {
        let candidates = vec![
            candidate(0, "host-a", "smolvm", 0),
            candidate(1, "host-b", "smolvm", 0),
            candidate(2, "host-c", "smolvm", 4),
        ];
        let ranked = LeastLoaded.rank(&PlacementRequest::default(), &candidates);
        assert_eq!(ranked.len(), candidates.len());
        for candidate in &candidates {
            assert_eq!(ranked.iter().filter(|id| **id == candidate.id).count(), 1);
        }
    }

    #[test]
    fn built_in_placements_resolve_by_name() {
        assert_eq!(NAMES.len(), 1);
        assert_eq!(NAMES[0], "least-loaded");
        assert_eq!(by_name("least-loaded").unwrap().name(), "least-loaded");
        assert!(by_name("warmest").is_none());
    }
}
