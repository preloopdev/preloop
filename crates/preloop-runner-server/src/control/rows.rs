//! Backend-neutral row codecs for decomposed table families.
//!
//! Each family that used to be one sealed blob per key is stored as plain
//! rows. The codecs here convert between the domain struct and a flat row so
//! SQLite and Postgres bind/read the exact same columns and can never drift
//! in encoding.

use crate::models::{StepKind, StepRecord};

/// One `job_steps` / `step_history` row. `position` preserves the manifest
/// order (`Vec` index); `step_id` is the protocol identity (`TaskStep.id`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepRow {
    pub(crate) step_id: String,
    pub(crate) position: i64,
    pub(crate) kind: &'static str,
    pub(crate) workflow_index: Option<i64>,
    pub(crate) runner_number: Option<i64>,
    pub(crate) context_name: Option<String>,
    pub(crate) name: String,
    pub(crate) conclusion: String,
    pub(crate) started_at_us: Option<i64>,
    pub(crate) finished_at_us: Option<i64>,
}

pub(crate) fn step_kind_str(kind: StepKind) -> &'static str {
    match kind {
        StepKind::Workflow => "workflow",
        StepKind::Synthetic => "synthetic",
    }
}

/// Unknown kinds decode as `Synthetic`, the variant that refuses `--step`
/// resolution rather than guessing.
pub(crate) fn step_kind_parse(kind: &str) -> StepKind {
    match kind {
        "workflow" => StepKind::Workflow,
        _ => StepKind::Synthetic,
    }
}

impl StepRow {
    pub(crate) fn from_record(position: usize, record: &StepRecord) -> Self {
        StepRow {
            step_id: record.id.clone(),
            position: position as i64,
            kind: step_kind_str(record.kind),
            workflow_index: record.workflow_index.map(|i| i as i64),
            runner_number: record.runner_number.map(i64::from),
            context_name: record.context_name.clone(),
            name: record.name.clone(),
            conclusion: record.conclusion.clone(),
            started_at_us: record.started_at.map(|t| t.timestamp_micros()),
            finished_at_us: record.finished_at.map(|t| t.timestamp_micros()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn into_record(
        step_id: String,
        kind: &str,
        workflow_index: Option<i64>,
        runner_number: Option<i64>,
        context_name: Option<String>,
        name: String,
        conclusion: String,
        started_at_us: Option<i64>,
        finished_at_us: Option<i64>,
    ) -> StepRecord {
        StepRecord {
            id: step_id,
            kind: step_kind_parse(kind),
            workflow_index: workflow_index.and_then(|i| usize::try_from(i).ok()),
            runner_number: runner_number.and_then(|n| u32::try_from(n).ok()),
            context_name,
            name,
            conclusion,
            started_at: started_at_us.and_then(chrono::DateTime::from_timestamp_micros),
            finished_at: finished_at_us.and_then(chrono::DateTime::from_timestamp_micros),
        }
    }
}

/// Row-level delta for one attempt's manifest against its loaded snapshot.
/// `upserts` are rows new or changed (content or position); `deletes` are
/// step ids present at load but gone now. An unchanged manifest yields an
/// empty delta, so write-back touches nothing.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct StepDelta {
    pub(crate) upserts: Vec<StepRow>,
    pub(crate) deletes: Vec<String>,
}

pub(crate) fn step_delta(before: Option<&[StepRecord]>, after: &[StepRecord]) -> StepDelta {
    let before_rows: std::collections::HashMap<&str, StepRow> = before
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(i, r)| (r.id.as_str(), StepRow::from_record(i, r)))
        .collect();
    let mut delta = StepDelta::default();
    let mut kept = std::collections::HashSet::new();
    for (i, record) in after.iter().enumerate() {
        let row = StepRow::from_record(i, record);
        kept.insert(record.id.as_str());
        if before_rows.get(record.id.as_str()) != Some(&row) {
            delta.upserts.push(row);
        }
    }
    for id in before_rows.keys() {
        if !kept.contains(id) {
            delta.deletes.push((*id).to_owned());
        }
    }
    delta.deletes.sort();
    delta
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(id: &str, conclusion: &str) -> StepRecord {
        StepRecord {
            id: id.to_owned(),
            kind: StepKind::Workflow,
            workflow_index: Some(0),
            runner_number: Some(2),
            context_name: Some("compile".to_owned()),
            name: "cargo build".to_owned(),
            conclusion: conclusion.to_owned(),
            started_at: chrono::DateTime::from_timestamp_micros(1_700_000_000_123_456),
            finished_at: None,
        }
    }

    #[test]
    fn row_round_trip_is_lossless() {
        let record = step("s1", "success");
        let row = StepRow::from_record(3, &record);
        let back = StepRow::into_record(
            row.step_id.clone(),
            row.kind,
            row.workflow_index,
            row.runner_number,
            row.context_name.clone(),
            row.name.clone(),
            row.conclusion.clone(),
            row.started_at_us,
            row.finished_at_us,
        );
        assert_eq!(StepRow::from_record(3, &back), row);
    }

    #[test]
    fn delta_touches_only_changed_steps() {
        let before = vec![step("s1", "success"), step("s2", "in_progress")];
        let after = vec![step("s1", "success"), step("s2", "failure")];
        let delta = step_delta(Some(&before), &after);
        assert_eq!(delta.upserts.len(), 1);
        assert_eq!(delta.upserts[0].step_id, "s2");
        assert!(delta.deletes.is_empty());
        assert_eq!(step_delta(Some(&after), &after), StepDelta::default());
    }

    #[test]
    fn delta_reports_removed_and_reordered_steps() {
        let before = vec![step("s1", "success"), step("s2", "success")];
        let after = vec![step("s2", "success")];
        let delta = step_delta(Some(&before), &after);
        assert_eq!(delta.deletes, vec!["s1".to_owned()]);
        // s2 moved from position 1 to 0: position is part of the row.
        assert_eq!(delta.upserts.len(), 1);
    }
}
