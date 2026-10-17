//! Automatic collection of the read-only volumes AgentENV holds.
//!
//! [`super::volume`] publishes one read-only volume per distinct runner tree
//! (`preloop-runner-<12 hex>`). A changed bundle publishes a *new* volume and
//! leaves the old one behind, because an unmounted volume may be about to be
//! mounted by another engine — so a host that upgrades its runner bundle a few
//! times carries one full runner tree per version. This module reclaims them.
//!
//! Nothing here talks to AgentENV: [`plan_gc`] is a pure function of what is
//! listed, what is in use, and how old a volume is, so every rule below has a
//! unit test that runs without a server. The provider supplies the facts and
//! executes the plan ([`run_pass`]).
//!
//! # What a pass keeps
//!
//! * a volume the caller is using right now ([`KeepReason::Live`]: the engine's
//!   own published set, or the trees `--keep` names),
//! * a volume a live sandbox mounts ([`KeepReason::Mounted`]),
//! * a volume a populate is writing, and the published volume that populate
//!   commits ([`KeepReason::Populating`]),
//! * a volume younger than the grace period ([`KeepReason::Young`]),
//! * a volume nothing recorded an age for ([`KeepReason::UnknownAge`]),
//! * the newest few superseded volumes, for a rollback ([`KeepReason::Previous`]),
//! * a volume no kind in the policy owns ([`KeepReason::Unmanaged`]).
//!
//! # What a pass deletes
//!
//! * a published volume whose tree nothing uses any more, past its grace period
//!   and beyond the rollback depth ([`DeleteReason::Superseded`]),
//! * a staging volume whose populate is gone ([`DeleteReason::Abandoned`]),
//! * a volume AgentENV reports as something other than `ready` ([`DeleteReason::Unusable`]),
//!   which this engine refuses to mount, so a start stays stuck until it is gone.
//!
//! # When a pass runs
//!
//! 1. in the background after the engine publishes a volume (a cold start or a
//!    bundle change — not on every start),
//! 2. periodically, every [`DEFAULT_INTERVAL_SECS`] by default, and
//! 3. by hand: `preloop volumes gc [--dry-run]`.
//!
//! `PRELOOP_VOLUMES_GC=off` turns 1 and 2 off and leaves 3 (like `prune`, an
//! explicit act is still allowed). The default is `auto`: nothing else removes a
//! superseded volume, and the cost of a mistake is bounded — the engine
//! revalidates a remembered volume before reusing it, so a volume deleted here
//! is populated again on the next start that needs it (~1 minute), not a
//! failure. See [`super::volume`] for what a populate costs.
//!
//! # Two engines, one AgentENV
//!
//! `cpane` can run more than one engine against one server. A pass keeps its
//! *own* live volumes, anything mounted, anything young, and its own in-flight
//! stages, but it cannot see another engine's in-memory working set: an idle
//! volume another engine has published and will mount again is a candidate for
//! `Superseded` once it is past the grace period. The engine that loses it
//! revalidates the remembered name before reuse and repopulates, so the cost is
//! one populate on that engine's next start, and the grace period covers the
//! window between publishing and mounting. Every deletion is logged with its
//! name, size and reason, so this is visible after the fact.

use super::volume::{self, STAGING_PREFIX, VOLUME_PREFIX};
use super::{AgentEnvProvider, AgentEnvVolume, PruneReport, VolumeIndex};
use crate::VmError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;

/// The mode [`Mode`] is read from: `auto` (the default) or `off`.
pub const MODE_ENV: &str = "PRELOOP_VOLUMES_GC";
/// How many superseded volumes to keep for a rollback.
pub const KEEP_PREVIOUS_ENV: &str = "PRELOOP_VOLUMES_KEEP_PREVIOUS";
/// How long a volume is left alone after it was created, in seconds.
pub const GRACE_ENV: &str = "PRELOOP_VOLUMES_GC_GRACE_SECS";
/// How often the periodic pass runs, in seconds.
pub const INTERVAL_ENV: &str = "PRELOOP_VOLUMES_GC_INTERVAL_SECS";

/// Superseded volumes kept by default: one, so a bundle update that goes wrong
/// can be rolled back without repopulating.
pub const DEFAULT_KEEP_PREVIOUS: usize = 1;
/// The default grace period: a volume created within the last hour is never
/// deleted, which is far longer than a populate (~1 minute) and covers the
/// window between publishing a volume and starting the first sandbox on it.
pub const DEFAULT_GRACE_SECS: u64 = 3600;
/// The default period between passes: six hours.
pub const DEFAULT_INTERVAL_SECS: u64 = 6 * 60 * 60;
/// The shortest gap between two trigger-driven passes in one process.
///
/// A bundle change publishes one volume, so a pass after each publish is
/// already rare; this bounds the burst when several trees are populated at once
/// (a cold pool). The periodic pass is not subject to it.
pub const TRIGGER_GAP_SECS: u64 = 300;

/// The status AgentENV reports for a volume that can be mounted.
const READY_STATUS: &str = "ready";
/// Hex digits of the archive digest a volume name carries (see [`super::volume`]).
const DIGEST_HEX: usize = 12;

/// The kind label a volume gets when no kind in the policy owns it.
const UNKNOWN_KIND: &str = "unknown";

/// The current time, in Unix seconds.
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default()
}

/// Whether automatic collection runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Collect after a populate and on a timer.
    Auto,
    /// Never collect on its own; `preloop volumes gc` still runs by hand.
    Off,
}

impl Mode {
    /// The mode `raw` names, or `None` when it names neither.
    pub fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "auto" | "on" | "true" | "yes" | "1" => Some(Mode::Auto),
            "off" | "false" | "no" | "never" | "0" => Some(Mode::Off),
            _ => None,
        }
    }

    /// The mode's own name, for reports and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Auto => "auto",
            Mode::Off => "off",
        }
    }

    /// Whether a pass may run without being asked for one.
    pub fn is_auto(self) -> bool {
        matches!(self, Mode::Auto)
    }
}

/// The knobs one process runs collection with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Whether passes run on their own.
    pub mode: Mode,
    /// Superseded volumes to keep per kind, newest first.
    pub keep_previous: usize,
    /// How long a freshly created volume is left alone.
    pub grace: Duration,
    /// How often the periodic pass runs. Zero disables it.
    pub interval: Duration,
}

impl Config {
    /// The knobs the process environment asks for.
    ///
    /// An unrecognized mode is off, with a warning: a typo must not enable
    /// deletion, and a typo must not silently keep a host filling up either —
    /// which is why the bad value is logged rather than ignored.
    pub fn from_environment() -> Self {
        let mode = match std::env::var(MODE_ENV) {
            Ok(raw) => Mode::parse(&raw).unwrap_or_else(|| {
                tracing::warn!(
                    env = MODE_ENV,
                    value = %raw,
                    "unrecognized volume collection mode; automatic collection is off"
                );
                Mode::Off
            }),
            Err(_) => Mode::Auto,
        };
        Config {
            mode,
            keep_previous: number(KEEP_PREVIOUS_ENV, DEFAULT_KEEP_PREVIOUS as u64) as usize,
            grace: Duration::from_secs(number(GRACE_ENV, DEFAULT_GRACE_SECS)),
            interval: Duration::from_secs(number(INTERVAL_ENV, DEFAULT_INTERVAL_SECS)),
        }
    }

    /// The defaults, for tests and for an embedder that wants them.
    pub fn defaults() -> Self {
        Config {
            mode: Mode::Auto,
            keep_previous: DEFAULT_KEEP_PREVIOUS,
            grace: Duration::from_secs(DEFAULT_GRACE_SECS),
            interval: Duration::from_secs(DEFAULT_INTERVAL_SECS),
        }
    }

    /// The defaults with automatic collection off.
    pub fn off() -> Self {
        Config {
            mode: Mode::Off,
            ..Config::defaults()
        }
    }

    /// The retention policy these knobs describe.
    pub fn policy(&self) -> Policy {
        Policy::runner_bundle(self.grace, self.keep_previous)
    }
}

/// A number from the environment, or `default` when it is unset or unreadable.
fn number(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(raw) => raw.trim().parse().unwrap_or_else(|_| {
            tracing::warn!(
                env = name,
                value = %raw,
                default,
                "not a number; using the default"
            );
            default
        }),
        Err(_) => default,
    }
}

/// How long a kind's volumes survive without being used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KindRetention {
    /// The grace period: a volume of this kind younger than this is kept.
    pub grace: Duration,
    /// How many superseded volumes of this kind to keep, newest first. Zero
    /// reclaims every superseded volume.
    pub keep_previous: usize,
}

/// One family of volumes, and how it is reclaimed.
///
/// Two kinds are defined here — the published runner bundles and the staging
/// volumes a populate writes — and a later kind with its own retention (a cache
/// volume mounted for an actions worktree, say) plugs in by implementing this
/// trait and joining the [`Policy`]. A volume no kind claims is never deleted.
pub trait VolumeKind: fmt::Debug + Send + Sync {
    /// The kind's name, as it appears in reports.
    fn kind(&self) -> &'static str;
    /// Whether this kind owns `volume`, as AgentENV names it.
    fn owns(&self, volume: &str) -> bool;
    /// Whether a volume of this kind is an unpublished copy a populate writes
    /// before it publishes one: a leftover is abandoned, never `Superseded`.
    fn is_staging(&self) -> bool;
    /// How long this kind's volumes survive without being used.
    fn retention(&self) -> KindRetention;
}

/// The published runner-bundle volumes: one per distinct runner tree.
#[derive(Debug, Clone, Copy)]
pub struct RunnerBundle {
    retention: KindRetention,
}

impl RunnerBundle {
    /// The kind, with the policy's knobs.
    pub fn new(grace: Duration, keep_previous: usize) -> Self {
        RunnerBundle {
            retention: KindRetention {
                grace,
                keep_previous,
            },
        }
    }
}

impl VolumeKind for RunnerBundle {
    fn kind(&self) -> &'static str {
        "runner-bundle"
    }

    fn owns(&self, volume: &str) -> bool {
        volume::is_volume_name(volume)
    }

    fn is_staging(&self) -> bool {
        false
    }

    fn retention(&self) -> KindRetention {
        self.retention
    }
}

/// The exclusive volumes a populate writes before it publishes one.
#[derive(Debug, Clone, Copy)]
pub struct RunnerBundleStaging {
    grace: Duration,
}

impl RunnerBundleStaging {
    /// The kind, with the policy's grace period.
    pub fn new(grace: Duration) -> Self {
        RunnerBundleStaging { grace }
    }
}

impl VolumeKind for RunnerBundleStaging {
    fn kind(&self) -> &'static str {
        "runner-bundle (staging)"
    }

    fn owns(&self, volume: &str) -> bool {
        volume.starts_with(STAGING_PREFIX)
    }

    fn is_staging(&self) -> bool {
        true
    }

    fn retention(&self) -> KindRetention {
        KindRetention {
            grace: self.grace,
            keep_previous: 0,
        }
    }
}

/// The kinds a pass classifies volumes with, in the order it tries them.
#[derive(Debug)]
pub struct Policy {
    kinds: Vec<Box<dyn VolumeKind>>,
}

impl Policy {
    /// The policy for the runner-bundle volumes this module collects.
    pub fn runner_bundle(grace: Duration, keep_previous: usize) -> Self {
        Policy {
            // Staging first: its prefix is the published kind's prefix plus a
            // suffix, and `RunnerBundle::owns` is strict about the digest.
            kinds: vec![
                Box::new(RunnerBundleStaging::new(grace)),
                Box::new(RunnerBundle::new(grace, keep_previous)),
            ],
        }
    }

    /// Add a kind. The first kind added that owns a volume classifies it, so a
    /// narrow kind goes in before a broad one.
    pub fn with_kind(mut self, kind: Box<dyn VolumeKind>) -> Self {
        self.kinds.push(kind);
        self
    }

    /// The kind that owns `volume`, when one does.
    pub fn classify(&self, volume: &str) -> Option<&dyn VolumeKind> {
        self.kinds
            .iter()
            .find(|kind| kind.owns(volume))
            .map(AsRef::as_ref)
    }
}

/// What the planner knows about one volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeFacts {
    /// The name AgentENV lists it under.
    pub name: String,
    /// Its size, in MiB.
    pub size_mb: u64,
    /// The status AgentENV reports.
    pub status: String,
    /// When it was created, in Unix seconds; `None` when neither the index nor
    /// the staging registry recorded it — a volume populated before either
    /// existed, or by another host. An unknown age keeps the volume.
    pub created: Option<u64>,
}

impl VolumeFacts {
    /// The age in seconds, when it is known.
    pub fn age_secs(&self, now: u64) -> Option<u64> {
        self.created.map(|created| now.saturating_sub(created))
    }
}

/// Why a pass keeps a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepReason {
    /// The caller is using the volume right now.
    Live,
    /// A live sandbox mounts it.
    Mounted,
    /// A populate is writing it, or the volume that populate commits.
    Populating,
    /// It appeared less than the grace period ago.
    Young,
    /// Nothing recorded when it appeared, so it is treated as young.
    UnknownAge,
    /// A superseded volume kept for a rollback to the tree it holds.
    Previous,
    /// No kind in the policy owns it.
    Unmanaged,
}

impl KeepReason {
    /// The reason's own name, for reports and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            KeepReason::Live => "live",
            KeepReason::Mounted => "mounted",
            KeepReason::Populating => "populating",
            KeepReason::Young => "young",
            KeepReason::UnknownAge => "unknown-age",
            KeepReason::Previous => "previous",
            KeepReason::Unmanaged => "unmanaged",
        }
    }
}

/// Why a pass deletes a volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteReason {
    /// A published volume whose tree nothing uses any more.
    Superseded,
    /// A staging volume whose populate is gone.
    Abandoned,
    /// A volume AgentENV reports as something other than `ready`. This engine
    /// refuses to mount such a volume, so the tree's starts stay stuck until it
    /// is gone and the next start populates it again.
    Unusable,
}

impl DeleteReason {
    /// The reason's own name, for reports and logs.
    pub fn as_str(self) -> &'static str {
        match self {
            DeleteReason::Superseded => "superseded",
            DeleteReason::Abandoned => "abandoned",
            DeleteReason::Unusable => "unusable",
        }
    }
}

/// What a pass decided about one volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Left in place, for the reason.
    Keep(KeepReason),
    /// To be deleted, for the reason.
    Delete(DeleteReason),
}

impl Disposition {
    /// Whether the volume is to be deleted.
    pub fn is_delete(self) -> bool {
        matches!(self, Disposition::Delete(_))
    }

    /// The decision's own name, for reports and logs.
    pub fn action(self) -> &'static str {
        if self.is_delete() { "delete" } else { "keep" }
    }

    /// The reason's own name, for reports and logs.
    pub fn reason(self) -> &'static str {
        match self {
            Disposition::Keep(reason) => reason.as_str(),
            Disposition::Delete(reason) => reason.as_str(),
        }
    }
}

/// The decision about one volume, with what the report needs to explain it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcVerdict {
    /// The volume's name.
    pub name: String,
    /// The kind that owns it, or [`UNKNOWN_KIND`].
    pub kind: &'static str,
    /// Its size, in MiB.
    pub size_mb: u64,
    /// The status AgentENV reports.
    pub status: String,
    /// When it was created, when that is known.
    pub created: Option<u64>,
    /// Keep or delete, and why.
    pub disposition: Disposition,
}

impl GcVerdict {
    /// The verdict for `volume`, owned by `kind`.
    fn new(volume: &VolumeFacts, kind: &'static str, disposition: Disposition) -> Self {
        GcVerdict {
            name: volume.name.clone(),
            kind,
            size_mb: volume.size_mb,
            status: volume.status.clone(),
            created: volume.created,
            disposition,
        }
    }

    /// The volume's age in seconds, when it is known.
    pub fn age_secs(&self, now: u64) -> Option<u64> {
        self.created.map(|created| now.saturating_sub(created))
    }
}

/// What one pass decided: every listed volume, in name order.
///
/// Sorted so two runs over the same facts report identically.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcPlan {
    verdicts: Vec<GcVerdict>,
}

impl GcPlan {
    /// Every verdict, in name order.
    pub fn verdicts(&self) -> &[GcVerdict] {
        &self.verdicts
    }

    /// The volumes a pass leaves in place.
    pub fn keep(&self) -> impl Iterator<Item = &GcVerdict> {
        self.verdicts
            .iter()
            .filter(|verdict| !verdict.disposition.is_delete())
    }

    /// The volumes a pass deletes.
    pub fn delete(&self) -> impl Iterator<Item = &GcVerdict> {
        self.verdicts
            .iter()
            .filter(|verdict| verdict.disposition.is_delete())
    }

    /// How many volumes a pass deletes.
    pub fn deleting(&self) -> usize {
        self.delete().count()
    }

    /// What a pass frees, in MiB.
    pub fn reclaimable_mib(&self) -> u64 {
        self.delete().map(|verdict| verdict.size_mb).sum()
    }

    /// Whether a pass would do nothing at all.
    pub fn is_noop(&self) -> bool {
        self.verdicts.is_empty() || self.deleting() == 0
    }

    /// The names a pass leaves in place: exactly what a deletion must be given
    /// as its keep set, since everything else it lists is to be deleted.
    pub fn keep_names(&self) -> Vec<String> {
        self.keep().map(|verdict| verdict.name.clone()).collect()
    }

    /// The names a pass deletes.
    pub fn delete_names(&self) -> Vec<String> {
        self.delete().map(|verdict| verdict.name.clone()).collect()
    }

    /// The plan as a table: one row per volume, then the tally.
    pub fn render(&self, now: u64) -> String {
        if self.verdicts.is_empty() {
            return "no runner volumes\n".to_owned();
        }
        let mut out = String::new();
        out.push_str(&format!(
            "{:<44} {:<22} {:>8} {:>8} {:<9} {:<7} {}\n",
            "NAME", "KIND", "SIZE_MB", "AGE_S", "STATUS", "ACTION", "REASON"
        ));
        for verdict in &self.verdicts {
            let age = match verdict.age_secs(now) {
                Some(seconds) => seconds.to_string(),
                None => "unknown".to_owned(),
            };
            let status = if verdict.status.trim().is_empty() {
                "-"
            } else {
                verdict.status.as_str()
            };
            out.push_str(&format!(
                "{:<44} {:<22} {:>8} {:>8} {:<9} {:<7} {}\n",
                verdict.name,
                verdict.kind,
                verdict.size_mb,
                age,
                status,
                verdict.disposition.action(),
                verdict.disposition.reason()
            ));
        }
        out.push_str(&format!(
            "\n{} kept, {} to delete ({} MiB reclaimable)\n",
            self.verdicts.len() - self.deleting(),
            self.deleting(),
            self.reclaimable_mib()
        ));
        out
    }
}

/// A superseded volume the rollback rule has yet to decide about.
struct Candidate {
    /// Where its verdict sits in the plan's list.
    position: usize,
    /// When it was created, in Unix seconds.
    created: u64,
    /// Its name, so ties are broken the same way on every run.
    name: String,
}

/// The superseded volumes of one kind, with that kind's rollback depth.
struct Rollback {
    /// How many of them a pass keeps.
    retention: KindRetention,
    /// All of them, newest first once sorted.
    candidates: Vec<Candidate>,
}

/// How old a volume is, against a kind's grace period.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Age {
    /// Nothing recorded when it appeared.
    Unknown,
    /// Younger than the grace period.
    Young,
    /// Older than the grace period.
    Old,
}

/// The age of `volume` against `retention`, at `now`.
fn age(volume: &VolumeFacts, retention: KindRetention, now: u64) -> Age {
    let Some(seconds) = volume.age_secs(now) else {
        return Age::Unknown;
    };
    if seconds < retention.grace.as_secs() {
        Age::Young
    } else {
        Age::Old
    }
}

/// The content digest in a published volume name.
fn digest_of(volume: &str) -> Option<&str> {
    volume::is_volume_name(volume).then(|| &volume[VOLUME_PREFIX.len()..])
}

/// The content digest a staging volume name carries.
fn staging_digest(staging: &str) -> Option<&str> {
    let rest = staging.strip_prefix(STAGING_PREFIX)?;
    let (digest, attempt) = rest.split_once('-')?;
    (digest.len() == DIGEST_HEX && !attempt.is_empty()).then_some(digest)
}

/// Decide what a pass keeps and what it deletes.
///
/// Pure: the caller supplies the facts ([`VolumeFacts`], with each volume's
/// creation time already resolved from the index or the staging registry),
/// which volumes it is using (`live`), which it knows are mounted (`mounted`)
/// and which populate is writing right now (`in_flight`, staging names), and
/// the policy. `now` is Unix seconds.
pub fn plan_gc(
    volumes: &[VolumeFacts],
    live: &BTreeSet<String>,
    mounted: &BTreeSet<String>,
    in_flight: &BTreeSet<String>,
    policy: &Policy,
    now: u64,
) -> GcPlan {
    // A populate writes a staging volume and then commits it to the published
    // name of the same digest, so while either is in flight both names of that
    // digest are off limits — including a published volume an older engine left
    // that this populate is about to find already taken.
    let populating: BTreeSet<&str> = in_flight
        .iter()
        .filter_map(|name| staging_digest(name))
        .collect();
    let mut verdicts: Vec<GcVerdict> = Vec::with_capacity(volumes.len());
    // Published volumes that only the rollback rule can save, by kind: the
    // position of their verdict, when they were created, and their name.
    let mut superseded: BTreeMap<&'static str, Rollback> = BTreeMap::new();

    for volume in volumes {
        let Some(kind) = policy.classify(&volume.name) else {
            verdicts.push(GcVerdict::new(
                volume,
                UNKNOWN_KIND,
                Disposition::Keep(KeepReason::Unmanaged),
            ));
            continue;
        };
        let retention = kind.retention();
        let disposition = if live.contains(&volume.name) {
            Disposition::Keep(KeepReason::Live)
        } else if mounted.contains(&volume.name) {
            Disposition::Keep(KeepReason::Mounted)
        } else if in_flight.contains(&volume.name)
            || digest_of(&volume.name).is_some_and(|digest| populating.contains(digest))
        {
            Disposition::Keep(KeepReason::Populating)
        } else {
            match age(volume, retention, now) {
                Age::Unknown => Disposition::Keep(KeepReason::UnknownAge),
                Age::Young => Disposition::Keep(KeepReason::Young),
                Age::Old if kind.is_staging() => Disposition::Delete(DeleteReason::Abandoned),
                Age::Old if volume.status != READY_STATUS => {
                    Disposition::Delete(DeleteReason::Unusable)
                }
                Age::Old => {
                    let rollback = superseded.entry(kind.kind()).or_insert_with(|| Rollback {
                        retention,
                        candidates: Vec::new(),
                    });
                    // `Old` means the age is known, so this is a real timestamp.
                    rollback.candidates.push(Candidate {
                        position: verdicts.len(),
                        created: volume.created.unwrap_or_default(),
                        name: volume.name.clone(),
                    });
                    // Provisional: the rollback rule below decides.
                    Disposition::Keep(KeepReason::Previous)
                }
            }
        };
        verdicts.push(GcVerdict::new(volume, kind.kind(), disposition));
    }

    // Rollback rule: per kind, the newest `keep_previous` superseded volumes are
    // kept and every older one is deleted. Without it a bundle update would
    // always repopulate on a rollback, and with it a host carries at most
    // `keep_previous` extra runner trees.
    for (_kind, mut rollback) in superseded {
        rollback.candidates.sort_by(|left, right| {
            right
                .created
                .cmp(&left.created)
                .then_with(|| left.name.cmp(&right.name))
        });
        for (rank, candidate) in rollback.candidates.iter().enumerate() {
            if rank >= rollback.retention.keep_previous {
                verdicts[candidate.position].disposition =
                    Disposition::Delete(DeleteReason::Superseded);
            }
        }
    }

    verdicts.sort_by(|left, right| left.name.cmp(&right.name));
    GcPlan { verdicts }
}

/// The name of the staging registry under the preloop home.
///
/// It is a separate file from the provider's volume index because it is written
/// while a populate runs, by whichever process is populating, and read by any
/// process that wants to know whether a staging volume is abandoned.
const STAGING_FILE: &str = "agentenv-volume-staging.json";

/// A staging volume a populate is writing, as the registry records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedVolume {
    /// The digest of the tree being written into it.
    #[serde(default)]
    pub digest: String,
    /// The host tree the archive came from.
    #[serde(default)]
    pub host: String,
    /// When the populate started, in Unix seconds.
    #[serde(default)]
    pub created: u64,
}

/// Every staging volume a host's populates have started and not finished.
///
/// A populate that completes removes its record; one whose process died leaves
/// it behind, which is exactly how a later pass tells an abandoned staging
/// volume from one a populate is writing right now (see [`Age`]).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingRegistry {
    /// Staging volume name → what is being written into it.
    #[serde(default)]
    pub staging: BTreeMap<String, StagedVolume>,
}

impl StagingRegistry {
    /// What is being written into `staging`, when a populate is or was.
    pub fn get(&self, staging: &str) -> Option<&StagedVolume> {
        self.staging.get(staging)
    }
}

/// The staging records in `path`, or none when it is missing or unreadable.
fn load_staging(path: &Path) -> StagingRegistry {
    match fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|error| {
            tracing::warn!(
                path = %path.display(),
                %error,
                "the volume staging registry is unreadable; starting empty"
            );
            StagingRegistry::default()
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => StagingRegistry::default(),
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                %error,
                "the volume staging registry could not be read; starting empty"
            );
            StagingRegistry::default()
        }
    }
}

/// Write `registry` to `path` atomically, best effort.
fn store_staging(path: &Path, registry: &StagingRegistry) {
    let Some(parent) = path.parent() else {
        return;
    };
    let write = || -> io::Result<()> {
        fs::create_dir_all(parent)?;
        let temporary = parent.join(format!(".{STAGING_FILE}.{}.tmp", std::process::id()));
        let bytes = serde_json::to_vec_pretty(registry)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        fs::write(&temporary, &bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
        }
        fs::rename(&temporary, path)
    };
    if let Err(error) = write() {
        tracing::warn!(
            path = %path.display(),
            %error,
            "the volume staging registry could not be written"
        );
    }
}

/// What one provider's processes know about collection: the knobs, the staging
/// volumes being written here, and the scheduler's own bookkeeping.
#[derive(Debug)]
pub struct GcState {
    /// The knobs this process runs with, read when it starts.
    config: Config,
    /// Staging volumes this process is writing right now.
    writing: Mutex<BTreeMap<String, StagedVolume>>,
    /// Where those records are persisted, when there is a preloop home.
    registry: Option<PathBuf>,
    /// When the last pass started, in Unix seconds; zero until one does.
    last_pass: AtomicU64,
    /// Whether the periodic pass was started, so it is started only once.
    scheduled: AtomicBool,
    /// Serialises passes: a trigger, the timer and a hand-run pass must not
    /// work on the same volumes at once.
    pass: AsyncMutex<()>,
}

impl GcState {
    /// The state a process runs collection with.
    pub fn new(config: Config, registry: Option<PathBuf>) -> Self {
        GcState {
            config,
            writing: Mutex::new(BTreeMap::new()),
            registry,
            last_pass: AtomicU64::new(0),
            scheduled: AtomicBool::new(false),
            pass: AsyncMutex::new(()),
        }
    }

    /// The state of a provider built without an environment: collection off.
    ///
    /// An embedder (and every unit test that builds a provider with `new`) gets
    /// this, so nothing is ever deleted unless it is asked for.
    pub fn disabled() -> Self {
        GcState::new(Config::off(), None)
    }

    /// The state `AgentEnvProvider::from_environment` runs with.
    pub fn from_environment() -> Self {
        GcState::new(
            Config::from_environment(),
            crate::effective_preloop_home().map(|home| home.join(STAGING_FILE)),
        )
    }

    /// The knobs this state runs with.
    pub fn config(&self) -> Config {
        self.config
    }

    /// The staging records, ignoring a poisoned lock: a panic in one task must
    /// not stop collection.
    fn writing(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, StagedVolume>> {
        self.writing
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Record that this process is writing `staging` for `digest`, from `host`.
    ///
    /// The returned guard removes the record when the populate ends, success or
    /// not — a process killed mid-populate leaves it behind on purpose, so the
    /// staging volume it was writing is reclaimed once it is old enough.
    pub fn begin_staging(
        self: &Arc<Self>,
        staging: &str,
        digest: &str,
        host: &Path,
    ) -> StagingGuard {
        let staged = StagedVolume {
            digest: digest.to_owned(),
            host: host.display().to_string(),
            created: unix_now(),
        };
        self.writing().insert(staging.to_owned(), staged.clone());
        self.persist_staging(|registry| {
            registry.staging.insert(staging.to_owned(), staged);
        });
        StagingGuard {
            state: Arc::clone(self),
            staging: staging.to_owned(),
        }
    }

    /// Drop the record for `staging`, after its populate ended.
    fn finish_staging(&self, staging: &str) {
        self.writing().remove(staging);
        self.persist_staging(|registry| {
            registry.staging.remove(staging);
        });
    }

    /// The staging records for this host: the persisted ones (which is how an
    /// abandoned populate is seen) overlaid with what this process is writing.
    pub fn staged(&self) -> StagingRegistry {
        let mut registry = self
            .registry
            .as_deref()
            .map(load_staging)
            .unwrap_or_default();
        for (name, staged) in self.writing().iter() {
            registry.staging.insert(name.clone(), staged.clone());
        }
        registry
    }

    /// The staging volumes this process is writing right now: never deleted,
    /// however old the record says they are.
    pub fn in_flight(&self) -> BTreeSet<String> {
        self.writing().keys().cloned().collect()
    }

    /// Drop the records of staging volumes a pass has deleted.
    fn forget_staged(&self, deleted: &[String]) {
        let staged: Vec<&String> = deleted
            .iter()
            .filter(|name| name.starts_with(STAGING_PREFIX))
            .collect();
        if staged.is_empty() {
            return;
        }
        {
            let mut writing = self.writing();
            for name in &staged {
                writing.remove(*name);
            }
        }
        self.persist_staging(|registry| {
            for name in &staged {
                registry.staging.remove(name.as_str());
            }
        });
    }

    /// Drop the records of staging volumes AgentENV no longer lists.
    ///
    /// A hand-run `preloop volumes prune` deletes staging leftovers without
    /// knowing about this registry, and a record for a volume that is gone would
    /// otherwise be read on every pass forever.
    ///
    /// Only records older than the grace period go: a record younger than that
    /// may name a staging volume another process has recorded but not created
    /// yet, and this process's own records are written before their volume
    /// exists. A record for a stage still in flight is never dropped.
    fn forget_missing(&self, listed: &[AgentEnvVolume], in_flight: &BTreeSet<String>, now: u64) {
        let listed: BTreeSet<&str> = listed.iter().map(|volume| volume.name.as_str()).collect();
        let grace = self.config.grace.as_secs();
        let stale: Vec<String> = self
            .staged()
            .staging
            .into_iter()
            .filter(|(name, record)| {
                !in_flight.contains(name)
                    && !listed.contains(name.as_str())
                    && record.created > 0
                    && now.saturating_sub(record.created) >= grace
            })
            .map(|(name, _)| name)
            .collect();
        if stale.is_empty() {
            return;
        }
        self.persist_staging(|registry| {
            registry.staging.retain(|name, _| !stale.contains(name));
        });
    }

    /// Read the registry, let `update` change it, and write it back.
    ///
    /// Read-modify-write so that one process's record does not drop another's:
    /// the file is shared by every engine on the host, and the worst a lost
    /// record can do is make a staging volume look ageless, which keeps it.
    fn persist_staging(&self, update: impl FnOnce(&mut StagingRegistry)) {
        let Some(path) = &self.registry else {
            return;
        };
        let mut registry = load_staging(path);
        update(&mut registry);
        store_staging(path, &registry);
    }
}

/// Removes a staging record when the populate that owns it ends.
///
/// Held across the whole populate, including the commit of the final volume, so
/// a pass never deletes a staging volume whose populate is still running.
#[derive(Debug)]
pub struct StagingGuard {
    state: Arc<GcState>,
    staging: String,
}

impl Drop for StagingGuard {
    fn drop(&mut self) {
        self.state.finish_staging(&self.staging);
    }
}

impl AgentEnvProvider {
    /// This provider's collection state.
    pub(crate) fn gc_state(&self) -> &Arc<GcState> {
        &self.volume_gc
    }

    /// The volumes this provider has published and still remembers.
    ///
    /// A pass must never delete one of these: the engine mounts a tree by the
    /// name it remembers and revalidates that name before reuse, so reclaiming
    /// one costs a repopulate (~1 minute) on the next start that needs the tree.
    pub async fn published_volume_names(&self) -> BTreeSet<String> {
        self.published_volumes
            .lock()
            .await
            .values()
            .map(|volume| volume.name.clone())
            .collect()
    }
}

/// What one pass was asked to do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Request {
    /// Volumes the caller is using right now: never deleted.
    ///
    /// A pass adds what the provider itself has published to this, so an
    /// automatic pass can leave it empty and still never strand its engine.
    pub live: BTreeSet<String>,
    /// Volumes the caller knows a live sandbox mounts.
    ///
    /// Best effort, and empty for the automatic passes: AgentENV has no read
    /// API that reports which sandbox reserves a volume, so the server's own
    /// refusal (`HTTP 409: volume is reserved by sandbox …`) is what actually
    /// keeps a mounted volume alive — a deletion it refuses is reported as in
    /// use, not as a failure.
    pub mounted: BTreeSet<String>,
    /// Report the plan and delete nothing.
    pub dry_run: bool,
}

impl Request {
    /// A pass the caller names nothing for: it still keeps what the provider
    /// itself published, and deletes what the policy allows.
    pub fn new() -> Self {
        Request::default()
    }

    /// Add volumes the pass must keep.
    pub fn with_live<I: IntoIterator<Item = String>>(mut self, live: I) -> Self {
        self.live.extend(live);
        self
    }

    /// Add volumes the caller knows are mounted.
    pub fn with_mounted<I: IntoIterator<Item = String>>(mut self, mounted: I) -> Self {
        self.mounted.extend(mounted);
        self
    }

    /// Report the plan without deleting anything.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }
}

/// What a pass decided and did.
#[derive(Debug)]
pub struct Outcome {
    /// The verdict for every volume AgentENV listed.
    pub plan: GcPlan,
    /// What the deletion did, volume by volume. Empty for a dry run.
    pub report: PruneReport,
    /// Whether the pass deleted nothing on purpose.
    pub dry_run: bool,
}

impl Outcome {
    /// The names a pass deleted.
    pub fn deleted(&self) -> &[String] {
        &self.report.deleted
    }

    /// The outcome as text: the plan, then what happened to each planned
    /// deletion. A volume a sandbox holds is reported as in use — a dry run
    /// cannot know which those are (see [`Request::mounted`]).
    pub fn render(&self, now: u64) -> String {
        let mut out = self.plan.render(now);
        if self.dry_run {
            return out;
        }
        for name in &self.report.deleted {
            out.push_str(&format!("deleted {name}\n"));
        }
        for name in &self.report.in_use {
            out.push_str(&format!(
                "in use {name} (a sandbox mounts it; left in place)\n"
            ));
        }
        for (name, error) in &self.report.failed {
            out.push_str(&format!("failed {name}: {error}\n"));
        }
        out
    }
}

/// The planner's view of the volumes AgentENV lists.
///
/// A creation time comes from the index for a volume this host populated, and
/// from the staging registry for a staging volume a populate wrote. A volume
/// neither recorded — populated before either existed, or by another host —
/// keeps an unknown age, and an unknown age keeps the volume.
fn facts_of(
    volumes: &[AgentEnvVolume],
    index: &VolumeIndex,
    staged: &StagingRegistry,
) -> Vec<VolumeFacts> {
    volumes
        .iter()
        .map(|volume| VolumeFacts {
            name: volume.name.clone(),
            size_mb: volume.size_mb,
            status: volume.status.clone(),
            created: index
                .get(&volume.name)
                .map(|entry| entry.created)
                .filter(|created| *created > 0)
                .or_else(|| {
                    staged
                        .get(&volume.name)
                        .map(|entry| entry.created)
                        .filter(|created| *created > 0)
                }),
        })
        .collect()
}

/// Plan a pass for `provider` and, unless it is a dry run, delete what the plan
/// says is superseded.
///
/// Safe while an engine runs: the plan keeps every volume the caller names, the
/// provider's own published volumes, everything a sandbox mounts (the server
/// refuses those deletes), everything younger than the grace period and any
/// staging volume this process is writing. It is not safe to interrupt a
/// populate: a pass keeps the staging volume of a populate this process runs and
/// the grace period covers one another process runs, and a volume deleted
/// between a populate's create and its first mount fails that populate.
pub async fn run_pass(provider: &AgentEnvProvider, request: Request) -> Result<Outcome, VmError> {
    let state = provider.gc_state();
    // One pass at a time in this process.
    let _pass = state.pass.lock().await;
    let now = unix_now();
    state.last_pass.store(now, Ordering::Relaxed);

    let index = provider.volume_index().await;
    let staged = state.staged();
    let in_flight = state.in_flight();
    let volumes = provider.list_volumes().await?;
    state.forget_missing(&volumes, &in_flight, now);
    // Whatever the caller named, a pass always keeps what this provider
    // published: it must never strand the engine it runs in. The read is after
    // the listing, so a volume published while the listing ran is in it too.
    let mut live = request.live.clone();
    live.extend(provider.published_volume_names().await);
    let facts = facts_of(&volumes, &index, &staged);
    let plan = plan_gc(
        &facts,
        &live,
        &request.mounted,
        &in_flight,
        &state.config.policy(),
        now,
    );

    if request.dry_run {
        tracing::debug!(
            volumes = plan.verdicts().len(),
            deleting = plan.deleting(),
            "AgentENV volume collection dry run"
        );
        return Ok(Outcome {
            plan,
            report: PruneReport::default(),
            dry_run: true,
        });
    }

    // The plan is the only authority on what this pass may touch: another engine
    // can publish a volume between the listing and the deletion, and deleting
    // what a keep set does not name would reclaim a volume no policy classified.
    let report = delete_planned(provider, &plan).await?;
    log_deletions(&plan, &report);
    state.forget_staged(&report.deleted);
    Ok(Outcome {
        plan,
        report,
        dry_run: false,
    })
}

/// Delete exactly the volumes `plan` decided to delete.
///
/// Driven by the plan rather than by a keep set: another engine can publish a
/// volume between the listing and the deletion, and deleting what a keep set
/// does not name would reclaim a volume no policy classified.
///
/// The report's `kept` is the plan's keep set, since that is the decision the
/// plan made; a mounted volume the server refuses comes back as `in use`.
async fn delete_planned(
    provider: &AgentEnvProvider,
    plan: &GcPlan,
) -> Result<PruneReport, VmError> {
    let mut report = provider.delete_volumes(&plan.delete_names()).await?;
    report.kept = plan.keep_names();
    Ok(report)
}

/// Log what a pass deleted, and what it could not.
///
/// Always at info: a deletion is the one irreversible thing a pass does, and
/// another engine's idle volume is a reason to be able to see exactly what went.
fn log_deletions(plan: &GcPlan, report: &PruneReport) {
    for verdict in plan.delete() {
        if report.deleted.contains(&verdict.name) {
            tracing::info!(
                volume = %verdict.name,
                size_mb = verdict.size_mb,
                kind = verdict.kind,
                reason = verdict.disposition.reason(),
                status = %verdict.status,
                created = ?verdict.created,
                "deleted a collected AgentENV volume"
            );
        }
    }
    for name in &report.in_use {
        tracing::debug!(
            volume = %name,
            "an AgentENV volume is mounted by a sandbox; left in place"
        );
    }
    for (name, error) in &report.failed {
        tracing::warn!(volume = %name, %error, "an AgentENV volume could not be deleted");
    }
}

/// The runtime to run a background pass on, when there is one.
fn runtime_handle() -> Option<tokio::runtime::Handle> {
    tokio::runtime::Handle::try_current().ok()
}

/// Start the periodic pass for `provider`, once per process.
///
/// Called wherever an engine first needs a volume, so an engine that never
/// publishes one (its volume is already there, published by an earlier run)
/// still collects. A provider with collection off, or an interval of zero,
/// starts nothing.
pub fn ensure_scheduled(provider: &AgentEnvProvider) {
    let state = provider.gc_state();
    if !state.config.mode.is_auto() || state.config.interval.is_zero() {
        return;
    }
    if state.scheduled.swap(true, Ordering::SeqCst) {
        return;
    }
    let Some(handle) = runtime_handle() else {
        // No runtime: there is no engine to schedule for, and the trigger that
        // found this will run in the engine's own runtime.
        state.scheduled.store(false, Ordering::SeqCst);
        return;
    };
    let provider = provider.clone();
    let interval = state.config.interval;
    handle.spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            if let Err(error) = run_pass(&provider, Request::new()).await {
                tracing::warn!(%error, "the periodic AgentENV volume collection pass failed");
            }
        }
    });
}

/// Ask for a pass after the engine published a volume, and return at once.
///
/// Nothing about a runner start waits for collection: the pass runs on its own
/// task, and a start that lands between two passes sees at worst a volume that
/// is still there. Passes triggered this way are spaced by [`TRIGGER_GAP_SECS`];
/// the periodic pass and `preloop volumes gc` are not.
pub fn request_pass(provider: &AgentEnvProvider) {
    let state = provider.gc_state();
    if !state.config.mode.is_auto() {
        return;
    }
    let now = unix_now();
    let last = state.last_pass.load(Ordering::Relaxed);
    if last != 0 && now.saturating_sub(last) < TRIGGER_GAP_SECS {
        return;
    }
    let Some(handle) = runtime_handle() else {
        return;
    };
    state.last_pass.store(now, Ordering::Relaxed);
    let provider = provider.clone();
    handle.spawn(async move {
        if let Err(error) = run_pass(&provider, Request::new()).await {
            tracing::warn!(%error, "the AgentENV volume collection pass after a populate failed");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agentenv::IndexedVolume;

    const NOW: u64 = 1_700_000_000;
    const GRACE: u64 = DEFAULT_GRACE_SECS;
    const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const OTHER_DIGEST: &str = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";

    /// A ready volume of unknown provenance, created `age` seconds ago.
    fn facts(name: &str, size_mb: u64, age: Option<u64>) -> VolumeFacts {
        VolumeFacts {
            name: name.to_owned(),
            size_mb,
            status: READY_STATUS.to_owned(),
            created: age.map(|seconds| NOW - seconds),
        }
    }

    fn policy() -> Policy {
        Policy::runner_bundle(Duration::from_secs(GRACE), DEFAULT_KEEP_PREVIOUS)
    }

    fn published(digest: &str) -> String {
        volume::volume_name(digest)
    }

    fn staging(digest: &str, attempt: &str) -> String {
        volume::staging_name(digest, attempt)
    }

    fn names<'a>(verdicts: impl Iterator<Item = &'a GcVerdict>) -> Vec<String> {
        verdicts.map(|verdict| verdict.name.clone()).collect()
    }

    fn disposition(plan: &GcPlan, name: &str) -> Disposition {
        plan.verdicts()
            .iter()
            .find(|verdict| verdict.name == name)
            .unwrap_or_else(|| panic!("no verdict for {name}"))
            .disposition
    }

    #[test]
    fn no_volumes_plans_nothing() {
        let plan = plan_gc(
            &[],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert!(plan.is_noop());
        assert_eq!(plan.verdicts().len(), 0);
        assert_eq!(plan.keep_names(), Vec::<String>::new());
        assert_eq!(plan.render(NOW), "no runner volumes\n");
    }

    #[test]
    fn the_live_volume_is_kept_however_old_it_is() {
        let live: BTreeSet<String> = [published(DIGEST)].into_iter().collect();
        let plan = plan_gc(
            &[facts(&published(DIGEST), 1024, Some(30 * GRACE))],
            &live,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &published(DIGEST)),
            Disposition::Keep(KeepReason::Live)
        );
        assert!(plan.is_noop());
        assert_eq!(plan.keep_names(), vec![published(DIGEST)]);
    }

    #[test]
    fn a_mounted_volume_is_kept() {
        let mounted: BTreeSet<String> = [published(DIGEST)].into_iter().collect();
        let plan = plan_gc(
            &[facts(&published(DIGEST), 1024, Some(30 * GRACE))],
            &BTreeSet::new(),
            &mounted,
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &published(DIGEST)),
            Disposition::Keep(KeepReason::Mounted)
        );
        assert!(plan.is_noop());
    }

    #[test]
    fn a_volume_inside_the_grace_period_is_kept() {
        for age in [0, 1, GRACE - 1] {
            let plan = plan_gc(
                &[facts(&published(DIGEST), 1024, Some(age))],
                &BTreeSet::new(),
                &BTreeSet::new(),
                &BTreeSet::new(),
                &policy(),
                NOW,
            );
            assert_eq!(
                disposition(&plan, &published(DIGEST)),
                Disposition::Keep(KeepReason::Young),
                "a volume {age} seconds old must be young"
            );
        }
    }

    #[test]
    fn the_grace_period_ends_inclusively() {
        // Rollback depth zero, so the age rule is what this asserts and not the
        // rollback rule: at exactly the grace period a volume is expired.
        let plan = plan_gc(
            &[facts(&published(DIGEST), 1024, Some(GRACE))],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &Policy::runner_bundle(Duration::from_secs(GRACE), 0),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &published(DIGEST)),
            Disposition::Delete(DeleteReason::Superseded)
        );
    }

    #[test]
    fn a_volume_with_no_recorded_age_is_kept() {
        let plan = plan_gc(
            &[facts(&published(DIGEST), 1024, None)],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &published(DIGEST)),
            Disposition::Keep(KeepReason::UnknownAge)
        );
        assert!(plan.is_noop());
    }

    #[test]
    fn a_superseded_volume_past_its_grace_period_is_deleted() {
        // The default rollback depth is one, so a pass deletes the older of two
        // superseded volumes and keeps the one a rollback would use.
        let live: BTreeSet<String> = [published(OTHER_DIGEST)].into_iter().collect();
        let plan = plan_gc(
            &[
                facts(&published(DIGEST), 1024, Some(2 * GRACE)),
                facts("preloop-runner-aaaaaaaaaaaa", 1024, Some(3 * GRACE)),
                facts(&published(OTHER_DIGEST), 1024, Some(GRACE)),
            ],
            &live,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &published(DIGEST)),
            Disposition::Keep(KeepReason::Previous)
        );
        assert_eq!(
            disposition(&plan, "preloop-runner-aaaaaaaaaaaa"),
            Disposition::Delete(DeleteReason::Superseded)
        );
        assert_eq!(plan.deleting(), 1);
        assert_eq!(plan.reclaimable_mib(), 1024);
        assert_eq!(
            plan.keep_names(),
            vec![published(DIGEST), published(OTHER_DIGEST)]
        );
    }

    #[test]
    fn the_newest_superseded_volume_is_kept_for_a_rollback() {
        let live: BTreeSet<String> = [published(DIGEST)].into_iter().collect();
        let plan = plan_gc(
            &[
                facts(&published(DIGEST), 4096, Some(0)),
                facts(&published(OTHER_DIGEST), 1024, Some(4 * GRACE)),
                facts("preloop-runner-aaaaaaaaaaaa", 512, Some(9 * GRACE)),
            ],
            &live,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &published(OTHER_DIGEST)),
            Disposition::Keep(KeepReason::Previous)
        );
        assert_eq!(
            disposition(&plan, "preloop-runner-aaaaaaaaaaaa"),
            Disposition::Delete(DeleteReason::Superseded)
        );
        assert_eq!(plan.deleting(), 1);
    }

    #[test]
    fn a_rollback_depth_of_zero_deletes_every_superseded_volume() {
        let other = published(OTHER_DIGEST);
        let plan = plan_gc(
            &[
                facts(&published(DIGEST), 4096, Some(GRACE)),
                facts(&other, 1024, Some(4 * GRACE)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &Policy::runner_bundle(Duration::from_secs(GRACE), 0),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &other),
            Disposition::Delete(DeleteReason::Superseded)
        );
        assert_eq!(
            disposition(&plan, &published(DIGEST)),
            Disposition::Delete(DeleteReason::Superseded)
        );
    }

    #[test]
    fn a_rollback_depth_above_one_keeps_that_many_newest() {
        let oldest = published("1111111111111111111111111111111111111111111111111111111111111111");
        let middle = "preloop-runner-222222222222";
        let newest = "preloop-runner-333333333333";
        let plan = plan_gc(
            &[
                facts(&oldest, 1, Some(9 * GRACE)),
                facts(middle, 2, Some(5 * GRACE)),
                facts(newest, 3, Some(2 * GRACE)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &Policy::runner_bundle(Duration::from_secs(GRACE), 2),
            NOW,
        );
        assert_eq!(
            disposition(&plan, newest),
            Disposition::Keep(KeepReason::Previous)
        );
        assert_eq!(
            disposition(&plan, middle),
            Disposition::Keep(KeepReason::Previous)
        );
        assert_eq!(
            disposition(&plan, &oldest),
            Disposition::Delete(DeleteReason::Superseded)
        );
    }

    #[test]
    fn the_rollback_rule_is_per_kind() {
        // A cache-shaped kind with its own depth: its volumes do not compete
        // with the runner bundles for the rollback depth.
        #[derive(Debug)]
        struct Cache {
            retention: KindRetention,
        }
        impl VolumeKind for Cache {
            fn kind(&self) -> &'static str {
                "docker-cache"
            }
            fn owns(&self, volume: &str) -> bool {
                volume.starts_with("preloop-cache-")
            }
            fn is_staging(&self) -> bool {
                false
            }
            fn retention(&self) -> KindRetention {
                self.retention
            }
        }

        let policy = policy().with_kind(Box::new(Cache {
            retention: KindRetention {
                grace: Duration::from_secs(GRACE),
                keep_previous: 2,
            },
        }));
        let plan = plan_gc(
            &[
                facts("preloop-cache-aaaaaaaa", 10, Some(2 * GRACE)),
                facts("preloop-cache-bbbbbbbb", 10, Some(3 * GRACE)),
                facts("preloop-cache-cccccccc", 10, Some(4 * GRACE)),
                facts(&published(DIGEST), 10, Some(5 * GRACE)),
                facts(&published(OTHER_DIGEST), 10, Some(6 * GRACE)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy,
            NOW,
        );
        // Two newest cache volumes survive, the third does not.
        assert_eq!(
            disposition(&plan, "preloop-cache-aaaaaaaa"),
            Disposition::Keep(KeepReason::Previous)
        );
        assert_eq!(
            disposition(&plan, "preloop-cache-bbbbbbbb"),
            Disposition::Keep(KeepReason::Previous)
        );
        assert_eq!(
            disposition(&plan, "preloop-cache-cccccccc"),
            Disposition::Delete(DeleteReason::Superseded)
        );
        // The runner bundle keeps its own one.
        assert_eq!(
            disposition(&plan, &published(DIGEST)),
            Disposition::Keep(KeepReason::Previous)
        );
        assert_eq!(
            disposition(&plan, &published(OTHER_DIGEST)),
            Disposition::Delete(DeleteReason::Superseded)
        );
    }

    #[test]
    fn a_volume_no_kind_owns_is_kept() {
        let plan = plan_gc(
            &[
                facts("preloop-cache-aaaaaaaa", 10, Some(9 * GRACE)),
                facts("preloop-runner-nonsense", 10, Some(9 * GRACE)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        for name in ["preloop-cache-aaaaaaaa", "preloop-runner-nonsense"] {
            assert_eq!(
                disposition(&plan, name),
                Disposition::Keep(KeepReason::Unmanaged)
            );
            assert_eq!(
                plan.verdicts()
                    .iter()
                    .find(|verdict| verdict.name == name)
                    .map(|verdict| verdict.kind),
                Some(UNKNOWN_KIND)
            );
        }
        assert!(plan.is_noop());
    }

    #[test]
    fn an_abandoned_staging_volume_is_deleted_but_a_fresh_one_is_not() {
        let stale = staging(DIGEST, "aaaaaaaa");
        let fresh = staging(OTHER_DIGEST, "bbbbbbbb");
        let plan = plan_gc(
            &[
                facts(&stale, 1024, Some(2 * GRACE)),
                facts(&fresh, 1024, Some(60)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &stale),
            Disposition::Delete(DeleteReason::Abandoned)
        );
        assert_eq!(
            disposition(&plan, &fresh),
            Disposition::Keep(KeepReason::Young)
        );
        assert!(plan.keep_names().contains(&fresh));
        assert!(!plan.keep_names().contains(&stale));
        let row = plan
            .verdicts()
            .iter()
            .find(|verdict| verdict.name == stale)
            .expect("a verdict for the stale stage");
        assert_eq!(row.kind, "runner-bundle (staging)");
    }

    #[test]
    fn a_staging_volume_with_no_recorded_age_is_kept() {
        let stale = staging(DIGEST, "aaaaaaaa");
        let plan = plan_gc(
            &[facts(&stale, 1024, None)],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &stale),
            Disposition::Keep(KeepReason::UnknownAge)
        );
    }

    #[test]
    fn a_populate_in_flight_keeps_its_stage_and_its_digest() {
        let in_flight_stage = staging(DIGEST, "aaaaaaaa");
        let same_digest = published(DIGEST);
        let in_flight: BTreeSet<String> = [in_flight_stage.clone()].into_iter().collect();
        let plan = plan_gc(
            &[
                facts(&in_flight_stage, 1024, Some(9 * GRACE)),
                facts(&same_digest, 1024, Some(9 * GRACE)),
                facts(&staging(OTHER_DIGEST, "cccccccc"), 1024, Some(9 * GRACE)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &in_flight,
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &in_flight_stage),
            Disposition::Keep(KeepReason::Populating)
        );
        assert_eq!(
            disposition(&plan, &same_digest),
            Disposition::Keep(KeepReason::Populating)
        );
        assert_eq!(
            disposition(&plan, &staging(OTHER_DIGEST, "cccccccc")),
            Disposition::Delete(DeleteReason::Abandoned)
        );
    }

    #[test]
    fn a_volume_the_server_cannot_mount_is_deleted_once_it_is_old() {
        let failed = published(DIGEST);
        let mut unusable = facts(&failed, 1024, Some(2 * GRACE));
        unusable.status = "failed".to_owned();
        let mut young = facts("preloop-runner-aaaaaaaaaaaa", 1024, Some(GRACE - 1));
        young.status = "failed".to_owned();
        let mut unknown = facts(&published(OTHER_DIGEST), 1024, None);
        unknown.status = "uploading".to_owned();
        let plan = plan_gc(
            &[unusable, young, unknown],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &failed),
            Disposition::Delete(DeleteReason::Unusable)
        );
        assert_eq!(
            disposition(&plan, "preloop-runner-aaaaaaaaaaaa"),
            Disposition::Keep(KeepReason::Young)
        );
        assert_eq!(
            disposition(&plan, &published(OTHER_DIGEST)),
            Disposition::Keep(KeepReason::UnknownAge)
        );
    }

    #[test]
    fn a_live_volume_is_kept_even_when_the_server_calls_it_unusable() {
        let name = published(DIGEST);
        let mut volume = facts(&name, 1024, Some(9 * GRACE));
        volume.status = "failed".to_owned();
        let live: BTreeSet<String> = [name.clone()].into_iter().collect();
        let plan = plan_gc(
            &[volume],
            &live,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &name),
            Disposition::Keep(KeepReason::Live)
        );
    }

    #[test]
    fn every_listed_volume_is_either_kept_or_deleted_exactly_once() {
        let volumes = vec![
            facts(&published(DIGEST), 1, Some(0)),
            facts(&published(OTHER_DIGEST), 2, Some(9 * GRACE)),
            facts(&staging(DIGEST, "aaaaaaaa"), 3, Some(9 * GRACE)),
            facts(&staging(OTHER_DIGEST, "bbbbbbbb"), 4, None),
            facts("docker-cache-x", 5, Some(9 * GRACE)),
        ];
        let live: BTreeSet<String> = [published(DIGEST)].into_iter().collect();
        let mounted: BTreeSet<String> = [published(OTHER_DIGEST)].into_iter().collect();
        let plan = plan_gc(&volumes, &live, &mounted, &BTreeSet::new(), &policy(), NOW);
        let mut kept = plan.keep_names();
        let mut deleted = names(plan.delete());
        assert_eq!(kept.len() + deleted.len(), volumes.len());
        kept.append(&mut deleted);
        kept.sort();
        let mut listed: Vec<String> = volumes.iter().map(|volume| volume.name.clone()).collect();
        listed.sort();
        assert_eq!(kept, listed);
    }

    #[test]
    fn the_table_reports_every_row_with_a_reason() {
        let plan = plan_gc(
            &[
                facts(&published(DIGEST), 1024, Some(0)),
                facts(&published(OTHER_DIGEST), 2048, Some(9 * GRACE)),
                facts(&staging(DIGEST, "aaaaaaaa"), 512, Some(9 * GRACE)),
                facts("docker-cache-x", 8, Some(9 * GRACE)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        let table = plan.render(NOW);
        for name in [
            published(DIGEST),
            published(OTHER_DIGEST),
            staging(DIGEST, "aaaaaaaa"),
            "docker-cache-x".to_owned(),
        ] {
            assert!(table.contains(&name), "the table omits {name}:\n{table}");
        }
        assert!(table.contains("young"), "{table}");
        assert!(table.contains("previous"), "{table}");
        assert!(table.contains("abandoned"), "{table}");
        assert!(table.contains("unmanaged"), "{table}");
        assert!(
            table.contains("1 to delete (512 MiB reclaimable)"),
            "{table}"
        );
        assert_eq!(table, plan.render(NOW));
    }

    #[test]
    fn a_volume_created_in_the_future_is_young() {
        let future = VolumeFacts {
            name: published(DIGEST),
            size_mb: 1024,
            status: READY_STATUS.to_owned(),
            created: Some(NOW + 3600),
        };
        let plan = plan_gc(
            &[future],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        assert_eq!(
            disposition(&plan, &published(DIGEST)),
            Disposition::Keep(KeepReason::Young)
        );
    }

    #[test]
    fn the_verdicts_are_sorted_by_name() {
        let plan = plan_gc(
            &[
                facts(&published(OTHER_DIGEST), 1, Some(0)),
                facts(&published(DIGEST), 2, Some(0)),
                facts(&staging(DIGEST, "aaaaaaaa"), 3, Some(0)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &policy(),
            NOW,
        );
        let listed: Vec<&str> = plan
            .verdicts()
            .iter()
            .map(|verdict| verdict.name.as_str())
            .collect();
        let mut sorted = listed.clone();
        sorted.sort_unstable();
        assert_eq!(listed, sorted);
    }

    #[test]
    fn the_naming_helpers_read_a_digest_out_of_both_shapes() {
        assert_eq!(digest_of(&published(DIGEST)), Some(&DIGEST[..12]));
        assert_eq!(digest_of(&staging(DIGEST, "aaaaaaaa")), None);
        assert_eq!(digest_of("docker-cache-x"), None);
        assert_eq!(
            staging_digest(&staging(DIGEST, "aaaaaaaa")),
            Some(&DIGEST[..12])
        );
        assert_eq!(staging_digest(&published(DIGEST)), None);
        assert_eq!(staging_digest(STAGING_PREFIX), None);
        assert_eq!(
            staging_digest(&format!("{STAGING_PREFIX}0123456789ab-")),
            None
        );
    }

    #[test]
    fn the_mode_parser_accepts_what_the_docs_promise() {
        assert_eq!(Mode::parse("auto"), Some(Mode::Auto));
        assert_eq!(Mode::parse(" off "), Some(Mode::Off));
        assert_eq!(Mode::parse("AUTO"), Some(Mode::Auto));
        assert_eq!(Mode::parse("sometimes"), None);
        assert_eq!(Config::defaults().mode, Mode::Auto);
        assert_eq!(Config::defaults().keep_previous, DEFAULT_KEEP_PREVIOUS);
        assert_eq!(
            Config::defaults().grace,
            Duration::from_secs(DEFAULT_GRACE_SECS)
        );
        assert_eq!(
            Config::defaults().interval,
            Duration::from_secs(DEFAULT_INTERVAL_SECS)
        );
        assert!(!Config::off().mode.is_auto());
    }

    /// The staging registry a test writes to, and its state.
    fn state_with_registry(directory: &Path) -> (Arc<GcState>, PathBuf) {
        let path = directory.join(STAGING_FILE);
        (
            Arc::new(GcState::new(Config::defaults(), Some(path.clone()))),
            path,
        )
    }

    fn listed(name: &str, size_mb: u64, status: &str) -> AgentEnvVolume {
        AgentEnvVolume {
            volume_id: String::new(),
            name: name.to_owned(),
            mode: "ro".to_owned(),
            size_mb,
            status: status.to_owned(),
        }
    }

    fn indexed(entries: &[(&str, u64)]) -> VolumeIndex {
        let mut volumes = BTreeMap::new();
        for (name, created) in entries {
            let entry: IndexedVolume = serde_json::from_value(serde_json::json!({
                "created": created,
            }))
            .expect("an index entry");
            volumes.insert((*name).to_owned(), entry);
        }
        VolumeIndex { volumes }
    }

    #[test]
    fn a_staging_volume_is_recorded_until_its_populate_ends() {
        let directory = tempfile::tempdir().unwrap();
        let (state, path) = state_with_registry(directory.path());
        let staging = staging(DIGEST, "aaaaaaaa");

        let guard = state.begin_staging(&staging, DIGEST, Path::new("/opt/preloop/bundle"));
        let staged = state.staged();
        let record = staged.get(&staging).expect("the stage is recorded");
        assert_eq!(record.digest, DIGEST);
        assert_eq!(record.host, "/opt/preloop/bundle");
        assert_eq!(record.created, unix_now());
        assert_eq!(state.in_flight(), [staging.clone()].into_iter().collect());
        assert!(
            load_staging(&path).get(&staging).is_some(),
            "the record is persisted, so another process sees it"
        );

        drop(guard);
        assert!(state.staged().get(&staging).is_none());
        assert!(state.in_flight().is_empty());
        assert!(load_staging(&path).get(&staging).is_none());
    }

    #[test]
    fn a_record_another_process_wrote_survives_this_one() {
        let directory = tempfile::tempdir().unwrap();
        let (state, path) = state_with_registry(directory.path());
        let foreign = staging(OTHER_DIGEST, "bbbbbbbb");
        let mut registry = StagingRegistry::default();
        registry.staging.insert(
            foreign.clone(),
            StagedVolume {
                digest: OTHER_DIGEST.to_owned(),
                host: "/other/engine".to_owned(),
                created: NOW - 10,
            },
        );
        store_staging(&path, &registry);

        let ours = staging(DIGEST, "aaaaaaaa");
        let guard = state.begin_staging(&ours, DIGEST, Path::new("/opt/preloop/bundle"));
        drop(guard);

        let after = load_staging(&path);
        assert!(after.get(&ours).is_none());
        assert_eq!(
            after.get(&foreign).expect("the other engine's record"),
            &StagedVolume {
                digest: OTHER_DIGEST.to_owned(),
                host: "/other/engine".to_owned(),
                created: NOW - 10,
            }
        );
    }

    #[test]
    fn a_deleted_staging_volume_loses_its_record() {
        let directory = tempfile::tempdir().unwrap();
        let (state, path) = state_with_registry(directory.path());
        let staging = staging(DIGEST, "aaaaaaaa");
        let _guard = state.begin_staging(&staging, DIGEST, Path::new("/opt/preloop/bundle"));

        state.forget_staged(&[staging.clone(), published(DIGEST)]);

        assert!(state.staged().get(&staging).is_none());
        assert!(state.in_flight().is_empty());
        assert!(load_staging(&path).get(&staging).is_none());
    }

    #[test]
    fn the_in_process_record_wins_over_a_stale_file() {
        let directory = tempfile::tempdir().unwrap();
        let (state, path) = state_with_registry(directory.path());
        let staging = staging(DIGEST, "aaaaaaaa");
        let mut registry = StagingRegistry::default();
        registry.staging.insert(
            staging.clone(),
            StagedVolume {
                digest: DIGEST.to_owned(),
                host: "/old/attempt".to_owned(),
                created: NOW - 9 * GRACE,
            },
        );
        store_staging(&path, &registry);

        let _guard = state.begin_staging(&staging, DIGEST, Path::new("/opt/preloop/bundle"));

        let record = state.staged().get(&staging).cloned().expect("a record");
        assert_eq!(record.created, unix_now());
        assert_eq!(record.host, "/opt/preloop/bundle");
    }

    #[test]
    fn facts_take_an_age_from_the_index_and_from_the_staging_registry() {
        let published_volume = published(DIGEST);
        let staging_volume = staging(OTHER_DIGEST, "aaaaaaaa");
        let unrecorded = "preloop-runner-999999999999";
        let mut registry = StagingRegistry::default();
        registry.staging.insert(
            staging_volume.clone(),
            StagedVolume {
                digest: OTHER_DIGEST.to_owned(),
                host: "/opt/preloop/bundle".to_owned(),
                created: NOW - 60,
            },
        );

        let facts = facts_of(
            &[
                listed(&published_volume, 1024, READY_STATUS),
                listed(&staging_volume, 512, "exclusive"),
                listed(unrecorded, 2048, READY_STATUS),
            ],
            &indexed(&[
                (&published_volume, NOW - 7200),
                (staging_volume.as_str(), NOW - 9),
            ]),
            &registry,
        );

        assert_eq!(facts[0].created, Some(NOW - 7200));
        assert_eq!(facts[1].created, Some(NOW - 9));
        assert_eq!(facts[2].created, None);
        assert_eq!(facts[0].size_mb, 1024);
        assert_eq!(facts[1].status, "exclusive");
    }

    #[test]
    fn an_index_entry_without_a_time_is_an_unknown_age() {
        let volume = published(DIGEST);
        let facts = facts_of(
            &[listed(&volume, 1024, READY_STATUS)],
            &indexed(&[(&volume, 0)]),
            &StagingRegistry::default(),
        );
        assert_eq!(facts[0].created, None);
    }

    #[test]
    fn a_request_accumulates_what_it_must_keep() {
        let request = Request::new()
            .with_live([published(DIGEST)])
            .with_mounted([published(OTHER_DIGEST)])
            .dry_run(true);
        assert!(request.dry_run);
        assert_eq!(request.live, [published(DIGEST)].into_iter().collect());
        assert_eq!(
            request.mounted,
            [published(OTHER_DIGEST)].into_iter().collect()
        );
        assert!(Request::new().live.is_empty());
        assert!(!Request::new().dry_run);
    }

    #[test]
    fn a_dry_run_renders_the_plan_and_reports_no_deletion() {
        let plan = plan_gc(
            &[facts(&published(DIGEST), 1024, Some(9 * GRACE))],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &Policy::runner_bundle(Duration::from_secs(GRACE), 0),
            NOW,
        );
        let outcome = Outcome {
            plan,
            report: PruneReport::default(),
            dry_run: true,
        };
        let rendered = outcome.render(NOW);
        assert!(rendered.contains(&published(DIGEST)));
        assert!(rendered.contains("superseded"));
        assert!(!rendered.contains("deleted "));
        assert!(outcome.deleted().is_empty());
    }

    #[test]
    fn an_outcome_reports_what_a_deletion_did() {
        let deleted = published(DIGEST);
        let mounted = published(OTHER_DIGEST);
        let failed = "preloop-runner-999999999999";
        let plan = plan_gc(
            &[
                facts(&deleted, 1024, Some(9 * GRACE)),
                facts(&mounted, 1024, Some(9 * GRACE)),
                facts(failed, 1024, Some(9 * GRACE)),
            ],
            &BTreeSet::new(),
            &BTreeSet::new(),
            &BTreeSet::new(),
            &Policy::runner_bundle(Duration::from_secs(GRACE), 0),
            NOW,
        );
        let outcome = Outcome {
            plan,
            report: PruneReport {
                deleted: vec![deleted.clone()],
                kept: Vec::new(),
                in_use: vec![mounted.clone()],
                candidates: Vec::new(),
                failed: vec![(failed.to_owned(), "the server said no".to_owned())],
            },
            dry_run: false,
        };
        assert_eq!(outcome.deleted(), std::slice::from_ref(&deleted));
        let rendered = outcome.render(NOW);
        assert!(
            rendered.contains(&format!("deleted {deleted}")),
            "{rendered}"
        );
        assert!(
            rendered.contains(&format!(
                "in use {mounted} (a sandbox mounts it; left in place)"
            )),
            "{rendered}"
        );
        assert!(
            rendered.contains(&format!("failed {failed}: the server said no")),
            "{rendered}"
        );
    }

    #[test]
    fn a_record_for_a_stage_that_is_gone_is_dropped_once_it_is_old() {
        let directory = tempfile::tempdir().unwrap();
        let (state, path) = state_with_registry(directory.path());
        let gone = staging(DIGEST, "aaaaaaaa");
        let listed_stage = staging(OTHER_DIGEST, "bbbbbbbb");
        let in_flight = staging(DIGEST, "cccccccc");
        let young = staging(OTHER_DIGEST, "dddddddd");
        plant_staging(
            &path,
            &[
                (&gone, DIGEST, NOW - 2 * GRACE),
                (&listed_stage, OTHER_DIGEST, NOW - 2 * GRACE),
                (&in_flight, DIGEST, NOW - 2 * GRACE),
                (&young, OTHER_DIGEST, NOW - 60),
            ],
        );
        let flight: BTreeSet<String> = [in_flight.clone()].into_iter().collect();

        state.forget_missing(&[listed(&listed_stage, 0, READY_STATUS)], &flight, NOW);

        let records = load_staging(&path);
        assert!(records.get(&gone).is_none(), "a stage the server dropped");
        assert!(records.get(&listed_stage).is_some(), "a stage still listed");
        assert!(records.get(&in_flight).is_some(), "a stage in flight");
        assert!(records.get(&young).is_some(), "a record too young to judge");
    }

    /// Write staging records to `path`, as a populate that died would leave them.
    fn plant_staging(path: &Path, staged: &[(&String, &str, u64)]) {
        let registry = StagingRegistry {
            staging: staged
                .iter()
                .map(|(name, digest, created)| {
                    (
                        (*name).clone(),
                        StagedVolume {
                            digest: (*digest).to_owned(),
                            host: "/opt/preloop/bundle".to_owned(),
                            created: *created,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        };
        store_staging(path, &registry);
    }

    #[test]
    fn a_collection_state_without_a_home_keeps_its_records_in_memory() {
        let state = Arc::new(GcState::new(Config::defaults(), None));
        let staging = staging(DIGEST, "aaaaaaaa");
        let guard = state.begin_staging(&staging, DIGEST, Path::new("/opt/preloop/bundle"));
        assert!(state.staged().get(&staging).is_some());
        drop(guard);
        assert!(state.staged().get(&staging).is_none());
    }

    #[test]
    fn the_disabled_state_collects_nothing() {
        let state = GcState::disabled();
        assert_eq!(state.config().mode, Mode::Off);
        assert!(state.staged().staging.is_empty());
        assert!(state.in_flight().is_empty());
    }
}
