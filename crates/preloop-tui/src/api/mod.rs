//! Lenient serde mirrors of the engine's JSON.
//!
//! Every struct is `#[serde(default)]` and ignores unknown fields, and every
//! status enum has an `Unknown` fallback. A newer or older engine therefore
//! degrades to blank cells instead of failing a whole view. Ids are kept as
//! strings so this module needs no `uuid` dependency.

pub mod feed;
pub mod ops;
pub mod run;
pub mod status;

pub use feed::{Annotation, FeedEvent, LiveLogChunk, StepStatus, parse_feed_line};
pub use ops::{
    AppHealth, AuditEntry, Breaker, DebugSession, DebugSessions, Delivery, FailedStep,
    PoolBinding, PoolBindings, RunAccepted, RunnerInfo, RunnersList, SecretEntry, SecretList,
    WebhookDeliveries, WebhookHealth, WebhookStats, Watchdog,
};
pub use run::{ExecStatus, JobDetail, PushState, RunRecord, StepRecord, Submission, sort_steps};
pub use status::{Condition, Exemplar, OperationalSnapshot, Overall};
