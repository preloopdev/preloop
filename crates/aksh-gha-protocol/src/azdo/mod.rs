//! Azure DevOps wire-format DTOs for the runner protocol.
//!
//! These types model the exact JSON shapes the official `actions/runner`
//! (`Runner.Listener`) sends and expects. Field names follow the C#
//! property casing conventions from `GitHub.DistributedTask.WebApi`.
//!
//! Source of truth:
//! - `actions/runner` (C# client side): `src/Runner.Common/Util/RunnerServer.cs`
//! - `runner.server` (C# server side): `src/Runner.Server/Controllers/MessageController.cs`
//! - `GitHub.DistributedTask.WebApi` NuGet package (shared DTOs)
#![allow(missing_docs)]

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

mod completion;
mod context_data;
mod job;
mod lifecycle;
mod messages;
mod resources;
mod timeline;
mod variables;

pub use completion::*;
pub use context_data::*;
pub use job::*;
pub(crate) use job::{find_expression_end, template_string_token};
pub use lifecycle::*;
pub use messages::message_type;
pub use messages::*;
pub use resources::*;
pub use timeline::*;
pub use variables::*;

#[cfg(test)]
#[path = "azdo_tests.rs"]
mod tests;
