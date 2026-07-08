//! `DebuggerConfig` and `DebuggerTunnelInfo` — per-job debugger configuration.
//!
//! 1:1 port of `src/Runner.Worker/Dap/DebuggerConfig.cs` and
//! `src/Sdk/DTPipelines/Pipelines/DebuggerTunnelInfo.cs`.
//!
//! These types are populated by the runner's `ExecutionContext` from
//! the corresponding fields on `AgentJobRequestMessage`:
//! - `message.enableDebugger` → [`DebuggerConfig::enabled`]
//! - `message.debuggerTunnel` → [`DebuggerConfig::tunnel`]
//! - `message.debuggerWelcomeMessage` + `actions_runner_override_debugger_welcome_message`
//!   feature flag → [`DebuggerConfig::override_welcome_message`] and
//!   [`DebuggerConfig::welcome_message`]

use serde::{Deserialize, Serialize};

/// Dev Tunnel details for remote debugging.
///
/// Mirrors `src/Sdk/DTPipelines/Pipelines/DebuggerTunnelInfo.cs` in
/// `actions/runner` v2.335.1. Required when [`DebuggerConfig::enabled`]
/// is `true`. The runner uses `host_token` to authenticate to the
/// Microsoft Dev Tunnels relay at `<cluster>-data.rel.tunnels.api.visualstudio.com`,
/// presents `tunnel_id` as the tunnel it wants to host, and binds
/// the local DAP server to [`crate::DAP_TUNNEL_PORT`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DebuggerTunnelInfo {
    /// Opaque tunnel identifier (e.g. `"neat-ocean-5b7j1lw"`).
    /// This is the URL segment the host and clients use to find each
    /// other in the multi-tenant relay.
    #[serde(rename = "tunnelId", default)]
    pub tunnel_id: String,

    /// Azure region for the relay (e.g. `"use2"`). The runner
    /// connects to `<cluster>-data.rel.tunnels.api.visualstudio.com`.
    #[serde(rename = "clusterId", default)]
    pub cluster_id: String,

    /// Bearer credential presented by the runner when opening the
    /// outbound WebSocket to the relay.
    #[serde(rename = "hostToken", default)]
    pub host_token: String,

    /// Local TCP port the DAP server must bind to. The upstream
    /// backend hard-codes this to `4711` and the runner rejects
    /// anything else (see `DapDebugger.cs::StartAsyncUsesPortFromTunnelConfig`).
    #[serde(rename = "port", default)]
    pub port: u16,
}

impl DebuggerTunnelInfo {
    /// Returns `true` iff every field is populated. A half-populated
    /// tunnel info is a server-side error and the runner logs and
    /// skips debugger startup rather than guessing.
    pub fn is_valid(&self) -> bool {
        !self.tunnel_id.is_empty()
            && !self.cluster_id.is_empty()
            && !self.host_token.is_empty()
            && self.port != 0
    }

    /// Validate against the expected port. Mirrors
    /// `DebuggerConfig.cs::IsValid` which fails the job if the backend
    /// did not return the hard-coded port.
    pub fn is_valid_for_port(&self, expected_port: u16) -> bool {
        self.is_valid() && self.port == expected_port
    }
}

/// Consolidated runtime configuration for the job debugger.
///
/// Populated once from the acquire response and owned by
/// `GlobalContext.Debugger`. Mirrors `DebuggerConfig.cs` in
/// `actions/runner`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct DebuggerConfig {
    /// Whether the debugger is enabled for this job.
    #[serde(rename = "enabled", default)]
    pub enabled: bool,

    /// Dev Tunnel details. Required when `enabled` is `true`.
    #[serde(rename = "tunnel", default, skip_serializing_if = "Option::is_none")]
    pub tunnel: Option<DebuggerTunnelInfo>,

    /// When `true`, the runner overrides the default welcome
    /// message with [`Self::welcome_message`]. A null/empty
    /// `welcome_message` with `override_welcome_message=true` results
    /// in no welcome message at all.
    #[serde(
        rename = "overrideWelcomeMessage",
        default,
        skip_serializing_if = "is_false"
    )]
    pub override_welcome_message: bool,

    /// Replacement welcome text. Mirrors
    /// `AgentJobRequestMessage::DebuggerWelcomeMessage`.
    #[serde(
        rename = "welcomeMessage",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub welcome_message: Option<String>,
}

impl DebuggerConfig {
    /// Build a config from the wire-level acquire-response fields.
    /// Mirrors `ExecutionContext.cs`'s construction of
    /// `new Dap.DebuggerConfig(message.EnableDebugger, message.DebuggerTunnel, ...)`.
    pub fn new(
        enabled: bool,
        tunnel: Option<DebuggerTunnelInfo>,
        override_welcome_message: bool,
        welcome_message: Option<String>,
    ) -> Self {
        Self {
            enabled,
            tunnel,
            override_welcome_message,
            welcome_message,
        }
    }

    /// Returns `true` iff the config is enabled *and* the tunnel
    /// info is fully populated. The runner refuses to start the
    /// debugger otherwise.
    pub fn is_runnable(&self) -> bool {
        self.enabled && self.tunnel.as_ref().is_some_and(|t| t.is_valid())
    }
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_tunnel() -> DebuggerTunnelInfo {
        DebuggerTunnelInfo {
            tunnel_id: "neat-ocean-5b7j1lw".into(),
            cluster_id: "use2".into(),
            host_token: "secret".into(),
            port: 4711,
        }
    }

    #[test]
    fn tunnel_is_valid_only_when_all_fields_set() {
        let mut t = DebuggerTunnelInfo::default();
        assert!(!t.is_valid());
        t.tunnel_id = "x".into();
        assert!(!t.is_valid());
        t.cluster_id = "use2".into();
        assert!(!t.is_valid());
        t.host_token = "tok".into();
        assert!(!t.is_valid());
        t.port = 4711;
        assert!(t.is_valid());
    }

    #[test]
    fn tunnel_validates_against_expected_port() {
        let mut t = sample_tunnel();
        assert!(t.is_valid_for_port(4711));
        t.port = 1234;
        assert!(!t.is_valid_for_port(4711));
    }

    #[test]
    fn config_is_runnable_requires_enabled_and_valid_tunnel() {
        let mut cfg = DebuggerConfig::default();
        assert!(!cfg.is_runnable());

        cfg.enabled = true;
        assert!(!cfg.is_runnable());

        cfg.tunnel = Some(sample_tunnel());
        assert!(cfg.is_runnable());
    }

    #[test]
    fn config_json_round_trip() {
        let cfg = DebuggerConfig::new(
            true,
            Some(sample_tunnel()),
            true,
            Some("hello debugger".into()),
        );
        let s = serde_json::to_string(&cfg).unwrap();
        let back: DebuggerConfig = serde_json::from_str(&s).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn override_welcome_message_omitted_when_false() {
        let cfg = DebuggerConfig::new(true, Some(sample_tunnel()), false, None);
        let v: serde_json::Value = serde_json::to_value(&cfg).unwrap();
        assert!(v.get("overrideWelcomeMessage").is_none());
    }
}
