//! Re-export VM telemetry types from `preloop-observability` so `preloop-vm`
//! and `preloop-orchestrator` share the same registry without a circular dep.

pub use preloop_observability::vm_telemetry::{
    HostSample, VmRuntimeInfo, VmTelemetryRegistry, build_fleet_snapshot, sample_host,
};
