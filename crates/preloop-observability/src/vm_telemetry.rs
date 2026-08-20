use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;

use crate::status::{VmConfigured, VmCount, VmFleetSnapshot, VmHostUsage, VmSource, VmTopConsumer};

#[derive(Debug, Clone)]
pub struct VmRuntimeInfo {
    pub name: String,
    pub role: String,
    pub activity: String,
    pub pid: Option<u32>,
    pub start_time: Option<u64>,
    pub cpus: u16,
    pub memory_mib: u32,
    pub storage_gb: u32,
    pub overlay_gb: Option<u32>,
    pub data_dir: Option<PathBuf>,
    pub created_at: Option<SystemTime>,
}

#[derive(Debug, Default)]
pub struct VmTelemetryRegistry {
    inner: RwLock<HashMap<String, VmRuntimeInfo>>,
}

impl VmTelemetryRegistry {
    pub fn register(&self, info: VmRuntimeInfo) {
        self.inner.write().insert(info.name.clone(), info);
    }

    pub fn deregister(&self, name: &str) {
        self.inner.write().remove(name);
    }

    pub fn snapshot(&self) -> Vec<VmRuntimeInfo> {
        self.inner.read().values().cloned().collect()
    }
}

pub fn sample_host(_pid: Option<u32>, _data_dir: Option<&Path>) -> HostSample {
    HostSample::unavailable()
}

#[derive(Debug, Clone)]
pub struct HostSample {
    pub cpu_time_secs: Option<f64>,
    pub throttled_secs: Option<f64>,
    pub memory_bytes: Option<u64>,
    pub memory_limit_bytes: Option<u64>,
    pub pids_current: Option<u32>,
    pub sparse_allocated_bytes: Option<u64>,
    pub pid_valid: bool,
}

impl HostSample {
    pub fn unavailable() -> Self {
        Self {
            cpu_time_secs: None,
            throttled_secs: None,
            memory_bytes: None,
            memory_limit_bytes: None,
            pids_current: None,
            sparse_allocated_bytes: None,
            pid_valid: false,
        }
    }
}

pub fn build_fleet_snapshot(
    registry: &VmTelemetryRegistry,
    sample_age: Option<Duration>,
    capabilities: HashMap<String, bool>,
) -> VmFleetSnapshot {
    let infos = registry.snapshot();
    let runner = infos.iter().filter(|i| i.role == "runner").count() as u32;
    let golden = infos.iter().filter(|i| i.role == "golden").count() as u32;
    let vcpus: u32 = infos.iter().map(|i| u32::from(i.cpus)).sum();
    let memory_bytes: u64 = infos.iter().map(|i| u64::from(i.memory_mib) * 1024 * 1024).sum();
    let storage_bytes: u64 = infos.iter().map(|i| u64::from(i.storage_gb) * 1024 * 1024 * 1024).sum();
    let overlay_bytes: u64 = infos
        .iter()
        .filter_map(|i| i.overlay_gb.map(|v| u64::from(v) * 1024 * 1024 * 1024))
        .sum();

    let source = if capabilities.get("cpu").copied().unwrap_or(false) {
        VmSource::CgroupV2
    } else if capabilities.get("process").copied().unwrap_or(false) {
        VmSource::Process
    } else {
        VmSource::Unavailable
    };

    VmFleetSnapshot {
        source,
        sample_age_seconds: sample_age.map(|d| d.as_secs_f64()),
        capabilities,
        count: VmCount {
            runner,
            golden,
            unavailable: 0,
        },
        configured: VmConfigured {
            vcpus,
            memory_bytes,
            storage_bytes,
            overlay_bytes,
        },
        host_usage: VmHostUsage {
            cpu_cores: 0.0,
            memory_bytes: 0,
            sparse_disk_allocated_bytes: 0,
        },
        top_consumers: Vec::new(),
    }
}
