//! Behavioral conformance for sandbox [`Driver`] implementations.
//!
//! The suite runs against any driver, in process or behind the out-of-process
//! protocol, and checks the parts of the contract the service depends on:
//! capabilities are coherent, native names round-trip through inventory,
//! create/destroy are idempotent, exec streams output and reports the
//! documented exit codes, copy moves a tar, optional operations are either
//! declared and work or refuse with `failed_precondition`, and host reports
//! return.
//!
//! Every case creates its own sandbox from a minimal [`CreateWork`], so cases
//! do not share state and a failure cannot poison the next one.
//! [`ConformanceOptions::template`] supplies the image: the default is an
//! empty template (the driver applies its own default), while real-VM
//! conformance passes a digest-pinned artifact.

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use buffa::MessageField;
use tokio::sync::mpsc;

use crate::api::{
    CreateWork, ExecEvent, ExecRequest, NetworkMode, NetworkPolicy, Observed, Shape, Template,
    exec_event,
};
use crate::driver::{Driver, EXEC_CANCELLED, EXEC_TIMED_OUT, native_name, parse_native_name};
use crate::error::Result as DriverResult;
use crate::error::{Code, message};

/// A future returned by a case body.
type CaseFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;

/// What a conformance run needs from its caller.
#[derive(Debug, Clone)]
pub struct ConformanceOptions {
    /// Template every case's sandbox is created from.
    ///
    /// The default is an empty [`Template`], which asks the driver for its
    /// default image. Real-VM runs should pass a digest-pinned artifact here:
    /// conformance must never boot an unpinned image.
    pub template: Template,
    /// Whether the guest has a POSIX shell at `/bin/sh`.
    ///
    /// Exec-based cases (`exec_streams`, `exec_timeout`, `cancel_exec`) are
    /// marked skipped when this is false; the driver is still checked for
    /// create, inventory, copy and the optional operations.
    pub guest_supports_shell: bool,
    /// Deadline passed to exec requests. The timeout case expects this to
    /// elapse and the driver to report [`EXEC_TIMED_OUT`].
    pub exec_timeout: Duration,
    /// Wall-clock budget for one case, covering create, work and cleanup.
    pub case_timeout: Duration,
    /// Extra driver hints copied into every `CreateWork.options`.
    pub create_options: BTreeMap<String, String>,
}

impl Default for ConformanceOptions {
    fn default() -> Self {
        Self {
            template: Template::default(),
            guest_supports_shell: true,
            exec_timeout: Duration::from_secs(5),
            case_timeout: Duration::from_secs(180),
            create_options: BTreeMap::new(),
        }
    }
}

impl ConformanceOptions {
    /// Options creating cases from `template`.
    pub fn new(template: Template) -> Self {
        Self {
            template,
            ..Self::default()
        }
    }

    /// Declare whether the guest has `/bin/sh`.
    pub fn with_shell(mut self, guest_supports_shell: bool) -> Self {
        self.guest_supports_shell = guest_supports_shell;
        self
    }

    /// Set the exec deadline the timeout case uses.
    pub fn with_exec_timeout(mut self, exec_timeout: Duration) -> Self {
        self.exec_timeout = exec_timeout;
        self
    }

    /// Set the per-case budget.
    pub fn with_case_timeout(mut self, case_timeout: Duration) -> Self {
        self.case_timeout = case_timeout;
        self
    }

    /// Pass driver hints with every create.
    pub fn with_create_options(mut self, create_options: BTreeMap<String, String>) -> Self {
        self.create_options = create_options;
        self
    }
}

/// The result of one conformance case.
#[derive(Debug)]
pub struct CaseResult {
    /// Stable case name (`create_inventory`, `exec_timeout`, …).
    pub name: &'static str,
    /// `Ok(())` when the case passed (or was skipped); the reason otherwise.
    pub outcome: Result<(), String>,
    /// The case did not run because the options say the guest lacks
    /// something it needs. `outcome` is `Ok(())` for a skipped case.
    pub skipped: bool,
}

impl CaseResult {
    fn pass(name: &'static str) -> Self {
        Self {
            name,
            outcome: Ok(()),
            skipped: false,
        }
    }

    fn failed(name: &'static str, reason: impl Into<String>) -> Self {
        Self {
            name,
            outcome: Err(reason.into()),
            skipped: false,
        }
    }

    fn skipped(name: &'static str) -> Self {
        Self {
            name,
            outcome: Ok(()),
            skipped: true,
        }
    }

    /// Whether the case ran and passed. Skipped cases are not passing.
    pub fn passed(&self) -> bool {
        !self.skipped && self.outcome.is_ok()
    }
}

/// Run every conformance case against `driver`.
///
/// Cases run sequentially. A driver failure is reported as that case's
/// `outcome`; `run` itself does not fail. Every sandbox a case created is
/// destroyed afterwards, including after a case error or timeout.
pub async fn run(driver: Arc<dyn Driver>, options: &ConformanceOptions) -> Vec<CaseResult> {
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let prefix = format!("sbx-conf-{nonce}-");
    let mut results = Vec::new();

    record(
        &mut results,
        &driver,
        &prefix,
        "capabilities",
        options.case_timeout,
        case_capabilities(&driver),
    )
    .await;

    record(
        &mut results,
        &driver,
        &prefix,
        "create_inventory",
        options.case_timeout,
        case_create_inventory(&driver, options, &nonce),
    )
    .await;

    record(
        &mut results,
        &driver,
        &prefix,
        "create_idempotent",
        options.case_timeout,
        case_create_idempotent(&driver, options, &nonce),
    )
    .await;

    record(
        &mut results,
        &driver,
        &prefix,
        "destroy_idempotent",
        options.case_timeout,
        case_destroy_idempotent(&driver, options, &nonce),
    )
    .await;

    if options.guest_supports_shell {
        record(
            &mut results,
            &driver,
            &prefix,
            "exec_streams",
            options.case_timeout,
            case_exec_streams(&driver, options, &nonce),
        )
        .await;
        record(
            &mut results,
            &driver,
            &prefix,
            "exec_timeout",
            options.case_timeout,
            case_exec_timeout(&driver, options, &nonce),
        )
        .await;
        record(
            &mut results,
            &driver,
            &prefix,
            "cancel_exec",
            options.case_timeout,
            case_cancel_exec(&driver, options, &nonce),
        )
        .await;
    } else {
        results.push(CaseResult::skipped("exec_streams"));
        results.push(CaseResult::skipped("exec_timeout"));
        results.push(CaseResult::skipped("cancel_exec"));
    }

    record(
        &mut results,
        &driver,
        &prefix,
        "copy_round_trip",
        options.case_timeout,
        case_copy_round_trip(&driver, options, &nonce),
    )
    .await;

    let capabilities = driver.capabilities();
    if capabilities.snapshot {
        record(
            &mut results,
            &driver,
            &prefix,
            "snapshot",
            options.case_timeout,
            case_snapshot(&driver, options, &nonce),
        )
        .await;
    } else {
        record(
            &mut results,
            &driver,
            &prefix,
            "snapshot",
            options.case_timeout,
            case_snapshot_refused(&driver, options, &nonce),
        )
        .await;
    }

    if capabilities.pause {
        record(
            &mut results,
            &driver,
            &prefix,
            "stop_start",
            options.case_timeout,
            case_stop_start(&driver, options, &nonce),
        )
        .await;
        record(
            &mut results,
            &driver,
            &prefix,
            "pause_resume",
            options.case_timeout,
            case_pause_resume(&driver, options, &nonce),
        )
        .await;
    } else {
        record(
            &mut results,
            &driver,
            &prefix,
            "stop_start",
            options.case_timeout,
            case_stop_start_refused(&driver, options, &nonce),
        )
        .await;
        record(
            &mut results,
            &driver,
            &prefix,
            "pause_resume",
            options.case_timeout,
            case_pause_resume_refused(&driver, options, &nonce),
        )
        .await;
    }

    record(
        &mut results,
        &driver,
        &prefix,
        "host_report",
        options.case_timeout,
        case_host_report(&driver, options),
    )
    .await;

    // A timed-out create can finish after its case future was dropped and
    // after the per-timeout sweep ran. Sweep once more at the end so no
    // conformance sandbox is left behind.
    let leftovers = sweep(&driver, &prefix).await;
    if !leftovers.is_empty() {
        results.push(CaseResult::failed(
            "cleanup",
            format!("sandboxes left behind: {}", leftovers.join("; ")),
        ));
    }
    results
}

/// Run one case under its budget, sweeping leftovers after a timeout.
async fn record<F>(
    results: &mut Vec<CaseResult>,
    driver: &Arc<dyn Driver>,
    prefix: &str,
    name: &'static str,
    budget: Duration,
    case: F,
) where
    F: Future<Output = Result<(), String>>,
{
    match tokio::time::timeout(budget, case).await {
        Ok(Ok(())) => results.push(CaseResult::pass(name)),
        Ok(Err(reason)) => results.push(CaseResult::failed(name, reason)),
        Err(_) => {
            let leftovers = sweep(driver, prefix).await;
            let detail = if leftovers.is_empty() {
                String::new()
            } else {
                format!("; cleanup failed for {}", leftovers.join("; "))
            };
            results.push(CaseResult::failed(
                name,
                format!("case did not finish within {budget:?}{detail}"),
            ));
        }
    }
}

/// Destroy every sandbox this run created but a lost future did not unpin.
async fn sweep(driver: &Arc<dyn Driver>, prefix: &str) -> Vec<String> {
    let Ok(inventory) = driver.inventory().await else {
        return Vec::new();
    };
    let mut errors = Vec::new();
    for observed in inventory {
        let sandbox_id =
            parse_native_name(&observed.native_ref).unwrap_or_else(|| observed.sandbox_id.clone());
        if sandbox_id.starts_with(prefix)
            && let Err(error) = driver.destroy(&observed.native_ref).await
        {
            errors.push(format!("{}: {error}", observed.native_ref));
        }
    }
    errors
}

/// A sandbox created for one case.
#[derive(Clone, Debug)]
struct SandboxTarget {
    sandbox_id: String,
    native_ref: String,
}

/// Destroys every tracked sandbox, even if the case panics or is cancelled.
struct Cleanup {
    driver: Arc<dyn Driver>,
    refs: Vec<String>,
}

impl Cleanup {
    fn new(driver: &Arc<dyn Driver>) -> Self {
        Self {
            driver: Arc::clone(driver),
            refs: Vec::new(),
        }
    }

    fn track(&mut self, native_ref: impl Into<String>) {
        self.refs.push(native_ref.into());
    }

    async fn destroy_all(&mut self) -> Result<(), String> {
        let refs = std::mem::take(&mut self.refs);
        let mut errors = Vec::new();
        for native_ref in refs {
            if let Err(error) = self.driver.destroy(&native_ref).await {
                errors.push(format!("destroy `{native_ref}`: {error}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        if self.refs.is_empty() {
            return;
        }
        let refs = std::mem::take(&mut self.refs);
        let driver = Arc::clone(&self.driver);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                for native_ref in refs {
                    let _ = driver.destroy(&native_ref).await;
                }
            });
        }
    }
}

fn create_work(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    sandbox_id: &str,
    snapshot_capable: bool,
) -> CreateWork {
    let template = if options.template == Template::default() {
        MessageField::default()
    } else {
        MessageField::some(options.template.clone())
    };
    CreateWork {
        sandbox_id: sandbox_id.to_owned(),
        driver: driver.name().to_owned(),
        template,
        shape: MessageField::some(Shape::default()),
        network: MessageField::some(NetworkPolicy::default()),
        options: options
            .create_options
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        snapshot_capable,
        ..CreateWork::default()
    }
}

async fn create_sandbox(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
    tag: &str,
    snapshot_capable: bool,
    cleanup: &mut Cleanup,
) -> Result<SandboxTarget, String> {
    let sandbox_id = format!("sbx-conf-{nonce}-{tag}");
    let work = create_work(driver, options, &sandbox_id, snapshot_capable);
    let native_ref = driver
        .create(&work)
        .await
        .map_err(|error| format!("create `{sandbox_id}`: {error}"))?;
    if native_ref.is_empty() {
        return Err(format!(
            "create `{sandbox_id}` returned an empty native ref"
        ));
    }
    cleanup.track(native_ref.clone());
    Ok(SandboxTarget {
        sandbox_id,
        native_ref,
    })
}

/// Create one sandbox, run `body`, then destroy whatever the case tracked.
async fn with_sandbox<F>(
    driver: Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
    tag: &str,
    snapshot_capable: bool,
    body: F,
) -> Result<(), String>
where
    F: FnOnce(SandboxTarget, Arc<dyn Driver>) -> CaseFuture,
{
    let mut cleanup = Cleanup::new(&driver);
    let target =
        create_sandbox(&driver, options, nonce, tag, snapshot_capable, &mut cleanup).await?;
    let outcome = body(target, driver).await;
    let destroyed = cleanup.destroy_all().await;
    match (outcome, destroyed) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(reason), Ok(())) => Err(reason),
        (Ok(()), Err(cleanup)) => Err(format!("cleanup after case: {cleanup}")),
        (Err(reason), Err(cleanup)) => Err(format!("{reason}; cleanup after case: {cleanup}")),
    }
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

async fn case_capabilities(driver: &Arc<dyn Driver>) -> Result<(), String> {
    if driver.name().trim().is_empty() {
        return Err("driver name is empty".into());
    }
    let capabilities = driver.capabilities();
    if capabilities.os.trim().is_empty() {
        return Err("capabilities.os is empty".into());
    }
    if capabilities.os != capabilities.os.to_ascii_lowercase() {
        return Err(format!(
            "capabilities.os `{}` is not lowercase; the contract uses `linux`, `macos`, `windows`",
            capabilities.os
        ));
    }
    if capabilities.arch.is_empty() {
        return Err("capabilities.arch is empty".into());
    }
    for arch in &capabilities.arch {
        if !matches!(arch.as_str(), "x86_64" | "aarch64") {
            return Err(format!(
                "capabilities.arch `{arch}` is not `x86_64` or `aarch64`"
            ));
        }
    }
    no_duplicates(&capabilities.arch, "capabilities.arch")?;
    no_duplicates(&capabilities.features, "capabilities.features")?;

    let mut modes: Vec<NetworkMode> = Vec::new();
    for mode in &capabilities.network_modes {
        let Some(mode) = mode.as_known() else {
            return Err(format!(
                "capabilities.networkModes contains unknown value {}",
                mode.to_i32()
            ));
        };
        if mode == NetworkMode::NetworkModeUnspecified {
            return Err(
                "capabilities.networkModes advertises NETWORK_MODE_UNSPECIFIED; list the modes the driver can enforce"
                    .into(),
            );
        }
        if modes.contains(&mode) {
            return Err(format!(
                "capabilities.networkModes lists {mode:?} more than once"
            ));
        }
        modes.push(mode);
    }

    if let Some(default_shape) = capabilities.default_shape.as_option() {
        if capabilities.max_vcpus > 0 && default_shape.cpus > capabilities.max_vcpus {
            return Err(format!(
                "capabilities.defaultShape.cpus {} exceeds maxVcpus {}",
                default_shape.cpus, capabilities.max_vcpus
            ));
        }
        if capabilities.max_memory_mib > 0 && default_shape.memory_mib > capabilities.max_memory_mib
        {
            return Err(format!(
                "capabilities.defaultShape.memoryMib {} exceeds maxMemoryMib {}",
                default_shape.memory_mib, capabilities.max_memory_mib
            ));
        }
    }
    Ok(())
}

fn no_duplicates(values: &[String], field: &str) -> Result<(), String> {
    let mut seen = std::collections::BTreeSet::new();
    for value in values {
        if value.trim().is_empty() {
            return Err(format!("{field} contains an empty value"));
        }
        if !seen.insert(value.as_str()) {
            return Err(format!("{field} lists `{value}` more than once"));
        }
    }
    Ok(())
}

async fn case_create_inventory(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    with_sandbox(
        Arc::clone(driver),
        options,
        nonce,
        "inventory",
        false,
        move |target, driver| {
            Box::pin(async move {
                let sandbox_id = parse_native_name(&target.native_ref).ok_or_else(|| {
                    format!(
                        "native ref `{}` is not parseable; name machines with `native_name(sandbox_id)`",
                        target.native_ref
                    )
                })?;
                if sandbox_id != target.sandbox_id {
                    return Err(format!(
                        "native ref `{}` parses to `{sandbox_id}`, expected `{}`",
                        target.native_ref, target.sandbox_id
                    ));
                }
                let inventory = driver
                    .inventory()
                    .await
                    .map_err(|error| format!("inventory: {error}"))?;
                let matches: Vec<&Observed> = inventory
                    .iter()
                    .filter(|observed| observed.native_ref == target.native_ref)
                    .collect();
                if matches.len() != 1 {
                    return Err(format!(
                        "inventory lists {} entries for `{}`, expected exactly one",
                        matches.len(),
                        target.native_ref
                    ));
                }
                let observed = matches[0];
                if !observed.sandbox_id.is_empty() && observed.sandbox_id != target.sandbox_id {
                    return Err(format!(
                        "inventory reports sandboxId `{}` for `{}`, expected `{}`",
                        observed.sandbox_id, target.native_ref, target.sandbox_id
                    ));
                }

                driver
                    .destroy(&target.native_ref)
                    .await
                    .map_err(|error| format!("destroy after create: {error}"))?;
                let inventory = driver
                    .inventory()
                    .await
                    .map_err(|error| format!("inventory after destroy: {error}"))?;
                if inventory
                    .iter()
                    .any(|observed| observed.native_ref == target.native_ref)
                {
                    return Err(format!(
                        "inventory still lists `{}` after destroy",
                        target.native_ref
                    ));
                }
                Ok(())
            })
        },
    )
    .await
}

async fn case_create_idempotent(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    let driver = Arc::clone(driver);
    let template = options.template.clone();
    let create_options = options.create_options.clone();
    with_sandbox(
        driver,
        options,
        nonce,
        "idempotent",
        false,
        move |target, driver| {
            Box::pin(async move {
                let work = CreateWork {
                    sandbox_id: target.sandbox_id.clone(),
                    driver: driver.name().to_owned(),
                    template: if template == Template::default() {
                        MessageField::default()
                    } else {
                        MessageField::some(template)
                    },
                    shape: MessageField::some(Shape::default()),
                    network: MessageField::some(NetworkPolicy::default()),
                    options: create_options
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                    ..CreateWork::default()
                };
                let again = driver
                    .create(&work)
                    .await
                    .map_err(|error| format!("redelivered create: {error}"))?;
                if again != target.native_ref {
                    // Do not leak the second machine while reporting the
                    // contract violation.
                    let _ = driver.destroy(&again).await;
                    return Err(format!(
                        "redelivered create for `{}` returned `{again}`, expected the same native ref `{}`",
                        target.sandbox_id, target.native_ref
                    ));
                }
                let inventory = driver
                    .inventory()
                    .await
                    .map_err(|error| format!("inventory after redelivered create: {error}"))?;
                let count = inventory
                    .iter()
                    .filter(|observed| observed.native_ref == target.native_ref)
                    .count();
                if count != 1 {
                    return Err(format!(
                        "inventory lists {count} entries for `{}` after a redelivered create, expected one",
                        target.native_ref
                    ));
                }
                Ok(())
            })
        },
    )
    .await
}

async fn case_destroy_idempotent(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    let driver = Arc::clone(driver);
    let unknown = native_name(&format!("sbx-conf-{nonce}-unknown"));
    with_sandbox(
        driver,
        options,
        nonce,
        "destroy",
        false,
        move |target, driver| {
            Box::pin(async move {
                driver
                    .destroy(&target.native_ref)
                    .await
                    .map_err(|error| format!("first destroy: {error}"))?;
                driver
                    .destroy(&target.native_ref)
                    .await
                    .map_err(|error| format!("second destroy: {error}"))?;
                driver
                    .destroy(&unknown)
                    .await
                    .map_err(|error| format!("destroying unknown ref `{unknown}`: {error}"))?;
                Ok(())
            })
        },
    )
    .await
}

#[derive(Default)]
struct ExecOutput {
    status: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    exit_events: Vec<i32>,
    events_after_exit: bool,
}

impl ExecOutput {
    fn check_terminal(&self) -> Result<(), String> {
        if self.exit_events.len() > 1 {
            return Err(format!(
                "exec stream carried {} exit events, expected at most one: {:?}",
                self.exit_events.len(),
                self.exit_events
            ));
        }
        if self.events_after_exit {
            return Err("exec stream continued after its exit event".into());
        }
        // An in-process driver reports the exit status by return value and
        // need not also emit an exit event; a plugin synthesizes one. When
        // the event is present it must agree with the return value.
        if self
            .exit_events
            .first()
            .is_some_and(|code| *code != self.status)
        {
            return Err(format!(
                "exec stream exited {} but the call returned {}",
                self.exit_events[0], self.status
            ));
        }
        Ok(())
    }
}

/// Run one exec to completion, collecting its stream.
///
/// The caller's deadline is separate from the request's `timeoutSecs`: the
/// request deadline is what the driver enforces in the guest, while this one
/// keeps a hung plugin from hanging the suite.
async fn exec_collect(
    driver: &Arc<dyn Driver>,
    target: &SandboxTarget,
    exec_id: &str,
    request: &ExecRequest,
    budget: Duration,
) -> Result<ExecOutput, String> {
    let (output, mut events) = mpsc::channel::<ExecEvent>(64);
    let collect = async move {
        let mut output = ExecOutput::default();
        let mut saw_exit = false;
        while let Some(event) = events.recv().await {
            match event.event {
                Some(exec_event::Event::Stdout(bytes)) => {
                    if saw_exit {
                        output.events_after_exit = true;
                    }
                    output.stdout.extend(bytes);
                }
                Some(exec_event::Event::Stderr(bytes)) => {
                    if saw_exit {
                        output.events_after_exit = true;
                    }
                    output.stderr.extend(bytes);
                }
                Some(exec_event::Event::ExitCode(code)) => {
                    if saw_exit {
                        output.events_after_exit = true;
                    }
                    saw_exit = true;
                    output.exit_events.push(code);
                }
                None => {
                    if saw_exit {
                        output.events_after_exit = true;
                    }
                }
            }
        }
        output
    };
    let run = async {
        driver
            .exec(&target.native_ref, exec_id, request, output)
            .await
    };
    let (status, events) = tokio::time::timeout(budget, async { tokio::join!(run, collect) })
        .await
        .map_err(|_| format!("exec did not return within {budget:?}"))?;
    let status = status.map_err(|error| format!("exec: {error}"))?;
    Ok(ExecOutput { status, ..events })
}

/// A request deadline comfortably longer than the driver's exec deadline.
fn exec_budget(options: &ConformanceOptions) -> Duration {
    options
        .exec_timeout
        .saturating_mul(4)
        .saturating_add(Duration::from_secs(10))
}

fn shell_request(target: &SandboxTarget, script: &str, timeout_secs: u64) -> ExecRequest {
    ExecRequest {
        sandbox_id: target.sandbox_id.clone(),
        argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
        timeout_secs,
        ..ExecRequest::default()
    }
}

async fn case_exec_streams(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    let driver = Arc::clone(driver);
    let exec_timeout = options.exec_timeout.as_secs().max(1);
    let budget = exec_budget(options);
    with_sandbox(
        driver,
        options,
        nonce,
        "exec-streams",
        false,
        move |target, driver| {
            Box::pin(async move {
                let request =
                    shell_request(&target, "printf out; printf err >&2; exit 7", exec_timeout);
                let output =
                    exec_collect(&driver, &target, "exe-conf-exec-streams", &request, budget)
                        .await?;
                output.check_terminal()?;
                if output.status != 7 {
                    return Err(format!(
                        "exec exited {}, expected 7 (stdout {:?}, stderr {:?})",
                        output.status,
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ));
                }
                if output.stdout != b"out" {
                    return Err(format!(
                        "stdout was {:?}, expected b\"out\"",
                        String::from_utf8_lossy(&output.stdout)
                    ));
                }
                if output.stderr != b"err" {
                    return Err(format!(
                        "stderr was {:?}, expected b\"err\"",
                        String::from_utf8_lossy(&output.stderr)
                    ));
                }
                Ok(())
            })
        },
    )
    .await
}

async fn case_exec_timeout(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    let driver = Arc::clone(driver);
    let exec_timeout = options.exec_timeout.as_secs().max(1);
    let budget = exec_budget(options);
    with_sandbox(
        driver,
        options,
        nonce,
        "exec-timeout",
        false,
        move |target, driver| {
            Box::pin(async move {
                let request = shell_request(&target, "sleep 30", exec_timeout);
                let output = exec_collect(
                    &driver,
                    &target,
                    "exe-conf-exec-timeout",
                    &request,
                    budget,
                )
                .await?;
                if output.status != EXEC_TIMED_OUT {
                    return Err(format!(
                        "exec past its {exec_timeout}s deadline returned {}, expected {EXEC_TIMED_OUT}",
                        output.status
                    ));
                }
                if output
                    .exit_events
                    .last()
                    .is_some_and(|code| *code != EXEC_TIMED_OUT)
                {
                    return Err(format!(
                        "exec stream ended with {}, expected {EXEC_TIMED_OUT}",
                        output.exit_events.last().copied().unwrap_or_default()
                    ));
                }
                Ok(())
            })
        },
    )
    .await
}

async fn case_cancel_exec(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    let driver = Arc::clone(driver);
    let budget = exec_budget(options);
    with_sandbox(
        driver,
        options,
        nonce,
        "cancel",
        false,
        move |target, driver| {
            Box::pin(async move {
                // (1) A cancel that arrives before the exec starts refuses it
                // instead of running the command.
                let early_id = "exe-conf-cancel-early";
                driver
                    .cancel_exec(&target.native_ref, early_id)
                    .await
                    .map_err(|error| format!("cancel before exec: {error}"))?;
                let early_request = shell_request(&target, "sleep 30", 60);
                let early =
                    exec_collect(&driver, &target, early_id, &early_request, budget).await?;
                if early.status != EXEC_CANCELLED {
                    return Err(format!(
                        "exec cancelled before it started returned {}, expected {EXEC_CANCELLED}",
                        early.status
                    ));
                }
                if early
                    .exit_events
                    .last()
                    .is_some_and(|code| *code != EXEC_CANCELLED)
                {
                    return Err(format!(
                        "pre-cancelled exec stream ended with {}, expected {EXEC_CANCELLED}",
                        early.exit_events.last().copied().unwrap_or_default()
                    ));
                }

                // (2) Cancelling a running exec ends it with 130.
                let running_id = "exe-conf-cancel-running";
                let request = shell_request(&target, "sleep 30", 60);
                let (events_tx, events_rx) = mpsc::channel(64);
                let running = driver.exec(&target.native_ref, running_id, &request, events_tx);
                let collecting = async move {
                    let mut output = ExecOutput::default();
                    let mut events_rx = events_rx;
                    while let Some(event) = events_rx.recv().await {
                        match event.event {
                            Some(exec_event::Event::Stdout(bytes)) => {
                                output.stdout.extend(bytes);
                            }
                            Some(exec_event::Event::Stderr(bytes)) => {
                                output.stderr.extend(bytes);
                            }
                            Some(exec_event::Event::ExitCode(code)) => {
                                output.exit_events.push(code);
                            }
                            None => {}
                        }
                    }
                    output
                };
                let cancelling = async {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    driver.cancel_exec(&target.native_ref, running_id).await
                };
                let joined = tokio::time::timeout(Duration::from_secs(15), async {
                    tokio::join!(running, collecting, cancelling)
                })
                .await
                .map_err(|_| "cancelling a running exec did not return within 15s".to_string())?;
                let (status, events, cancelled) = joined;
                let status = status.map_err(|error| format!("running exec: {error}"))?;
                cancelled.map_err(|error| format!("cancel_exec: {error}"))?;
                if status != EXEC_CANCELLED {
                    return Err(format!(
                        "cancelled exec returned {}, expected {EXEC_CANCELLED}",
                        status
                    ));
                }
                if events
                    .exit_events
                    .last()
                    .is_some_and(|code| *code != EXEC_CANCELLED)
                {
                    return Err(format!(
                        "cancelled exec stream ended with {}, expected {EXEC_CANCELLED}",
                        events.exit_events.last().copied().unwrap_or_default()
                    ));
                }
                Ok(())
            })
        },
    )
    .await
}

async fn case_copy_round_trip(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    let driver = Arc::clone(driver);
    let host = HostTempDir::new(nonce, "copy")?;
    let guest_root = format!("/tmp/preloop-conformance-{nonce}");
    with_sandbox(
        driver,
        options,
        nonce,
        "copy",
        false,
        move |target, driver| {
            Box::pin(async move {
                let payload = host.path().join("payload");
                std::fs::create_dir_all(payload.join("nested"))
                    .map_err(|error| format!("creating payload: {error}"))?;
                std::fs::write(payload.join("hello.txt"), b"hello conformance\n")
                    .map_err(|error| format!("writing payload: {error}"))?;
                std::fs::write(payload.join("nested/data.bin"), [0u8, 1, 2, 255])
                    .map_err(|error| format!("writing payload: {error}"))?;

                let input = host.path().join("in.tar");
                build_tar(&payload, &input)?;
                driver
                    .copy_in(&target.native_ref, &input, &guest_root)
                    .await
                    .map_err(|error| format!("copy_in: {error}"))?;

                let output = host.path().join("out.tar");
                driver
                    .copy_out(
                        &target.native_ref,
                        &format!("{guest_root}/payload"),
                        &output,
                    )
                    .await
                    .map_err(|error| format!("copy_out: {error}"))?;

                let expected = read_tar(&input)?;
                let actual = read_tar(&output)?;
                if expected.is_empty() {
                    return Err("the input tar was empty".into());
                }
                if expected != actual {
                    let mut missing: Vec<&String> =
                        expected.keys().filter(|key| !actual.contains_key(*key)).collect();
                    let mut extra: Vec<&String> =
                        actual.keys().filter(|key| !expected.contains_key(*key)).collect();
                    missing.sort();
                    extra.sort();
                    let changed: Vec<&String> = expected
                        .iter()
                        .filter(|(key, value)| actual.get(*key).is_some_and(|other| other != *value))
                        .map(|(key, _)| key)
                        .collect();
                    return Err(format!(
                        "copy round-trip differs: missing {missing:?}, extra {extra:?}, changed {changed:?}"
                    ));
                }
                Ok(())
            })
        },
    )
    .await
}

async fn case_snapshot(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    let driver = Arc::clone(driver);
    let template_name = format!("conformance-{nonce}");
    with_sandbox(
        driver,
        options,
        nonce,
        "snapshot",
        true,
        move |target, driver| {
            Box::pin(async move {
                let snapshot = driver
                    .snapshot(&target.native_ref, &template_name)
                    .await
                    .map_err(|error| format!("snapshot: {error}"))?;
                if snapshot.is_empty() {
                    return Err("snapshot returned an empty native ref".into());
                }
                driver
                    .drop_snapshot(&snapshot)
                    .await
                    .map_err(|error| format!("drop_snapshot `{snapshot}`: {error}"))?;
                Ok(())
            })
        },
    )
    .await
}

async fn case_snapshot_refused(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    with_sandbox(
        Arc::clone(driver),
        options,
        nonce,
        "snapshot-refused",
        false,
        move |target, driver| {
            Box::pin(async move {
                expect_precondition(
                    driver.snapshot(&target.native_ref, "conformance").await,
                    "snapshot",
                )
            })
        },
    )
    .await
}

async fn case_stop_start(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    with_sandbox(
        Arc::clone(driver),
        options,
        nonce,
        "stop-start",
        false,
        move |target, driver| {
            Box::pin(async move {
                driver
                    .stop(&target.native_ref)
                    .await
                    .map_err(|error| format!("stop: {error}"))?;
                driver
                    .start(&target.native_ref, false)
                    .await
                    .map_err(|error| format!("start: {error}"))?;
                Ok(())
            })
        },
    )
    .await
}

async fn case_stop_start_refused(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    with_sandbox(
        Arc::clone(driver),
        options,
        nonce,
        "stop-start-refused",
        false,
        move |target, driver| {
            Box::pin(async move {
                expect_precondition(driver.stop(&target.native_ref).await, "stop")?;
                expect_precondition(driver.start(&target.native_ref, false).await, "start")
            })
        },
    )
    .await
}

async fn case_pause_resume(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    with_sandbox(
        Arc::clone(driver),
        options,
        nonce,
        "pause-resume",
        false,
        move |target, driver| {
            Box::pin(async move {
                driver
                    .pause(&target.native_ref)
                    .await
                    .map_err(|error| format!("pause: {error}"))?;
                driver
                    .resume(&target.native_ref)
                    .await
                    .map_err(|error| format!("resume: {error}"))?;
                Ok(())
            })
        },
    )
    .await
}

async fn case_pause_resume_refused(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
    nonce: &str,
) -> Result<(), String> {
    with_sandbox(
        Arc::clone(driver),
        options,
        nonce,
        "pause-resume-refused",
        false,
        move |target, driver| {
            Box::pin(async move {
                expect_precondition(driver.pause(&target.native_ref).await, "pause")?;
                expect_precondition(driver.resume(&target.native_ref).await, "resume")
            })
        },
    )
    .await
}

async fn case_host_report(
    driver: &Arc<dyn Driver>,
    options: &ConformanceOptions,
) -> Result<(), String> {
    let budget = options.case_timeout;
    let report = tokio::time::timeout(budget, driver.host_report())
        .await
        .map_err(|_| format!("host_report did not return within {budget:?}"))?;
    let mut seen = std::collections::BTreeSet::new();
    for sandbox in &report.sandboxes {
        if sandbox.native_ref.is_empty() {
            return Err("host_report contains a sandbox with an empty nativeRef".into());
        }
        if !seen.insert(sandbox.native_ref.as_str()) {
            return Err(format!(
                "host_report lists `{}` more than once",
                sandbox.native_ref
            ));
        }
    }
    Ok(())
}

fn expect_precondition<T>(result: DriverResult<T>, operation: &str) -> Result<(), String> {
    match result {
        Ok(_) => Err(format!(
            "{operation} succeeded although capabilities do not declare it"
        )),
        Err(error) if error.code == Code::FailedPrecondition => Ok(()),
        Err(error) => Err(format!(
            "{operation}: expected failed_precondition, got {} (`{}`)",
            error.code.as_str(),
            message(&error)
        )),
    }
}

// ---------------------------------------------------------------------------
// Host-side tar helpers
// ---------------------------------------------------------------------------

/// A unique host directory removed when the case ends.
struct HostTempDir(PathBuf);

impl HostTempDir {
    fn new(nonce: &str, tag: &str) -> Result<Self, String> {
        let dir = std::env::temp_dir().join(format!("preloop-conf-{nonce}-{tag}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir)
            .map_err(|error| format!("creating the conformance temp dir: {error}"))?;
        Ok(Self(dir))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for HostTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Build a tar with `payload/` as its top-level entry.
fn build_tar(payload: &Path, tar: &Path) -> Result<(), String> {
    let file = std::fs::File::create(tar).map_err(|error| format!("creating tar: {error}"))?;
    let mut builder = tar::Builder::new(file);
    builder
        .append_dir_all("payload", payload)
        .map_err(|error| format!("writing tar: {error}"))?;
    builder
        .finish()
        .map_err(|error| format!("finishing tar: {error}"))
}

/// Read a tar into `path -> bytes`, ignoring directory entries.
fn read_tar(tar: &Path) -> Result<BTreeMap<String, Vec<u8>>, String> {
    let file = std::fs::File::open(tar).map_err(|error| format!("opening tar: {error}"))?;
    let mut archive = tar::Archive::new(file);
    let mut entries = BTreeMap::new();
    let entries_iter = archive
        .entries()
        .map_err(|error| format!("reading tar: {error}"))?;
    for entry in entries_iter {
        let mut entry = entry.map_err(|error| format!("reading tar entry: {error}"))?;
        if entry.header().entry_type().is_dir() {
            continue;
        }
        let path = entry
            .path()
            .map_err(|error| format!("reading tar entry path: {error}"))?
            .to_string_lossy()
            .trim_start_matches("./")
            .to_owned();
        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .map_err(|error| format!("reading tar entry `{path}`: {error}"))?;
        entries.insert(path, data);
    }
    Ok(entries)
}
