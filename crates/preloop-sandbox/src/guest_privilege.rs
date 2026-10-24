//! Root-or-sudo wrapping for guest shell steps.
//!
//! A guest exec channel runs as the image's own user, not necessarily root: the
//! official runner image declares `USER runner` (uid 1001), and AgentENV's
//! `aenv exec` has no user flag. Steps that write under `/run`, own root-only
//! directories, or remove root-owned files need root, so they are wrapped here
//! rather than run bare. The orchestrator's golden contract and the AgentENV
//! provider share this one convention.

use base64::Engine as _;

/// Carry a guest shell script so its root-only steps run either directly (the
/// exec landed on root: locally baked goldens from plain bases declare no USER)
/// or via passwordless sudo (the official runner image declares `USER runner`,
/// and `machine exec` runs as that image user). The script is embedded base64
/// so every quoting form survives both shells.
///
/// Best-effort: a refused sudo is ignored, so the step's status is not
/// authoritative. Use [`run_as_root_or_sudo_strict`] wherever the caller's
/// exit-code check decides success.
pub fn run_as_root_or_sudo(script: &str) -> String {
    run_as_root_or_sudo_impl(script, true)
}

/// Same root-or-sudo fallback as [`run_as_root_or_sudo`], but status-preserving:
/// a refused sudo or a failing script propagates a nonzero exit, so the caller's
/// exit-code check is authoritative. Required for provisioning that has no
/// independent verification step: the lenient form would turn a
/// passwordless-sudo miss into a false success.
pub fn run_as_root_or_sudo_strict(script: &str) -> String {
    run_as_root_or_sudo_impl(script, false)
}

fn run_as_root_or_sudo_impl(script: &str, best_effort: bool) -> String {
    // The script is spliced inline as `then {script}; else …`, so a trailing
    // newline would leave that `;` alone on a line and dash would reject the
    // whole command (`Syntax error: ";" unexpected`). Scripts that live in a
    // file end in a newline by convention, and trailing whitespace means
    // nothing to the shell, so drop it here instead of asking every caller to
    // remember.
    let script = script.trim_end();
    let b64 = base64::engine::general_purpose::STANDARD.encode(script);
    let fallback = if best_effort {
        "sudo -n sh 2>/dev/null || true"
    } else {
        "sudo -n sh"
    };
    format!(
        "if [ \"$(id -u)\" -eq 0 ]; then {script}; else \
           printf %s '{b64}' | base64 -d | {fallback}; fi"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_wrapper_propagates_a_refused_sudo() {
        let strict = run_as_root_or_sudo_strict("mkdir -p /run/x");
        assert!(
            strict.starts_with("if [ \"$(id -u)\" -eq 0 ]; then mkdir -p /run/x; else "),
            "{strict}"
        );
        assert!(strict.ends_with("| sudo -n sh; fi"), "{strict}");
    }

    #[test]
    fn best_effort_wrapper_ignores_a_refused_sudo() {
        let lenient = run_as_root_or_sudo("rm -f /run/x");
        assert!(
            lenient.ends_with("| sudo -n sh 2>/dev/null || true; fi"),
            "{lenient}"
        );
    }
}
