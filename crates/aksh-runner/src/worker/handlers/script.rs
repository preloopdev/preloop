//! Script step handler — runs inline `run:` scripts.
//!
//! Mirrors `ScriptHandler.cs` / `ScriptHandlerHelpers.cs` from the official runner.

use anyhow::{Context, Result};
use std::path::Path;
use tracing::debug;

use crate::process;
use crate::worker::execution_context::StepContext;

/// Run an inline script step.
pub async fn run_script(
    script: &str,
    shell: Option<&str>,
    working_directory: &str,
    ctx: &mut StepContext<'_>,
    cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<()> {
    // Write script to temp file
    let temp_dir = Path::new(working_directory)
        .parent()
        .unwrap_or(Path::new("."))
        .join("_temp");
    std::fs::create_dir_all(&temp_dir)?;

    let script_id = uuid::Uuid::new_v4();
    let (script_path, program, args) = resolve_shell(shell, &temp_dir, &script_id)?;

    // Write the script content
    std::fs::write(&script_path, script)
        .with_context(|| format!("writing script to {}", script_path.display()))?;

    // Make executable on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755))?;
    }

    debug!("Running script: {program} {args:?}");

    // Build environment
    let env = ctx.build_env();

    // Execute
    let result = process::invoke(
        &program,
        &args.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        Path::new(working_directory),
        &env,
        Some(Box::new({
            let masks = ctx.job.masks.clone();
            move |line: &str| {
                let mut masked = line.to_string();
                for secret in &masks {
                    if !secret.is_empty() {
                        masked = masked.replace(secret, "***");
                    }
                }
            }
        })),
        cancel_rx,
    )
    .await?;

    // Collect log lines
    for line in &result.lines {
        ctx.log(line);
    }

    // Check exit code
    if result.exit_code != 0 {
        ctx.log(&format!(
            "##[error]Process completed with exit code {}.",
            result.exit_code
        ));
        anyhow::bail!("process exit code {}", result.exit_code);
    }

    Ok(())
}

/// Run an inline script step inside a job container via `docker exec`.
///
/// The script is written to the host temp dir (which is bind-mounted into the
/// container as `/__w/_temp`), then executed via `docker exec` with path
/// translation.
pub async fn run_script_in_container(
    script: &str,
    shell: Option<&str>,
    working_directory: &str,
    container_id: &str,
    ctx: &mut StepContext<'_>,
    cancel_rx: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<()> {
    // Write script to host temp dir (mounted as /__w/_temp in container)
    let temp_dir = Path::new(working_directory)
        .parent()
        .unwrap_or(Path::new("."))
        .join("_temp");
    std::fs::create_dir_all(&temp_dir)?;

    let script_id = uuid::Uuid::new_v4();
    let (script_path, program, args) = resolve_shell(shell, &temp_dir, &script_id)?;

    // Write the script content
    std::fs::write(&script_path, script)
        .with_context(|| format!("writing script to {}", script_path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755))?;
    }

    // Translate paths to container paths
    let host_work = Path::new(working_directory)
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();

    let container_workdir =
        crate::worker::container_ops::translate_to_container_path(working_directory, &host_work);
    let container_program =
        crate::worker::container_ops::translate_to_container_path(&program, &host_work);
    let container_args: Vec<String> = args
        .iter()
        .map(|a| crate::worker::container_ops::translate_to_container_path(a, &host_work))
        .collect();

    debug!("Running script in container: docker exec {container_id} {container_program} {container_args:?}");

    // Build environment and translate path-valued vars
    let mut env = ctx.build_env();
    for key in &[
        "GITHUB_WORKSPACE",
        "GITHUB_ENV",
        "GITHUB_PATH",
        "GITHUB_OUTPUT",
        "GITHUB_STATE",
        "GITHUB_STEP_SUMMARY",
        "RUNNER_TEMP",
        "RUNNER_TOOL_CACHE",
    ] {
        if let Some(val) = env.get(*key).cloned() {
            env.insert(
                key.to_string(),
                crate::worker::container_ops::translate_to_container_path(&val, &host_work),
            );
        }
    }
    env.insert("HOME".to_string(), "/github/home".to_string());

    let container_args_ref: Vec<&str> = container_args.iter().map(|s| s.as_str()).collect();
    let result = crate::worker::container_ops::docker_exec(
        container_id,
        &container_program,
        &container_args_ref,
        &container_workdir,
        &env,
        cancel_rx,
    )
    .await?;

    // Collect log lines
    for line in &result.lines {
        ctx.log(line);
    }

    // Check exit code
    if result.exit_code != 0 {
        ctx.log(&format!(
            "##[error]Process completed with exit code {}.",
            result.exit_code
        ));
        anyhow::bail!("process exit code {}", result.exit_code);
    }

    Ok(())
}

/// Resolve the shell to use and return (script_path, program, args).
fn resolve_shell(
    shell: Option<&str>,
    temp_dir: &Path,
    script_id: &uuid::Uuid,
) -> Result<(std::path::PathBuf, String, Vec<String>)> {
    let shell = shell.unwrap_or_else(|| {
        // Default: bash if available, else sh
        if Path::new("/bin/bash").exists() || Path::new("/usr/bin/bash").exists() {
            "bash"
        } else {
            "sh"
        }
    });

    match shell {
        "bash" => {
            let path = temp_dir.join(format!("{script_id}.sh"));
            Ok((
                path.clone(),
                "bash".to_string(),
                vec![
                    "--noprofile".to_string(),
                    "--norc".to_string(),
                    "-e".to_string(),
                    "-o".to_string(),
                    "pipefail".to_string(),
                    path.to_string_lossy().to_string(),
                ],
            ))
        }
        "sh" => {
            let path = temp_dir.join(format!("{script_id}.sh"));
            Ok((
                path.clone(),
                "sh".to_string(),
                vec!["-e".to_string(), path.to_string_lossy().to_string()],
            ))
        }
        "python" => {
            let path = temp_dir.join(format!("{script_id}.py"));
            Ok((
                path.clone(),
                "python".to_string(),
                vec![path.to_string_lossy().to_string()],
            ))
        }
        "pwsh" => {
            let path = temp_dir.join(format!("{script_id}.ps1"));
            Ok((
                path.clone(),
                "pwsh".to_string(),
                vec![
                    "-command".to_string(),
                    format!(". '{}'", path.to_string_lossy()),
                ],
            ))
        }
        custom => {
            // Custom shell template: e.g. "perl {0}"
            let path = temp_dir.join(format!("{script_id}.sh"));
            let parts: Vec<&str> = custom.splitn(2, ' ').collect();
            let program = parts[0].to_string();
            let mut args = Vec::new();
            if parts.len() > 1 {
                let template = parts[1];
                args.push(template.replace("{0}", &path.to_string_lossy()));
            } else {
                args.push(path.to_string_lossy().to_string());
            }
            Ok((path, program, args))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_bash_shell() {
        let dir = std::path::PathBuf::from("/tmp");
        let id = uuid::Uuid::nil();
        let (path, prog, args) = resolve_shell(Some("bash"), &dir, &id).unwrap();
        assert_eq!(prog, "bash");
        assert!(args.contains(&"--noprofile".to_string()));
        assert!(args.contains(&"-e".to_string()));
        assert!(path.to_string_lossy().ends_with(".sh"));
    }

    #[test]
    fn resolve_custom_shell() {
        let dir = std::path::PathBuf::from("/tmp");
        let id = uuid::Uuid::nil();
        let (_, prog, args) = resolve_shell(Some("perl {0}"), &dir, &id).unwrap();
        assert_eq!(prog, "perl");
        assert!(args[0].ends_with(".sh"));
    }
}
