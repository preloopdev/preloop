//! Action manifest loading and handler factory.

use anyhow::{Context, Result};
use std::path::Path;

/// Parsed action manifest (action.yml / action.yaml).
#[derive(Debug, Clone)]
pub struct ActionManifest {
    pub name: String,
    pub description: String,
    pub runs_using: String,
    pub runs_main: Option<String>,
    pub runs_pre: Option<String>,
    pub runs_pre_if: Option<String>,
    pub runs_post: Option<String>,
    pub runs_post_if: Option<String>,
    pub runs_steps: Option<Vec<serde_json::Value>>,
    pub runs_image: Option<String>,
    pub runs_entrypoint: Option<String>,
    pub runs_args: Option<Vec<String>>,
    pub runs_env: Option<serde_json::Map<String, serde_json::Value>>,
    pub inputs: Option<serde_json::Map<String, serde_json::Value>>,
    pub outputs: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Load an action manifest from a directory.
pub fn load_action_manifest(action_dir: &Path) -> Result<ActionManifest> {
    let yml_path = action_dir.join("action.yml");
    let yaml_path = action_dir.join("action.yaml");

    let manifest_path = if yml_path.exists() {
        yml_path
    } else if yaml_path.exists() {
        yaml_path
    } else {
        anyhow::bail!(
            "No action.yml or action.yaml found in {}",
            action_dir.display()
        );
    };

    let content = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;

    let doc: serde_json::Value = serde_yaml::from_str(&content)
        .with_context(|| format!("parsing {}", manifest_path.display()))?;

    let name = doc
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let description = doc
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let runs = doc.get("runs").context("action manifest missing 'runs'")?;

    let runs_using = runs
        .get("using")
        .and_then(|v| v.as_str())
        .filter(|s| !s.trim().is_empty())
        .context("action manifest missing 'runs.using'")?
        .to_string();
    let runs_main = runs.get("main").and_then(|v| v.as_str()).map(String::from);
    let runs_pre = runs
        .get("pre")
        .or_else(|| runs.get("pre-entrypoint"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let runs_pre_if = if runs_pre.is_some() {
        Some(
            runs.get("pre-if")
                .and_then(|v| v.as_str())
                .unwrap_or("always()")
                .to_string(),
        )
    } else {
        None
    };
    let runs_post = runs
        .get("post")
        .or_else(|| runs.get("post-entrypoint"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let runs_post_if = if runs_post.is_some() {
        Some(
            runs.get("post-if")
                .and_then(|v| v.as_str())
                .unwrap_or("always()")
                .to_string(),
        )
    } else {
        None
    };
    let runs_steps = runs.get("steps").and_then(|v| v.as_array()).cloned();
    let runs_image = runs.get("image").and_then(|v| v.as_str()).map(String::from);
    let runs_entrypoint = runs
        .get("entrypoint")
        .and_then(|v| v.as_str())
        .map(String::from);
    let runs_args = runs.get("args").and_then(|v| {
        v.as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
    });
    let runs_env = runs.get("env").and_then(|v| v.as_object()).cloned();

    let inputs = doc.get("inputs").and_then(|v| v.as_object()).cloned();
    let outputs = doc.get("outputs").and_then(|v| v.as_object()).cloned();

    Ok(ActionManifest {
        name,
        description,
        runs_using,
        runs_main,
        runs_pre,
        runs_pre_if,
        runs_post,
        runs_post_if,
        runs_steps,
        runs_image,
        runs_entrypoint,
        runs_args,
        inputs,
        runs_env,
        outputs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn load_node_action_manifest() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("action.yml"),
            r#"
name: Test Action
description: A test action
inputs:
  name:
    description: The name
    required: true
    default: world
outputs:
  result:
    description: The result
runs:
  using: node20
  main: index.js
  pre: setup.js
  pre-if: always()
  post: cleanup.js
"#,
        )
        .unwrap();

        let manifest = load_action_manifest(dir.path()).unwrap();
        assert_eq!(manifest.name, "Test Action");
        assert_eq!(manifest.runs_using, "node20");
        assert_eq!(manifest.runs_main.as_deref(), Some("index.js"));
        assert_eq!(manifest.runs_pre.as_deref(), Some("setup.js"));
        assert_eq!(manifest.runs_pre_if.as_deref(), Some("always()"));
        assert_eq!(manifest.runs_post.as_deref(), Some("cleanup.js"));
        assert!(manifest.inputs.is_some());
        let inputs = manifest.inputs.unwrap();
        assert!(inputs.contains_key("name"));
        assert_eq!(
            inputs["name"].get("default").and_then(|v| v.as_str()),
            Some("world")
        );
    }

    #[test]
    fn load_composite_action_manifest() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("action.yaml"),
            r#"
name: Composite
description: A composite action
runs:
  using: composite
  steps:
    - run: echo hello
      shell: bash
    - run: echo world
"#,
        )
        .unwrap();

        let manifest = load_action_manifest(dir.path()).unwrap();
        assert_eq!(manifest.runs_using, "composite");
        assert!(manifest.runs_steps.is_some());
        assert_eq!(manifest.runs_steps.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn load_docker_action_manifest() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("action.yml"),
            r#"
name: Docker Action
description: A docker action
runs:
  using: docker
  image: Dockerfile
  entrypoint: /entrypoint.sh
  args:
    - --flag
    - value
  pre-entrypoint: /pre.sh
  pre-if: success()
  post-entrypoint: /post.sh
  post-if: always()
"#,
        )
        .unwrap();

        let manifest = load_action_manifest(dir.path()).unwrap();
        assert_eq!(manifest.runs_using, "docker");
        assert_eq!(manifest.runs_image.as_deref(), Some("Dockerfile"));
        assert_eq!(manifest.runs_entrypoint.as_deref(), Some("/entrypoint.sh"));
        assert_eq!(manifest.runs_args.as_ref().unwrap(), &["--flag", "value"]);
        assert_eq!(manifest.runs_pre.as_deref(), Some("/pre.sh"));
        assert_eq!(manifest.runs_pre_if.as_deref(), Some("success()"));
        assert_eq!(manifest.runs_post.as_deref(), Some("/post.sh"));
        assert_eq!(manifest.runs_post_if.as_deref(), Some("always()"));
    }

    #[test]
    fn lifecycle_conditions_default_to_always_when_entrypoints_exist() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("action.yml"),
            r#"
name: Docker Action
runs:
  using: docker
  image: Dockerfile
  entrypoint: /entrypoint.sh
  pre-entrypoint: /pre.sh
  post-entrypoint: /post.sh
"#,
        )
        .unwrap();

        let manifest = load_action_manifest(dir.path()).unwrap();
        assert_eq!(manifest.runs_pre_if.as_deref(), Some("always()"));
        assert_eq!(manifest.runs_post_if.as_deref(), Some("always()"));
    }

    #[test]
    fn lifecycle_conditions_absent_without_entrypoints() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("action.yml"),
            r#"
name: Docker Action
runs:
  using: docker
  image: Dockerfile
  entrypoint: /entrypoint.sh
"#,
        )
        .unwrap();

        let manifest = load_action_manifest(dir.path()).unwrap();
        assert_eq!(manifest.runs_pre_if, None);
        assert_eq!(manifest.runs_post_if, None);
    }

    #[test]
    fn load_docker_action_manifest_with_dockerhub_image_and_optional_fields_absent() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("action.yml"),
            r#"
name: DockerHub Action
runs:
  using: docker
  image: docker://alpine:3.20
"#,
        )
        .unwrap();

        let manifest = load_action_manifest(dir.path()).unwrap();
        assert_eq!(manifest.runs_using, "docker");
        assert_eq!(manifest.runs_image.as_deref(), Some("docker://alpine:3.20"));
        assert_eq!(manifest.runs_entrypoint, None);
        assert_eq!(manifest.runs_args, None);
        assert_eq!(manifest.runs_env, None);
        assert_eq!(manifest.runs_pre, None);
        assert_eq!(manifest.runs_post, None);
    }

    #[test]
    fn action_yml_takes_precedence_over_action_yaml() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("action.yml"),
            r#"
name: Primary
runs:
  using: node20
  main: primary.js
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("action.yaml"),
            r#"
name: Secondary
runs:
  using: node20
  main: secondary.js
"#,
        )
        .unwrap();

        let manifest = load_action_manifest(dir.path()).unwrap();
        assert_eq!(manifest.name, "Primary");
        assert_eq!(manifest.runs_main.as_deref(), Some("primary.js"));
    }

    #[test]
    fn missing_runs_using_returns_error() {
        let dir = TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("action.yml"),
            r#"
name: Broken
runs:
  main: index.js
"#,
        )
        .unwrap();

        let err = load_action_manifest(dir.path()).unwrap_err();
        assert!(err.to_string().contains("runs.using"));
    }

    #[test]
    fn missing_manifest_returns_error() {
        let dir = TempDir::new().unwrap();
        let result = load_action_manifest(dir.path());
        assert!(result.is_err());
    }
}
