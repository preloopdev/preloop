//! Workflow and action YAML parsing.

use crate::{ActionMetadata, ParserError, Workflow};

/// Parse workflow YAML.
pub fn parse_workflow(input: &str) -> Result<Workflow, ParserError> {
    let mut value: serde_yaml::Value = serde_yaml::from_str(input)?;
    normalize_yaml_keys(&mut value);
    let workflow: Workflow = serde_yaml::from_value(value)?;
    if workflow.jobs.is_empty() {
        return Err(ParserError::EmptyJobs);
    }
    Ok(workflow)
}

/// Parse local action metadata from `action.yml` or `action.yaml`.
pub fn parse_action_metadata(input: &str) -> Result<ActionMetadata, ParserError> {
    let mut value: serde_yaml::Value = serde_yaml::from_str(input)?;
    normalize_yaml_keys(&mut value);
    Ok(serde_yaml::from_value(value)?)
}

fn normalize_yaml_keys(value: &mut serde_yaml::Value) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            if let Some(on_value) = map.remove(serde_yaml::Value::Bool(true)) {
                map.insert(serde_yaml::Value::String("on".to_owned()), on_value);
            }
            let keys = map.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                if !matches!(key, serde_yaml::Value::String(_)) {
                    if let Some(value) = map.remove(key.clone()) {
                        map.insert(serde_yaml::Value::String(yaml_key_to_string(&key)), value);
                    }
                }
            }
            for value in map.values_mut() {
                normalize_yaml_keys(value);
            }
        }
        serde_yaml::Value::Sequence(values) => {
            for value in values {
                normalize_yaml_keys(value);
            }
        }
        _ => {}
    }
}

fn yaml_key_to_string(value: &serde_yaml::Value) -> String {
    match value {
        serde_yaml::Value::String(value) => value.clone(),
        serde_yaml::Value::Bool(value) => value.to_string(),
        serde_yaml::Value::Number(value) => value.to_string(),
        serde_yaml::Value::Null => "null".to_owned(),
        other => serde_yaml::to_string(other)
            .unwrap_or_default()
            .trim()
            .to_owned(),
    }
}
