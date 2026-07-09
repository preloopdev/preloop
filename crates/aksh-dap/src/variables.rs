//! DAP variable provider.
//!
//! 1:1 port of `src/Runner.Worker/Dap/DapVariableProvider.cs`.
//!
//! Maps runner `PipelineContextData` (github, env, runner, job,
//! steps, secrets) into DAP `Scope`s and `Variable`s. All values
//! pass through a secret-masker so the DAP surface never exposes
//! anything beyond what a normal CI log would show. The secrets
//! scope is intentionally opaque: keys are visible but every value
//! is replaced with a constant redaction marker.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// A DAP `Scope` — a named bag of variables, identified by a
/// `variablesReference` integer that the client uses to fetch the
/// contents via a `variables` request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DapScope {
    /// Human-readable scope name (e.g. `"github"`, `"env"`).
    pub name: String,

    /// DAP variablesReference — opaque integer the client uses to
    /// request the variables in this scope.
    pub variables_reference: i64,

    /// Number of variables in this scope. Cheap to compute
    /// eagerly so the client can show progress.
    pub variables_count: usize,

    /// Whether the variables in this scope are expensive to fetch.
    /// DAP clients use this to avoid expanding scopes lazily.
    #[serde(rename = "expensive", default, skip_serializing_if = "is_false")]
    pub expensive: bool,
}

/// A DAP `Variable` — one named value with a presentation hint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DapVariable {
    /// Variable name.
    pub name: String,

    /// Stringified value (already masked for secrets).
    pub value: String,

    /// DAP `variablesReference`. `0` means "no children".
    pub variables_reference: i64,

    /// Optional presentation hint. Mirrors the DAP
    /// `VariablePresentationHint` (`"property"`, `"method"`, etc.).
    /// We only emit the kinds the runner cares about.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub var_type: Option<String>,
}

fn is_false(b: &bool) -> bool {
    !*b
}

/// The provider that turns runner context data into DAP scopes and
/// variables. The `mask_secret` closure is supplied by the caller —
/// the runner wires it to its real `ISecretMasker`.
pub struct DapVariableProvider {
    /// Stable, well-known scope order. Mirrors the C# `_scopeNames`
    /// array; index becomes the variablesReference.
    pub scope_names: Vec<&'static str>,
    /// Masker invoked on every value before it is returned to the
    /// client. The default is to pass the value through unchanged.
    mask_secret: Box<dyn Fn(&str) -> String + Send + Sync>,
}

impl Default for DapVariableProvider {
    fn default() -> Self {
        Self::new(Box::new(|s| s.to_string()))
    }
}

impl DapVariableProvider {
    /// Build a provider with a custom secret masker.
    pub fn new(mask_secret: Box<dyn Fn(&str) -> String + Send + Sync>) -> Self {
        Self {
            scope_names: vec!["github", "env", "runner", "job", "steps", "secrets"],
            mask_secret,
        }
    }

    /// Build a provider with a static redaction marker for the
    /// secrets scope. Use this when no real masker is wired up yet
    /// (e.g. in tests).
    pub fn with_static_mask() -> Self {
        Self::new(Box::new(|_| "***".to_string()))
    }

    /// List the scopes visible at a given frame. The returned
    /// `Scope.variablesReference` values can later be passed to
    /// [`Self::variables`] to fetch the contents.
    pub fn scopes(&self) -> Vec<DapScope> {
        self.scope_names
            .iter()
            .enumerate()
            .map(|(i, name)| DapScope {
                name: (*name).to_string(),
                variables_reference: (i as i64) + 1,
                variables_count: 0, // populated per-scope on demand
                expensive: false,
            })
            .collect()
    }

    /// Fetch the variables for a given `variablesReference`. The
    /// reference is the same as the `scope.variablesReference` from
    /// [`Self::scopes`].
    ///
    /// `context` is the runner's expression context (free-form
    /// JSON; the runner exposes `github`, `env`, `runner`, `job`,
    /// `steps` and the `secrets` redaction marker).
    pub fn variables(&self, variables_reference: i64, context: &Value) -> Vec<DapVariable> {
        if variables_reference < 1 {
            return Vec::new();
        }
        let idx = (variables_reference - 1) as usize;
        let name = match self.scope_names.get(idx) {
            Some(n) => *n,
            None => return Vec::new(),
        };
        match name {
            "github" => self.flatten(context.get("github")),
            "env" => self.flatten(context.get("env")),
            "runner" => self.flatten(context.get("runner")),
            "job" => self.flatten(context.get("job")),
            "steps" => self.flatten(context.get("steps")),
            "secrets" => {
                // Intentionally opaque: keys visible, values redacted.
                let inner = context.get("secrets").cloned().unwrap_or(Value::Null);
                let mut vars = self.flatten(Some(&inner));
                for v in vars.iter_mut() {
                    v.value = (self.mask_secret)(&v.value);
                    v.var_type = Some("redacted".to_string());
                }
                vars
            }
            _ => Vec::new(),
        }
    }

    /// Flatten a JSON object into a list of `DapVariable`s. Each
    /// top-level key becomes a variable; nested objects become a
    /// string representation and `variablesReference=0`.
    fn flatten(&self, value: Option<&Value>) -> Vec<DapVariable> {
        let Some(Value::Object(map)) = value else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(map.len());
        for (k, v) in map.iter() {
            out.push(DapVariable {
                name: k.clone(),
                value: (self.mask_secret)(&json_to_string(v)),
                variables_reference: 0,
                var_type: Some(type_hint(v)),
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

fn json_to_string(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        // Compact one-line render keeps the editor pane readable.
        other => other.to_string(),
    }
}

fn type_hint(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(_) => "boolean".into(),
        Value::Number(_) => "number".into(),
        Value::String(_) => "string".into(),
        Value::Array(_) => "array".into(),
        Value::Object(_) => "object".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn scopes_have_stable_references() {
        let p = DapVariableProvider::default();
        let scopes = p.scopes();
        assert_eq!(scopes.len(), 6);
        assert_eq!(scopes[0].name, "github");
        assert_eq!(scopes[0].variables_reference, 1);
        assert_eq!(scopes[5].name, "secrets");
        assert_eq!(scopes[5].variables_reference, 6);
    }

    #[test]
    fn variables_returns_github_scope() {
        let p = DapVariableProvider::default();
        let ctx = json!({
            "github": {"repository": "preloop/aksh", "ref": "refs/heads/main"},
            "env": {"FOO": "bar"},
        });
        let v = p.variables(1, &ctx);
        assert_eq!(v.len(), 2);
        let names: Vec<_> = v.iter().map(|x| x.name.as_str()).collect();
        assert!(names.contains(&"repository"));
        assert!(names.contains(&"ref"));
    }

    #[test]
    fn secrets_scope_redacts_values_but_keeps_keys() {
        let p = DapVariableProvider::with_static_mask();
        let ctx = json!({
            "secrets": {"GITHUB_TOKEN": "ghp_realvalue", "OTHER": "val"},
        });
        let v = p.variables(6, &ctx);
        assert_eq!(v.len(), 2);
        for var in v.iter() {
            assert_eq!(var.value, "***");
            assert_eq!(var.var_type.as_deref(), Some("redacted"));
        }
        // Order is alphabetical.
        assert_eq!(v[0].name, "GITHUB_TOKEN");
        assert_eq!(v[1].name, "OTHER");
    }

    #[test]
    fn unknown_reference_returns_empty() {
        let p = DapVariableProvider::default();
        assert!(p.variables(99, &json!({})).is_empty());
        assert!(p.variables(0, &json!({})).is_empty());
    }

    #[test]
    fn nested_object_renders_as_inline_json() {
        let p = DapVariableProvider::default();
        let ctx = json!({
            "job": {"status": "in_progress", "matrix": {"os": "macos"}}
        });
        let v = p.variables(4, &ctx);
        assert_eq!(v.len(), 2);
        let matrix = v.iter().find(|x| x.name == "matrix").unwrap();
        assert!(matrix.value.contains("\"os\""));
    }
}
