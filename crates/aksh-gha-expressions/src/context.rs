use std::collections::BTreeMap;

use serde_json::Value;

/// Hierarchical expression context.
#[derive(Debug, Clone)]
pub struct Context {
    pub(crate) roots: BTreeMap<String, Value>,
    pub(crate) success: bool,
    pub(crate) failure: bool,
    pub(crate) cancelled: bool,
    /// Workspace directory for hashFiles() evaluation.
    pub(crate) workspace_dir: Option<String>,
}

impl Context {
    /// Create an empty context with default successful status.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set status-function values for this evaluation.
    pub fn with_status(mut self, success: bool, failure: bool, cancelled: bool) -> Self {
        self.success = success;
        self.failure = failure;
        self.cancelled = cancelled;
        self
    }

    /// Set workspace directory for hashFiles() evaluation (F027).
    pub fn with_workspace(mut self, dir: String) -> Self {
        self.workspace_dir = Some(dir);
        self
    }

    /// Insert a root object.
    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        self.roots.insert(key.into(), value);
    }

    /// Resolve a dotted path such as `github.event_name`.
    /// Resolve a path such as `github.event_name`, with bracket access and wildcard support.
    ///
    /// - Numeric segment: array index (e.g. path built from `a[0]`)
    /// - `*` segment: collect all values from an object/array, then apply next segment
    pub fn resolve(&self, path: &[String]) -> Value {
        let Some((first, rest)) = path.split_first() else {
            return Value::Null;
        };
        let mut current = self.roots.get(first).cloned().unwrap_or(Value::Null);
        for segment in rest {
            if segment == "*" {
                // Object filter: collect values from object or array
                current = match current {
                    Value::Object(map) => Value::Array(map.into_values().collect()),
                    Value::Array(arr) => Value::Array(arr),
                    _ => Value::Null,
                };
                continue;
            }
            current = match current {
                Value::Object(map) => map.get(segment).cloned().unwrap_or(Value::Null),
                // After a wildcard, apply the next segment to each element
                Value::Array(arr) => Value::Array(
                    arr.into_iter()
                        .filter_map(|v| match v {
                            Value::Object(ref m) => m.get(segment).cloned(),
                            Value::Array(ref a) => segment
                                .parse::<usize>()
                                .ok()
                                .and_then(|i| a.get(i))
                                .cloned(),
                            _ => None,
                        })
                        .collect(),
                ),
                // Numeric index into array (from bracket access `a[0]`)
                _ => Value::Null,
            };
        }
        current
    }

    /// Resolve a path against an existing value (used for member access on expression results).
    pub fn resolve_value(mut current: Value, path: &[String]) -> Value {
        for segment in path {
            if segment == "*" {
                current = match current {
                    Value::Object(map) => Value::Array(map.into_values().collect()),
                    Value::Array(arr) => Value::Array(arr),
                    _ => Value::Null,
                };
                continue;
            }
            current = match current {
                Value::Object(map) => map.get(segment).cloned().unwrap_or(Value::Null),
                Value::Array(arr) => Value::Array(
                    arr.into_iter()
                        .filter_map(|v| match v {
                            Value::Object(ref m) => m.get(segment).cloned(),
                            Value::Array(ref a) => segment
                                .parse::<usize>()
                                .ok()
                                .and_then(|i| a.get(i))
                                .cloned(),
                            _ => None,
                        })
                        .collect(),
                ),
                _ => Value::Null,
            };
        }
        current
    }
}

impl Default for Context {
    fn default() -> Self {
        Self {
            roots: BTreeMap::new(),
            success: true,
            failure: false,
            cancelled: false,
            workspace_dir: None,
        }
    }
}
