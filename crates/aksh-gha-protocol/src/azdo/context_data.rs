use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

// ─── Context data DTOs ────────────────────────────────────────────────────

/// Pipeline context data — the union type for all context values.
///
/// In GitHub's SDK this is `PipelineContextData`, a discriminated union
/// that can hold a string, number, boolean, array, dictionary, or
/// `ContextDictionary`. We model it as a tagged enum.
///
/// Upstream source: `Pipelines.ContextData.PipelineContextData.cs`
#[derive(Debug, Clone)]
pub enum PipelineContextData {
    Null,
    String(String),
    Bool(bool),
    Number(f64),
    Array(Vec<PipelineContextData>),
    Dict(BTreeMap<String, PipelineContextData>),
}

impl PipelineContextData {
    /// Convert a plain `serde_json::Value` (expression-context JSON) into
    /// `PipelineContextData`.
    ///
    /// This is **not** the wire codec (which handles the tagged `{"t":…}` format
    /// used by `Serialize`/`Deserialize`). This converts the simple JSON shape
    /// used in expression evaluation, job-message context data, and concurrency
    /// group resolution.
    ///
    /// `Value::Null` maps to `PipelineContextData::Null` — preserving null
    /// semantics throughout the parser→server→runner pipeline.
    pub fn from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::String(s) => PipelineContextData::String(s.clone()),
            serde_json::Value::Bool(b) => PipelineContextData::Bool(*b),
            serde_json::Value::Number(n) => PipelineContextData::Number(n.as_f64().unwrap_or(0.0)),
            serde_json::Value::Array(arr) => {
                PipelineContextData::Array(arr.iter().map(PipelineContextData::from_json).collect())
            }
            serde_json::Value::Object(map) => PipelineContextData::Dict(
                map.iter()
                    .map(|(k, v)| (k.clone(), PipelineContextData::from_json(v)))
                    .collect(),
            ),
            serde_json::Value::Null => PipelineContextData::Null,
        }
    }

    /// Convert `PipelineContextData` back to a plain `serde_json::Value`.
    ///
    /// Inverse of [`from_json`](Self::from_json). `Null` round-trips as
    /// `Value::Null`.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            PipelineContextData::Null => serde_json::Value::Null,
            PipelineContextData::String(s) => serde_json::Value::String(s.clone()),
            PipelineContextData::Bool(b) => serde_json::Value::Bool(*b),
            PipelineContextData::Number(n) => serde_json::json!(n),
            PipelineContextData::Array(items) => {
                serde_json::Value::Array(items.iter().map(PipelineContextData::to_json).collect())
            }
            PipelineContextData::Dict(map) => {
                let mut obj = serde_json::Map::new();
                for (k, v) in map {
                    obj.insert(k.clone(), v.to_json());
                }
                serde_json::Value::Object(obj)
            }
        }
    }
}

impl Serialize for PipelineContextData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::{SerializeMap, SerializeSeq};

        match self {
            PipelineContextData::Null => serializer.serialize_none(),
            PipelineContextData::String(value) => serializer.serialize_str(value),
            PipelineContextData::Bool(value) => serializer.serialize_bool(*value),
            PipelineContextData::Number(value) => serializer.serialize_f64(*value),
            PipelineContextData::Array(values) => {
                struct ArrayValues<'a>(&'a [PipelineContextData]);

                impl Serialize for ArrayValues<'_> {
                    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                    where
                        S: Serializer,
                    {
                        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                        for value in self.0 {
                            seq.serialize_element(value)?;
                        }
                        seq.end()
                    }
                }

                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("t", &1)?;
                map.serialize_entry("a", &ArrayValues(values))?;
                map.end()
            }
            PipelineContextData::Dict(values) => {
                let pairs: Vec<PipelineContextDataPair<'_>> = values
                    .iter()
                    .map(|(key, value)| PipelineContextDataPair { key, value })
                    .collect();
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("t", &2)?;
                map.serialize_entry("d", &pairs)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for PipelineContextData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        pipeline_context_from_json(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Serialize)]
struct PipelineContextDataPair<'a> {
    #[serde(rename = "k")]
    key: &'a str,
    #[serde(rename = "v")]
    value: &'a PipelineContextData,
}

fn pipeline_context_from_json(value: serde_json::Value) -> Result<PipelineContextData, String> {
    match value {
        serde_json::Value::String(value) => Ok(PipelineContextData::String(value)),
        serde_json::Value::Bool(value) => Ok(PipelineContextData::Bool(value)),
        serde_json::Value::Number(value) => Ok(PipelineContextData::Number(
            value.as_f64().unwrap_or_default(),
        )),
        serde_json::Value::Array(values) => values
            .into_iter()
            .map(pipeline_context_from_json)
            .collect::<Result<Vec<_>, _>>()
            .map(PipelineContextData::Array),
        serde_json::Value::Object(mut object) => {
            match object.remove("t").and_then(|value| value.as_i64()) {
                None | Some(0) => Ok(PipelineContextData::String(
                    object
                        .remove("s")
                        .and_then(|value| value.as_str().map(str::to_owned))
                        .unwrap_or_default(),
                )),
                Some(1) => object
                    .remove("a")
                    .and_then(|value| value.as_array().cloned())
                    .unwrap_or_default()
                    .into_iter()
                    .map(pipeline_context_from_json)
                    .collect::<Result<Vec<_>, _>>()
                    .map(PipelineContextData::Array),
                Some(2) | Some(5) => {
                    let mut values = BTreeMap::new();
                    let pairs = object
                        .remove("d")
                        .and_then(|value| value.as_array().cloned())
                        .unwrap_or_default();
                    for pair in pairs {
                        let Some(pair) = pair.as_object() else {
                            continue;
                        };
                        let Some(key) = pair.get("k").and_then(|value| value.as_str()) else {
                            continue;
                        };
                        let value = pair.get("v").cloned().unwrap_or(serde_json::Value::Null);
                        values.insert(key.to_owned(), pipeline_context_from_json(value)?);
                    }
                    Ok(PipelineContextData::Dict(values))
                }
                Some(3) => Ok(PipelineContextData::Bool(
                    object
                        .remove("b")
                        .and_then(|value| value.as_bool())
                        .unwrap_or_default(),
                )),
                Some(4) => Ok(PipelineContextData::Number(
                    object
                        .remove("n")
                        .and_then(|value| value.as_f64())
                        .unwrap_or_default(),
                )),
                Some(other) => Err(format!("unsupported PipelineContextData type {other}")),
            }
        }
        serde_json::Value::Null => Ok(PipelineContextData::Null),
    }
}
