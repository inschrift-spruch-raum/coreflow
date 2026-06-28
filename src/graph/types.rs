use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _, ser::SerializeMap};

use crate::{
    FieldPath, SourceSelector, Value,
    kernel::{ExecutionPolicy, PickerStrategy},
};

pub type CommitId = String;

// PlugKind 标识可复用的实现类型；同一种 kind 可以挂载成多个 graph-local plug。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlugKind(String);

impl PlugKind {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for PlugKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// PlugName 标识当前 graph 内的节点名，flow 和输出都按这个名字寻址。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlugName(String);

impl PlugName {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl std::fmt::Display for PlugName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for PlugName {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

// GraphStore 是可序列化文件协议：保留当前完整 graph，也保留提交链供审计或重放。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStore {
    pub head: CommitId,
    pub graph: crate::Graph,
    pub commits: BTreeMap<CommitId, GraphCommit>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphCommit {
    pub id: CommitId,
    pub parent: Option<CommitId>,
    pub message: String,
    pub changes: Vec<GraphChange>,
}

#[derive(Debug, Clone)]
pub enum GraphChange {
    PlugIn {
        kind: PlugKind,
        name: PlugName,
    },
    PlugOut(PlugName),
    FlowIn {
        target: PlugName,
        input: FieldPath,
        source: SourceSelector,
    },
    FlowOut {
        target: PlugName,
        input: FieldPath,
    },
    Replace {
        graph: Box<crate::Graph>,
    },
}

impl Serialize for GraphChange {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::PlugIn { kind, name } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("operation", "plug_in")?;
                map.serialize_entry("kind", kind)?;
                map.serialize_entry("name", name)?;
                map.end()
            }
            Self::PlugOut(name) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("operation", "plug_out")?;
                map.serialize_entry("name", name)?;
                map.end()
            }
            Self::FlowIn {
                target,
                input,
                source,
            } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("operation", "flow_in")?;
                map.serialize_entry(&target_selector(target, input), source)?;
                map.end()
            }
            Self::FlowOut { target, input } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("operation", "flow_out")?;
                map.serialize_entry("target", &target_selector(target, input))?;
                map.end()
            }
            Self::Replace { graph } => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("operation", "replace")?;
                map.serialize_entry("graph", graph)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for GraphChange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = crate::Value::deserialize(deserializer)?;
        let object = value.as_object().ok_or_else(|| {
            D::Error::custom("graph change must be an object with an operation field")
        })?;
        let operation = object
            .get("operation")
            .and_then(crate::Value::as_str)
            .ok_or_else(|| D::Error::custom("graph change operation must be a string"))?;

        match operation {
            "plug_in" => Ok(Self::PlugIn {
                kind: value_field(&value, "kind")?,
                name: value_field(&value, "name")?,
            }),
            "plug_out" => Ok(Self::PlugOut(value_field(&value, "name")?)),
            "flow_in" => {
                let (target, source) = flow_in_entry(object).map_err(D::Error::custom)?;
                let (target, input) = parse_target_selector(target).map_err(D::Error::custom)?;
                Ok(Self::FlowIn {
                    target,
                    input,
                    source: SourceSelector::deserialize(source.clone())
                        .map_err(D::Error::custom)?,
                })
            }
            "flow_out" => {
                let target = string_field(&value, "target")?;
                let (target, input) = parse_target_selector(target).map_err(D::Error::custom)?;
                Ok(Self::FlowOut { target, input })
            }
            "replace" => Ok(Self::Replace {
                graph: Box::new(value_field(&value, "graph")?),
            }),
            _ => Err(D::Error::custom(format!(
                "unknown graph change operation `{operation}`"
            ))),
        }
    }
}

fn value_field<T, E>(value: &crate::Value, field: &str) -> Result<T, E>
where
    T: for<'de> Deserialize<'de>,
    E: serde::de::Error,
{
    let field_value = value
        .get(field)
        .ok_or_else(|| E::custom(format!("graph change missing `{field}`")))?;
    T::deserialize(field_value.clone()).map_err(E::custom)
}

fn string_field<'a, E>(value: &'a crate::Value, field: &str) -> Result<&'a str, E>
where
    E: serde::de::Error,
{
    value
        .get(field)
        .and_then(crate::Value::as_str)
        .ok_or_else(|| E::custom(format!("graph change `{field}` must be a string")))
}

fn flow_in_entry<'a>(
    object: &'a serde_json::Map<String, Value>,
) -> Result<(&'a str, &'a Value), String> {
    let mut entries = object
        .iter()
        .filter(|(field, _)| field.as_str() != "operation");
    let Some((target, source)) = entries.next() else {
        return Err("graph change flow_in missing target-source mapping".to_string());
    };
    if entries.next().is_some() {
        return Err(
            "graph change flow_in must contain exactly one target-source mapping".to_string(),
        );
    }
    Ok((target, source))
}

fn target_selector(target: &PlugName, input: &FieldPath) -> String {
    if input.0.is_empty() {
        target.to_string()
    } else {
        format!("{target}.{}", input.0)
    }
}

fn parse_target_selector(selector: &str) -> Result<(PlugName, FieldPath), String> {
    if selector.is_empty() {
        return Err("graph change target must include a plug name".to_string());
    }
    if let Some((plug, path)) = selector.split_once('.') {
        if plug.is_empty() {
            return Err("graph change target must include a plug name".to_string());
        }
        Ok((PlugName::new(plug), FieldPath::new(path)))
    } else {
        Ok((PlugName::new(selector), FieldPath::new("")))
    }
}

// Run 收束一次执行的输入、入口 seed、失败策略和 ready tick 选择策略。
#[derive(Debug, Clone)]
pub struct Run {
    pub(crate) initial: Value,
    pub(crate) seeds: Option<Vec<PlugName>>,
    pub(crate) policy: ExecutionPolicy,
    pub(crate) picker: PickerStrategy,
}

impl Run {
    #[must_use]
    pub fn new(initial: Value) -> Self {
        Self {
            initial,
            seeds: None,
            policy: ExecutionPolicy::default(),
            picker: PickerStrategy::Fifo,
        }
    }

    #[must_use]
    pub fn resume(previous: &crate::GraphResult) -> Self {
        let mut initial = serde_json::Map::new();
        for (plug, value) in &previous.outputs {
            initial.insert(plug.to_string(), value.clone());
        }
        Self::new(Value::Object(initial))
    }

    #[must_use]
    pub fn seeds(mut self, plugs: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        self.seeds = Some(
            plugs
                .into_iter()
                .map(|plug| PlugName::new(plug.as_ref()))
                .collect(),
        );
        self
    }

    #[must_use]
    pub fn policy(mut self, policy: ExecutionPolicy) -> Self {
        self.policy = policy;
        self
    }

    #[must_use]
    pub fn picker(mut self, picker: PickerStrategy) -> Self {
        self.picker = picker;
        self
    }
}

impl From<Value> for Run {
    fn from(value: Value) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GraphLimits {
    pub plugs: usize,
    pub flow_edges: usize,
    pub path_depth: usize,
    pub concurrency: usize,
}

impl Default for GraphLimits {
    fn default() -> Self {
        Self {
            plugs: 1024,
            flow_edges: 4096,
            path_depth: 64,
            concurrency: 256,
        }
    }
}

impl GraphLimits {
    pub(crate) fn check(self) -> crate::CoreResult<Self> {
        for (limit, value) in [
            ("max_plugs", self.plugs),
            ("max_flow_edges", self.flow_edges),
            ("max_path_depth", self.path_depth),
            ("max_concurrency", self.concurrency),
        ] {
            if value == 0 {
                return Err(crate::CoreError::ResourceLimitExceeded {
                    limit: limit.to_string(),
                    value,
                });
            }
        }
        Ok(self)
    }
}
