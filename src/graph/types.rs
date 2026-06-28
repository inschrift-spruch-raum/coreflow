use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphMutationRequest {
    pub message: String,
    pub changes: Vec<GraphChange>,
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
