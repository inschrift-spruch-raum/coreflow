use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{CoreResult, Value};

// GraphRunStatus 描述一次运行是否已收束；需要人工审批时会通过事件和输出表达。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphRunStatus {
    Idle,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingApproval {
    #[serde(default = "pending_approval_true")]
    pub pending_approval: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl PendingApproval {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            pending_approval: true,
            reason: Some(reason.into()),
        }
    }

    pub(crate) fn from_value(value: &Value) -> Option<Self> {
        if value
            .as_object()
            .and_then(|object| object.get("pending_approval"))
            .and_then(Value::as_bool)
            != Some(true)
        {
            return None;
        }
        let pending: Self = serde_json::from_value(value.clone()).ok()?;
        pending.pending_approval.then_some(pending)
    }
}

fn pending_approval_true() -> bool {
    true
}

// RunEvent 是 kernel 的可审计事实流，调用方可用它解释调度、失败和等待时间。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunEvent {
    GraphStarted,
    TickQueued {
        plug: crate::PlugName,
        tick: u64,
    },
    PlugInputBuilt {
        plug: crate::PlugName,
        tick: u64,
    },
    JobStarted {
        plug: crate::PlugName,
        tick: u64,
    },
    JobDone {
        plug: crate::PlugName,
        tick: u64,
    },
    JobFailed {
        plug: crate::PlugName,
        tick: u64,
        error: crate::CoreError,
    },
    FlowPropagated {
        source: crate::PlugName,
        target: crate::PlugName,
    },
    Duration {
        micros: u128,
    },
    TickWaitTime {
        plug: crate::PlugName,
        tick: u64,
        micros: u128,
    },
    PendingApproval {
        plug: crate::PlugName,
        tick: u64,
        reason: Option<String>,
    },
    GraphIdle,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphResult {
    pub graph_commit: crate::CommitId,
    pub outputs: BTreeMap<crate::PlugName, Value>,
    pub events: Vec<RunEvent>,
    pub status: GraphRunStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct OutputVersion {
    pub plug: crate::PlugName,
    pub tick: u64,
    pub version: u64,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct DoneTick {
    pub plug: crate::PlugName,
    pub tick: u64,
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UnfinishedTick {
    pub plug: crate::PlugName,
    pub tick: u64,
    pub state: UnfinishedTickState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UnfinishedTickState {
    BlockedByFailure,
    Superseded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PlugFailure {
    pub plug: crate::PlugName,
    pub tick: u64,
    pub error: crate::CoreError,
}

impl GraphResult {
    #[must_use]
    pub fn output(&self) -> GraphOutput<'_> {
        GraphOutput {
            outputs: &self.outputs,
        }
    }
}

// GraphOutput 是稳定输出视图：按 plug name 解码最终可见值，而不是暴露内部版本表。
#[derive(Debug, Clone, Copy)]
pub struct GraphOutput<'a> {
    outputs: &'a BTreeMap<crate::PlugName, Value>,
}

impl GraphOutput<'_> {
    /// # Errors
    ///
    /// 当指定 plug 没有稳定输出，或输出无法解码为目标类型时返回错误。
    pub fn get<T: DeserializeOwned>(&self, plug: &str) -> CoreResult<T> {
        let value = self
            .outputs
            .get(&crate::PlugName::new(plug))
            .ok_or_else(|| crate::CoreError::UnknownPlug {
                name: plug.to_string(),
            })?;

        serde_json::from_value(value.clone()).map_err(crate::CoreError::from)
    }
}
