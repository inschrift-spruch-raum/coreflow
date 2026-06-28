// graph 模块只做分层导出：surface 放用户 API，types 放存储协议，check/output 放派生结果。
pub(crate) mod check;
pub(crate) mod output;
mod surface;
mod types;

pub use output::{GraphOutput, GraphResult, GraphRunStatus, PendingApproval, RunEvent};
pub use surface::Graph;
pub(crate) use types::GraphLimits;
pub use types::{
    CommitId, GraphChange, GraphCommit, GraphMutationRequest, GraphStore, PlugKind, PlugName, Run,
};
