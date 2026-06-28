// crate 的公开入口保持 Graph-first：外部只需要从这里拿到建图、运行和取值类型。
mod error;
pub(crate) mod flow;
mod graph;
mod kernel;
mod plug;
mod value;

pub use error::{CoreError, CoreResult};
pub use flow::{FieldPath, Flow, InputBind, InputMap, PlugInput, SourceSelector};
pub use graph::{CommitId, GraphChange, GraphCommit, GraphStore, PlugKind, PlugName, Run};
pub use graph::{Graph, GraphOutput, GraphResult, GraphRunStatus, PendingApproval, RunEvent};
pub use kernel::{ExecutionPolicy, FailurePolicy, PickerStrategy};
pub(crate) use plug::{Plug, PlugImplementation};
pub use serde_json::json;
pub use value::{JsonValueCodec, Value, ValueCodec};
