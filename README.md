# coreflow

`coreflow` 是一个最小 AI workflow kernel。公共入口是 `Graph`：注册 plug，加入 graph-local plug，声明 flow，运行 graph，再从 `GraphOutput` 读取结果。

## Graph-first 快速路径

```rust
use coreflow::{Graph, GraphRunStatus, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct EchoInput {
    value: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct EchoOutput {
    value: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let mut graph = Graph::new();

    graph
        .plugup("coreflow.echo.v1", |input: EchoInput| async move {
            Ok(EchoOutput { value: input.value })
        })?
        .plugin("echo", "coreflow.echo.v1")?;

    let result = graph.run(json!({ "value": "hello" })).await?;
    let output = result.output().get::<EchoOutput>("echo")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.value, "hello");
    Ok(())
}
```

核心 API 速查：

- `Graph::new()`：创建 workflow
- `plugup(kind, func)`：注册 Rust serde plug 实现
- `plugin(name, kind)`：把已注册 kind 加入当前 Graph
- `flowin(json!(...))`：声明 plug 依赖和字段来源
- `run(...)`：执行当前 checked graph
- `result.output().get::<T>(name)`：读取最新稳定输出

## Key concepts

- `Plug`：单一职责函数。它接收一个 serde-compatible input value，返回一个 output value 或结构化错误。
- `Flow`：plug 之间的依赖、反馈和字段流向。selector 只描述从哪里取值，业务转换放在 plug 中。
- `Graph`：公共 workflow surface。它保存 plug 注册、graph-local plug、flow 查询和运行入口。
- `GraphStore`：可序列化 graph 文件协议。它保存当前完整 graph、head 和历史 GraphCommit。
- `GraphResult`：一次运行的输出和运行事实。`GraphOutput` 是按 plug name 读取稳定输出的视图。
- `ExecutionPolicy`：一次 run 的调度策略，例如失败策略、最大并发和资源并发限制。
- `PickerStrategy`：一次 run 的公开 tick 选择策略，用于决定 ready tick 的启动顺序；默认是 FIFO。

## Run 配置

`Run` 收束一次运行的输入、seed、`ExecutionPolicy` 和 `PickerStrategy`。默认 `graph.run(json!(...))` 使用 FIFO picker；需要显式选择 ready tick 顺序时，可以通过 `Run::picker(...)` 传入公开的 `PickerStrategy`。

```rust
use coreflow::{PickerStrategy, Run, json};

let result = graph
    .run(Run::new(json!({})).picker(PickerStrategy::Lifo))
    .await?;
```

## Flow 示例

字段 flow 用 target-keyed JSON 声明。下面把 `extract_user.profile.email` 写入 `send_email.recipient`：

```rust
graph.flowin(json!({
    "send_email": {
        "recipient": "extract_user.profile.email",
        "display_name": "extract_user.profile.name"
    }
}))?;
```

更多可运行示例：

- `examples/manual_surface.rs`：最小 Graph surface。
- `examples/field_flow.rs`：嵌套字段选择和重命名。
- `examples/fan_in.rs`：多个上游 plug 组成下游输入。
- `examples/feedback_timeout.rs`：反馈 flow、timeout fact 和最终收束。
- `examples/loop_control.rs`：用反馈 flow 表达循环控制。
- `examples/branch_control.rs`：用 route 字段表达分支控制。
- `examples/concurrent_execution.rs`：用 `ExecutionPolicy` 展示并发执行。
- `examples/tool_registry.rs`：用 operation 字符串选择宿主 registry 中的函数。

运行示例：

```sh
cargo run --example field_flow
cargo run --example fan_in
cargo run --example feedback_timeout
cargo run --example loop_control
cargo run --example branch_control
cargo run --example concurrent_execution
cargo run --example tool_registry
```

## GraphStore

`graph.store()?` 导出当前 GraphStore。GraphStore 同时保留最后一次完整 `graph`、当前 `head` 和历史 `commits`，因此读取当前 graph 不需要回放提交链；审计或重放时可以使用 commit 链。

```rust
let store = graph.store()?;
let json = serde_json::to_string_pretty(&store)?;
```

Plug 实现不写入 GraphStore。导入 graph 文件后，宿主环境需要重新 `plugup` 对应 kind 的 Rust 实现。

## Design source

`docs/CORE_DESIGN.md` 是核心设计标准。公共 API、GraphStore 协议、kernel 状态机、示例集和评审清单都以该文件为准。
