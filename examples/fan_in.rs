use coreflow::{Graph, GraphRunStatus, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Part {
    value: String,
}

#[derive(Debug, Deserialize)]
struct ComposeInput {
    left: Part,
    right: Part,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ComposeOutput {
    value: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let mut graph = Graph::new();

    graph
        .plugup("left", |_: coreflow::Value| async move {
            Ok(Part {
                value: "hello".to_string(),
            })
        })?
        .plugup("right", |_: coreflow::Value| async move {
            Ok(Part {
                value: "world".to_string(),
            })
        })?
        .plugup("compose", |input: ComposeInput| async move {
            Ok(ComposeOutput {
                value: format!("{} {}", input.left.value, input.right.value),
            })
        })?
        .plugin("left", "left")?
        .plugin("right", "right")?
        .plugin("compose", "compose")?
        .flowin(json!({ "compose": ["left", "right"] }))?;

    let result = graph.run(json!({})).await?;
    let output = result.output().get::<ComposeOutput>("compose")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.value, "hello world");
    println!("fan_in: {}", output.value);

    Ok(())
}
