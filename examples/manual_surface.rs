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

    graph.check()?;
    let result = graph.run(json!({ "value": "manual-ok" })).await?;
    let output = result.output().get::<EchoOutput>("echo")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.value, "manual-ok");
    println!("manual_surface: {}", output.value);

    Ok(())
}
