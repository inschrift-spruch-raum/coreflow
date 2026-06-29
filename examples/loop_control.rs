use coreflow::{Graph, GraphRunStatus, Run, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct StepInput {
    count: u64,
    done: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct StepOutput {
    count: u64,
    done: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ReceiptOutput {
    value: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let mut graph = Graph::new();

    graph
        .plugup("advance", |input: StepInput| async move {
            let count = if input.done {
                input.count
            } else {
                input.count + 1
            };
            Ok(StepOutput {
                count,
                done: count >= 3,
            })
        })?
        .plugup("receipt", |input: StepOutput| async move {
            Ok(ReceiptOutput {
                value: if input.done {
                    format!("loop stopped at {}", input.count)
                } else {
                    String::new()
                },
            })
        })?
        .plugin("advance", "advance")?
        .plugin("receipt", "receipt")?
        .flowin(json!({
            "advance": {
                "count": "advance.count",
                "done": "advance.done"
            },
            "receipt": "advance"
        }))?;

    let result = graph
        .run(Run::new(json!({ "advance": { "count": 0, "done": false } })).seeds(["advance"]))
        .await?;
    let output = result.output().get::<ReceiptOutput>("receipt")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.value, "loop stopped at 3");
    println!("loop_control: {}", output.value);

    Ok(())
}
