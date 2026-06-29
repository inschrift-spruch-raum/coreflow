use std::time::{Duration, Instant};

use coreflow::{ExecutionPolicy, FailurePolicy, Graph, GraphRunStatus, Run, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct WorkerOutput {
    value: String,
}

#[derive(Debug, Deserialize)]
struct JoinInput {
    left: WorkerOutput,
    right: WorkerOutput,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct JoinOutput {
    value: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let mut graph = Graph::new();

    graph
        .plugup("left_worker", |_: coreflow::Value| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(WorkerOutput {
                value: "left".to_string(),
            })
        })?
        .plugup("right_worker", |_: coreflow::Value| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(WorkerOutput {
                value: "right".to_string(),
            })
        })?
        .plugup("join", |input: JoinInput| async move {
            Ok(JoinOutput {
                value: format!("{}+{}", input.left.value, input.right.value),
            })
        })?
        .plugin("left_worker", "left_worker")?
        .plugin("right_worker", "right_worker")?
        .plugin("join", "join")?
        .flowin(json!({
            "join": {
                "left": "left_worker",
                "right": "right_worker"
            }
        }))?;

    let started = Instant::now();
    let result = graph
        .run(Run::new(json!({})).policy(ExecutionPolicy {
            failure: FailurePolicy::FailFast,
            max_concurrency: 2,
            resource_limits: Default::default(),
            inline_small_plugs: false,
        }))
        .await?;
    let elapsed = started.elapsed();
    let output = result.output().get::<JoinOutput>("join")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.value, "left+right");
    assert!(elapsed < Duration::from_millis(180));
    println!("concurrent_execution: {} in {:?}", output.value, elapsed);

    Ok(())
}
