use std::{collections::BTreeMap, sync::Arc};

use coreflow::{Graph, GraphRunStatus, Value, json};
use serde::{Deserialize, Serialize};

type Tool = fn(Value) -> coreflow::CoreResult<Value>;

#[derive(Debug, Deserialize)]
struct RequestInput {
    text: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PlanOutput {
    operation: String,
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct ExecuteInput {
    operation: String,
    payload: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct ExecuteOutput {
    value: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let tools = Arc::new(BTreeMap::from([
        ("summarize".to_string(), summarize as Tool),
        ("send_email".to_string(), send_email as Tool),
    ]));
    let executor_tools = Arc::clone(&tools);

    let mut graph = Graph::new();

    graph
        .plugup("planner", |input: RequestInput| async move {
            Ok(PlanOutput {
                operation: if input.text.contains('@') {
                    "send_email".to_string()
                } else {
                    "summarize".to_string()
                },
                payload: json!({ "text": input.text }),
            })
        })?
        .plugup("executor", move |input: ExecuteInput| {
            let tools = Arc::clone(&executor_tools);
            async move {
                let tool = tools.get(&input.operation).ok_or_else(|| {
                    coreflow::CoreError::InvalidFlow {
                        message: format!("unknown tool `{}`", input.operation),
                    }
                })?;
                let value = tool(input.payload)?;
                let output: ExecuteOutput = serde_json::from_value(value)?;
                Ok(output)
            }
        })?
        .plugin("planner", "planner")?
        .plugin("executor", "executor")?
        .flowin(json!({
            "executor": {
                "operation": "planner.operation",
                "payload": "planner.payload"
            }
        }))?;

    let result = graph
        .run(json!({ "text": "Ada wants a short status" }))
        .await?;
    let output = result.output().get::<ExecuteOutput>("executor")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.value, "summary: Ada wants...");
    println!("tool_registry: {}", output.value);

    Ok(())
}

fn summarize(payload: Value) -> coreflow::CoreResult<Value> {
    let text = payload.get("text").and_then(Value::as_str).ok_or_else(|| {
        coreflow::CoreError::InvalidFlow {
            message: "summarize payload missing text".to_string(),
        }
    })?;
    Ok(json!({ "value": format!("summary: {}...", first_words(text, 2)) }))
}

fn send_email(payload: Value) -> coreflow::CoreResult<Value> {
    let text = payload.get("text").and_then(Value::as_str).ok_or_else(|| {
        coreflow::CoreError::InvalidFlow {
            message: "send_email payload missing text".to_string(),
        }
    })?;
    Ok(json!({ "value": format!("email sent: {text}") }))
}

fn first_words(text: &str, count: usize) -> String {
    text.split_whitespace()
        .take(count)
        .collect::<Vec<_>>()
        .join(" ")
}
