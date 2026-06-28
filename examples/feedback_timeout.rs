use coreflow::{Graph, GraphRunStatus, Run, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct DraftInput {
    count: u64,
    #[serde(default)]
    route: Option<String>,
    #[serde(default)]
    prompt: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CountOutput {
    count: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClockOutput {
    expired: bool,
}

#[derive(Debug, Deserialize)]
struct RouteInput {
    count: u64,
    expired: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct RouteOutput {
    count: u64,
    route: String,
    prompt: String,
    final_text: String,
}

#[derive(Debug, Deserialize)]
struct FinalInput {
    route: String,
    final_text: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FinalOutput {
    value: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let mut graph = Graph::new();

    graph
        .plugup("clock", |_: coreflow::Value| async move {
            Ok(ClockOutput { expired: false })
        })?
        .plugup("draft", |input: DraftInput| async move {
            let should_continue = input.route.as_deref().is_none_or(|route| route == "draft")
                && input
                    .prompt
                    .as_deref()
                    .is_none_or(|prompt| !prompt.is_empty());
            Ok(CountOutput {
                count: if should_continue {
                    (input.count + 1).min(2)
                } else {
                    input.count
                },
            })
        })?
        .plugup("review", |input: CountOutput| async move { Ok(input) })?
        .plugup("route_review", |input: RouteInput| async move {
            let (route, final_text, prompt) = if input.expired || input.count >= 2 {
                (
                    "final_receipt".to_string(),
                    "ready".to_string(),
                    String::new(),
                )
            } else {
                ("draft".to_string(), String::new(), "continue".to_string())
            };
            Ok(RouteOutput {
                count: input.count,
                route,
                prompt,
                final_text,
            })
        })?
        .plugup("final_receipt", |input: FinalInput| async move {
            Ok(FinalOutput {
                value: if input.route == "final_receipt" {
                    input.final_text
                } else {
                    String::new()
                },
            })
        })?
        .plugin("clock", "clock")?
        .plugin("draft", "draft")?
        .plugin("review", "review")?
        .plugin("route_review", "route_review")?
        .plugin("final_receipt", "final_receipt")?
        .flowin(json!({
            "review": "draft",
            "route_review": {
                "count": "review.count",
                "expired": "clock.expired"
            },
            "draft": {
                "count": "route_review.count",
                "route": "route_review.route",
                "prompt": "route_review.prompt"
            },
            "final_receipt": {
                "route": "route_review.route",
                "final_text": "route_review.final_text"
            }
        }))?;

    let result = graph
        .run(Run::new(json!({ "draft": { "count": 0 }, "clock": {} })).seeds(["draft", "clock"]))
        .await?;
    let output = result.output().get::<FinalOutput>("final_receipt")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.value, "ready");
    println!("feedback_timeout: {}", output.value);

    Ok(())
}
