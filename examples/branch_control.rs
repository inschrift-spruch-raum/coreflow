use coreflow::{Graph, GraphRunStatus, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ClassifyInput {
    score: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct RouteOutput {
    score: u64,
    route: String,
}

#[derive(Debug, Deserialize)]
struct BranchInput {
    score: u64,
    route: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BranchOutput {
    message: String,
}

#[derive(Debug, Deserialize)]
struct MergeInput {
    approved: String,
    rejected: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct MergeOutput {
    value: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let mut graph = Graph::new();

    graph
        .plugup("classify", |input: ClassifyInput| async move {
            Ok(RouteOutput {
                score: input.score,
                route: if input.score >= 80 {
                    "approved".to_string()
                } else {
                    "rejected".to_string()
                },
            })
        })?
        .plugup("approved_branch", |input: BranchInput| async move {
            Ok(BranchOutput {
                message: if input.route == "approved" {
                    format!("approved score {}", input.score)
                } else {
                    String::new()
                },
            })
        })?
        .plugup("rejected_branch", |input: BranchInput| async move {
            Ok(BranchOutput {
                message: if input.route == "rejected" {
                    format!("rejected score {}", input.score)
                } else {
                    String::new()
                },
            })
        })?
        .plugup("merge", |input: MergeInput| async move {
            Ok(MergeOutput {
                value: if input.approved.is_empty() {
                    input.rejected
                } else {
                    input.approved
                },
            })
        })?
        .plugin("classify", "classify")?
        .plugin("approved_branch", "approved_branch")?
        .plugin("rejected_branch", "rejected_branch")?
        .plugin("merge", "merge")?
        .flowin(json!({
            "approved_branch": {
                "score": "classify.score",
                "route": "classify.route"
            },
            "rejected_branch": {
                "score": "classify.score",
                "route": "classify.route"
            },
            "merge": {
                "approved": "approved_branch.message",
                "rejected": "rejected_branch.message"
            }
        }))?;

    let result = graph.run(json!({ "score": 86 })).await?;
    let output = result.output().get::<MergeOutput>("merge")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.value, "approved score 86");
    println!("branch_control: {}", output.value);

    Ok(())
}
