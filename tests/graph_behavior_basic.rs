use coreflow::{CoreError, Graph, GraphRunStatus, json};
use serde::{Deserialize, Serialize};
use tokio::sync::Barrier;

#[derive(Debug, Deserialize)]
struct ExtractInput {
    profile: Profile,
}

#[derive(Debug, Deserialize)]
struct Profile {
    email: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct ExtractOutput {
    profile: ProfileOutput,
}

#[derive(Debug, Serialize)]
struct ProfileOutput {
    email: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct EmailInput {
    recipient: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct EmailOutput {
    sent_to: String,
    greeting: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NamedOutput {
    value: String,
}

#[derive(Debug, Deserialize)]
struct SerdeOnlyInput {
    value: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct SerdeOnlyOutput {
    value: String,
}

#[derive(Debug, Deserialize)]
struct JoinInput {
    left: String,
    right: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct JoinOutput {
    combined: String,
}

#[tokio::test]
async fn graph_runs_linear_serde_plugs_with_field_flow() {
    let mut graph = Graph::new();

    graph
        .plugup("extract_user", |input: ExtractInput| async move {
            Ok(ExtractOutput {
                profile: ProfileOutput {
                    email: input.profile.email,
                    name: input.profile.name,
                },
            })
        })
        .unwrap()
        .plugup("send_email", |input: EmailInput| async move {
            Ok(EmailOutput {
                sent_to: input.recipient,
                greeting: format!("Hello {}", input.display_name),
            })
        })
        .unwrap()
        .plugin("extract_user", "extract_user")
        .unwrap()
        .plugin("send_email", "send_email")
        .unwrap()
        .flowin(json!({
            "send_email": {
                "recipient": "extract_user.profile.email",
                "display_name": "extract_user.profile.name"
            }
        }))
        .unwrap();

    graph.check().unwrap();
    let result = graph
        .run(json!({
            "profile": {
                "email": "ada@example.com",
                "name": "Ada"
            }
        }))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(
        result.output().get::<EmailOutput>("send_email").unwrap(),
        EmailOutput {
            sent_to: "ada@example.com".to_string(),
            greeting: "Hello Ada".to_string(),
        },
        "downstream plug should receive fields selected from upstream output"
    );
}

#[tokio::test]
async fn graph_plugup_accepts_serde_only_types() {
    let mut graph = Graph::new();

    graph
        .plugup("serde_only", |input: SerdeOnlyInput| async move {
            Ok(SerdeOnlyOutput { value: input.value })
        })
        .unwrap()
        .plugin("serde_only", "serde_only")
        .unwrap();

    let result = graph.run(json!({ "value": "serde" })).await.unwrap();

    assert_eq!(
        result
            .output()
            .get::<SerdeOnlyOutput>("serde_only")
            .unwrap(),
        SerdeOnlyOutput {
            value: "serde".to_string()
        },
        "plugup should register plain serde plug types without extra validation layers"
    );
}

#[tokio::test]
async fn graph_check_rejects_flow_from_unknown_source_plug() {
    let mut graph = Graph::new();

    graph
        .plugup("send_email", |input: EmailInput| async move {
            Ok(EmailOutput {
                sent_to: input.recipient,
                greeting: format!("Hello {}", input.display_name),
            })
        })
        .unwrap()
        .plugin("send_email", "send_email")
        .unwrap()
        .flowin(json!({
            "send_email": {
                "recipient": "missing_user.profile.email"
            }
        }))
        .unwrap();

    let error = graph.check().unwrap_err();

    assert_eq!(
        error,
        CoreError::UnknownFlowSource {
            target: "send_email".to_string(),
            source: "missing_user".to_string(),
        },
        "check should reject flow selectors that reference an unregistered source plug"
    );
}

#[tokio::test]
async fn graph_check_rejects_flow_to_unknown_target_plug() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "source".to_string(),
            })
        })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .flowin(json!({ "missing_target": "source" }))
        .unwrap();

    let error = graph.check().unwrap_err();

    assert_eq!(
        error,
        CoreError::UnknownFlowTarget {
            target: "missing_target".to_string()
        },
        "check should reject flow declarations keyed by an unregistered target plug"
    );
}

#[tokio::test]
async fn graph_store_serializes_plug_names_and_target_keyed_flow() {
    let mut graph = Graph::new();

    graph
        .plugup("extract_user", |input: ExtractInput| async move {
            Ok(ExtractOutput {
                profile: ProfileOutput {
                    email: input.profile.email,
                    name: input.profile.name,
                },
            })
        })
        .unwrap()
        .plugup("send_email", |input: EmailInput| async move {
            Ok(EmailOutput {
                sent_to: input.recipient,
                greeting: format!("Hello {}", input.display_name),
            })
        })
        .unwrap()
        .plugin("extract_user", "extract_user")
        .unwrap()
        .plugin("send_email", "send_email")
        .unwrap()
        .flowin(json!({
            "send_email": {
                "recipient": "extract_user.profile.email",
                "display_name": "extract_user.profile.name"
            }
        }))
        .unwrap();

    let store = graph.store().unwrap();
    let value = serde_json::to_value(&store).unwrap();

    assert_eq!(
        value["graph"]["plugs"],
        json!({
            "extract_user": ["extract_user"],
            "send_email": ["send_email"]
        }),
        "GraphStore should persist plug kind to plug name lists, not Rust implementations"
    );
    assert_eq!(
        value["graph"]["flow"],
        json!({
            "send_email": {
                "display_name": "extract_user.profile.name",
                "recipient": "extract_user.profile.email"
            }
        }),
        "GraphStore should persist flow as the target-keyed JSON declaration shape"
    );
    assert!(
        value["graph"].get("runtime_plugs").is_none(),
        "runtime plug table must not be part of graph file storage"
    );
}

#[tokio::test]
async fn graph_plugup_registers_kind_and_plugin_adds_graph_local_names() {
    let mut graph = Graph::new();

    graph
        .plugup("coreflow.identity.v1", |input: NamedOutput| async move {
            Ok(input)
        })
        .unwrap()
        .plugin("left", "coreflow.identity.v1")
        .unwrap()
        .plugin("right", "coreflow.identity.v1")
        .unwrap();

    let value = serde_json::to_value(graph.store().unwrap()).unwrap();

    assert_eq!(
        value["graph"]["plugs"],
        json!({
            "coreflow.identity.v1": ["left", "right"]
        }),
        "GraphStore should persist one plug kind mapped to multiple graph-local plug names"
    );
}

#[tokio::test]
async fn graph_run_performs_check_automatically() {
    let mut graph = Graph::new();

    graph
        .plugup("echo", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("echo", "echo")
        .unwrap();

    let result = graph.run(json!({ "value": "hello" })).await.unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(
        result.output().get::<NamedOutput>("echo").unwrap(),
        NamedOutput {
            value: "hello".to_string()
        },
        "run should refresh declaration checks before executing"
    );
}

#[test]
fn graph_plugin_rejects_unregistered_kind() {
    let mut graph = Graph::new();

    let error = graph.plugin("echo", "coreflow.echo.v1").unwrap_err();

    assert_eq!(
        error,
        CoreError::UnknownPlug {
            name: "coreflow.echo.v1".to_string()
        },
        "plugin should reject graph-local plugs whose kind has not been registered with plugup"
    );
}

#[tokio::test]
async fn graph_rejects_duplicate_plug_names() {
    let mut graph = Graph::new();

    graph
        .plugup("send_email", |input: EmailInput| async move {
            Ok(EmailOutput {
                sent_to: input.recipient,
                greeting: format!("Hello {}", input.display_name),
            })
        })
        .unwrap();
    graph.plugin("send_email", "send_email").unwrap();

    let error = graph.plugin("send_email", "send_email").unwrap_err();

    assert_eq!(
        error,
        CoreError::DuplicatePlug {
            name: "send_email".to_string()
        }
    );
}

#[tokio::test]
async fn graph_flowin_rejects_duplicate_target_input_binding() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "source".to_string(),
            })
        })
        .unwrap()
        .plugup("target", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("target", "target")
        .unwrap()
        .flowin(json!({ "target": { "value": "source.value" } }))
        .unwrap();

    let error = graph
        .flowin(json!({ "target": { "value": "source.value" } }))
        .unwrap_err();

    assert_eq!(
        error,
        CoreError::DuplicateFlowInput {
            target: "target".to_string(),
            input: "value".to_string(),
        },
        "flowin should reject a second binding for the same target input field"
    );
}

#[tokio::test]
async fn graph_flowout_removes_selected_input_flow() {
    let mut graph = Graph::new();

    graph
        .plugup("extract_user", |input: ExtractInput| async move {
            Ok(ExtractOutput {
                profile: ProfileOutput {
                    email: input.profile.email,
                    name: input.profile.name,
                },
            })
        })
        .unwrap()
        .plugup("send_email", |input: EmailInput| async move {
            Ok(EmailOutput {
                sent_to: input.recipient,
                greeting: format!("Hello {}", input.display_name),
            })
        })
        .unwrap()
        .plugin("extract_user", "extract_user")
        .unwrap()
        .plugin("send_email", "send_email")
        .unwrap()
        .flowin(json!({
            "send_email": {
                "recipient": "extract_user.profile.email",
                "display_name": "extract_user.profile.name"
            }
        }))
        .unwrap()
        .flowout(json!({
            "send_email": ["recipient"]
        }))
        .unwrap();

    assert_eq!(
        serde_json::to_value(graph.store().unwrap()).unwrap()["graph"]["flow"],
        json!({
            "send_email": {
                "display_name": "extract_user.profile.name"
            }
        })
    );
}

#[tokio::test]
async fn graph_plugout_rejects_plugs_still_referenced_by_flow() {
    let mut graph = Graph::new();

    graph
        .plugup("extract_user", |input: ExtractInput| async move {
            Ok(ExtractOutput {
                profile: ProfileOutput {
                    email: input.profile.email,
                    name: input.profile.name,
                },
            })
        })
        .unwrap()
        .plugup("send_email", |input: EmailInput| async move {
            Ok(EmailOutput {
                sent_to: input.recipient,
                greeting: format!("Hello {}", input.display_name),
            })
        })
        .unwrap()
        .plugin("extract_user", "extract_user")
        .unwrap()
        .plugin("send_email", "send_email")
        .unwrap()
        .flowin(json!({
            "send_email": {
                "recipient": "extract_user.profile.email"
            }
        }))
        .unwrap();

    let error = graph.plugout("extract_user").unwrap_err();

    assert_eq!(
        error,
        CoreError::PlugReferencedByFlow {
            name: "extract_user".to_string()
        },
        "plugout should require explicit flowout before deleting a referenced plug"
    );

    graph
        .flowout(json!({
            "send_email": ["recipient"]
        }))
        .unwrap()
        .plugout("extract_user")
        .unwrap();

    graph.check().unwrap();
    let value = serde_json::to_value(graph.store().unwrap()).unwrap();

    assert_eq!(
        value["graph"]["plugs"],
        json!({"send_email": ["send_email"]})
    );
    assert_eq!(value["graph"]["flow"], json!({}));
}

#[tokio::test]
async fn graph_runs_independent_sources_concurrently_and_joins_when_ready() {
    let mut graph = Graph::new();
    let barrier = std::sync::Arc::new(Barrier::new(2));
    let left_barrier = std::sync::Arc::clone(&barrier);
    let right_barrier = std::sync::Arc::clone(&barrier);

    graph
        .plugup("left", move |_: coreflow::Value| {
            let barrier = std::sync::Arc::clone(&left_barrier);
            async move {
                barrier.wait().await;
                Ok(NamedOutput {
                    value: "L".to_string(),
                })
            }
        })
        .unwrap()
        .plugup("right", move |_: coreflow::Value| {
            let barrier = std::sync::Arc::clone(&right_barrier);
            async move {
                barrier.wait().await;
                Ok(NamedOutput {
                    value: "R".to_string(),
                })
            }
        })
        .unwrap()
        .plugup("join", |input: JoinInput| async move {
            Ok(JoinOutput {
                combined: format!("{}{}", input.left, input.right),
            })
        })
        .unwrap()
        .plugin("left", "left")
        .unwrap()
        .plugin("right", "right")
        .unwrap()
        .plugin("join", "join")
        .unwrap()
        .flowin(json!({
            "join": {
                "left": "left.value",
                "right": "right.value"
            }
        }))
        .unwrap();

    graph.check().unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), graph.run(json!({})))
        .await
        .expect("independent source plugs should both start instead of deadlocking")
        .unwrap();

    assert_eq!(
        result.output().get::<JoinOutput>("join").unwrap(),
        JoinOutput {
            combined: "LR".to_string()
        }
    );
}
