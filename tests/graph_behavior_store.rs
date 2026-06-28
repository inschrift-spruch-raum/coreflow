use coreflow::{
    CoreError, ExecutionPolicy, FailurePolicy, Graph, GraphChange, GraphMutationRequest,
    GraphRunStatus, Run, json,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use tokio::sync::Barrier;

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

#[test]
fn graph_store_saves_and_loads_single_graph_json_file() {
    let mut graph = Graph::new();

    graph
        .plugup("echo", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("echo", "echo")
        .unwrap();

    let path = std::env::temp_dir().join(format!("coreflow-graph-{}.json", std::process::id()));

    graph.save(&path).unwrap();
    let loaded = Graph::load(&path).unwrap();
    std::fs::remove_file(&path).unwrap();

    let value = serde_json::to_value(loaded.store().unwrap()).unwrap();

    assert_eq!(value["graph"]["plugs"], json!({ "echo": ["echo"] }));
}

#[test]
fn graph_store_saves_and_loads_fixed_graph_json_file() {
    let mut graph = Graph::new();

    graph
        .plugup("echo", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("echo", "echo")
        .unwrap();

    let dir = std::env::temp_dir().join(format!("coreflow-store-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let graph_json = dir.join("graph.json");

    graph.save(&graph_json).unwrap();
    let loaded = Graph::load(&graph_json).unwrap();
    let saved = std::fs::read_to_string(&graph_json).unwrap();
    std::fs::remove_file(&graph_json).unwrap();
    std::fs::remove_dir(&dir).unwrap();

    assert!(
        saved.contains("\"graph\""),
        "save should write the fixed graph.json protocol file"
    );
    assert_eq!(
        serde_json::to_value(loaded.store().unwrap()).unwrap()["graph"]["plugs"],
        json!({ "echo": ["echo"] })
    );
}

#[tokio::test]
async fn graph_store_does_not_persist_execution_policy() {
    let mut graph = Graph::new();

    graph
        .plugup("echo", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("echo", "echo")
        .unwrap();

    let _ = graph
        .run(
            Run::new(json!({ "value": "policy" })).policy(ExecutionPolicy {
                failure: FailurePolicy::ContinueIndependent,
                max_concurrency: 7,
                resource_limits: BTreeMap::new(),
                inline_small_plugs: false,
            }),
        )
        .await
        .unwrap();

    let value = serde_json::to_value(graph.store().unwrap()).unwrap();

    assert!(
        value["graph"].get("policy").is_none(),
        "GraphStore should not persist run-scoped ExecutionPolicy in graph file data"
    );
}

#[test]
fn graph_store_into_graph_does_not_carry_runtime_plug_registry() {
    let mut graph = Graph::new();

    graph
        .plugup("echo", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("echo", "echo")
        .unwrap();

    let mut loaded = graph.store().unwrap().into_graph().unwrap();
    let error = loaded.check().unwrap_err();

    assert_eq!(
        error,
        CoreError::UnknownPlug {
            name: "echo".to_string()
        },
        "in-memory GraphStore should be a pure graph-file snapshot, not carry runtime plug registry"
    );
}

#[tokio::test]
async fn graph_flowin_records_one_commit_with_all_field_changes() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(json!({ "email": "ada@example.com", "name": "Ada" }))
        })
        .unwrap()
        .plugup("target", |input: EmailInput| async move {
            Ok(EmailOutput {
                sent_to: input.recipient,
                greeting: format!("Hello {}", input.display_name),
            })
        })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("target", "target")
        .unwrap();

    let commits_before = serde_json::to_value(graph.store().unwrap()).unwrap()["commits"]
        .as_object()
        .unwrap()
        .len();

    graph
        .flowin(json!({
            "target": {
                "recipient": "source.email",
                "display_name": "source.name"
            }
        }))
        .unwrap();

    let store = serde_json::to_value(graph.store().unwrap()).unwrap();
    let commits_after = store["commits"].as_object().unwrap().len();

    assert_eq!(
        commits_after - commits_before,
        1,
        "flowin should persist one GraphCommit for the caller-level graph mutation"
    );
    assert_eq!(
        store["commits"]
            .as_object()
            .unwrap()
            .values()
            .filter(|commit| commit["message"] == "flowin")
            .map(|commit| commit["changes"].as_array().unwrap().len())
            .max(),
        Some(2),
        "GraphCommit should store every field-level GraphChange under one commit message"
    );
}

#[tokio::test]
async fn graph_flowin_commit_records_exact_field_change_shape() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(json!({ "email": "ada@example.com" }))
        })
        .unwrap()
        .plugup("target", |input: coreflow::Value| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("target", "target")
        .unwrap()
        .commit(GraphMutationRequest {
            message: "connect recipient".to_string(),
            changes: vec![GraphChange::FlowIn {
                target: "target".into(),
                input: "recipient".into(),
                source: serde_json::from_value(json!("source.email")).unwrap(),
            }],
        })
        .unwrap();

    let store = serde_json::to_value(graph.store().unwrap()).unwrap();
    let commit = store["commits"]
        .as_object()
        .unwrap()
        .values()
        .find(|commit| commit["message"] == "connect recipient")
        .unwrap();

    assert_eq!(
        commit["changes"],
        json!([{ "flow_in": { "target": "target", "input": "recipient", "source": "source.email" } }]),
        "GraphCommit should persist flowin changes under the caller-level commit message"
    );
}

#[tokio::test]
async fn graph_flowout_commit_records_exact_field_change_shape() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(json!({ "email": "ada@example.com" }))
        })
        .unwrap()
        .plugup("target", |input: coreflow::Value| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("target", "target")
        .unwrap()
        .flowin(json!({ "target": { "recipient": "source.email" } }))
        .unwrap()
        .commit(GraphMutationRequest {
            message: "remove recipient".to_string(),
            changes: vec![GraphChange::FlowOut {
                target: "target".into(),
                input: "recipient".into(),
            }],
        })
        .unwrap();

    let store = serde_json::to_value(graph.store().unwrap()).unwrap();
    let commit = store["commits"]
        .as_object()
        .unwrap()
        .values()
        .find(|commit| commit["message"] == "remove recipient")
        .unwrap();

    assert_eq!(
        commit["changes"],
        json!([{ "flow_out": { "target": "target", "input": "recipient" } }]),
        "GraphCommit should persist flowout changes under the caller-level commit message"
    );
}

#[tokio::test]
async fn graph_plugout_commit_records_exact_plug_name_change_shape() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move { Ok(json!({})) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .commit(GraphMutationRequest {
            message: "remove source".to_string(),
            changes: vec![GraphChange::PlugOut("source".into())],
        })
        .unwrap();

    let store = serde_json::to_value(graph.store().unwrap()).unwrap();
    let commit = store["commits"]
        .as_object()
        .unwrap()
        .values()
        .find(|commit| commit["message"] == "remove source")
        .unwrap();

    assert_eq!(
        commit["changes"],
        json!([{ "plug_out": "source" }]),
        "GraphCommit should persist plugout under the caller-level commit message"
    );
}

#[tokio::test]
async fn graph_store_replays_commit_chain_to_current_graph() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(json!({ "email": "ada@example.com" }))
        })
        .unwrap()
        .plugup("target", |input: coreflow::Value| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("target", "target")
        .unwrap()
        .flowin(json!({ "target": { "recipient": "source.email" } }))
        .unwrap();

    let mut store = graph.store().unwrap();
    store.graph = Graph::new();

    let replayed = store.replay().unwrap();
    let value = serde_json::to_value(replayed.store().unwrap()).unwrap();

    assert_eq!(
        value["graph"]["plugs"],
        json!({ "source": ["source"], "target": ["target"] }),
        "GraphStore replay should rebuild plug declarations from GraphCommit records"
    );
    assert_eq!(
        value["graph"]["flow"],
        json!({ "target": { "recipient": "source.email" } }),
        "GraphStore replay should rebuild flow declarations from GraphCommit records"
    );
}

#[tokio::test]
async fn graph_store_loads_current_graph_without_replay() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(json!({ "email": "ada@example.com" }))
        })
        .unwrap()
        .plugin("source", "source")
        .unwrap();

    let mut store = graph.store().unwrap();
    store.graph = Graph::new();

    let loaded = store.into_graph().unwrap();
    let value = serde_json::to_value(loaded.store().unwrap()).unwrap();

    assert_eq!(
        value["graph"]["plugs"],
        json!({}),
        "GraphStore::into_graph should load the current stored graph without replay"
    );
}

#[tokio::test]
async fn graph_local_plugs_from_same_kind_run_concurrently() {
    let mut graph = Graph::new();
    let barrier = std::sync::Arc::new(Barrier::new(2));

    graph
        .plugup("coreflow.wait.v1", {
            let barrier = std::sync::Arc::clone(&barrier);
            move |input: NamedOutput| {
                let barrier = std::sync::Arc::clone(&barrier);
                async move {
                    barrier.wait().await;
                    Ok(input)
                }
            }
        })
        .unwrap()
        .plugin("left", "coreflow.wait.v1")
        .unwrap()
        .plugin("right", "coreflow.wait.v1")
        .unwrap();

    graph.check().unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        graph.run(json!({
            "left": { "value": "L" },
            "right": { "value": "R" }
        })),
    )
    .await
    .expect("graph-local plugs from the same kind should not share one serial executor lock")
    .unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
}

#[tokio::test]
async fn graph_commit_accepts_explicit_commit_messages() {
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
        .commit(GraphMutationRequest {
            message: "connect target".to_string(),
            changes: vec![GraphChange::FlowIn {
                target: "target".into(),
                input: "".into(),
                source: serde_json::from_value(json!("source")).unwrap(),
            }],
        })
        .unwrap();

    let store = serde_json::to_value(graph.store().unwrap()).unwrap();
    let commits = store["commits"].as_object().unwrap();

    assert!(
        commits
            .values()
            .any(|commit| commit["message"] == "connect target"),
        "GraphCommit should preserve caller-provided graph change messages"
    );
}

#[tokio::test]
async fn graph_public_flow_mutation_invalidates_checked_snapshot() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "current".to_string(),
            })
        })
        .unwrap()
        .plugup("target", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("target", "target")
        .unwrap();

    graph.check().unwrap();
    graph.flowin(json!({ "target": "source" })).unwrap();

    let result = graph.run(json!({})).await.unwrap();

    assert_eq!(
        result.output().get::<NamedOutput>("target").unwrap(),
        NamedOutput {
            value: "current".to_string(),
        },
        "public graph mutation APIs should invalidate any previously checked snapshot before run"
    );
}
