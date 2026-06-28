use coreflow::{CoreError, ExecutionPolicy, FailurePolicy, Graph, GraphRunStatus, Run, json};
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
fn graph_store_into_graph_does_not_carry_runtime_plug_table() {
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
        "in-memory GraphStore should be a pure graph-file snapshot, not carry runtime plug table"
    );
}

#[tokio::test]
async fn graph_plain_changes_stage_until_commit_message() {
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
        0,
        "plain graph changes should stage GraphChange records without creating GraphCommit records"
    );
    assert_eq!(
        store["graph"]["flow"],
        json!({
            "target": {
                "recipient": "source.email",
                "display_name": "source.name"
            }
        }),
        "plain graph changes should still update the stored working graph"
    );

    graph.commit("connect email fields").unwrap();
    let committed = serde_json::to_value(graph.store().unwrap()).unwrap();
    let commit = committed["commits"]
        .as_object()
        .unwrap()
        .values()
        .find(|commit| commit["message"] == "connect email fields")
        .unwrap();

    assert_eq!(
        commit["changes"],
        json!([
            { "operation": "plug_in", "kind": "source", "name": "source" },
            { "operation": "plug_in", "kind": "target", "name": "target" },
            { "operation": "flow_in", "target.display_name": "source.name" },
            { "operation": "flow_in", "target.recipient": "source.email" }
        ]),
        "commit should flush all staged GraphChange records under the caller-provided message"
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
        .commit("add plugs")
        .unwrap()
        .flowin(json!({ "target": { "recipient": "source.email" } }))
        .unwrap()
        .commit("connect recipient")
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
        json!([{ "operation": "flow_in", "target.recipient": "source.email" }]),
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
        .commit("connect recipient")
        .unwrap()
        .flowout(json!({ "target": ["recipient"] }))
        .unwrap()
        .commit("remove recipient")
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
        json!([{ "operation": "flow_out", "target": "target.recipient" }]),
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
        .commit("add source")
        .unwrap()
        .plugout("source")
        .unwrap()
        .commit("remove source")
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
        json!([{ "operation": "plug_out", "name": "source" }]),
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
        .unwrap()
        .commit("build email graph")
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
        .flowin(json!({ "target": "source" }))
        .unwrap()
        .commit("connect target")
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
