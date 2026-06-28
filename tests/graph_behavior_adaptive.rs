use coreflow::{Graph, Run, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NamedOutput {
    value: String,
}

#[tokio::test]
async fn graph_run_request_seeds_and_resume_trigger_explicit_plugs() {
    let mut graph = Graph::new();

    graph
        .plugup("echo", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("echo", "echo")
        .unwrap();

    graph.check().unwrap();
    let manual = graph
        .run(Run::new(json!({ "echo": { "value": "manual" } })).seeds(["echo"]))
        .await
        .unwrap();

    assert_eq!(
        manual.output().get::<NamedOutput>("echo").unwrap(),
        NamedOutput {
            value: "manual".to_string()
        }
    );

    let resumed = graph
        .run(Run::resume(&manual).seeds(["echo"]))
        .await
        .unwrap();

    assert_eq!(
        resumed.output().get::<NamedOutput>("echo").unwrap(),
        NamedOutput {
            value: "manual".to_string()
        }
    );
}

#[tokio::test]
async fn graph_run_applies_plug_emitted_mutation_requests() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "mutated".to_string(),
            })
        })
        .unwrap()
        .plugup("adapt", |_: coreflow::Value| async move {
            Ok(coreflow::GraphMutationRequest {
                message: "connect target".to_string(),
                changes: vec![coreflow::GraphChange::FlowIn {
                    target: coreflow::PlugName::new("target"),
                    input: coreflow::FieldPath::new(""),
                    source: coreflow::SourceSelector {
                        plug: coreflow::PlugName::new("source"),
                        path: coreflow::FieldPath::new(""),
                    },
                }],
            })
        })
        .unwrap()
        .plugup("target", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("adapt", "adapt")
        .unwrap()
        .plugin("target", "target")
        .unwrap();

    graph.check().unwrap();
    let store_before = serde_json::to_value(graph.store().unwrap()).unwrap();
    let commits_before = store_before["commits"].as_object().unwrap().len();
    let result = graph
        .run(Run::new(json!({})).seeds(["adapt"]))
        .await
        .unwrap();
    let store = serde_json::to_value(graph.store().unwrap()).unwrap();

    assert_eq!(
        result.output().get::<NamedOutput>("target").unwrap(),
        NamedOutput {
            value: "mutated".to_string(),
        },
        "plug-emitted graph mutation requests should update the graph and rerun against the new checked snapshot"
    );
    assert_ne!(
        store["head"], store_before["head"],
        "plug-emitted graph mutation requests should advance the caller graph head"
    );
    assert_eq!(
        store["commits"].as_object().unwrap().len(),
        commits_before + 1,
        "runtime-applied mutation requests should append persisted graph commits to the caller graph"
    );

    let rerun = graph
        .run(Run::new(json!({})).seeds(["source"]))
        .await
        .unwrap();
    assert_eq!(
        rerun.output().get::<NamedOutput>("target").unwrap(),
        NamedOutput {
            value: "mutated".to_string(),
        },
        "the next run should use the new persisted graph head without re-emitting the mutation request"
    );
}

#[tokio::test]
async fn graph_run_applies_plug_emitted_next_graph() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "next-graph".to_string(),
            })
        })
        .unwrap()
        .plugup("adapt", |_: coreflow::Value| async move {
            let mut next = Graph::new();
            next.plugup("source", |_: coreflow::Value| async move {
                Ok(NamedOutput {
                    value: "unused".to_string(),
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
            .unwrap();
            Ok(next)
        })
        .unwrap()
        .plugup("target", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("adapt", "adapt")
        .unwrap();

    let result = graph
        .run(Run::new(json!({})).seeds(["adapt"]))
        .await
        .unwrap();
    let store = serde_json::to_value(graph.store().unwrap()).unwrap();

    assert_eq!(
        result.output().get::<NamedOutput>("target").unwrap(),
        NamedOutput {
            value: "next-graph".to_string(),
        },
        "plug-emitted next Graph should replace the checked graph and rerun against it"
    );
    assert_eq!(
        store["graph"]["flow"],
        json!({ "target": { "": "source" } }),
        "plug-emitted next Graph should persist the replacement graph structure"
    );
}

#[tokio::test]
async fn graph_default_bindings_cover_single_source_and_multi_source_inputs() {
    let mut single = Graph::new();

    single
        .plugup("source", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "single".to_string(),
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
        .unwrap();

    single.check().unwrap();
    let single_result = single.run(json!({})).await.unwrap();

    assert_eq!(
        single_result.output().get::<NamedOutput>("target").unwrap(),
        NamedOutput {
            value: "single".to_string()
        }
    );

    let mut multi = Graph::new();

    multi
        .plugup("left", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "L".to_string(),
            })
        })
        .unwrap()
        .plugup("right", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "R".to_string(),
            })
        })
        .unwrap()
        .plugup("join", |input: coreflow::Value| async move { Ok(input) })
        .unwrap()
        .plugin("left", "left")
        .unwrap()
        .plugin("right", "right")
        .unwrap()
        .plugin("join", "join")
        .unwrap()
        .flowin(json!({ "join": ["left", "right"] }))
        .unwrap();

    multi.check().unwrap();
    let multi_result = multi.run(json!({})).await.unwrap();

    assert_eq!(
        multi_result
            .output()
            .get::<coreflow::Value>("join")
            .unwrap(),
        json!({
            "left": { "value": "L" },
            "right": { "value": "R" }
        })
    );
}
