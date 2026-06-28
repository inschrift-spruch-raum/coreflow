use coreflow::{Graph, Run, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct EchoValue {
    value: String,
}

#[tokio::test]
async fn graph_run_rebuilds_indexes_after_flowout_mutation() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(EchoValue {
                value: "checked".to_string(),
            })
        })
        .unwrap()
        .plugup("target", |input: EchoValue| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("target", "target")
        .unwrap()
        .flowin(json!({ "target": "source" }))
        .unwrap();

    graph.check().unwrap();
    graph.flowout(json!({ "target": null })).unwrap();

    let result = graph
        .run(Run::new(json!({})).seeds(["source"]))
        .await
        .unwrap();

    assert_eq!(
        result.output().get::<EchoValue>("source").unwrap(),
        EchoValue {
            value: "checked".to_string(),
        },
        "run should rebuild checked indexes after graph mutation APIs update flow before executing"
    );
    assert!(matches!(
        result.output().get::<EchoValue>("target"),
        Err(coreflow::CoreError::UnknownPlug { name }) if name == "target"
    ));
}
