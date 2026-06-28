use coreflow::{
    CoreError, ExecutionPolicy, FailurePolicy, Graph, GraphRunStatus, PendingApproval, Run,
    RunEvent, json,
};
use serde::ser::Error as _;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NamedOutput {
    value: String,
}

struct FailingSerialize;

impl Serialize for FailingSerialize {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Err(S::Error::custom("intentional encode failure"))
    }
}

fn job_failed_error(event: &RunEvent) -> Option<CoreError> {
    match event {
        RunEvent::JobFailed { error, .. } => Some(error.clone()),
        _ => None,
    }
}

#[tokio::test]
async fn graph_inline_small_plugs_runs_ready_tick_without_spawning() {
    let mut graph = Graph::new();
    let ran_without_spawned_task_id = Arc::new(AtomicBool::new(false));

    graph
        .plugup("inline", {
            let ran_without_spawned_task_id = Arc::clone(&ran_without_spawned_task_id);
            move |input: NamedOutput| {
                let ran_without_spawned_task_id = Arc::clone(&ran_without_spawned_task_id);
                async move {
                    if tokio::task::try_id().is_none() {
                        ran_without_spawned_task_id.store(true, Ordering::SeqCst);
                    }
                    Ok(input)
                }
            }
        })
        .unwrap()
        .plugup("other", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("inline", "inline")
        .unwrap()
        .plugin("other", "other")
        .unwrap();

    let result = graph
        .run(
            Run::new(json!({
                "inline": { "value": "inline" },
                "other": { "value": "other" }
            }))
            .policy(ExecutionPolicy {
                failure: FailurePolicy::FailFast,
                max_concurrency: 2,
                resource_limits: BTreeMap::new(),
                inline_small_plugs: true,
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert!(
        ran_without_spawned_task_id.load(Ordering::SeqCst),
        "inline_small_plugs should execute at least the next ready tick on the graph task instead of spawning it"
    );
}

#[tokio::test]
async fn graph_inline_small_plugs_runs_ready_tick_while_job_is_in_flight() {
    let mut graph = Graph::new();
    let ran_without_spawned_task_id = Arc::new(AtomicBool::new(false));

    graph
        .plugup("fast", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugup("slow", |input: NamedOutput| async move {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Ok(input)
        })
        .unwrap()
        .plugup("fast_downstream", {
            let ran_without_spawned_task_id = Arc::clone(&ran_without_spawned_task_id);
            move |input: NamedOutput| {
                let ran_without_spawned_task_id = Arc::clone(&ran_without_spawned_task_id);
                async move {
                    if tokio::task::try_id().is_none() {
                        ran_without_spawned_task_id.store(true, Ordering::SeqCst);
                    }
                    Ok(input)
                }
            }
        })
        .unwrap()
        .plugin("fast", "fast")
        .unwrap()
        .plugin("slow", "slow")
        .unwrap()
        .plugin("fast_downstream", "fast_downstream")
        .unwrap()
        .flowin(json!({ "fast_downstream": "fast" }))
        .unwrap();

    let result = graph
        .run(
            Run::new(json!({
                "fast": { "value": "fast" },
                "slow": { "value": "slow" }
            }))
            .policy(ExecutionPolicy {
                failure: FailurePolicy::FailFast,
                max_concurrency: 2,
                resource_limits: BTreeMap::new(),
                inline_small_plugs: false,
            }),
        )
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert!(
        ran_without_spawned_task_id.load(Ordering::SeqCst),
        "a single queued ready tick should inline even while an unrelated job is in flight"
    );
}

#[tokio::test]
async fn graph_emits_pending_approval_event_without_stopping_independent_work() {
    let mut graph = Graph::new();

    graph
        .plugup("approval", |_: coreflow::Value| async move {
            Ok(PendingApproval::new("human review"))
        })
        .unwrap()
        .plugup("later", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "should wait".to_string(),
            })
        })
        .unwrap()
        .plugin("approval", "approval")
        .unwrap()
        .plugin("later", "later")
        .unwrap();

    graph.check().unwrap();
    let result = graph
        .run(Run::new(json!({})).policy(ExecutionPolicy {
            failure: FailurePolicy::FailFast,
            max_concurrency: 1,
            resource_limits: BTreeMap::new(),
            inline_small_plugs: false,
        }))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert!(result.events.iter().any(|event| matches!(
        event,
        RunEvent::PendingApproval { plug, tick: _, reason }
            if plug.to_string() == "approval" && reason.as_deref() == Some("human review")
    )));
    assert_eq!(
        result.output().get::<NamedOutput>("later").unwrap(),
        NamedOutput {
            value: "should wait".to_string()
        },
        "pending approval is an observable plug output fact, not a kernel scheduling stop signal"
    );
}

#[tokio::test]
async fn graph_run_is_the_default_persistent_entrypoint() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "created".to_string(),
            })
        })
        .unwrap()
        .plugup("adapt", |_: coreflow::Value| async move {
            Ok(coreflow::GraphMutationRequest {
                message: "connect source to target".to_string(),
                changes: vec![
                    coreflow::GraphChange::PlugIn {
                        kind: coreflow::PlugKind::new("source"),
                        name: coreflow::PlugName::new("source"),
                    },
                    coreflow::GraphChange::PlugIn {
                        kind: coreflow::PlugKind::new("target"),
                        name: coreflow::PlugName::new("target"),
                    },
                    coreflow::GraphChange::FlowIn {
                        target: coreflow::PlugName::new("target"),
                        input: coreflow::FieldPath::new(""),
                        source: coreflow::SourceSelector {
                            plug: coreflow::PlugName::new("source"),
                            path: coreflow::FieldPath::new(""),
                        },
                    },
                ],
            })
        })
        .unwrap()
        .plugup("target", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("adapt", "adapt")
        .unwrap();

    graph.check().unwrap();
    let store_before = serde_json::to_value(graph.store().unwrap()).unwrap();
    let result = graph.run(json!({})).await.unwrap();
    let store_after = serde_json::to_value(graph.store().unwrap()).unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(
        result.output().get::<NamedOutput>("target").unwrap(),
        NamedOutput {
            value: "created".to_string()
        }
    );
    assert_ne!(
        store_after["head"], store_before["head"],
        "run should be the default mutable entrypoint and persist plug-emitted graph changes"
    );
}

#[tokio::test]
async fn graph_reuse_isolates_failed_run_from_later_successful_run() {
    let mut graph = Graph::new();

    graph
        .plugup("echo", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("echo", "echo")
        .unwrap();

    let failed = graph.run(json!({ "wrong": true })).await.unwrap();
    let succeeded = graph.run(json!({ "value": "ok" })).await.unwrap();

    assert_eq!(failed.status, GraphRunStatus::Failed);
    assert_eq!(succeeded.status, GraphRunStatus::Idle);
    assert_eq!(
        succeeded.output().get::<NamedOutput>("echo").unwrap(),
        NamedOutput {
            value: "ok".to_string()
        },
        "a failed run should not poison later runs of the same graph"
    );
}

#[tokio::test]
async fn graph_distinguishes_plug_decode_and_encode_failures() {
    let mut decode_graph = Graph::new();

    decode_graph
        .plugup("typed", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("typed", "typed")
        .unwrap();

    decode_graph.check().unwrap();
    let decode_result = decode_graph.run(json!({ "wrong": true })).await.unwrap();

    assert!(matches!(
        decode_result.events.iter().find_map(job_failed_error),
        Some(CoreError::PlugDecode { plug, .. }) if plug == "typed"
    ));

    let mut encode_graph = Graph::new();

    encode_graph
        .plugup("bad_output", |_: coreflow::Value| async move {
            Ok(FailingSerialize)
        })
        .unwrap()
        .plugin("bad_output", "bad_output")
        .unwrap();

    encode_graph.check().unwrap();
    let encode_result = encode_graph.run(json!({})).await.unwrap();

    assert!(matches!(
        encode_result.events.iter().find_map(job_failed_error),
        Some(CoreError::PlugEncode { plug, message })
            if plug == "bad_output" && message.contains("intentional encode failure")
    ));
}
