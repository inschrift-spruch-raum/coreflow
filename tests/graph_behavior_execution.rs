use coreflow::{
    CoreError, ExecutionPolicy, FailurePolicy, Graph, GraphRunStatus, PickerStrategy, Run,
    RunEvent, json,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::Barrier;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NamedOutput {
    value: String,
}

#[derive(Debug, Deserialize)]
struct FeedbackInput {
    count: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct FeedbackOutput {
    count: u64,
}

fn job_failed_error(event: &RunEvent) -> Option<CoreError> {
    match event {
        RunEvent::JobFailed { error, .. } => Some(error.clone()),
        _ => None,
    }
}

fn job_failed_errors(events: &[RunEvent]) -> impl Iterator<Item = CoreError> + '_ {
    events.iter().filter_map(job_failed_error)
}

fn job_done_plug(event: &RunEvent) -> Option<String> {
    match event {
        RunEvent::JobDone { plug, .. } => Some(plug.to_string()),
        _ => None,
    }
}

fn intentional_panic_value() -> coreflow::Value {
    panic!("intentional panic")
}

#[tokio::test]
async fn graph_feedback_flow_records_each_completed_tick_until_idle() {
    let mut graph = Graph::new();

    graph
        .plugup("advance", |input: FeedbackInput| async move {
            Ok(FeedbackOutput {
                count: (input.count + 1).min(2),
            })
        })
        .unwrap()
        .plugin("advance", "advance")
        .unwrap()
        .flowin(json!({
            "advance": {
                "count": "advance.count"
            }
        }))
        .unwrap();

    graph.check().unwrap();
    let result = graph
        .run(Run::new(json!({ "advance": { "count": 0 } })).seeds(["advance"]))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert!(
        result
            .events
            .iter()
            .filter(|event| matches!(event, RunEvent::JobDone { plug, .. } if plug.to_string() == "advance"))
            .count()
            >= 3,
        "feedback flow should record each completed tick until the input snapshot stabilizes"
    );
    assert_eq!(
        result.output().get::<FeedbackOutput>("advance").unwrap(),
        FeedbackOutput { count: 2 }
    );
}

#[tokio::test]
async fn graph_run_request_seed_triggers_isolated_plug() {
    let mut graph = Graph::new();

    graph
        .plugup("echo", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("echo", "echo")
        .unwrap();

    graph.check().unwrap();
    let result = graph
        .run(Run::new(json!({ "echo": { "value": "seeded" } })).seeds(["echo"]))
        .await
        .unwrap();

    assert_eq!(
        result.output().get::<NamedOutput>("echo").unwrap(),
        NamedOutput {
            value: "seeded".to_string()
        }
    );
}

#[tokio::test]
async fn graph_continue_independent_policy_runs_unblocked_branch_after_failure() {
    let mut graph = Graph::new();

    graph
        .plugup("fail", |_: coreflow::Value| async move {
            Err::<coreflow::Value, _>(CoreError::PlugFailed {
                plug: "fail".to_string(),
                message: "boom".to_string(),
            })
        })
        .unwrap()
        .plugup("ok", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "ok".to_string(),
            })
        })
        .unwrap()
        .plugin("fail", "fail")
        .unwrap()
        .plugin("ok", "ok")
        .unwrap();

    graph.check().unwrap();
    let result = graph
        .run(Run::new(json!({})).policy(ExecutionPolicy {
            failure: FailurePolicy::ContinueIndependent,
            max_concurrency: 2,
            resource_limits: BTreeMap::new(),
            inline_small_plugs: false,
        }))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Failed);
    assert_eq!(
        result.output().get::<NamedOutput>("ok").unwrap(),
        NamedOutput {
            value: "ok".to_string()
        },
        "independent branch should finish under ContinueIndependent"
    );
    assert_eq!(job_failed_errors(&result.events).count(), 1);
    assert!(
        result.events.iter().any(|event| matches!(
            event,
            RunEvent::JobFailed {
                plug,
                tick: _,
                error: CoreError::PlugFailed { message, .. }
            } if plug.to_string() == "fail" && message == "boom"
        )),
        "failed jobs should record structured failure details in GraphResult events"
    );
}

#[tokio::test]
async fn graph_records_dependents_blocked_by_failed_source() {
    let mut graph = Graph::new();

    graph
        .plugup("fail", |_: coreflow::Value| async move {
            Err::<NamedOutput, _>(CoreError::PlugFailed {
                plug: "fail".to_string(),
                message: "boom".to_string(),
            })
        })
        .unwrap()
        .plugup("downstream", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("fail", "fail")
        .unwrap()
        .plugin("downstream", "downstream")
        .unwrap()
        .flowin(json!({ "downstream": "fail" }))
        .unwrap();

    graph.check().unwrap();
    let result = graph
        .run(Run::new(json!({})).policy(ExecutionPolicy {
            failure: FailurePolicy::ContinueIndependent,
            max_concurrency: 2,
            resource_limits: BTreeMap::new(),
            inline_small_plugs: false,
        }))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Failed);
    assert!(
        matches!(result.output().get::<NamedOutput>("downstream"), Err(CoreError::UnknownPlug { name }) if name == "downstream")
    );
    assert!(result.events.iter().any(|event| matches!(event, RunEvent::JobFailed { plug, error: CoreError::PlugFailed { message, .. }, .. } if plug.to_string() == "fail" && message == "boom")));
}

#[tokio::test]
async fn graph_continue_independent_keeps_same_source_sibling_targets_after_build_error() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(json!({
                "ok": {
                    "value": "ok"
                }
            }))
        })
        .unwrap()
        .plugup("bad", |input: coreflow::Value| async move { Ok(input) })
        .unwrap()
        .plugup("z_ok", |input: NamedOutput| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("bad", "bad")
        .unwrap()
        .plugin("z_ok", "z_ok")
        .unwrap()
        .flowin(json!({
            "bad": {
                "value": "source.missing"
            },
            "z_ok": "source.ok"
        }))
        .unwrap();

    let result = graph
        .run(Run::new(json!({})).policy(ExecutionPolicy {
            failure: FailurePolicy::ContinueIndependent,
            max_concurrency: 2,
            resource_limits: BTreeMap::new(),
            inline_small_plugs: false,
        }))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Failed);
    assert!(matches!(
        result.events.iter().find_map(job_failed_error),
        Some(CoreError::FlowPathNotFound { plug, path })
            if plug == "source" && path == "missing"
    ));
    assert_eq!(
        result.output().get::<NamedOutput>("z_ok").unwrap(),
        NamedOutput {
            value: "ok".to_string()
        },
        "ContinueIndependent should keep running sibling targets that do not depend on the failed target"
    );
}

#[tokio::test]
async fn graph_run_reports_missing_flow_path_at_runtime() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move { Ok(json!({})) })
        .unwrap()
        .plugup("target", |input: coreflow::Value| async move { Ok(input) })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("target", "target")
        .unwrap()
        .flowin(json!({ "target": { "value": "source.value" } }))
        .unwrap();

    let result = graph.run(json!({})).await.unwrap();

    assert_eq!(result.status, GraphRunStatus::Failed);
    assert!(matches!(
        result.events.iter().find_map(job_failed_error),
        Some(CoreError::FlowPathNotFound { plug, path })
            if plug == "source" && path == "value"
    ));
}

#[tokio::test]
async fn graph_run_reports_decode_failure_for_flow_built_target_input() {
    let mut graph = Graph::new();

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(json!({ "value": 7 }))
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

    let result = graph.run(json!({})).await.unwrap();

    assert_eq!(result.status, GraphRunStatus::Failed);
    assert!(matches!(
        result.events.iter().find_map(job_failed_error),
        Some(CoreError::PlugDecode { plug, .. }) if plug == "target"
    ));
}

#[tokio::test]
async fn graph_run_maps_plug_panic_to_structured_failure() {
    let mut graph = Graph::new();

    graph
        .plugup("panic", |_: coreflow::Value| async move {
            Ok::<coreflow::Value, CoreError>(intentional_panic_value())
        })
        .unwrap()
        .plugin("panic", "panic")
        .unwrap();

    let result = graph.run(json!({})).await.unwrap();

    assert_eq!(result.status, GraphRunStatus::Failed);
    assert!(matches!(
        result.events.iter().find_map(job_failed_error),
        Some(CoreError::PlugFailed { plug, message })
            if plug == "panic" && message == "plug panicked"
    ));
}

#[tokio::test]
async fn graph_policy_max_concurrency_limits_simultaneous_jobs() {
    let mut graph = Graph::new();
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));

    for name in ["left", "right"] {
        let active = Arc::clone(&active);
        let max_active = Arc::clone(&max_active);
        graph
            .plugup(name, move |_: coreflow::Value| {
                let active = Arc::clone(&active);
                let max_active = Arc::clone(&max_active);
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    max_active.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(NamedOutput {
                        value: "done".to_string(),
                    })
                }
            })
            .unwrap()
            .plugin(name, name)
            .unwrap();
    }

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
    assert_eq!(
        max_active.load(Ordering::SeqCst),
        1,
        "max_concurrency=1 should prevent independent ready plugs from overlapping"
    );
}

#[tokio::test]
async fn graph_fan_out_runs_multiple_downstream_plugs_concurrently() {
    let mut graph = Graph::new();
    let barrier = Arc::new(Barrier::new(2));

    graph
        .plugup("source", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "fanout".to_string(),
            })
        })
        .unwrap()
        .plugup("left", {
            let barrier = Arc::clone(&barrier);
            move |input: NamedOutput| {
                let barrier = Arc::clone(&barrier);
                async move {
                    barrier.wait().await;
                    Ok(input)
                }
            }
        })
        .unwrap()
        .plugup("right", {
            let barrier = Arc::clone(&barrier);
            move |input: NamedOutput| {
                let barrier = Arc::clone(&barrier);
                async move {
                    barrier.wait().await;
                    Ok(input)
                }
            }
        })
        .unwrap()
        .plugin("source", "source")
        .unwrap()
        .plugin("left", "left")
        .unwrap()
        .plugin("right", "right")
        .unwrap()
        .flowin(json!({
            "left": "source",
            "right": "source"
        }))
        .unwrap();

    let result = tokio::time::timeout(std::time::Duration::from_secs(1), graph.run(json!({})))
        .await
        .expect("fan-out downstream plugs should run concurrently instead of deadlocking")
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(
        result.output().get::<NamedOutput>("left").unwrap(),
        NamedOutput {
            value: "fanout".to_string()
        }
    );
    assert_eq!(
        result.output().get::<NamedOutput>("right").unwrap(),
        NamedOutput {
            value: "fanout".to_string()
        }
    );
}

#[tokio::test]
async fn graph_fast_branch_propagates_before_unrelated_slow_branch_finishes() {
    let mut graph = Graph::new();
    let slow_done = Arc::new(AtomicBool::new(false));

    graph
        .plugup("fast", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "fast".to_string(),
            })
        })
        .unwrap()
        .plugup("slow", {
            let slow_done = Arc::clone(&slow_done);
            move |_: coreflow::Value| {
                let slow_done = Arc::clone(&slow_done);
                async move {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    slow_done.store(true, Ordering::SeqCst);
                    Ok(NamedOutput {
                        value: "slow".to_string(),
                    })
                }
            }
        })
        .unwrap()
        .plugup("fast_downstream", {
            let slow_done = Arc::clone(&slow_done);
            move |input: NamedOutput| {
                let slow_done = Arc::clone(&slow_done);
                async move {
                    assert!(
                        !slow_done.load(Ordering::SeqCst),
                        "fast downstream work should start before unrelated slow branch finishes"
                    );
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
        .run(Run::new(json!({})).policy(ExecutionPolicy {
            failure: FailurePolicy::FailFast,
            max_concurrency: 3,
            resource_limits: BTreeMap::new(),
            inline_small_plugs: false,
        }))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(
        result
            .output()
            .get::<NamedOutput>("fast_downstream")
            .unwrap(),
        NamedOutput {
            value: "fast".to_string()
        }
    );
}

#[tokio::test]
async fn graph_fail_fast_stops_scheduling_queued_independent_ticks() {
    let mut graph = Graph::new();

    graph
        .plugup("fail", |_: coreflow::Value| async move {
            Err::<coreflow::Value, _>(CoreError::PlugFailed {
                plug: "fail".to_string(),
                message: "boom".to_string(),
            })
        })
        .unwrap()
        .plugup("z_ok", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "should not run".to_string(),
            })
        })
        .unwrap()
        .plugin("fail", "fail")
        .unwrap()
        .plugin("z_ok", "z_ok")
        .unwrap();

    let result = graph
        .run(Run::new(json!({})).policy(ExecutionPolicy {
            failure: FailurePolicy::FailFast,
            max_concurrency: 1,
            resource_limits: BTreeMap::new(),
            inline_small_plugs: false,
        }))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Failed);
    assert!(matches!(
        result.output().get::<NamedOutput>("z_ok"),
        Err(CoreError::UnknownPlug { name }) if name == "z_ok"
    ));
    assert!(result.events.iter().any(|event| matches!(event, RunEvent::JobFailed { plug, error: CoreError::PlugFailed { message, .. }, .. } if plug.to_string() == "fail" && message == "boom")));
}

#[tokio::test]
async fn graph_fail_fast_retains_completed_in_flight_outputs() {
    let mut graph = Graph::new();

    graph
        .plugup("fail", |_: coreflow::Value| async move {
            Err::<coreflow::Value, _>(CoreError::PlugFailed {
                plug: "fail".to_string(),
                message: "boom".to_string(),
            })
        })
        .unwrap()
        .plugup("ok", |_: coreflow::Value| async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            Ok(NamedOutput {
                value: "ok".to_string(),
            })
        })
        .unwrap()
        .plugin("fail", "fail")
        .unwrap()
        .plugin("ok", "ok")
        .unwrap();

    let result = graph
        .run(Run::new(json!({})).policy(ExecutionPolicy {
            failure: FailurePolicy::FailFast,
            max_concurrency: 2,
            resource_limits: BTreeMap::new(),
            inline_small_plugs: false,
        }))
        .await
        .unwrap();

    assert_eq!(result.status, GraphRunStatus::Failed);
    assert_eq!(
        result.output().get::<NamedOutput>("ok").unwrap(),
        NamedOutput {
            value: "ok".to_string()
        },
        "FailFast should retain outputs from jobs that were already in flight and completed"
    );
}

#[tokio::test]
async fn graph_policy_picker_changes_ready_tick_order() {
    let mut graph = Graph::new();

    graph
        .plugup("alpha", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "alpha".to_string(),
            })
        })
        .unwrap()
        .plugup("beta", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "beta".to_string(),
            })
        })
        .unwrap()
        .plugin("alpha", "alpha")
        .unwrap()
        .plugin("beta", "beta")
        .unwrap();

    graph.check().unwrap();
    let result = graph
        .run(
            Run::new(json!({}))
                .policy(ExecutionPolicy {
                    failure: FailurePolicy::FailFast,
                    max_concurrency: 1,
                    resource_limits: BTreeMap::new(),
                    inline_small_plugs: false,
                })
                .picker(PickerStrategy::Lifo),
        )
        .await
        .unwrap();

    assert_eq!(
        result.events.iter().find_map(job_done_plug).unwrap(),
        "beta",
        "LIFO picker should run the most recently queued ready tick before earlier ones"
    );
}

#[tokio::test]
async fn graph_default_fifo_picker_runs_earliest_ready_tick_first() {
    let mut graph = Graph::new();

    graph
        .plugup("alpha", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "alpha".to_string(),
            })
        })
        .unwrap()
        .plugup("beta", |_: coreflow::Value| async move {
            Ok(NamedOutput {
                value: "beta".to_string(),
            })
        })
        .unwrap()
        .plugin("alpha", "alpha")
        .unwrap()
        .plugin("beta", "beta")
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

    assert_eq!(
        result.events.iter().find_map(job_done_plug).unwrap(),
        "alpha",
        "FIFO picker should run the earliest queued ready tick before later ready ticks"
    );
}

#[test]
fn graph_execution_policy_exposes_inline_small_plugs_switch() {
    let policy = ExecutionPolicy {
        failure: FailurePolicy::FailFast,
        max_concurrency: 1,
        resource_limits: BTreeMap::new(),
        inline_small_plugs: true,
    };

    assert_eq!(
        serde_json::to_value(policy).unwrap()["inline_small_plugs"],
        json!(true),
        "ExecutionPolicy should expose the documented inline_small_plugs fast-path switch"
    );
}
