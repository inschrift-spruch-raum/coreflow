use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    panic::AssertUnwindSafe,
    time::Instant,
};

use futures_util::FutureExt;
use serde_json::Map;
use tokio::task::{Id as TaskId, JoinSet};

use crate::{
    CoreError, CoreResult, GraphRunStatus, InputBind, Plug, PlugName, RunEvent, Value, flow,
    graph::{
        GraphLimits,
        output::{DoneTick, OutputVersion, PlugFailure, UnfinishedTick, UnfinishedTickState},
    },
};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Tick {
    pub id: u64,
    pub plug: PlugName,
    pub input: Value,
    pub queued_at: Instant,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum JobOutcome {
    Done {
        plug: PlugName,
        tick: u64,
    },
    Failed {
        plug: PlugName,
        tick: u64,
        error: CoreError,
    },
}

pub(crate) struct RunGraphArgs<'a> {
    pub(crate) plugs: &'a BTreeMap<PlugName, Plug>,
    pub(crate) input_binds: &'a BTreeMap<PlugName, InputBind>,
    pub(crate) reverse_dependencies: &'a BTreeMap<PlugName, Vec<PlugName>>,
    pub(crate) suppressed_entries: &'a BTreeSet<PlugName>,
    pub(crate) initial: Value,
    pub(crate) seeds: Option<Vec<PlugName>>,
    pub(crate) graph_commit: crate::CommitId,
    pub(crate) policy: ExecutionPolicy,
    pub(crate) picker_strategy: PickerStrategy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FailurePolicy {
    FailFast,
    ContinueIndependent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PickerStrategy {
    #[default]
    Fifo,
    Lifo,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExecutionPolicy {
    pub failure: FailurePolicy,
    pub max_concurrency: usize,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub resource_limits: BTreeMap<String, usize>,
    #[serde(default, skip_serializing_if = "is_default")]
    pub inline_small_plugs: bool,
}

impl ExecutionPolicy {
    /// # Errors
    ///
    /// 当最大并发数或任一资源并发限制为 0 时返回错误。
    pub fn check(self) -> CoreResult<Self> {
        if self.max_concurrency == 0 {
            return Err(CoreError::ResourceLimitExceeded {
                limit: "max_concurrency".to_string(),
                value: self.max_concurrency,
            });
        }
        for (resource, limit) in &self.resource_limits {
            if *limit == 0 {
                return Err(CoreError::ResourceLimitExceeded {
                    limit: format!("resource:{resource}"),
                    value: *limit,
                });
            }
        }
        Ok(self)
    }
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    value == &T::default()
}

impl Default for ExecutionPolicy {
    fn default() -> Self {
        let limits = GraphLimits::default();
        Self {
            failure: FailurePolicy::FailFast,
            max_concurrency: limits.concurrency,
            resource_limits: BTreeMap::new(),
            inline_small_plugs: false,
        }
    }
}

pub(crate) trait Picker {
    fn pick(&mut self, queue: &mut VecDeque<Tick>, capacity: usize) -> Vec<Tick>;

    fn on_done(&mut self, outcome: &JobOutcome);
}

#[derive(Debug, Default)]
pub(crate) struct FifoPicker;

impl Picker for FifoPicker {
    fn pick(&mut self, queue: &mut VecDeque<Tick>, capacity: usize) -> Vec<Tick> {
        let mut ticks = Vec::new();
        while ticks.len() < capacity {
            let Some(tick) = queue.pop_front() else {
                break;
            };
            ticks.push(tick);
        }
        ticks
    }

    fn on_done(&mut self, _outcome: &JobOutcome) {}
}

#[derive(Debug, Default)]
pub(crate) struct LifoPicker;

impl Picker for LifoPicker {
    fn pick(&mut self, queue: &mut VecDeque<Tick>, capacity: usize) -> Vec<Tick> {
        let mut ticks = Vec::new();
        while ticks.len() < capacity {
            let Some(tick) = queue.pop_back() else {
                break;
            };
            ticks.push(tick);
        }
        ticks
    }

    fn on_done(&mut self, _outcome: &JobOutcome) {}
}

pub(crate) async fn run_graph(args: RunGraphArgs<'_>) -> CoreResult<crate::GraphResult> {
    KernelRun::new(args)?.run().await
}

struct KernelRun<'a> {
    plugs: &'a BTreeMap<PlugName, Plug>,
    input_binds: &'a BTreeMap<PlugName, InputBind>,
    reverse_dependencies: &'a BTreeMap<PlugName, Vec<PlugName>>,
    graph_commit: crate::CommitId,
    policy: ExecutionPolicy,
    picker: Box<dyn Picker>,
    run_started_at: Instant,
    tick_queue: VecDeque<Tick>,
    next_tick: u64,
    events: Vec<RunEvent>,
    unfinished_ticks: Vec<UnfinishedTick>,
    input_snapshots: BTreeMap<PlugName, Value>,
    jobs: JoinSet<(Tick, CoreResult<Value>)>,
    outputs: BTreeMap<PlugName, Value>,
    output_versions: Vec<OutputVersion>,
    done_ticks: Vec<DoneTick>,
    versions: BTreeMap<PlugName, u64>,
    failures: Vec<PlugFailure>,
    failed: bool,
    running_ticks: BTreeMap<TaskId, Tick>,
    inline_completed: Option<(Tick, CoreResult<Value>)>,
}

impl<'a> KernelRun<'a> {
    fn new(args: RunGraphArgs<'a>) -> CoreResult<Self> {
        let RunGraphArgs {
            plugs,
            input_binds,
            reverse_dependencies,
            suppressed_entries,
            initial,
            seeds,
            graph_commit,
            policy,
            picker_strategy,
        } = args;
        let policy = policy.check()?;
        let picker: Box<dyn Picker> = match picker_strategy {
            PickerStrategy::Fifo => Box::new(FifoPicker),
            PickerStrategy::Lifo => Box::new(LifoPicker),
        };
        let mut run = Self {
            plugs,
            input_binds,
            reverse_dependencies,
            graph_commit,
            policy,
            picker,
            run_started_at: Instant::now(),
            tick_queue: VecDeque::new(),
            next_tick: 0,
            events: vec![RunEvent::GraphStarted],
            unfinished_ticks: Vec::new(),
            input_snapshots: BTreeMap::new(),
            jobs: JoinSet::new(),
            outputs: BTreeMap::new(),
            output_versions: Vec::new(),
            done_ticks: Vec::new(),
            versions: BTreeMap::new(),
            failures: Vec::new(),
            failed: false,
            running_ticks: BTreeMap::new(),
            inline_completed: None,
        };
        run.queue_entry_ticks(&initial, seeds, suppressed_entries);
        Ok(run)
    }

    async fn run(mut self) -> CoreResult<crate::GraphResult> {
        loop {
            if !self.failed {
                self.schedule_ready_ticks().await?;
            }
            if let Some(result) = self.finish_if_idle() {
                return Ok(result);
            }
            let Some((tick, output)) = self.take_completed_tick().await? else {
                continue;
            };
            self.handle_completed_tick(&tick, output)?;
        }
    }

    fn queue_entry_ticks(
        &mut self, initial: &Value, seeds: Option<Vec<PlugName>>,
        suppressed_entries: &BTreeSet<PlugName>,
    ) {
        let dependent_targets: BTreeSet<_> = self.input_binds.keys().cloned().collect();
        let mut entry_plugs = seeds.unwrap_or_else(|| {
            self.plugs
                .keys()
                .filter(|plug| {
                    !dependent_targets.contains(*plug) && !suppressed_entries.contains(*plug)
                })
                .cloned()
                .collect()
        });
        if let Some(initial_object) = initial.as_object() {
            for plug in self.plugs.keys().filter(|plug| {
                initial_object.contains_key(&plug.to_string())
                    && !suppressed_entries.contains(*plug)
            }) {
                if !entry_plugs.contains(plug) {
                    entry_plugs.push(plug.clone());
                }
            }
        }
        for plug in entry_plugs {
            let input = initial
                .as_object()
                .and_then(|object| object.get(&plug.to_string()))
                .cloned()
                .unwrap_or_else(|| initial.clone());
            self.input_snapshots.insert(plug.clone(), input.clone());
            self.queue_tick(plug, input);
        }
    }

    async fn schedule_ready_ticks(&mut self) -> CoreResult<()> {
        let capacity = self.policy.max_concurrency.saturating_sub(self.jobs.len());
        let queued = self.tick_queue.len();
        let mut deferred_ticks = VecDeque::new();
        let mut started_ticks = 0;
        let picked_ticks = self.picker.pick(&mut self.tick_queue, queued).into_iter();
        for tick in picked_ticks {
            if started_ticks >= capacity {
                deferred_ticks.push_back(tick);
                continue;
            }
            let Some(plug) = self.plugs.get(&tick.plug).cloned() else {
                return Err(CoreError::UnknownPlug {
                    name: tick.plug.to_string(),
                });
            };
            self.record_tick_start(&tick);
            self.start_tick(tick, plug, queued == 1).await;
            started_ticks += 1;
            if self.inline_completed.is_some() {
                break;
            }
        }
        while let Some(tick) = deferred_ticks.pop_back() {
            self.tick_queue.push_front(tick);
        }
        Ok(())
    }

    fn record_tick_start(&mut self, tick: &Tick) {
        push_event(
            &mut self.events,
            RunEvent::PlugInputBuilt {
                plug: tick.plug.clone(),
                tick: tick.id,
            },
        );
        push_event(
            &mut self.events,
            RunEvent::TickWaitTime {
                plug: tick.plug.clone(),
                tick: tick.id,
                micros: tick.queued_at.elapsed().as_micros(),
            },
        );
    }

    async fn start_tick(&mut self, tick: Tick, plug: Plug, only_queued_tick: bool) {
        push_event(
            &mut self.events,
            RunEvent::JobStarted {
                plug: tick.plug.clone(),
                tick: tick.id,
            },
        );
        if only_queued_tick || self.policy.inline_small_plugs {
            let output = call_plug(&plug, &tick).await;
            self.inline_completed = Some((tick, output));
            return;
        }
        let running_tick = tick.clone();
        let abort = self.jobs.spawn(async move {
            let output = call_plug(&plug, &tick).await;
            (tick, output)
        });
        self.running_ticks.insert(abort.id(), running_tick);
    }

    fn finish_if_idle(&mut self) -> Option<crate::GraphResult> {
        if self.inline_completed.is_some() || !self.jobs.is_empty() {
            return None;
        }
        if self.failed || !self.failures.is_empty() {
            for tick in self.tick_queue.drain(..) {
                self.unfinished_ticks.push(UnfinishedTick {
                    plug: tick.plug,
                    tick: tick.id,
                    state: UnfinishedTickState::BlockedByFailure,
                });
            }
            self.push_duration();
            return Some(crate::GraphResult {
                graph_commit: self.graph_commit.clone(),
                outputs: std::mem::take(&mut self.outputs),
                events: std::mem::take(&mut self.events),
                status: GraphRunStatus::Failed,
            });
        }
        push_event(&mut self.events, RunEvent::GraphIdle);
        self.push_duration();
        Some(crate::GraphResult {
            graph_commit: self.graph_commit.clone(),
            outputs: std::mem::take(&mut self.outputs),
            events: std::mem::take(&mut self.events),
            status: GraphRunStatus::Idle,
        })
    }

    async fn take_completed_tick(&mut self) -> CoreResult<Option<(Tick, CoreResult<Value>)>> {
        if let Some(completed) = self.inline_completed.take() {
            return Ok(Some(completed));
        }
        let Some(joined) = self.jobs.join_next_with_id().await else {
            return Ok(None);
        };
        match joined {
            Ok((task_id, joined)) => {
                let _ = self.running_ticks.remove(&task_id);
                Ok(Some(joined))
            }
            Err(error) => {
                let Some(tick) = self.running_ticks.remove(&error.id()) else {
                    return Err(CoreError::InvalidFlow {
                        message: format!("join error from unknown task: {error}"),
                    });
                };
                self.record_failure(
                    &tick,
                    CoreError::PlugFailed {
                        plug: tick.plug.to_string(),
                        message: error.to_string(),
                    },
                );
                self.failed = true;
                Ok(None)
            }
        }
    }

    fn handle_completed_tick(&mut self, tick: &Tick, output: CoreResult<Value>) -> CoreResult<()> {
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                self.record_failure(tick, error);
                if self.policy.failure == FailurePolicy::FailFast {
                    self.failed = true;
                }
                return Ok(());
            }
        };
        let Some(plug) = self.plugs.get(&tick.plug) else {
            return Err(CoreError::UnknownPlug {
                name: tick.plug.to_string(),
            });
        };
        let _ = plug;
        self.record_success(tick, output);
        if !self.failed {
            self.propagate_flow(tick);
        }
        Ok(())
    }

    fn record_success(&mut self, tick: &Tick, output: Value) {
        let version = self.versions.entry(tick.plug.clone()).or_insert(0);
        *version += 1;
        let version = *version;
        self.output_versions.push(OutputVersion {
            plug: tick.plug.clone(),
            tick: tick.id,
            version,
            value: output.clone(),
        });
        self.done_ticks.push(DoneTick {
            plug: tick.plug.clone(),
            tick: tick.id,
            version,
        });
        self.picker.on_done(&JobOutcome::Done {
            plug: tick.plug.clone(),
            tick: tick.id,
        });
        let pending = crate::PendingApproval::from_value(&output);
        self.outputs.insert(tick.plug.clone(), output);
        push_event(
            &mut self.events,
            RunEvent::JobDone {
                plug: tick.plug.clone(),
                tick: tick.id,
            },
        );
        if let Some(pending) = pending {
            push_event(
                &mut self.events,
                RunEvent::PendingApproval {
                    plug: tick.plug.clone(),
                    tick: tick.id,
                    reason: pending.reason,
                },
            );
        }
    }

    fn propagate_flow(&mut self, tick: &Tick) {
        let Some(targets) = self.reverse_dependencies.get(&tick.plug) else {
            return;
        };
        for target in targets {
            let Some(bind) = self.input_binds.get(target) else {
                continue;
            };
            let input = match build_if_ready(bind, &self.outputs) {
                Ok(Some(input)) => input,
                Ok(None) => continue,
                Err(error) => {
                    self.record_target_failure(target, tick.id, error);
                    if self.policy.failure == FailurePolicy::FailFast {
                        self.failed = true;
                        break;
                    }
                    continue;
                }
            };
            if self.input_snapshots.get(target) == Some(&input) {
                continue;
            }
            self.input_snapshots.insert(target.clone(), input.clone());
            push_event(
                &mut self.events,
                RunEvent::FlowPropagated {
                    source: tick.plug.clone(),
                    target: target.clone(),
                },
            );
            self.queue_tick(target.clone(), input);
        }
    }

    fn record_failure(&mut self, tick: &Tick, error: CoreError) {
        self.failures.push(PlugFailure {
            plug: tick.plug.clone(),
            tick: tick.id,
            error: error.clone(),
        });
        self.picker.on_done(&JobOutcome::Failed {
            plug: tick.plug.clone(),
            tick: tick.id,
            error: error.clone(),
        });
        push_event(
            &mut self.events,
            RunEvent::JobFailed {
                plug: tick.plug.clone(),
                tick: tick.id,
                error,
            },
        );
        block_dependents(
            &tick.plug,
            tick.id,
            self.reverse_dependencies,
            &mut self.unfinished_ticks,
        );
    }

    fn record_target_failure(&mut self, target: &PlugName, tick: u64, error: CoreError) {
        self.failures.push(PlugFailure {
            plug: target.clone(),
            tick,
            error: error.clone(),
        });
        push_event(
            &mut self.events,
            RunEvent::JobFailed {
                plug: target.clone(),
                tick,
                error,
            },
        );
    }

    fn queue_tick(&mut self, plug: PlugName, input: Value) -> u64 {
        let tick = self.next_tick;
        self.tick_queue.push_back(Tick {
            id: tick,
            plug: plug.clone(),
            input,
            queued_at: Instant::now(),
        });
        self.next_tick += 1;
        push_event(&mut self.events, RunEvent::TickQueued { plug, tick });
        tick
    }

    fn push_duration(&mut self) {
        push_event(
            &mut self.events,
            RunEvent::Duration {
                micros: self.run_started_at.elapsed().as_micros(),
            },
        );
    }
}

async fn call_plug(plug: &Plug, tick: &Tick) -> CoreResult<Value> {
    AssertUnwindSafe(plug.call(tick.input.clone()))
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
            Err(CoreError::PlugFailed {
                plug: tick.plug.to_string(),
                message: "plug panicked".to_string(),
            })
        })
}

fn block_dependents(
    failed_plug: &PlugName, tick: u64, reverse_dependencies: &BTreeMap<PlugName, Vec<PlugName>>,
    unfinished_ticks: &mut Vec<UnfinishedTick>,
) {
    let mut blocked = VecDeque::from([failed_plug.clone()]);
    let mut seen = BTreeSet::new();

    while let Some(source) = blocked.pop_front() {
        let Some(targets) = reverse_dependencies.get(&source) else {
            continue;
        };

        for target in targets {
            if !seen.insert(target.clone()) {
                continue;
            }
            unfinished_ticks.push(UnfinishedTick {
                plug: target.clone(),
                tick,
                state: UnfinishedTickState::BlockedByFailure,
            });
            blocked.push_back(target.clone());
        }
    }
}

fn push_event(events: &mut Vec<RunEvent>, event: RunEvent) {
    events.push(event);
}

fn build_if_ready(
    bind: &InputBind, outputs: &BTreeMap<PlugName, Value>,
) -> CoreResult<Option<Value>> {
    let mut input = Value::Object(Map::default());

    for (target_field, selector) in &bind.input.0 {
        let Some(source_output) = outputs.get(&selector.plug) else {
            return Ok(None);
        };
        let source_value = flow::read_path(source_output, &selector.path).ok_or_else(|| {
            CoreError::FlowPathNotFound {
                plug: selector.plug.to_string(),
                path: selector.path.0.clone(),
            }
        })?;
        flow::write_path(&mut input, target_field, source_value.clone())?;
    }

    Ok(Some(input))
}
