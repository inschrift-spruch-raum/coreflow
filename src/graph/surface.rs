use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::json;

use crate::{
    CoreError, CoreResult, FieldPath, Flow, Plug, PlugImplementation, SourceSelector, Value,
    kernel::{ExecutionPolicy, PickerStrategy},
};

use super::{
    GraphLimits,
    check::{self, GraphIndexes},
    types::{CommitId, GraphChange, GraphCommit, GraphStore, PlugKind, PlugName, Run},
};

impl GraphStore {
    // 快速导入使用 store 中的完整 graph；提交链只作为历史保留，不参与当前态重建。
    /// # Errors
    ///
    /// 当存储的 graph 超过资源限制，包含非法 plug 名，或 flow 超过限制时返回错误。
    pub fn into_graph(self) -> CoreResult<Graph> {
        let mut graph = self.graph;
        graph.limits.check()?;
        let plug_names = graph.checked_plug_names()?;
        let plug_count = plug_names.len();
        if plug_count > graph.limits.plugs {
            return Err(CoreError::ResourceLimitExceeded {
                limit: "max_plugs".to_string(),
                value: plug_count,
            });
        }
        graph
            .flow
            .check_limits(graph.limits.flow_edges, graph.limits.path_depth)?;
        graph.head = Some(self.head);
        graph.next_commit = next_commit_number(&self.commits);
        graph.commits = self.commits;
        Ok(graph)
    }

    // 严格重放从 head 沿 parent 回到根提交，再按时间顺序重新应用每个变更。
    /// # Errors
    ///
    /// 当提交链缺失、成环，或重放后的 graph 不满足资源和 flow 限制时返回错误。
    pub fn replay(self) -> CoreResult<Graph> {
        let mut graph = self.graph;
        graph.replay_commits(&self.head, &self.commits)?;
        graph.limits.check()?;
        let plug_names = graph.checked_plug_names()?;
        let plug_count = plug_names.len();
        if plug_count > graph.limits.plugs {
            return Err(CoreError::ResourceLimitExceeded {
                limit: "max_plugs".to_string(),
                value: plug_count,
            });
        }
        graph
            .flow
            .check_limits(graph.limits.flow_edges, graph.limits.path_depth)?;
        graph.head = Some(self.head);
        graph.next_commit = next_commit_number(&self.commits);
        graph.commits = self.commits;
        Ok(graph)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Graph {
    // plugs/flow 是可存储的声明态；实现、索引和提交游标只属于当前进程。
    plugs: BTreeMap<PlugKind, Vec<PlugName>>,
    flow: Flow,
    #[serde(skip)]
    implementations: BTreeMap<PlugKind, PlugImplementation>,
    #[serde(skip)]
    head: Option<CommitId>,
    #[serde(skip)]
    commits: BTreeMap<CommitId, GraphCommit>,
    #[serde(skip)]
    changes: Vec<GraphChange>,
    #[serde(skip)]
    next_commit: u64,
    #[serde(skip)]
    limits: GraphLimits,
    #[serde(skip)]
    indexes: Option<GraphIndexes>,
}

impl Graph {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn plugs(&self) -> &BTreeMap<PlugKind, Vec<PlugName>> {
        &self.plugs
    }

    #[must_use]
    pub fn flow(&self) -> &Flow {
        &self.flow
    }

    fn replay_commits(
        &mut self, head: &CommitId, commits: &BTreeMap<CommitId, GraphCommit>,
    ) -> CoreResult<()> {
        let mut ordered = Vec::new();
        let mut current = Some(head.clone());
        let mut seen = BTreeSet::new();

        while let Some(id) = current {
            if !seen.insert(id.clone()) {
                return Err(CoreError::InvalidFlow {
                    message: format!("commit chain contains cycle at `{id}`"),
                });
            }
            let commit = commits.get(&id).ok_or_else(|| CoreError::InvalidFlow {
                message: format!("head references missing commit `{id}`"),
            })?;
            ordered.push(commit);
            let next = commit.parent.clone();
            current = next;
        }

        self.plugs.clear();
        self.flow = Flow::default();

        for commit in ordered.into_iter().rev() {
            for change in &commit.changes {
                self.replay_change(change);
            }
        }

        Ok(())
    }

    fn replay_change(&mut self, change: &GraphChange) {
        match change {
            GraphChange::PlugIn { kind, name } => {
                self.plugs
                    .entry(kind.clone())
                    .or_default()
                    .push(name.clone());
            }
            GraphChange::PlugOut(name) => {
                for names in self.plugs.values_mut() {
                    names.retain(|plug_name| plug_name != name);
                }
                self.plugs.retain(|_, names| !names.is_empty());
            }
            GraphChange::FlowIn {
                target,
                input,
                source,
            } => {
                self.flow
                    .0
                    .entry(target.clone())
                    .or_default()
                    .0
                    .insert(input.clone(), source.clone());
            }
            GraphChange::FlowOut { target, input } => {
                if let Some(inputs) = self.flow.0.get_mut(target) {
                    inputs.0.remove(input);
                    if inputs.0.is_empty() {
                        self.flow.0.remove(target);
                    }
                }
            }
            GraphChange::Replace { graph } => {
                self.plugs = graph.plugs.clone();
                self.flow = graph.flow.clone();
            }
        }
    }

    /// # Errors
    ///
    /// 当 plug kind 非法时返回错误。
    pub fn plugup<I, O, F, Fut>(&mut self, kind: &str, function: F) -> CoreResult<&mut Self>
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = CoreResult<O>> + Send + 'static,
    {
        let kind = PlugKind::new(kind);
        validate_plug_kind(&kind)?;
        let implementation = Plug::implementation(function);
        self.implementations
            .insert(kind.clone(), implementation.clone());
        Ok(self)
    }

    /// # Errors
    ///
    /// 当 plug name/kind 非法、kind 未注册、plug 重名，或超过 plug 数量限制时返回错误。
    pub fn plugin(&mut self, name: &str, kind: &str) -> CoreResult<&mut Self> {
        let kind = PlugKind::new(kind);
        let name = PlugName::new(name);
        self.apply_plugin(&kind, &name)?;
        self.changes.push(GraphChange::PlugIn { kind, name });
        Ok(self)
    }

    /// # Errors
    ///
    /// 当 plug 不存在，或仍被 flow 引用时返回错误。
    pub fn plugout(&mut self, name: &str) -> CoreResult<&mut Self> {
        let name = PlugName::new(name);
        self.apply_plugout(&name)?;
        self.changes.push(GraphChange::PlugOut(name));
        Ok(self)
    }

    fn flow_references_plug(&self, name: &PlugName) -> bool {
        self.flow.0.contains_key(name)
            || self
                .flow
                .0
                .values()
                .any(|inputs| inputs.0.values().any(|selector| &selector.plug == name))
    }

    /// # Errors
    ///
    /// 当 flow 声明非法、引用重复输入，或超过 flow 边数量/路径深度限制时返回错误。
    pub fn flowin(&mut self, flow: impl Borrow<Value>) -> CoreResult<&mut Self> {
        let flow = flow.borrow();
        let flow = self.flowin_changes(flow)?;
        self.apply_flowin(flow.clone())?;
        self.indexes = None;
        for (target, inputs) in flow.0 {
            for (input, source) in inputs.0 {
                self.changes.push(GraphChange::FlowIn {
                    target: target.clone(),
                    input,
                    source,
                });
            }
        }
        Ok(self)
    }

    fn flowin_changes(&self, flow: &Value) -> CoreResult<Flow> {
        let flow = Flow::parse(flow)?;
        flow.check_limits(self.limits.flow_edges, self.limits.path_depth)?;
        Ok(flow)
    }

    fn apply_flowin(&mut self, flow: Flow) -> CoreResult<()> {
        if let Some((target, input)) = self.duplicate_flow_input(&flow) {
            return Err(CoreError::DuplicateFlowInput {
                target: target.to_string(),
                input: input.0.clone(),
            });
        }
        let next_edge_count = self.flow.edge_count() + flow.edge_count();
        if next_edge_count > self.limits.flow_edges {
            return Err(CoreError::ResourceLimitExceeded {
                limit: "max_flow_edges".to_string(),
                value: next_edge_count,
            });
        }
        self.flow.merge(flow);
        self.indexes = None;
        Ok(())
    }

    fn duplicate_flow_input(&self, flow: &Flow) -> Option<(PlugName, FieldPath)> {
        flow.0.iter().find_map(|(target, inputs)| {
            let existing = self.flow.0.get(target)?;
            let input = inputs
                .0
                .keys()
                .find(|input| existing.0.contains_key(*input))?;
            Some((target.clone(), input.clone()))
        })
    }

    /// # Errors
    ///
    /// 当 flowout 声明非法时返回错误。
    pub fn flowout(&mut self, flow: impl Borrow<Value>) -> CoreResult<&mut Self> {
        let flow = flow.borrow();
        let removed = self.flow.removal_flow(flow)?;
        self.flow.remove(flow)?;
        self.indexes = None;
        for (target, inputs) in removed.0 {
            for input in inputs.0.into_keys() {
                self.changes.push(GraphChange::FlowOut {
                    target: target.clone(),
                    input,
                });
            }
        }
        Ok(self)
    }

    /// # Errors
    ///
    /// 当提交中的任一 graph change 非法时返回错误。
    pub fn commit(&mut self, message: impl Into<String>) -> CoreResult<&mut Self> {
        if self.changes.is_empty() {
            return Ok(self);
        }
        let changes = std::mem::take(&mut self.changes);
        let next_commit = if self.next_commit == 0 {
            1
        } else {
            self.next_commit
        };
        let id = format!("C{next_commit:08}");
        let commit = GraphCommit {
            id: id.clone(),
            parent: self.head.clone(),
            message: message.into(),
            changes,
        };
        self.commits.insert(id.clone(), commit);
        self.head = Some(id);
        self.next_commit = next_commit + 1;
        self.indexes = None;
        Ok(self)
    }

    /// # Errors
    ///
    /// 当 graph 包含非法 plug、未知实现、非法 flow，或超过资源限制时返回错误。
    pub fn check(&mut self) -> CoreResult<()> {
        self.indexes = Some(self.build_indexes()?);
        Ok(())
    }

    fn build_indexes(&self) -> CoreResult<GraphIndexes> {
        self.limits.check()?;
        let plug_names = self.checked_plug_names()?;
        for kind in self.plugs.keys() {
            if !self.implementations.contains_key(kind) {
                return Err(CoreError::UnknownPlug {
                    name: kind.to_string(),
                });
            }
        }
        let plug_count = plug_names.len();
        if plug_count > self.limits.plugs {
            return Err(CoreError::ResourceLimitExceeded {
                limit: "max_plugs".to_string(),
                value: plug_count,
            });
        }
        self.flow
            .check_limits(self.limits.flow_edges, self.limits.path_depth)?;
        check::check_graph(&plug_names, &self.flow)
    }

    /// # Errors
    ///
    /// 当 graph 检查失败、plug 执行失败，或运行时 graph mutation 非法时返回错误。
    pub async fn run(&mut self, run: impl Into<Run>) -> CoreResult<crate::GraphResult> {
        let run = run.into();
        self.run_checked_mut(run.initial, run.seeds, run.policy, run.picker)
            .await
    }

    async fn run_checked_mut(
        &mut self, initial: Value, seeds: Option<Vec<PlugName>>, policy: ExecutionPolicy,
        picker: PickerStrategy,
    ) -> CoreResult<crate::GraphResult> {
        // 运行使用 working_graph，只有本轮决定返回结果时才写回 self，避免半轮 mutation 污染原图。
        let mut working_graph = self.clone();
        working_graph.check()?;

        let mut run_initial = initial;
        let mut run_seeds = seeds;
        let mut suppressed_entries = BTreeSet::new();

        loop {
            let runtime_plugs = working_graph.build_runtime_plugs()?;
            let indexes = working_graph
                .indexes
                .as_ref()
                .ok_or(CoreError::GraphNotChecked)?;
            let graph_commit = working_graph
                .head
                .clone()
                .unwrap_or_else(|| "working-tree".to_string());
            let result = crate::kernel::run_graph(crate::kernel::RunGraphArgs {
                plugs: &runtime_plugs,
                input_binds: &indexes.input_binds,
                reverse_dependencies: &indexes.reverse_dependencies,
                suppressed_entries: &suppressed_entries,
                initial: run_initial,
                seeds: run_seeds,
                graph_commit,
                policy: policy.clone(),
                picker_strategy: picker,
            })
            .await?;

            if result.status != crate::GraphRunStatus::Idle {
                *self = working_graph;
                return Ok(result);
            }

            let graph_updates = result
                .outputs
                .iter()
                .filter_map(|(plug, value)| {
                    serde_json::from_value::<GraphChange>(value.clone())
                        .ok()
                        .map(GraphUpdate::GraphChange)
                        .or_else(|| {
                            serde_json::from_value::<Graph>(value.clone())
                                .ok()
                                .map(GraphUpdate::NextGraph)
                        })
                        .map(|update| (plug.clone(), update))
                })
                .collect::<Vec<_>>();
            if graph_updates.is_empty() {
                *self = working_graph;
                return Ok(result);
            }

            for (plug, update) in &graph_updates {
                suppressed_entries.insert(plug.clone());
                working_graph.apply_graph_update(update)?;
            }
            working_graph.check()?;
            run_initial = Value::Object(
                result
                    .outputs
                    .into_iter()
                    .filter(|(plug, _)| !suppressed_entries.contains(plug))
                    .map(|(plug, value)| (plug.to_string(), value))
                    .collect(),
            );
            run_seeds = None;
        }
    }

    fn apply_graph_update(&mut self, update: &GraphUpdate) -> CoreResult<()> {
        match update {
            GraphUpdate::GraphChange(change) => {
                self.apply_change(change)?;
                self.changes.push(change.clone());
                self.commit("commit").map(|_| ())
            }
            GraphUpdate::NextGraph(graph) => {
                let change = GraphChange::Replace {
                    graph: Box::new(graph.storage_snapshot()),
                };
                self.apply_change(&change)?;
                self.changes.push(change);
                self.commit("replace graph").map(|_| ())
            }
        }
    }

    fn apply_change(&mut self, change: &GraphChange) -> CoreResult<()> {
        match change {
            GraphChange::PlugIn { kind, name } => self.apply_plugin(kind, name),
            GraphChange::PlugOut(name) => self.apply_plugout(name),
            GraphChange::FlowIn {
                target,
                input,
                source,
            } => {
                let flow = flowin_change_value(target, input, source)?;
                let flow = self.flowin_changes(&flow)?;
                self.apply_flowin(flow)
            }
            GraphChange::FlowOut { target, input } => {
                let flow = json!({ target.to_string(): [input.0.clone()] });
                self.flow.remove(&flow)?;
                self.indexes = None;
                Ok(())
            }
            GraphChange::Replace { graph } => self.apply_replace_graph(graph),
        }
    }

    fn apply_plugin(&mut self, kind: &PlugKind, name: &PlugName) -> CoreResult<()> {
        validate_plug_name(name)?;
        validate_plug_kind(kind)?;

        if !self.implementations.contains_key(kind) {
            return Err(CoreError::UnknownPlug {
                name: kind.to_string(),
            });
        }

        if self.plug_names().contains(name) {
            return Err(CoreError::DuplicatePlug {
                name: name.to_string(),
            });
        }

        let next_plug_count = self.plug_names().len() + 1;
        if next_plug_count > self.limits.plugs {
            return Err(CoreError::ResourceLimitExceeded {
                limit: "max_plugs".to_string(),
                value: next_plug_count,
            });
        }

        self.plugs
            .entry(kind.clone())
            .or_default()
            .push(name.clone());
        self.indexes = None;
        Ok(())
    }

    fn apply_plugout(&mut self, name: &PlugName) -> CoreResult<()> {
        if !self.plug_names().contains(name) {
            return Err(CoreError::UnknownPlug {
                name: name.to_string(),
            });
        }

        if self.flow_references_plug(name) {
            return Err(CoreError::PlugReferencedByFlow {
                name: name.to_string(),
            });
        }

        for names in self.plugs.values_mut() {
            names.retain(|plug_name| plug_name != name);
        }
        self.plugs.retain(|_, names| !names.is_empty());
        self.indexes = None;
        Ok(())
    }

    fn apply_replace_graph(&mut self, graph: &Graph) -> CoreResult<()> {
        graph.limits.check()?;
        let plug_names = graph.checked_plug_names()?;
        if plug_names.len() > self.limits.plugs {
            return Err(CoreError::ResourceLimitExceeded {
                limit: "max_plugs".to_string(),
                value: plug_names.len(),
            });
        }
        graph
            .flow
            .check_limits(self.limits.flow_edges, self.limits.path_depth)?;

        self.plugs = graph.plugs.clone();
        self.flow = graph.flow.clone();
        self.indexes = None;
        Ok(())
    }

    fn storage_snapshot(&self) -> Graph {
        let mut graph = self.clone();
        graph.implementations.clear();
        graph.indexes = None;
        graph.limits = GraphLimits::default();
        graph.head = None;
        graph.commits.clear();
        graph.changes.clear();
        graph.next_commit = 0;
        graph
    }

    /// # Errors
    ///
    /// 当当前 graph 无法转换为可序列化存储快照时返回错误。
    pub fn store(&self) -> CoreResult<GraphStore> {
        let graph = self.storage_snapshot();
        Ok(GraphStore {
            head: self
                .head
                .clone()
                .unwrap_or_else(|| "working-tree".to_string()),
            graph,
            commits: self.commits.clone(),
        })
    }

    /// # Errors
    ///
    /// 当 store 序列化失败，或目标路径写入失败时返回错误。
    pub fn save(&self, path: impl AsRef<Path>) -> CoreResult<()> {
        let json = serde_json::to_string_pretty(&self.store()?)?;
        std::fs::write(path, json).map_err(|error| CoreError::Io {
            message: error.to_string(),
        })
    }

    /// # Errors
    ///
    /// 当文件读取失败、JSON 解析失败，或 store 无法导入 graph 时返回错误。
    pub fn load(path: impl AsRef<Path>) -> CoreResult<Self> {
        let json = std::fs::read_to_string(path).map_err(|error| CoreError::Io {
            message: error.to_string(),
        })?;
        let store: GraphStore = serde_json::from_str(&json)?;
        store.into_graph()
    }

    fn plug_names(&self) -> BTreeSet<PlugName> {
        self.plugs
            .values()
            .flat_map(|names| names.iter().cloned())
            .collect()
    }

    fn checked_plug_names(&self) -> CoreResult<BTreeSet<PlugName>> {
        let mut seen = BTreeSet::new();
        for name in self.plugs.values().flatten() {
            validate_plug_name(name)?;
            if !seen.insert(name.clone()) {
                return Err(CoreError::DuplicatePlug {
                    name: name.to_string(),
                });
            }
        }
        Ok(seen)
    }

    fn build_runtime_plugs(&self) -> CoreResult<BTreeMap<PlugName, Plug>> {
        let mut runtime_plugs = BTreeMap::new();
        for (kind, names) in &self.plugs {
            let implementation =
                self.implementations
                    .get(kind)
                    .cloned()
                    .ok_or_else(|| CoreError::UnknownPlug {
                        name: kind.to_string(),
                    })?;
            for name in names {
                runtime_plugs.insert(
                    name.clone(),
                    Plug::from_implementation(name.clone(), implementation.clone()),
                );
            }
        }
        Ok(runtime_plugs)
    }
}

enum GraphUpdate {
    GraphChange(GraphChange),
    NextGraph(Graph),
}

fn flowin_change_value(
    target: &PlugName, input: &FieldPath, source: &SourceSelector,
) -> CoreResult<Value> {
    let source = serde_json::to_value(source)?
        .as_str()
        .expect("source selector serializes to string")
        .to_string();
    if input.0.is_empty() {
        return Ok(json!({ target.to_string(): source }));
    }
    Ok(json!({
        target.to_string(): {
            input.0.clone(): source
        }
    }))
}

fn validate_plug_name(name: &PlugName) -> CoreResult<()> {
    let name = name.to_string();
    if name.is_empty() || name.contains('.') || name.chars().any(char::is_control) {
        return Err(CoreError::InvalidFlow {
            message: format!(
                "plug name `{name}` must be non-empty and cannot contain dots or control characters"
            ),
        });
    }
    Ok(())
}

fn validate_plug_kind(kind: &PlugKind) -> CoreResult<()> {
    let kind = kind.to_string();
    if kind.is_empty() || kind.chars().any(char::is_control) {
        return Err(CoreError::InvalidFlow {
            message: format!(
                "plug kind `{kind}` must be non-empty and cannot contain control characters"
            ),
        });
    }
    Ok(())
}

fn next_commit_number(commits: &BTreeMap<CommitId, GraphCommit>) -> u64 {
    commits
        .keys()
        .filter_map(|id| id.strip_prefix('C'))
        .filter_map(|number| number.parse::<u64>().ok())
        .max()
        .map_or(1, |number| number + 1)
}
