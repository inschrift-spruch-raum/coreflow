use std::collections::{BTreeMap, BTreeSet};

use crate::{CoreError, CoreResult, Flow, InputBind, PlugName};

// GraphIndexes 是 check 阶段的派生索引，kernel 运行时只读取它，不重新解释 flow JSON。
#[derive(Debug, Clone, Default)]
pub(crate) struct GraphIndexes {
    pub(crate) input_binds: BTreeMap<PlugName, InputBind>,
    pub(crate) reverse_dependencies: BTreeMap<PlugName, Vec<PlugName>>,
}

pub(crate) fn check_graph(
    plugs: &BTreeSet<PlugName>, _registry: &BTreeMap<PlugName, crate::Plug>, flow: &Flow,
) -> CoreResult<GraphIndexes> {
    let mut indexes = GraphIndexes::default();

    for (target, input_map) in &flow.0 {
        if !plugs.contains(target) {
            return Err(CoreError::UnknownFlowTarget {
                target: target.to_string(),
            });
        }

        for selector in input_map.0.values() {
            if !plugs.contains(&selector.plug) {
                return Err(CoreError::UnknownFlowSource {
                    target: target.to_string(),
                    source: selector.plug.to_string(),
                });
            }

            indexes
                .reverse_dependencies
                .entry(selector.plug.clone())
                .or_default()
                .push(target.clone());
        }

        indexes.input_binds.insert(
            target.clone(),
            InputBind {
                target: target.clone(),
                input: input_map.clone(),
            },
        );
    }

    Ok(indexes)
}
