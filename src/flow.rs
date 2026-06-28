use std::{borrow::Borrow, collections::BTreeMap};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::Map;

use crate::{CoreError, CoreResult, Value};

// FieldPath 使用点号路径描述 JSON 字段；空路径表示整个 plug 输出或输入。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldPath(pub String);

impl FieldPath {
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.0.split('.').filter(|segment| !segment.is_empty())
    }
}

impl From<&str> for FieldPath {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

// SourceSelector 形如 `plug` 或 `plug.path.to.field`，用于声明一个输入字段来自哪个上游输出。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSelector {
    pub plug: crate::PlugName,
    pub path: FieldPath,
}

impl Serialize for SourceSelector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.path.0.is_empty() {
            serializer.serialize_str(&self.plug.to_string())
        } else {
            serializer.serialize_str(&format!("{}.{}", self.plug, self.path.0))
        }
    }
}

impl<'de> Deserialize<'de> for SourceSelector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let selector = String::deserialize(deserializer)?;
        parse_selector(&selector).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct InputMap(pub BTreeMap<FieldPath, SourceSelector>);

// Flow 是 target-keyed 声明：每个目标 plug 映射到它需要的输入字段来源。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Flow(pub BTreeMap<crate::PlugName, InputMap>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputBind {
    pub target: crate::PlugName,
    pub input: InputMap,
}

pub type PlugInput = Value;

impl Flow {
    /// # Errors
    ///
    /// 当 flow 声明不是 target-keyed JSON object，selector 形态非法，或字段重复时返回错误。
    pub fn parse(value: impl Borrow<Value>) -> CoreResult<Self> {
        let value = value.borrow();
        // 支持三种声明形态：整值输入、多个上游 fan-in、字段级映射。
        let object = value.as_object().ok_or_else(|| CoreError::InvalidFlow {
            message: "flow declaration must be a JSON object keyed by target plug".to_string(),
        })?;

        let mut flow = BTreeMap::new();

        for (target, inputs) in object {
            let mut input_map = BTreeMap::new();
            if let Some(source) = inputs.as_str() {
                let selector = parse_selector(source)?;
                insert_input(target, &mut input_map, FieldPath::new(""), selector)?;
            } else if let Some(sources) = inputs.as_array() {
                for source in sources {
                    let source = source.as_str().ok_or_else(|| CoreError::InvalidFlow {
                        message: format!("flow target `{target}` sources must be strings"),
                    })?;
                    let selector = parse_selector(source)?;
                    let input = FieldPath::new(selector.plug.to_string());
                    insert_input(target, &mut input_map, input, selector)?;
                }
            } else {
                let inputs = inputs.as_object().ok_or_else(|| CoreError::InvalidFlow {
                    message: format!("flow target `{target}` must map input fields to sources"),
                })?;

                for (input, source) in inputs {
                    let source = source.as_str().ok_or_else(|| CoreError::InvalidFlow {
                        message: format!("flow `{target}.{input}` source must be a string"),
                    })?;
                    let input = FieldPath::new(input);
                    let selector = parse_selector(source)?;
                    insert_input(target, &mut input_map, input, selector)?;
                }
            }

            flow.entry(crate::PlugName::new(target.clone()))
                .or_insert_with(InputMap::default)
                .0
                .extend(input_map);
        }

        Ok(Self(flow))
    }

    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.0.values().map(|inputs| inputs.0.len()).sum()
    }

    /// # Errors
    ///
    /// 当当前 flow 的边数量或任一输入/来源路径深度超过限制时返回错误。
    pub fn check_limits(&self, max_edges: usize, max_path_depth: usize) -> CoreResult<()> {
        let edge_count = self.edge_count();
        if edge_count > max_edges {
            return Err(CoreError::ResourceLimitExceeded {
                limit: "max_flow_edges".to_string(),
                value: edge_count,
            });
        }

        for inputs in self.0.values() {
            for (input, selector) in &inputs.0 {
                validate_path_depth(input, max_path_depth)?;
                validate_path_depth(&selector.path, max_path_depth)?;
            }
        }

        Ok(())
    }

    pub fn merge(&mut self, other: Flow) {
        for (target, inputs) in other.0 {
            self.0.entry(target).or_default().0.extend(inputs.0);
        }
    }

    /// # Errors
    ///
    /// 当 flowout 声明不是 target-keyed JSON object，或字段列表不是字符串数组时返回错误。
    pub fn remove(&mut self, value: impl Borrow<Value>) -> CoreResult<()> {
        let value = value.borrow();
        let object = value.as_object().ok_or_else(|| CoreError::InvalidFlow {
            message: "flowout declaration must be a JSON object keyed by target plug".to_string(),
        })?;

        for (target, fields) in object {
            let target = crate::PlugName::new(target.clone());
            if fields.is_null() {
                self.0.remove(&target);
                continue;
            }

            let fields = fields.as_array().ok_or_else(|| CoreError::InvalidFlow {
                message: "flowout target must be null or an array of input fields".to_string(),
            })?;
            if let Some(inputs) = self.0.get_mut(&target) {
                for field in fields {
                    let field = field.as_str().ok_or_else(|| CoreError::InvalidFlow {
                        message: "flowout fields must be strings".to_string(),
                    })?;
                    inputs.0.remove(&FieldPath::new(field));
                }
                if inputs.0.is_empty() {
                    self.0.remove(&target);
                }
            }
        }

        Ok(())
    }

    /// # Errors
    ///
    /// 当 flowout 声明非法时返回错误。
    pub fn removal_changes(&self, value: &Value) -> CoreResult<Vec<(crate::PlugName, FieldPath)>> {
        Ok(self
            .removal_flow(value)?
            .0
            .into_iter()
            .flat_map(|(target, inputs)| {
                inputs
                    .0
                    .into_keys()
                    .map(move |input| (target.clone(), input))
            })
            .collect())
    }

    /// # Errors
    ///
    /// 当 flowout 声明非法时返回错误。
    pub fn removal_flow(&self, value: &Value) -> CoreResult<Flow> {
        let object = value.as_object().ok_or_else(|| CoreError::InvalidFlow {
            message: "flowout declaration must be a JSON object keyed by target plug".to_string(),
        })?;

        let mut removed = BTreeMap::new();
        for (target, fields) in object {
            let target = crate::PlugName::new(target.clone());
            if fields.is_null() {
                if let Some(inputs) = self.0.get(&target) {
                    removed.insert(target, inputs.clone());
                }
                continue;
            }

            let fields = fields.as_array().ok_or_else(|| CoreError::InvalidFlow {
                message: "flowout target must be null or an array of input fields".to_string(),
            })?;
            for field in fields {
                let field = field.as_str().ok_or_else(|| CoreError::InvalidFlow {
                    message: "flowout fields must be strings".to_string(),
                })?;
                if let Some(inputs) = self.0.get(&target)
                    && let Some(selector) = inputs.0.get(&FieldPath::new(field))
                {
                    removed
                        .entry(target.clone())
                        .or_insert_with(InputMap::default)
                        .0
                        .insert(FieldPath::new(field), selector.clone());
                }
            }
        }

        Ok(Flow(removed))
    }
}

fn insert_input(
    target: &str, input_map: &mut BTreeMap<FieldPath, SourceSelector>, input: FieldPath,
    selector: SourceSelector,
) -> CoreResult<()> {
    if input_map.insert(input.clone(), selector).is_some() {
        return Err(CoreError::DuplicateFlowInput {
            target: target.to_string(),
            input: input.0,
        });
    }
    Ok(())
}

fn validate_path_depth(path: &FieldPath, max_path_depth: usize) -> CoreResult<()> {
    let depth = path.segments().count();
    if depth > max_path_depth {
        return Err(CoreError::ResourceLimitExceeded {
            limit: "max_path_depth".to_string(),
            value: depth,
        });
    }
    Ok(())
}

fn parse_selector(selector: &str) -> CoreResult<SourceSelector> {
    if !selector.contains('.') {
        if selector.is_empty() {
            return Err(CoreError::InvalidFlow {
                message: "source selector must include a plug name".to_string(),
            });
        }
        return Ok(SourceSelector {
            plug: crate::PlugName::new(selector),
            path: FieldPath::new(""),
        });
    }

    let (plug, path) = selector
        .split_once('.')
        .ok_or_else(|| CoreError::InvalidFlow {
            message: format!("source selector `{selector}` must be `plug.path`"),
        })?;

    if plug.is_empty() || path.is_empty() {
        return Err(CoreError::InvalidFlow {
            message: format!("source selector `{selector}` must include plug and path"),
        });
    }

    Ok(SourceSelector {
        plug: crate::PlugName::new(plug),
        path: FieldPath::new(path),
    })
}

pub fn read_path<'a>(value: &'a Value, path: &FieldPath) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.segments() {
        current = match current {
            Value::Object(object) => object.get(segment)?,
            Value::Array(array) => array.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

pub fn write_path(target: &mut Value, path: &FieldPath, value: Value) -> CoreResult<()> {
    let segments: Vec<_> = path.segments().collect();
    if segments.is_empty() {
        *target = value;
        return Ok(());
    }

    let mut current = target;
    for segment in &segments[..segments.len() - 1] {
        if let Ok(index) = segment.parse::<usize>() {
            ensure_array(current, path)?;
            let array = current.as_array_mut().expect("array created above");
            while array.len() <= index {
                array.push(Value::Null);
            }
            current = &mut array[index];
        } else {
            ensure_object(current, path)?;
            current = current
                .as_object_mut()
                .expect("object created above")
                .entry((*segment).to_string())
                .or_insert(Value::Null);
        }
    }

    let final_segment = segments[segments.len() - 1];
    if let Ok(index) = final_segment.parse::<usize>() {
        ensure_array(current, path)?;
        let array = current.as_array_mut().expect("array created above");
        while array.len() <= index {
            array.push(Value::Null);
        }
        if !array[index].is_null() {
            return Err(CoreError::InputConflict {
                path: path.0.clone(),
            });
        }
        array[index] = value;
    } else {
        ensure_object(current, path)?;
        let object = current.as_object_mut().expect("object created above");
        if object.insert(final_segment.to_string(), value).is_some() {
            return Err(CoreError::InputConflict {
                path: path.0.clone(),
            });
        }
    }
    Ok(())
}

fn ensure_array(value: &mut Value, path: &FieldPath) -> CoreResult<()> {
    if value.is_array() {
        return Ok(());
    }
    if !value.is_null() {
        return Err(CoreError::InputConflict {
            path: path.0.clone(),
        });
    }
    *value = Value::Array(Vec::new());
    Ok(())
}

fn ensure_object(value: &mut Value, path: &FieldPath) -> CoreResult<()> {
    if value.is_object() {
        return Ok(());
    }
    if !value.is_null() {
        return Err(CoreError::InputConflict {
            path: path.0.clone(),
        });
    }
    *value = Value::Object(Map::default());
    Ok(())
}
