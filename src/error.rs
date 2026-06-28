use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CoreError {
    DuplicatePlug { name: String },
    UnknownPlug { name: String },
    PlugReferencedByFlow { name: String },
    UnknownFlowSource { target: String, source: String },
    UnknownFlowTarget { target: String },
    DuplicateFlowInput { target: String, input: String },
    GraphNotChecked,
    InvalidFlow { message: String },
    FlowPathNotFound { plug: String, path: String },
    InputConflict { path: String },
    PlugDecode { plug: String, message: String },
    PlugEncode { plug: String, message: String },
    PlugFailed { plug: String, message: String },
    ResourceLimitExceeded { limit: String, value: usize },
    Io { message: String },
    Json { message: String },
}

impl Display for CoreError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DuplicatePlug { name } => write!(f, "duplicate plug `{name}`"),
            Self::UnknownPlug { name } => write!(f, "unknown plug `{name}`"),
            Self::PlugReferencedByFlow { name } => {
                write!(f, "plug `{name}` is still referenced by flow")
            }
            Self::UnknownFlowSource { target, source } => {
                write!(
                    f,
                    "flow target `{target}` references unknown source `{source}`"
                )
            }
            Self::UnknownFlowTarget { target } => write!(f, "flow target `{target}` is unknown"),
            Self::DuplicateFlowInput { target, input } => write!(
                f,
                "flow target `{target}` already has input binding `{input}`"
            ),
            Self::GraphNotChecked => write!(f, "graph must be checked before run"),
            Self::InvalidFlow { message } => write!(f, "invalid flow: {message}"),
            Self::FlowPathNotFound { plug, path } => {
                write!(f, "plug `{plug}` output has no path `{path}`")
            }
            Self::InputConflict { path } => {
                write!(f, "input path `{path}` conflicts with existing value")
            }
            Self::PlugDecode { plug, message } => {
                write!(f, "plug `{plug}` input decode failed: {message}")
            }
            Self::PlugEncode { plug, message } => {
                write!(f, "plug `{plug}` output encode failed: {message}")
            }
            Self::PlugFailed { plug, message } => write!(f, "plug `{plug}` failed: {message}"),
            Self::ResourceLimitExceeded { limit, value } => {
                write!(f, "resource limit `{limit}` exceeded by value {value}")
            }
            Self::Io { message } => write!(f, "io error: {message}"),
            Self::Json { message } => write!(f, "json error: {message}"),
        }
    }
}

impl std::error::Error for CoreError {}

impl From<serde_json::Error> for CoreError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json {
            message: value.to_string(),
        }
    }
}
