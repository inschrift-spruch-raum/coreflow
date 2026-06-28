use std::{future::Future, pin::Pin, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Mutex;

use crate::{CoreError, CoreResult, Value};

type PlugFuture = Pin<Box<dyn Future<Output = CoreResult<Value>> + Send>>;
type PlugExecutor = dyn FnMut(Value) -> PlugFuture + Send;

// Plug 是已挂载到 graph 的可执行节点；实现闭包本身通过 PlugImplementation 共享。
#[derive(Clone)]
pub struct Plug {
    pub(crate) name: crate::PlugName,
    implementation: PlugImplementation,
    serial_gate: Arc<Mutex<()>>,
}

// PlugExecution 描述执行约束：是否允许同一 plug 重入，以及占用哪个资源并发桶。
#[derive(Debug, Default, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PlugExecution {
    pub reentrant: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
}

#[derive(Clone)]
pub(crate) struct PlugImplementation {
    pub(crate) execution: PlugExecution,
    executor: PlugExecutorMode,
}

impl std::fmt::Debug for PlugImplementation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlugImplementation")
            .field("execution", &self.execution)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum PlugExecutorMode {
    Serial(Arc<Mutex<Box<PlugExecutor>>>),
}

impl std::fmt::Debug for Plug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plug")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Plug {
    pub(crate) fn implementation<I, O, F, Fut>(
        execution: PlugExecution, mut function: F,
    ) -> PlugImplementation
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: FnMut(I) -> Fut + Send + 'static,
        Fut: Future<Output = CoreResult<O>> + Send + 'static,
    {
        let executor = move |value: Value| match serde_json::from_value::<I>(value) {
            Ok(input) => {
                let future = function(input);
                Box::pin(async move {
                    let output = future.await?;
                    serde_json::to_value(output).map_err(|error| CoreError::PlugEncode {
                        plug: String::new(),
                        message: error.to_string(),
                    })
                }) as PlugFuture
            }
            Err(error) => Box::pin(async move {
                Err(CoreError::PlugDecode {
                    plug: String::new(),
                    message: error.to_string(),
                })
            }) as PlugFuture,
        };

        PlugImplementation {
            execution,
            executor: PlugExecutorMode::Serial(Arc::new(Mutex::new(Box::new(executor)))),
        }
    }

    pub(crate) fn from_implementation(
        name: crate::PlugName, implementation: PlugImplementation,
    ) -> Self {
        Self {
            name,
            implementation,
            serial_gate: Arc::new(Mutex::new(())),
        }
    }

    #[must_use]
    pub fn execution(&self) -> PlugExecution {
        self.implementation.execution.clone()
    }

    pub(crate) fn resource(&self) -> Option<&str> {
        self.implementation.execution.resource.as_deref()
    }

    /// # Errors
    ///
    /// 当输入无法解码、输出无法编码，或 plug 实现返回错误时返回错误。
    pub async fn call(&self, input: Value) -> CoreResult<Value> {
        match &self.implementation.executor {
            PlugExecutorMode::Serial(executor) => {
                // 非 reentrant plug 在自身实例上串行，避免同一个闭包状态被并发改写。
                let _serial = if self.implementation.execution.reentrant {
                    None
                } else {
                    Some(self.serial_gate.lock().await)
                };
                let future = {
                    let mut executor = executor.lock().await;
                    (executor)(input)
                };
                future.await.map_err(|error| match error {
                    CoreError::PlugDecode { plug, message } if plug.is_empty() => {
                        CoreError::PlugDecode {
                            plug: self.name.to_string(),
                            message,
                        }
                    }
                    CoreError::PlugEncode { plug, message } if plug.is_empty() => {
                        CoreError::PlugEncode {
                            plug: self.name.to_string(),
                            message,
                        }
                    }
                    CoreError::PlugFailed { plug, message } if plug.is_empty() => {
                        CoreError::PlugFailed {
                            plug: self.name.to_string(),
                            message,
                        }
                    }
                    error => error,
                })
            }
        }
    }
}
