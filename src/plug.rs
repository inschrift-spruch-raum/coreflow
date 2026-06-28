use std::{future::Future, pin::Pin, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};

use crate::{CoreError, CoreResult, Value};

type PlugFuture = Pin<Box<dyn Future<Output = CoreResult<Value>> + Send>>;
type PlugExecutor = dyn Fn(Value) -> PlugFuture + Send + Sync;

#[derive(Clone)]
pub struct Plug {
    pub(crate) name: crate::PlugName,
    implementation: PlugImplementation,
}

#[derive(Clone)]
pub(crate) struct PlugImplementation {
    executor: PlugExecutorMode,
}

impl std::fmt::Debug for PlugImplementation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlugImplementation").finish_non_exhaustive()
    }
}

#[derive(Clone)]
enum PlugExecutorMode {
    Functional(Arc<PlugExecutor>),
}

impl std::fmt::Debug for Plug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plug")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl Plug {
    pub(crate) fn implementation<I, O, F, Fut>(function: F) -> PlugImplementation
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
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
            executor: PlugExecutorMode::Functional(Arc::new(executor)),
        }
    }

    pub(crate) fn from_implementation(
        name: crate::PlugName, implementation: PlugImplementation,
    ) -> Self {
        Self {
            name,
            implementation,
        }
    }

    /// # Errors
    ///
    /// 当输入无法解码、输出无法编码，或 plug 实现返回错误时返回错误。
    pub async fn call(&self, input: Value) -> CoreResult<Value> {
        match &self.implementation.executor {
            PlugExecutorMode::Functional(executor) => {
                let future = (executor)(input);
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
