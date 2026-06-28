// Value 目前固定为 serde_json::Value，给 Graph、Flow 和 Plug 保持同一种边界表示。
pub type Value = serde_json::Value;

// ValueCodec 把宿主类型和 coreflow 的边界 Value 互转，默认实现是 JSON codec。
pub trait ValueCodec {
    type Value;

    /// # Errors
    ///
    /// 当宿主类型无法编码为边界值时返回错误。
    fn encode<T: serde::Serialize>(&self, value: T) -> crate::CoreResult<Self::Value>;

    /// # Errors
    ///
    /// 当边界值无法解码为目标类型时返回错误。
    fn decode<T: serde::de::DeserializeOwned>(&self, value: Self::Value) -> crate::CoreResult<T>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct JsonValueCodec;

impl ValueCodec for JsonValueCodec {
    type Value = Value;

    fn encode<T: serde::Serialize>(&self, value: T) -> crate::CoreResult<Self::Value> {
        serde_json::to_value(value).map_err(crate::CoreError::from)
    }

    fn decode<T: serde::de::DeserializeOwned>(&self, value: Self::Value) -> crate::CoreResult<T> {
        serde_json::from_value(value).map_err(crate::CoreError::from)
    }
}
