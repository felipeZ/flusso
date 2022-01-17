use serde_json::Value;

pub trait Pipe: Send + Sync {
    fn filter(&self, val: &Value) -> bool;
    fn map(&self, val: &Value) -> &Value;
}
