use serde_json::Value;

pub trait Pipe {
    fn filter<F>(fun: F, val: &Value) -> bool
    where
        F: Fn(&Value) -> bool;
    fn map<F>(fun: F, val: &Value) -> &Value
    where
        F: Fn(&Value) -> &Value;
}
