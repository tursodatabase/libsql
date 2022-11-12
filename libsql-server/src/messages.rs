use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Message {
    Execute(String),
    ResultSet(Vec<String>),
    Error(String),
}
