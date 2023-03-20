//! These data structures correspond to the Hrana protocol.
//!
//! Please consult the Hrana specification in the `docs/` directory for more information.
use crate::hrana;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize, Debug)]
pub struct Stmt {
    pub sql: String,
    #[serde(default)]
    pub args: Vec<Value>,
    #[serde(default)]
    pub named_args: Vec<NamedArg>,
    pub want_rows: bool,
}

#[derive(Deserialize, Debug)]
pub struct NamedArg {
    pub name: String,
    pub value: Value,
}

#[derive(Serialize, Debug)]
pub struct StmtResult {
    pub cols: Vec<Col>,
    pub rows: Vec<Vec<Value>>,
    pub affected_row_count: u64,
    #[serde(with = "option_i64_as_str")]
    pub last_insert_rowid: Option<i64>,
}

#[derive(Serialize, Debug)]
pub struct Col {
    pub name: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct RunReq {
    pub stream_id: i32,
    pub prog: Prog,
}

#[derive(Serialize, Debug)]
pub struct RunResp {
    pub result: ProgResult,
}

#[derive(Deserialize, Debug)]
pub struct Prog {
    pub steps: Vec<ProgStep>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProgStep {
    Execute(ExecuteStep),
    Output { expr: ProgExpr },
    Op { ops: Vec<ProgOp> },
}

#[derive(Deserialize, Debug)]
pub struct ExecuteStep {
    pub stmt: Stmt,
    #[serde(default)]
    pub condition: Option<ProgExpr>,
    #[serde(default)]
    pub on_ok: Vec<ProgOp>,
    #[serde(default)]
    pub on_error: Vec<ProgOp>,
}

#[derive(Serialize, Debug)]
pub struct ProgResult {
    pub execute_results: Vec<Option<StmtResult>>,
    pub execute_errors: Vec<Option<hrana::proto::Error>>,
    pub outputs: Vec<Value>,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProgOp {
    Set { var: i32, expr: ProgExpr },
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum ProgExpr {
    Value(Value),
    Expr(ProgExpr_),
}

#[derive(Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProgExpr_ {
    Var { var: i32 },
    Not { expr: Box<ProgExpr> },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Value {
    Null,
    Integer {
        #[serde(with = "i64_as_str")]
        value: i64,
    },
    Float {
        value: f64,
    },
    Text {
        value: Arc<str>,
    },
    Blob {
        #[serde(with = "bytes_as_base64", rename = "base64")]
        value: Bytes,
    },
}

mod i64_as_str {
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &i64, ser: S) -> Result<S::Ok, S::Error> {
        value.to_string().serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<i64, D::Error> {
        let str_value = <&'de str as de::Deserialize>::deserialize(de)?;
        str_value.parse().map_err(|_| {
            D::Error::invalid_value(
                de::Unexpected::Str(str_value),
                &"decimal integer as a string",
            )
        })
    }
}

mod option_i64_as_str {
    use serde::{ser, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Option<i64>, ser: S) -> Result<S::Ok, S::Error> {
        value.map(|v| v.to_string()).serialize(ser)
    }
}

mod bytes_as_base64 {
    use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
    use bytes::Bytes;
    use serde::{de, ser};
    use serde::{de::Error as _, Serialize as _};

    pub fn serialize<S: ser::Serializer>(value: &Bytes, ser: S) -> Result<S::Ok, S::Error> {
        STANDARD_NO_PAD.encode(value).serialize(ser)
    }

    pub fn deserialize<'de, D: de::Deserializer<'de>>(de: D) -> Result<Bytes, D::Error> {
        let text = <&'de str as de::Deserialize>::deserialize(de)?;
        let text = text.trim_end_matches('=');
        let bytes = STANDARD_NO_PAD.decode(text).map_err(|_| {
            D::Error::invalid_value(de::Unexpected::Str(text), &"binary data encoded as base64")
        })?;
        Ok(Bytes::from(bytes))
    }
}
