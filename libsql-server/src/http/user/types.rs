use std::collections::HashMap;

use base64::prelude::BASE64_STANDARD_NO_PAD;
use base64::Engine;
use serde::de::Error as _;
use serde::{Deserialize, Serialize};

use crate::query;

#[derive(Debug, Deserialize, Serialize)]
pub struct HttpQuery {
    pub statements: Vec<QueryObject>,
    pub replication_index: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct QueryObject {
    pub q: String,
    pub params: QueryParams,
}

#[derive(Debug, Serialize)]
pub struct QueryParams(pub query::Params);

/// Wrapper type to deserialize a payload into a query::Value
struct ValueDeserializer(query::Value);

impl<'de> Deserialize<'de> for ValueDeserializer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = query::Value;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a valid SQLite value")
            }

            fn visit_none<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(query::Value::Null)
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(query::Value::Null)
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(query::Value::Text(v))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(query::Value::Text(v.to_string()))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(query::Value::Integer(v))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(query::Value::Integer(v as i64))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(query::Value::Real(v))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                match map.next_entry::<&str, &str>()? {
                    Some((k, v)) => {
                        if k == "base64" {
                            // FIXME: If the blog payload is too big, it may block the main thread
                            // for too long in an async context. In this case, it may be necessary
                            // to offload deserialization to a separate thread.
                            let data = BASE64_STANDARD_NO_PAD.decode(v).map_err(|e| {
                                A::Error::invalid_value(
                                    serde::de::Unexpected::Str(v),
                                    &e.to_string().as_str(),
                                )
                            })?;

                            Ok(query::Value::Blob(data))
                        } else {
                            Err(A::Error::unknown_field(k, &["blob"]))
                        }
                    }
                    None => Err(A::Error::missing_field("blob")),
                }
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(query::Value::Integer(v as _))
            }
        }

        deserializer.deserialize_any(Visitor).map(ValueDeserializer)
    }
}

impl<'de> Deserialize<'de> for QueryParams {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = QueryParams;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an array or a map of parameters")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut params = Vec::new();
                while let Some(val) = seq.next_element::<ValueDeserializer>()? {
                    params.push(val.0);
                }

                Ok(QueryParams(query::Params::new_positional(params)))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut inner = HashMap::new();
                while let Some((k, v)) = map.next_entry::<String, ValueDeserializer>()? {
                    inner.insert(k, v.0);
                }

                Ok(QueryParams(query::Params::new_named(inner)))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

impl<'de> Deserialize<'de> for QueryObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = QueryObject;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or an object")
            }

            fn visit_str<E>(self, q: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(QueryObject {
                    q: q.to_string(),
                    params: QueryParams(query::Params::empty()),
                })
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut q = None;
                let mut params = None;
                while let Some(k) = map.next_key::<&str>()? {
                    match k {
                        "q" => {
                            if q.is_none() {
                                q.replace(map.next_value::<String>()?);
                            } else {
                                return Err(A::Error::duplicate_field("q"));
                            }
                        }
                        "params" => {
                            if params.is_none() {
                                params.replace(map.next_value::<QueryParams>()?);
                            } else {
                                return Err(A::Error::duplicate_field("params"));
                            }
                        }
                        _ => return Err(A::Error::unknown_field(k, &["q", "params"])),
                    }
                }

                Ok(QueryObject {
                    q: q.ok_or_else(|| A::Error::missing_field("q"))?,
                    params: params.unwrap_or_else(|| QueryParams(query::Params::empty())),
                })
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_positional_params() {
        let json = r#"[1, "hello", 12.1, { "base64": "aGVsbG8K"}, null]"#; // blob: hello\n
        let found: QueryParams = serde_json::from_str(json).unwrap();
        insta::assert_json_snapshot!(found);
    }

    #[test]
    fn parse_named_params() {
        let json = r#"{":int": 1, "$real": 1.23, ":str": "hello", ":blob": { "base64": "aGVsbG8K"}, ":null": null, ":bool": false}"#;
        let found: QueryParams = serde_json::from_str(json).unwrap();
        insta::with_settings!({sort_maps => true}, {
            insta::assert_json_snapshot!(found);
        })
    }

    #[test]
    fn parse_http_query() {
        let json = r#"
            {
                "statements": [
                    "select * from test",
                    {"q": "select ?", "params": [12, true]},
                    {"q": "select ?", "params": {":foo": "bar"}}
                ]
            }"#;
        let found: HttpQuery = serde_json::from_str(json).unwrap();
        insta::with_settings!({sort_maps => true}, {
            insta::assert_json_snapshot!(found);
        })
    }

    #[test]
    fn parse_http_query_with_replication_index() {
        let json = r#"
            {
                "statements": [
                    "select * from test",
                    {"q": "select ?", "params": [12, true]},
                    {"q": "select ?", "params": {":foo": "bar"}}
                ],
                "replication_index": 1
            }"#;
        let found: HttpQuery = serde_json::from_str(json).unwrap();
        insta::with_settings!({sort_maps => true}, {
            insta::assert_json_snapshot!(found);
        })
    }
}
