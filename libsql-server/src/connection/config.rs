use crate::{namespace::NamespaceName, LIBSQL_PAGE_SIZE};
use bytesize::mb;
use rusqlite::types::ToSqlOutput;
use rusqlite::ToSql;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::str::FromStr;
use url::Url;

use super::TXN_TIMEOUT;
use libsql_replication::rpc::metadata;
use tokio::time::Duration;

#[derive(Debug, Clone, serde::Deserialize)]
pub struct DatabaseConfig {
    pub block_reads: bool,
    pub block_writes: bool,
    /// The reason why operations are blocked. This will be included in [`Error::Blocked`].
    pub block_reason: Option<String>,
    /// maximum db size (in pages)
    pub max_db_pages: u64,
    pub heartbeat_url: Option<Url>,
    pub bottomless_db_id: Option<String>,
    #[serde(default)]
    pub jwt_key: Option<String>,
    #[serde(default)]
    pub txn_timeout: Option<Duration>,
    #[serde(default)]
    pub allow_attach: bool,
    #[serde(default = "default_max_row_size")]
    pub max_row_size: u64,
    #[serde(default)]
    pub is_shared_schema: bool,
    #[serde(default)]
    pub shared_schema_name: Option<NamespaceName>,
    #[serde(default)]
    pub durability_mode: DurabilityMode,
}

const fn default_max_size() -> u64 {
    bytesize::ByteSize::pb(1000).as_u64() / LIBSQL_PAGE_SIZE
}

fn default_max_row_size() -> u64 {
    mb(5u64)
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            block_reads: Default::default(),
            block_writes: Default::default(),
            block_reason: Default::default(),
            max_db_pages: default_max_size(),
            heartbeat_url: None,
            bottomless_db_id: None,
            jwt_key: None,
            txn_timeout: Some(TXN_TIMEOUT),
            allow_attach: false,
            max_row_size: default_max_row_size(),
            is_shared_schema: false,
            shared_schema_name: None,
            durability_mode: DurabilityMode::default(),
        }
    }
}

impl From<&metadata::DatabaseConfig> for DatabaseConfig {
    fn from(value: &metadata::DatabaseConfig) -> Self {
        DatabaseConfig {
            block_reads: value.block_reads,
            block_writes: value.block_writes,
            block_reason: value.block_reason.clone(),
            max_db_pages: value.max_db_pages,
            heartbeat_url: value.heartbeat_url.as_ref().map(|s| Url::parse(s).unwrap()),
            bottomless_db_id: value.bottomless_db_id.clone(),
            jwt_key: value.jwt_key.clone(),
            txn_timeout: value.txn_timeout_s.map(Duration::from_secs),
            allow_attach: value.allow_attach,
            max_row_size: value.max_row_size.unwrap_or_else(default_max_row_size),
            is_shared_schema: value.shared_schema.unwrap_or(false),
            // namespace name is coming from primary, we assume it's valid
            shared_schema_name: value
                .shared_schema_name
                .clone()
                .map(NamespaceName::new_unchecked),
            durability_mode: match value.durability_mode {
                None => DurabilityMode::default(),
                Some(m) => DurabilityMode::from(metadata::DurabilityMode::try_from(m)),
            },
        }
    }
}

impl From<&DatabaseConfig> for metadata::DatabaseConfig {
    fn from(value: &DatabaseConfig) -> Self {
        metadata::DatabaseConfig {
            block_reads: value.block_reads,
            block_writes: value.block_writes,
            block_reason: value.block_reason.clone(),
            max_db_pages: value.max_db_pages,
            heartbeat_url: value.heartbeat_url.as_ref().map(|s| s.to_string()),
            bottomless_db_id: value.bottomless_db_id.clone(),
            jwt_key: value.jwt_key.clone(),
            txn_timeout_s: value.txn_timeout.map(|d| d.as_secs()),
            allow_attach: value.allow_attach,
            max_row_size: Some(value.max_row_size),
            shared_schema: Some(value.is_shared_schema),
            shared_schema_name: value.shared_schema_name.as_ref().map(|s| s.to_string()),
            durability_mode: Some(metadata::DurabilityMode::from(value.durability_mode).into()),
        }
    }
}

/// Durability mode specifies the `PRAGMA SYNCHRONOUS` setting for the connection
#[derive(PartialEq, Clone, Copy, Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum DurabilityMode {
    Extra,
    Strong,
    #[default]
    Relaxed,
    Off,
}

impl ToSql for DurabilityMode {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            DurabilityMode::Extra => Ok(ToSqlOutput::from("extra")),
            DurabilityMode::Strong => Ok(ToSqlOutput::from("full")),
            DurabilityMode::Relaxed => Ok(ToSqlOutput::from("normal")),
            DurabilityMode::Off => Ok(ToSqlOutput::from("off")),
        }
    }
}

impl FromStr for DurabilityMode {
    type Err = ();

    fn from_str(input: &str) -> Result<DurabilityMode, Self::Err> {
        match input {
            "extra" => Ok(DurabilityMode::Extra),
            "strong" => Ok(DurabilityMode::Strong),
            "relaxed" => Ok(DurabilityMode::Relaxed),
            "off" => Ok(DurabilityMode::Off),
            _ => Err(()),
        }
    }
}

impl Display for DurabilityMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let m = match self {
            DurabilityMode::Extra => "extra",
            DurabilityMode::Strong => "strong",
            DurabilityMode::Relaxed => "relaxed",
            DurabilityMode::Off => "off",
        };
        write!(f, "{m}")
    }
}

impl From<DurabilityMode> for metadata::DurabilityMode {
    fn from(value: DurabilityMode) -> Self {
        match value {
            DurabilityMode::Relaxed => metadata::DurabilityMode::Relaxed,
            DurabilityMode::Strong => metadata::DurabilityMode::Strong,
            DurabilityMode::Extra => metadata::DurabilityMode::Extra,
            DurabilityMode::Off => metadata::DurabilityMode::Off,
        }
    }
}

impl From<Result<metadata::DurabilityMode, prost::DecodeError>> for DurabilityMode {
    fn from(value: Result<metadata::DurabilityMode, prost::DecodeError>) -> Self {
        match value {
            Ok(mode) => match mode {
                metadata::DurabilityMode::Relaxed => DurabilityMode::Relaxed,
                metadata::DurabilityMode::Strong => DurabilityMode::Strong,
                metadata::DurabilityMode::Extra => DurabilityMode::Extra,
                metadata::DurabilityMode::Off => DurabilityMode::Off,
            },
            Err(_) => DurabilityMode::default(),
        }
    }
}
