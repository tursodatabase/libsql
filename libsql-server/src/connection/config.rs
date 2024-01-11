use crate::LIBSQL_PAGE_SIZE;
use url::Url;

use libsql_replication::rpc::metadata;

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub block_reads: bool,
    pub block_writes: bool,
    /// The reason why operations are blocked. This will be included in [`Error::Blocked`].
    pub block_reason: Option<String>,
    /// maximum db size (in pages)
    pub max_db_pages: u64,
    pub heartbeat_url: Option<Url>,
    pub bottomless_db_id: Option<String>,
    pub jwt_key: Option<String>,
}

const fn default_max_size() -> u64 {
    bytesize::ByteSize::pb(1000).as_u64() / LIBSQL_PAGE_SIZE
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
            heartbeat_url: value
                .heartbeat_url
                .as_ref()
                .map(|s| Url::parse(&s).unwrap()),
            bottomless_db_id: value.bottomless_db_id.clone(),
            jwt_key: value.jwt_key.clone(),
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
        }
    }
}
