use crate::LIBSQL_PAGE_SIZE;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default)]
    pub block_reads: bool,
    #[serde(default)]
    pub block_writes: bool,
    /// The reason why operations are blocked. This will be included in [`Error::Blocked`].
    #[serde(default)]
    pub block_reason: Option<String>,
    /// maximum db size (in pages)
    #[serde(default = "default_max_size")]
    pub max_db_pages: u64,
    #[serde(default)]
    pub heartbeat_url: Option<Url>,
    #[serde(default)]
    pub bottomless_db_id: Option<String>,
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
        }
    }
}
