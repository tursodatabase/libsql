/// Database config used to send db configs over the wire and stored
/// in the meta store.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseConfig {
    #[prost(bool, tag = "1")]
    pub block_reads: bool,
    #[prost(bool, tag = "2")]
    pub block_writes: bool,
    /// The reason why operations are blocked. This will be included in \[`Error::Blocked`\].
    #[prost(string, optional, tag = "3")]
    pub block_reason: ::core::option::Option<::prost::alloc::string::String>,
    /// maximum db size (in pages)
    #[prost(uint64, tag = "4")]
    pub max_db_pages: u64,
    #[prost(string, optional, tag = "5")]
    pub heartbeat_url: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "6")]
    pub bottomless_db_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "7")]
    pub jwt_key: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(uint64, optional, tag = "8")]
    pub txn_timeout_s: ::core::option::Option<u64>,
}
