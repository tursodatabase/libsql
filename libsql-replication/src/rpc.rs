pub mod proxy {
    #![allow(clippy::all)]
    include!("generated/proxy.rs");

    use rusqlite::types::ValueRef;

    impl From<ValueRef<'_>> for RowValue {
        fn from(value: ValueRef<'_>) -> Self {
            use row_value::Value;

            let value = Some(match value {
                ValueRef::Null => Value::Null(true),
                ValueRef::Integer(i) => Value::Integer(i),
                ValueRef::Real(x) => Value::Real(x),
                ValueRef::Text(s) => Value::Text(String::from_utf8(s.to_vec()).unwrap()),
                ValueRef::Blob(b) => Value::Blob(b.to_vec()),
            });

            RowValue { value }
        }
    }
}

pub mod replication {
    #![allow(clippy::all)]

    use uuid::Uuid;
    include!("generated/wal_log.rs");

    pub const NO_HELLO_ERROR_MSG: &str = "NO_HELLO";
    pub const NEED_SNAPSHOT_ERROR_MSG: &str = "NEED_SNAPSHOT";
    /// A tonic error code to signify that a namespace doesn't exist.
    pub const NAMESPACE_DOESNT_EXIST: &str = "NAMESPACE_DOESNT_EXIST";

    pub const SESSION_TOKEN_KEY: &str = "x-session-token";
    pub const NAMESPACE_METADATA_KEY: &str = "x-namespace-bin";

    // Verify that the session token is valid
    pub fn verify_session_token(
        token: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
        let s = std::str::from_utf8(token)?;
        s.parse::<Uuid>()?;

        Ok(())
    }

    impl HelloRequest {
        pub fn new() -> Self {
            Self {
                handshake_version: Some(1),
            }
        }
    }
}

pub mod metadata {
    #![allow(clippy::all)]
    include!("generated/metadata.rs");
}
