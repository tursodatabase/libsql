pub mod proxy {
    #![allow(clippy::all)]
    include!("generated/proxy.rs");
}

pub mod replication {
    #![allow(clippy::all)]

    use uuid::Uuid;
    include!("generated/wal_log.rs");

    pub const NO_HELLO_ERROR_MSG: &str = "NO_HELLO";
    pub const NEED_SNAPSHOT_ERROR_MSG: &str = "NEED_SNAPSHOT";

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
}
