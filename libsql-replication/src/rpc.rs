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
    use std::pin::Pin;

    use tokio_stream::Stream;
    use uuid::Uuid;

    pub type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

    use self::replication_log_server::ReplicationLog;
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

    pub type BoxReplicationService = Box<
        dyn ReplicationLog<
            LogEntriesStream = BoxStream<'static, Result<Frame, tonic::Status>>,
            SnapshotStream = BoxStream<'static, Result<Frame, tonic::Status>>,
        >,
    >;

    #[tonic::async_trait]
    impl ReplicationLog for BoxReplicationService {
        type LogEntriesStream = BoxStream<'static, Result<Frame, tonic::Status>>;
        type SnapshotStream = BoxStream<'static, Result<Frame, tonic::Status>>;

        async fn log_entries(
            &self,
            req: tonic::Request<LogOffset>,
        ) -> Result<tonic::Response<Self::LogEntriesStream>, tonic::Status> {
            self.as_ref().log_entries(req).await
        }

        async fn batch_log_entries(
            &self,
            req: tonic::Request<LogOffset>,
        ) -> Result<tonic::Response<Frames>, tonic::Status> {
            self.as_ref().batch_log_entries(req).await
        }

        async fn hello(
            &self,
            req: tonic::Request<HelloRequest>,
        ) -> Result<tonic::Response<HelloResponse>, tonic::Status> {
            self.as_ref().hello(req).await
        }

        async fn snapshot(
            &self,
            req: tonic::Request<LogOffset>,
        ) -> Result<tonic::Response<Self::SnapshotStream>, tonic::Status> {
            self.as_ref().snapshot(req).await
        }
    }
}

pub mod metadata {
    #![allow(clippy::all)]
    include!("generated/metadata.rs");
}
