use std::sync::Arc;

use crate::connection::libsql::LibSqlConnection;
use crate::connection::write_proxy::WriteProxyConnection;
use crate::connection::{Connection, MakeConnection, TrackedConnection};
use crate::replication::ReplicationLogger;

pub trait Database: Sync + Send + 'static {
    /// The connection type of the database
    type Connection: Connection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>>;
}

pub struct ReplicaDatabase {
    pub connection_maker:
        Arc<dyn MakeConnection<Connection = TrackedConnection<WriteProxyConnection>>>,
}

impl Database for ReplicaDatabase {
    type Connection = TrackedConnection<WriteProxyConnection>;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker.clone()
    }
}

pub struct PrimaryDatabase {
    pub logger: Arc<ReplicationLogger>,
    pub connection_maker: Arc<dyn MakeConnection<Connection = TrackedConnection<LibSqlConnection>>>,
}

impl Database for PrimaryDatabase {
    type Connection = TrackedConnection<LibSqlConnection>;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker.clone()
    }
}
