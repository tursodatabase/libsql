use std::fmt;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bottomless::replicator::Replicator;
use tokio::sync::watch;

use crate::connection::{MakeConnection, RequestContext};
use crate::replication::{FrameNo, ReplicationLogger};

pub use self::libsql_primary::{
    LibsqlPrimaryConnection, LibsqlPrimaryConnectionMaker, LibsqlPrimaryDatabase,
};
pub use self::libsql_replica::{LibsqlReplicaConnection, LibsqlReplicaDatabase};
pub use self::primary::{PrimaryConnection, PrimaryConnectionMaker, PrimaryDatabase};
pub use self::replica::{ReplicaConnection, ReplicaDatabase};
pub use self::schema::{SchemaConnection, SchemaDatabase};

mod libsql_primary;
mod libsql_replica;
mod primary;
mod replica;
mod schema;

#[derive(Debug, Clone, serde::Deserialize, Copy)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseKind {
    Primary,
    Replica,
}

impl DatabaseKind {
    /// Returns `true` if the database kind is [`Replica`].
    ///
    /// [`Replica`]: DatabaseKind::Replica
    #[must_use]
    pub fn is_replica(&self) -> bool {
        matches!(self, Self::Replica)
    }

    /// Returns `true` if the database kind is [`Primary`].
    ///
    /// [`Primary`]: DatabaseKind::Primary
    #[must_use]
    pub fn is_primary(&self) -> bool {
        matches!(self, Self::Primary)
    }
}

pub type Result<T> = anyhow::Result<T>;

pub enum Connection {
    Primary(PrimaryConnection),
    Replica(ReplicaConnection),
    Schema(SchemaConnection<PrimaryConnection>),
    LibsqlPrimary(LibsqlPrimaryConnection),
    LibsqlReplica(LibsqlReplicaConnection),
    LibsqlSchema(SchemaConnection<LibsqlPrimaryConnection>),
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primary(_) => write!(f, "Primary"),
            Self::Replica(_) => write!(f, "Replica"),
            Self::Schema(_) => write!(f, "Schema"),
            Self::LibsqlPrimary(_) => write!(f, "LibsqlPrimaryConnection"),
            Self::LibsqlReplica(_) => write!(f, "LibsqlReplicaConnection"),
            Self::LibsqlSchema(_) => write!(f, "LibsqlSchema"),
        }
    }
}

impl Connection {
    /// Returns `true` if the connection is [`Primary`].
    ///
    /// [`Primary`]: Connection::Primary
    #[must_use]
    pub fn is_primary(&self) -> bool {
        matches!(self, Self::Primary(..) | Self::LibsqlPrimary(_))
    }
}

#[async_trait::async_trait]
impl crate::connection::Connection for Connection {
    async fn execute_program<B: crate::query_result_builder::QueryResultBuilder>(
        &self,
        pgm: crate::connection::program::Program,
        ctx: RequestContext,
        response_builder: B,
        replication_index: Option<FrameNo>,
    ) -> crate::Result<B> {
        match self {
            Connection::Primary(conn) => {
                conn.execute_program(pgm, ctx, response_builder, replication_index)
                    .await
            }
            Connection::Replica(conn) => {
                conn.execute_program(pgm, ctx, response_builder, replication_index)
                    .await
            }
            Connection::Schema(conn) => {
                conn.execute_program(pgm, ctx, response_builder, replication_index)
                    .await
            }
            Connection::LibsqlPrimary(conn) => {
                conn.execute_program(pgm, ctx, response_builder, replication_index)
                    .await
            }
            Connection::LibsqlReplica(conn) => {
                conn.execute_program(pgm, ctx, response_builder, replication_index)
                    .await
            }
            Connection::LibsqlSchema(conn) => {
                conn.execute_program(pgm, ctx, response_builder, replication_index)
                    .await
            }
        }
    }

    async fn describe(
        &self,
        sql: String,
        ctx: RequestContext,
        replication_index: Option<FrameNo>,
    ) -> crate::Result<crate::Result<crate::connection::program::DescribeResponse>> {
        match self {
            Connection::Primary(conn) => conn.describe(sql, ctx, replication_index).await,
            Connection::Replica(conn) => conn.describe(sql, ctx, replication_index).await,
            Connection::Schema(conn) => conn.describe(sql, ctx, replication_index).await,
            Connection::LibsqlPrimary(conn) => conn.describe(sql, ctx, replication_index).await,
            Connection::LibsqlReplica(conn) => conn.describe(sql, ctx, replication_index).await,
            Connection::LibsqlSchema(conn) => conn.describe(sql, ctx, replication_index).await,
        }
    }

    async fn is_autocommit(&self) -> crate::Result<bool> {
        match self {
            Connection::Primary(conn) => conn.is_autocommit().await,
            Connection::Replica(conn) => conn.is_autocommit().await,
            Connection::Schema(conn) => conn.is_autocommit().await,
            Connection::LibsqlPrimary(conn) => conn.is_autocommit().await,
            Connection::LibsqlReplica(conn) => conn.is_autocommit().await,
            Connection::LibsqlSchema(conn) => conn.is_autocommit().await,
        }
    }

    async fn checkpoint(&self) -> crate::Result<()> {
        match self {
            Connection::Primary(conn) => conn.checkpoint().await,
            Connection::Replica(conn) => conn.checkpoint().await,
            Connection::Schema(conn) => conn.checkpoint().await,
            Connection::LibsqlPrimary(conn) => conn.checkpoint().await,
            Connection::LibsqlReplica(conn) => conn.checkpoint().await,
            Connection::LibsqlSchema(conn) => conn.checkpoint().await,
        }
    }

    async fn vacuum_if_needed(&self) -> crate::Result<()> {
        match self {
            Connection::Primary(conn) => conn.vacuum_if_needed().await,
            Connection::Replica(conn) => conn.vacuum_if_needed().await,
            Connection::Schema(conn) => conn.vacuum_if_needed().await,
            Connection::LibsqlPrimary(conn) => conn.vacuum_if_needed().await,
            Connection::LibsqlReplica(conn) => conn.vacuum_if_needed().await,
            Connection::LibsqlSchema(conn) => conn.vacuum_if_needed().await,
        }
    }

    fn diagnostics(&self) -> String {
        match self {
            Connection::Primary(conn) => conn.diagnostics(),
            Connection::Replica(conn) => conn.diagnostics(),
            Connection::Schema(conn) => conn.diagnostics(),
            Connection::LibsqlPrimary(conn) => conn.diagnostics(),
            Connection::LibsqlReplica(conn) => conn.diagnostics(),
            Connection::LibsqlSchema(conn) => conn.diagnostics(),
        }
    }

    fn with_raw<R>(&self, f: impl FnOnce(&mut rusqlite::Connection) -> R) -> R {
        match self {
            Connection::Primary(c) => c.with_raw(f),
            Connection::Replica(c) => c.with_raw(f),
            Connection::Schema(c) => c.with_raw(f),
            Connection::LibsqlPrimary(c) => c.with_raw(f),
            Connection::LibsqlReplica(c) => c.with_raw(f),
            Connection::LibsqlSchema(c) => c.with_raw(f),
        }
    }
}

pub enum Database {
    Primary(PrimaryDatabase),
    Replica(ReplicaDatabase),
    Schema(SchemaDatabase<PrimaryConnectionMaker>),
    LibsqlPrimary(LibsqlPrimaryDatabase),
    LibsqlReplica(LibsqlReplicaDatabase),
    LibsqlSchema(SchemaDatabase<LibsqlPrimaryConnectionMaker>),
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primary(_) => write!(f, "Primary"),
            Self::Replica(_) => write!(f, "Replica"),
            Self::Schema(_) => write!(f, "Schema"),
            Self::LibsqlPrimary(_) => write!(f, "LibsqlPrimary"),
            Self::LibsqlReplica(_) => write!(f, "LibsqlReplica"),
            Self::LibsqlSchema(_) => write!(f, "LibsqlSchema"),
        }
    }
}

impl Database {
    pub fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Connection>> {
        match self {
            Database::Primary(db) => Arc::new(db.connection_maker().map(Connection::Primary)),
            Database::Replica(db) => Arc::new(db.connection_maker().map(Connection::Replica)),
            Database::Schema(db) => Arc::new(db.connection_maker().map(Connection::Schema)),
            Database::LibsqlPrimary(db) => {
                Arc::new(db.connection_maker().map(Connection::LibsqlPrimary))
            }
            Database::LibsqlReplica(db) => {
                Arc::new(db.connection_maker().map(Connection::LibsqlReplica))
            }
            Database::LibsqlSchema(db) => {
                Arc::new(db.connection_maker().map(Connection::LibsqlSchema))
            }
        }
    }

    pub fn destroy(self) {
        match self {
            Database::Primary(db) => db.destroy(),
            Database::Replica(db) => db.destroy(),
            Database::Schema(db) => db.destroy(),
            Database::LibsqlPrimary(db) => db.destroy(),
            Database::LibsqlReplica(db) => db.destroy(),
            Database::LibsqlSchema(db) => db.destroy(),
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        match self {
            Database::Primary(db) => db.shutdown().await,
            Database::Replica(db) => db.shutdown().await,
            Database::Schema(db) => db.shutdown().await,
            Database::LibsqlPrimary(db) => db.shutdown().await,
            Database::LibsqlReplica(db) => db.shutdown().await,
            Database::LibsqlSchema(db) => db.shutdown().await,
        }
    }

    pub fn logger(&self) -> Option<Arc<ReplicationLogger>> {
        match self {
            Database::Primary(p) => Some(p.wal_wrapper.wrapper().logger()),
            Database::Replica(_) => None,
            Database::Schema(s) => Some(s.wal_wrapper.as_ref().unwrap().wrapper().logger()),
            Database::LibsqlPrimary(_) => None,
            Database::LibsqlReplica(_) => None,
            Database::LibsqlSchema(s) => Some(s.wal_wrapper.as_ref().unwrap().wrapper().logger()),
        }
    }

    pub fn block_writes(&self) -> Option<Arc<AtomicBool>> {
        match self {
            Self::Primary(p) => Some(p.block_writes.clone()),
            Self::LibsqlPrimary(p) => Some(p.block_writes.clone()),
            _ => None,
        }
    }

    pub fn notifier(&self) -> Option<watch::Receiver<Option<FrameNo>>> {
        match self {
            Database::Primary(p) => Some(
                p.wal_wrapper
                    .wrapper()
                    .logger()
                    .new_frame_notifier
                    .subscribe(),
            ),
            Database::Replica(_) => None,
            Database::Schema(s) => Some(s.new_frame_notifier.clone()),
            Database::LibsqlPrimary(p) => Some(p.new_frame_notifier.clone()),
            Database::LibsqlReplica(_) => None,
            Database::LibsqlSchema(s) => Some(s.new_frame_notifier.clone()),
        }
    }

    pub fn is_primary(&self) -> bool {
        match self {
            Self::LibsqlPrimary(_) | Self::Primary(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_schema(&self) -> bool {
        match self {
            Self::Schema(_) => true,
            _ => false,
        }
    }

    pub(crate) fn replicator(&self) -> Option<Arc<tokio::sync::Mutex<Option<Replicator>>>> {
        match self {
            Database::Primary(db) => db.replicator(),
            Database::Replica(_) => None,
            Database::Schema(db) => db.replicator(),
            Database::LibsqlPrimary(_) => None,
            Database::LibsqlReplica(_) => None,
            Database::LibsqlSchema(_) => None,
        }
    }
}
