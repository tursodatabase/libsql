use std::fmt;
use std::sync::Arc;

use bottomless::SavepointTracker;

use crate::connection::{MakeConnection, RequestContext};

pub use self::primary::{PrimaryConnection, PrimaryConnectionMaker, PrimaryDatabase};
pub use self::replica::{ReplicaConnection, ReplicaDatabase};
pub use self::schema::{SchemaConnection, SchemaDatabase};

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
    Schema(SchemaConnection),
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primary(_) => write!(f, "Primary"),
            Self::Replica(_) => write!(f, "Replica"),
            Self::Schema(_) => write!(f, "Schema"),
        }
    }
}

impl Connection {
    /// Returns `true` if the connection is [`Primary`].
    ///
    /// [`Primary`]: Connection::Primary
    #[must_use]
    pub fn is_primary(&self) -> bool {
        matches!(self, Self::Primary(..))
    }
}

#[async_trait::async_trait]
impl crate::connection::Connection for Connection {
    async fn execute_program<B: crate::query_result_builder::QueryResultBuilder>(
        &self,
        pgm: crate::connection::program::Program,
        ctx: RequestContext,
        response_builder: B,
        replication_index: Option<crate::replication::FrameNo>,
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
        }
    }

    async fn describe(
        &self,
        sql: String,
        ctx: RequestContext,
        replication_index: Option<crate::replication::FrameNo>,
    ) -> crate::Result<crate::Result<crate::connection::program::DescribeResponse>> {
        match self {
            Connection::Primary(conn) => conn.describe(sql, ctx, replication_index).await,
            Connection::Replica(conn) => conn.describe(sql, ctx, replication_index).await,
            Connection::Schema(conn) => conn.describe(sql, ctx, replication_index).await,
        }
    }

    async fn is_autocommit(&self) -> crate::Result<bool> {
        match self {
            Connection::Primary(conn) => conn.is_autocommit().await,
            Connection::Replica(conn) => conn.is_autocommit().await,
            Connection::Schema(conn) => conn.is_autocommit().await,
        }
    }

    async fn checkpoint(&self) -> crate::Result<()> {
        match self {
            Connection::Primary(conn) => conn.checkpoint().await,
            Connection::Replica(conn) => conn.checkpoint().await,
            Connection::Schema(conn) => conn.checkpoint().await,
        }
    }

    async fn vacuum_if_needed(&self) -> crate::Result<()> {
        match self {
            Connection::Primary(conn) => conn.vacuum_if_needed().await,
            Connection::Replica(conn) => conn.vacuum_if_needed().await,
            Connection::Schema(conn) => conn.vacuum_if_needed().await,
        }
    }

    fn diagnostics(&self) -> String {
        match self {
            Connection::Primary(conn) => conn.diagnostics(),
            Connection::Replica(conn) => conn.diagnostics(),
            Connection::Schema(conn) => conn.diagnostics(),
        }
    }
}

pub enum Database {
    Primary(PrimaryDatabase),
    Replica(ReplicaDatabase),
    Schema(SchemaDatabase),
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primary(_) => write!(f, "Primary"),
            Self::Replica(_) => write!(f, "Replica"),
            Database::Schema(_) => write!(f, "Schema"),
        }
    }
}

impl Database {
    pub fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Connection>> {
        match self {
            Database::Primary(db) => Arc::new(db.connection_maker().map(Connection::Primary)),
            Database::Replica(db) => Arc::new(db.connection_maker().map(Connection::Replica)),
            Database::Schema(db) => Arc::new(db.connection_maker().map(Connection::Schema)),
        }
    }

    pub fn destroy(self) {
        match self {
            Database::Primary(db) => db.destroy(),
            Database::Replica(db) => db.destroy(),
            Database::Schema(db) => db.destroy(),
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        match self {
            Database::Primary(db) => db.shutdown().await,
            Database::Replica(db) => db.shutdown().await,
            Database::Schema(db) => db.shutdown().await,
        }
    }

    pub fn as_primary(&self) -> Option<&PrimaryDatabase> {
        if let Self::Primary(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub(crate) fn as_schema(&self) -> Option<&SchemaDatabase> {
        if let Self::Schema(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub(crate) fn backup_savepoint(&self) -> Option<SavepointTracker> {
        match self {
            Database::Primary(db) => db.backup_savepoint(),
            Database::Replica(_) => None,
            Database::Schema(db) => db.backup_savepoint(),
        }
    }
}
