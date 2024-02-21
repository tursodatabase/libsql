use std::sync::Arc;

use async_trait::async_trait;

use crate::connection::libsql::LibSqlConnection;
use crate::connection::write_proxy::{RpcStream, WriteProxyConnection};
use crate::connection::{Connection, MakeConnection, TrackedConnection};
use crate::namespace::replication_wal::{ReplicationWal, ReplicationWalManager};

pub type PrimaryConnection = TrackedConnection<LibSqlConnection<ReplicationWal>>;

pub type Result<T> = anyhow::Result<T>;

#[async_trait]
pub trait Database: Sync + Send + 'static {
    /// The connection type of the database
    type Connection: Connection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>>;

    fn destroy(self);

    async fn shutdown(self) -> Result<()>;
}

pub struct ReplicaDatabase {
    pub connection_maker:
        Arc<dyn MakeConnection<Connection = TrackedConnection<WriteProxyConnection<RpcStream>>>>,
}

#[async_trait]
impl Database for ReplicaDatabase {
    type Connection = TrackedConnection<WriteProxyConnection<RpcStream>>;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker.clone()
    }

    fn destroy(self) {}

    async fn shutdown(self) -> Result<()> {
        Ok(())
    }
}

pub enum PrimaryDatabase {
    Standard(StandardPrimaryDatabase),
    SharedSchema(SharedSchemaDatabase),
}

pub struct StandardPrimaryDatabase {
    pub wal_manager: ReplicationWalManager,
    pub connection_maker: Arc<dyn MakeConnection<Connection = PrimaryConnection>>,
}

pub struct SharedSchemaDatabase {
    pub wal_manager: ReplicationWalManager,
    pub connection_maker: Arc<dyn MakeConnection<Connection = PrimaryConnection>>,
}

impl PrimaryDatabase {
    pub fn wal_manager(&self) -> &ReplicationWalManager {
        match self {
            PrimaryDatabase::Standard(db) => &db.wal_manager,
            PrimaryDatabase::SharedSchema(db) => &db.wal_manager,
        }
    }
    pub fn connection_maker(&self) -> &Arc<dyn MakeConnection<Connection = PrimaryConnection>> {
        match self {
            PrimaryDatabase::Standard(db) => &db.connection_maker,
            PrimaryDatabase::SharedSchema(db) => &db.connection_maker,
        }
    }
}

#[async_trait]
impl Database for PrimaryDatabase {
    type Connection = PrimaryConnection;

    fn connection_maker(&self) -> Arc<dyn MakeConnection<Connection = Self::Connection>> {
        self.connection_maker().clone()
    }

    fn destroy(self) {
        self.wal_manager()
            .wrapped()
            .logger()
            .closed_signal
            .send_replace(true);
    }

    async fn shutdown(self) -> Result<()> {
        self.wal_manager()
            .wrapped()
            .logger()
            .closed_signal
            .send_replace(true);
        if let Some(mut replicator) = tokio::task::spawn_blocking(move || {
            let wal_manager = self.wal_manager();
            wal_manager.wrapper().as_ref().and_then(|r| r.shutdown())
        })
        .await?
        {
            replicator.shutdown_gracefully().await?;
        }

        Ok(())
    }
}
