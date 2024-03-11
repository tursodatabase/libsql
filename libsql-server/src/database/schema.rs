#![allow(dead_code)]

use bottomless::SavepointTracker;
use std::sync::Arc;

use crate::connection::program::Program;
use crate::connection::{MakeConnection, RequestContext};
use crate::namespace::replication_wal::ReplicationWalManager;
use crate::namespace::NamespaceName;
use crate::query_result_builder::QueryBuilderConfig;
use crate::schema::{perform_migration, validate_migration, SchedulerHandle};

use super::primary::PrimaryConnectionMaker;
use super::PrimaryConnection;

pub struct SchemaConnection {
    migration_scheduler: SchedulerHandle,
    schema: NamespaceName,
    connection: Arc<PrimaryConnection>,
}

impl SchemaConnection {
    pub(crate) fn connection(&self) -> &PrimaryConnection {
        &self.connection
    }
}

#[async_trait::async_trait]
impl crate::connection::Connection for SchemaConnection {
    async fn execute_program<B: crate::query_result_builder::QueryResultBuilder>(
        &self,
        migration: Program,
        ctx: RequestContext,
        builder: B,
        replication_index: Option<crate::replication::FrameNo>,
    ) -> crate::Result<B> {
        if migration.is_read_only() {
            self.connection
                .execute_program(migration, ctx, builder, replication_index)
                .await
        } else {
            let connection = self.connection.clone();
            validate_migration(&migration)?;
            let migration = Arc::new(migration);
            let builder = tokio::task::spawn_blocking({
                let migration = migration.clone();
                move || {
                    connection.with_raw(|conn| -> crate::Result<_> {
                        let mut txn = conn
                            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                            .unwrap();
                        // TODO: pass proper config
                        let (ret, _) = perform_migration(
                            &mut txn,
                            &migration,
                            true,
                            builder,
                            &QueryBuilderConfig::default(),
                        );
                        txn.rollback().unwrap();
                        Ok(ret?)
                    })
                }
            })
            .await
            .unwrap()?;

            // if dry run is successfull, enqueue
            self.migration_scheduler
                .register_migration_task(self.schema.clone(), migration)
                .await?;
            // TODO here wait for dry run to be executed on all dbs

            Ok(builder)
        }
    }

    async fn describe(
        &self,
        sql: String,
        ctx: RequestContext,
        replication_index: Option<crate::replication::FrameNo>,
    ) -> crate::Result<crate::Result<crate::connection::program::DescribeResponse>> {
        self.connection.describe(sql, ctx, replication_index).await
    }

    async fn is_autocommit(&self) -> crate::Result<bool> {
        self.connection.is_autocommit().await
    }

    async fn checkpoint(&self) -> crate::Result<()> {
        self.connection.checkpoint().await
    }

    async fn vacuum_if_needed(&self) -> crate::Result<()> {
        self.connection.vacuum_if_needed().await
    }

    fn diagnostics(&self) -> String {
        self.connection.diagnostics()
    }
}

#[derive(Clone)]
pub struct SchemaDatabase {
    migration_scheduler: SchedulerHandle,
    schema: NamespaceName,
    connection_maker: Arc<PrimaryConnectionMaker>,
    pub wal_manager: ReplicationWalManager,
}

#[async_trait::async_trait]
impl MakeConnection for SchemaDatabase {
    type Connection = SchemaConnection;

    async fn create(&self) -> crate::Result<Self::Connection, crate::error::Error> {
        let connection = Arc::new(self.connection_maker.create().await?);
        Ok(SchemaConnection {
            migration_scheduler: self.migration_scheduler.clone(),
            schema: self.schema.clone(),
            connection,
        })
    }
}

impl SchemaDatabase {
    pub fn new(
        migration_scheduler: SchedulerHandle,
        schema: NamespaceName,
        connection_maker: PrimaryConnectionMaker,
        wal_manager: ReplicationWalManager,
    ) -> Self {
        Self {
            connection_maker: connection_maker.into(),
            migration_scheduler,
            schema,
            wal_manager,
        }
    }

    pub(crate) async fn shutdown(&self) -> Result<(), anyhow::Error> {
        todo!()
    }

    pub(crate) fn destroy(&self) {
        todo!()
    }

    pub(crate) fn connection_maker(&self) -> Self {
        self.clone()
    }

    pub fn backup_savepoint(&self) -> Option<SavepointTracker> {
        if let Some(wal) = self.wal_manager.wrapper() {
            if let Some(savepoint) = wal.backup_savepoint() {
                return Some(savepoint);
            }
        }
        None
    }
}
