#![allow(dead_code)]

use bottomless::SavepointTracker;
use std::sync::Arc;

use crate::connection::program::{check_program_auth, Program};
use crate::connection::{MakeConnection, RequestContext};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::replication_wal::ReplicationWalWrapper;
use crate::namespace::NamespaceName;
use crate::query_result_builder::QueryBuilderConfig;
use crate::schema::{perform_migration, validate_migration, MigrationJobStatus, SchedulerHandle};

use super::primary::PrimaryConnectionMaker;
use super::PrimaryConnection;

pub struct SchemaConnection {
    migration_scheduler: SchedulerHandle,
    schema: NamespaceName,
    connection: Arc<PrimaryConnection>,
    config: MetaStoreHandle,
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
            let res = self
                .connection
                .execute_program(migration, ctx, builder, replication_index)
                .await;

            // If the query was okay, verify if the connection is not in a txn state
            if res.is_ok() && self.connection.is_autocommit().await? {
                return Err(crate::Error::Migration(
                    crate::schema::Error::ConnectionInTxnState,
                ));
            }

            res
        } else {
            check_program_auth(&ctx, &migration, &self.config.get())?;
            let connection = self.connection.clone();
            validate_migration(&migration)?;
            let migration = Arc::new(migration);
            let builder = tokio::task::spawn_blocking({
                let migration = migration.clone();
                move || {
                    let res = connection.with_raw(|conn| -> crate::Result<_> {
                        let mut txn = conn
                            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                            .map_err(|_| {
                                crate::Error::Migration(
                                    crate::schema::Error::InteractiveTxnNotAllowed,
                                )
                            })?;
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
                    });

                    res
                }
            })
            .await
            .unwrap()?;

            // if dry run is successfull, enqueue
            let mut handle = self
                .migration_scheduler
                .register_migration_task(self.schema.clone(), migration)
                .await?;

            handle
                .wait_for(|status| match status {
                    MigrationJobStatus::DryRunSuccess
                    | MigrationJobStatus::DryRunFailure
                    | MigrationJobStatus::RunSuccess
                    | MigrationJobStatus::RunFailure => true,
                    _ => false,
                })
                .await;

            match self
                .migration_scheduler
                .get_job_status(handle.job_id())
                .await?
            {
                (MigrationJobStatus::DryRunFailure, Some(err)) => {
                    Err(crate::schema::Error::DryRunFailure(err))?
                }
                (MigrationJobStatus::RunFailure, Some(err)) => {
                    Err(crate::schema::Error::MigrationFailure(err))?
                }
                _ => (),
            }

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
    pub wal_wrapper: ReplicationWalWrapper,
    config: MetaStoreHandle,
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
            config: self.config.clone(),
        })
    }
}

impl SchemaDatabase {
    pub fn new(
        migration_scheduler: SchedulerHandle,
        schema: NamespaceName,
        connection_maker: PrimaryConnectionMaker,
        wal_wrapper: ReplicationWalWrapper,
        config: MetaStoreHandle,
    ) -> Self {
        Self {
            connection_maker: connection_maker.into(),
            migration_scheduler,
            schema,
            wal_wrapper,
            config,
        }
    }

    pub(crate) async fn shutdown(self) -> Result<(), anyhow::Error> {
        self.wal_wrapper
            .wrapper()
            .logger()
            .closed_signal
            .send_replace(true);
        let wal_manager = self.wal_wrapper;
        if let Some(mut replicator) = tokio::task::spawn_blocking(move || {
            wal_manager.wrapped().as_ref().and_then(|r| r.shutdown())
        })
        .await?
        {
            replicator.shutdown_gracefully().await?;
        }

        Ok(())
    }

    pub(crate) fn destroy(&self) {
        self.wal_wrapper
            .wrapper()
            .logger()
            .closed_signal
            .send_replace(true);
    }

    pub(crate) fn connection_maker(&self) -> Self {
        self.clone()
    }

    pub fn backup_savepoint(&self) -> Option<SavepointTracker> {
        if let Some(wal) = self.wal_wrapper.wrapped() {
            if let Some(savepoint) = wal.backup_savepoint() {
                return Some(savepoint);
            }
        }
        None
    }
}
