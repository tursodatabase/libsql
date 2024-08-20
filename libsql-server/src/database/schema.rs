#![allow(dead_code)]

use std::sync::Arc;

use tokio::sync::watch::Receiver;

use crate::connection::program::{check_program_auth, Program};
use crate::connection::{MakeConnection, RequestContext};
use crate::namespace::meta_store::MetaStoreHandle;
use crate::namespace::replication_wal::ReplicationWalWrapper;
use crate::namespace::NamespaceName;
use crate::query_result_builder::QueryBuilderConfig;
use crate::schema::{perform_migration, validate_migration, MigrationJobStatus, SchedulerHandle};

pub struct SchemaConnection<C> {
    migration_scheduler: SchedulerHandle,
    schema: NamespaceName,
    connection: Arc<C>,
    config: MetaStoreHandle,
}

impl<C> SchemaConnection<C> {
    pub(crate) fn connection(&self) -> &C {
        &self.connection
    }
}

#[async_trait::async_trait]
impl<C: crate::connection::Connection> crate::connection::Connection for SchemaConnection<C> {
    async fn execute_program<B: crate::query_result_builder::QueryResultBuilder>(
        &self,
        mut migration: Program,
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
            if res.is_ok() && !self.connection.is_autocommit().await? {
                return Err(crate::Error::Migration(
                    crate::schema::Error::ConnectionInTxnState,
                ));
            }

            res
        } else {
            check_program_auth(&ctx, &migration, &self.config.get()).await?;
            let connection = self.connection.clone();
            let disable_foreign_key = validate_migration(&mut migration)?;
            let migration = Arc::new(migration);
            let builder = tokio::task::spawn_blocking({
                let migration = migration.clone();
                move || {
                    let res = connection.with_raw(|conn| -> crate::Result<_> {
                        if disable_foreign_key {
                            conn.execute("PRAGMA foreign_keys=off", ())?;
                        }
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
                        if disable_foreign_key {
                            conn.execute("PRAGMA foreign_keys=on", ())?;
                        }
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
                    MigrationJobStatus::DryRunFailure
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

    fn with_raw<R>(&self, f: impl FnOnce(&mut rusqlite::Connection) -> R) -> R {
        self.connection().with_raw(f)
    }
}

pub struct SchemaDatabase<M> {
    migration_scheduler: SchedulerHandle,
    schema: NamespaceName,
    connection_maker: Arc<M>,
    pub wal_wrapper: Option<ReplicationWalWrapper>,
    config: MetaStoreHandle,
    pub new_frame_notifier: Receiver<Option<u64>>,
}

impl<M> Clone for SchemaDatabase<M> {
    fn clone(&self) -> Self {
        Self {
            migration_scheduler: self.migration_scheduler.clone(),
            schema: self.schema.clone(),
            connection_maker: self.connection_maker.clone(),
            wal_wrapper: self.wal_wrapper.clone(),
            config: self.config.clone(),
            new_frame_notifier: self.new_frame_notifier.clone(),
        }
    }
}

#[async_trait::async_trait]
impl<M: MakeConnection> MakeConnection for SchemaDatabase<M> {
    type Connection = SchemaConnection<M::Connection>;

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

impl<M> SchemaDatabase<M> {
    pub fn new(
        migration_scheduler: SchedulerHandle,
        schema: NamespaceName,
        connection_maker: Arc<M>,
        wal_wrapper: Option<ReplicationWalWrapper>,
        config: MetaStoreHandle,
        new_frame_notifier: Receiver<Option<u64>>,
    ) -> Self {
        Self {
            connection_maker,
            migration_scheduler,
            schema,
            wal_wrapper,
            config,
            new_frame_notifier,
        }
    }

    pub(crate) async fn shutdown(self) -> Result<(), anyhow::Error> {
        if let Some(wrapper) = self.wal_wrapper {
            wrapper.wrapper().logger().closed_signal.send_replace(true);
            let wal_manager = wrapper;

            if let Some(maybe_replicator) = wal_manager.wrapped().as_ref() {
                if let Some(mut replicator) = maybe_replicator.shutdown().await {
                    replicator.shutdown_gracefully().await?;
                }
            }
        }

        Ok(())
    }

    pub(crate) fn destroy(&self) {
        if let Some(ref wrapper) = self.wal_wrapper {
            wrapper.wrapper().logger().closed_signal.send_replace(true);
        }
    }

    pub(crate) fn connection_maker(&self) -> Self {
        self.clone()
    }

    pub(crate) fn replicator(
        &self,
    ) -> Option<Arc<tokio::sync::Mutex<Option<bottomless::replicator::Replicator>>>> {
        if let Some(ref wrapper) = self.wal_wrapper {
            if let Some(wal) = wrapper.wrapped() {
                return Some(wal.replicator());
            }
        }
        None
    }
}
