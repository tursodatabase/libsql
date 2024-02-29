#![allow(dead_code)]

use std::sync::Arc;

use crate::connection::program::Program;
use crate::connection::{MakeConnection, RequestContext};
use crate::namespace::NamespaceName;
use crate::schema_migration::SchedulerHandle;

use super::primary::PrimaryConnectionMaker;
use super::PrimaryConnection;

pub struct SchemaConnection {
    migration_scheduler: SchedulerHandle,
    schema: NamespaceName,
    connection: PrimaryConnection,
}

#[async_trait::async_trait]
impl crate::connection::Connection for SchemaConnection {
    async fn execute_program<B: crate::query_result_builder::QueryResultBuilder>(
        &self,
        pgm: Program,
        ctx: RequestContext,
        response_builder: B,
        replication_index: Option<crate::replication::FrameNo>,
    ) -> crate::Result<B> {
        self.migration_scheduler
            .register_migration_task(self.schema.clone(), pgm.clone())
            .await?;
        self.connection
            .execute_program(pgm, ctx, response_builder, replication_index)
            .await
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
}

#[async_trait::async_trait]
impl MakeConnection for SchemaDatabase {
    type Connection = SchemaConnection;

    async fn create(&self) -> crate::Result<Self::Connection, crate::error::Error> {
        let connection = self.connection_maker.create().await?;
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
    ) -> Self {
        Self {
            connection_maker: connection_maker.into(),
            migration_scheduler,
            schema,
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
}
