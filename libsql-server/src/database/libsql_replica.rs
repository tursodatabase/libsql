use std::sync::Arc;

use libsql_replication::rpc::proxy::ExecResp;
use tonic::Streaming;

use crate::connection::libsql::{LibsqlConnection, MakeLibsqlConnection};
use crate::connection::write_proxy::{MakeWriteProxyConn, WriteProxyConnection};
use crate::connection::{MakeThrottledConnection, TrackedConnection};

use super::Result;

pub type LibsqlReplicaConnection =
    TrackedConnection<WriteProxyConnection<Streaming<ExecResp>, LibsqlConnection>>;
type LibsqlReplicaConnectionMaker =
    MakeThrottledConnection<MakeWriteProxyConn<MakeLibsqlConnection>>;

pub struct LibsqlReplicaDatabase {
    pub connection_maker: Arc<LibsqlReplicaConnectionMaker>,
}

impl LibsqlReplicaDatabase {
    pub fn connection_maker(&self) -> Arc<LibsqlReplicaConnectionMaker> {
        self.connection_maker.clone()
    }

    pub fn destroy(self) {}

    pub async fn shutdown(self) -> Result<()> {
        Ok(())
    }
}
