use std::sync::Arc;

use libsql_replication::rpc::proxy::ExecResp;
use tonic::Streaming;

use crate::connection::write_proxy::{MakeWriteProxyConn, WriteProxyConnection};
use crate::connection::{MakeThrottledConnection, TrackedConnection};

use super::Result;

pub type ReplicaConnection = TrackedConnection<WriteProxyConnection<Streaming<ExecResp>>>;
type ReplicaConnectionMaker = MakeThrottledConnection<MakeWriteProxyConn>;

pub struct ReplicaDatabase {
    pub connection_maker: Arc<ReplicaConnectionMaker>,
}

impl ReplicaDatabase {
    pub fn connection_maker(&self) -> Arc<ReplicaConnectionMaker> {
        self.connection_maker.clone()
    }

    pub fn destroy(self) {}

    pub async fn shutdown(self) -> Result<()> {
        Ok(())
    }
}
