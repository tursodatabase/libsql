use std::sync::Arc;

use libsql_replication::rpc::proxy::ExecResp;
use libsql_sys::wal::wrapper::PassthroughWalWrapper;
use tonic::Streaming;

use crate::connection::legacy::{LegacyConnection, MakeLegacyConnection};
use crate::connection::write_proxy::{MakeWriteProxyConn, WriteProxyConnection};
use crate::connection::{MakeThrottledConnection, TrackedConnection};

use super::Result;

pub type ReplicaConnection = TrackedConnection<
    WriteProxyConnection<Streaming<ExecResp>, LegacyConnection<PassthroughWalWrapper>>,
>;
type ReplicaConnectionMaker =
    MakeThrottledConnection<MakeWriteProxyConn<MakeLegacyConnection<PassthroughWalWrapper>>>;

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
