use futures::stream::BoxStream;
use libsql_sync::sync::rpc::wal_sync_server::WalSync;

pub struct SyncService {}

#[tonic::async_trait]
impl WalSync for SyncService {
    type FetchDatabaseStream =
        BoxStream<'static, Result<libsql_sync::sync::rpc::DatabaseChunk, tonic::Status>>;

    async fn fetch_database(
        &self,
        _request: tonic::Request<libsql_sync::sync::rpc::FetchDatabaseRequest>,
    ) -> Result<tonic::Response<Self::FetchDatabaseStream>, tonic::Status> {
        unimplemented!()
    }

    async fn pull_wal(
        &self,
        _request: tonic::Request<libsql_sync::sync::rpc::PullWalRequest>,
    ) -> Result<tonic::Response<libsql_sync::sync::rpc::PullWalResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn push_wal(
        &self,
        _request: tonic::Request<libsql_sync::sync::rpc::PushWalRequest>,
    ) -> Result<tonic::Response<libsql_sync::sync::rpc::PushWalResponse>, tonic::Status> {
        unimplemented!()
    }
}
