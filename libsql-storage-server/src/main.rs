use anyhow::Result;
use clap::Parser;
use libsql_storage::rpc::storage_server::{Storage, StorageServer};
use libsql_storage::rpc::{
    DbSizeReq, DbSizeResp, FindFrameReq, FindFrameResp, InsertFramesReq, InsertFramesResp,
    ReadFrameReq, ReadFrameResp,
};
use libsql_storage_server::version::Version;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Response};
use tracing::trace;

/// libSQL storage server
#[derive(Debug, Parser)]
#[command(name = "libsql-storage-server")]
#[command(about = "libSQL storage server", version = Version::default(), long_about = None)]
struct Cli {
    /// The address and port the storage RPC protocol listens to. Example: `127.0.0.1:5002`.
    #[clap(
        long,
        env = "LIBSQL_STORAGE_LISTEN_ADDR",
        default_value = "127.0.0.1:5002"
    )]
    listen_addr: SocketAddr,
}

#[derive(Default)]
struct Service {
    pages: Arc<Mutex<BTreeMap<u64, bytes::Bytes>>>,
    db_size: AtomicU32,
}

impl Service {
    pub fn new() -> Self {
        Self::default()
    }
}

#[tonic::async_trait]
impl Storage for Service {
    async fn insert_frames(
        &self,
        request: tonic::Request<InsertFramesReq>,
    ) -> Result<tonic::Response<InsertFramesResp>, tonic::Status> {
        trace!("insert_frames()");
        let mut num_frames = 0;
        for frame in request.into_inner().frames {
            let mut pages = self.pages.lock().unwrap();
            trace!("inserting frame for page {}", frame.page_no);
            pages.insert(frame.page_no, frame.data.into());
            num_frames += 1;
            self.db_size
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(Response::new(InsertFramesResp { num_frames }))
    }

    async fn find_frame(
        &self,
        request: tonic::Request<FindFrameReq>,
    ) -> Result<tonic::Response<FindFrameResp>, tonic::Status> {
        let page_no = request.into_inner().page_no;
        trace!("find_frame(page_no={})", page_no);
        let pages = self.pages.lock().unwrap();
        if pages.contains_key(&page_no) {
            // We have 1:1 mapping between frames and pages to cheat a bit.
            Ok(Response::new(FindFrameResp {
                frame_no: Some(page_no),
            }))
        } else {
            Ok(Response::new(FindFrameResp { frame_no: None }))
        }
    }

    async fn read_frame(
        &self,
        request: tonic::Request<ReadFrameReq>,
    ) -> Result<tonic::Response<ReadFrameResp>, tonic::Status> {
        let frame_no = request.into_inner().frame_no;
        trace!("read_frame(frame_no={})", frame_no);
        let pages = self.pages.lock().unwrap();
        if let Some(data) = pages.get(&frame_no) {
            Ok(Response::new(ReadFrameResp {
                frame: Some(data.clone().into()),
            }))
        } else {
            Ok(Response::new(ReadFrameResp { frame: None }))
        }
    }

    async fn db_size(
        &self,
        request: tonic::Request<DbSizeReq>,
    ) -> Result<tonic::Response<DbSizeResp>, tonic::Status> {
        let size = self.db_size.load(std::sync::atomic::Ordering::SeqCst) as u64;
        Ok(Response::new(DbSizeResp { size }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let args = Cli::parse();

    let service = Service::default();

    println!("Starting libSQL storage server on {}", args.listen_addr);

    Server::builder()
        .add_service(StorageServer::new(service))
        .serve(args.listen_addr)
        .await?;

    Ok(())
}
