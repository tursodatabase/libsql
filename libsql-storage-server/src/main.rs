use anyhow::Result;
use clap::Parser;
use libsql_storage::rpc::storage_server::{Storage, StorageServer};
use libsql_storage::rpc::{
    DbSizeReq, DbSizeResp, FindFrameReq, FindFrameResp, FramePageNumReq, FramePageNumResp,
    FramesInWalReq, FramesInWalResp, InsertFramesReq, InsertFramesResp, ReadFrameReq,
    ReadFrameResp,
};
use libsql_storage_server::version::Version;
use std::collections::BTreeMap;
use std::iter::Map;
use std::net::SocketAddr;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, trace};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

/// libSQL storage server
#[derive(Debug, Parser)]
#[command(name = "libsql-storage-server")]
#[command(about = "libSQL storage server", version = Version::default(), long_about = None)]
struct Cli {
    /// The address and port the storage RPC protocol listens to. Example: `127.0.0.1:5002`.
    #[clap(
        long,
        env = "LIBSQL_STORAGE_LISTEN_ADDR",
        default_value = "0.0.0.0:5002"
    )]
    listen_addr: SocketAddr,
}

#[derive(Default)]

struct FrameData {
    page_no: u64,
    data: bytes::Bytes,
}

#[derive(Default)]
struct FrameStore {
    // contains a frame data, key is the frame number
    frames: BTreeMap<u64, FrameData>,
    // pages map contains the page number as a key and the list of frames for the page as a value
    pages: BTreeMap<u64, Vec<u64>>,
    max_frame_no: u64,
}

impl FrameStore {
    pub fn new() -> Self {
        Self::default()
    }

    // inserts a new frame for the page number and returns the new frame value
    pub fn insert_frame(&mut self, page_no: u64, frame: bytes::Bytes) -> u64 {
        let frame_no = self.max_frame_no + 1;
        self.max_frame_no = frame_no;
        self.frames.insert(
            frame_no,
            FrameData {
                page_no,
                data: frame,
            },
        );
        self.pages
            .entry(page_no)
            .or_insert_with(Vec::new)
            .push(frame_no);
        frame_no
    }

    pub fn read_frame(&self, frame_no: u64) -> Option<&bytes::Bytes> {
        self.frames.get(&frame_no).map(|frame| &frame.data)
    }

    // given a page number, return the maximum frame for the page
    pub fn find_frame(&self, page_no: u64) -> Option<u64> {
        self.pages
            .get(&page_no)
            .map(|frames| *frames.last().unwrap())
    }

    // given a frame num, return the page number
    pub fn frame_page_no(&self, frame_no: u64) -> Option<u64> {
        self.frames.get(&frame_no).map(|frame| frame.page_no)
    }

    pub fn frames_in_wal(&self) -> u64 {
        self.max_frame_no
    }
}

#[derive(Default)]
struct Service {
    store: Arc<Mutex<FrameStore>>,
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
        let mut store = self.store.lock().unwrap();
        for frame in request.into_inner().frames {
            trace!(
                "inserted for page {} frame {}",
                frame.page_no,
                store.insert_frame(frame.page_no, frame.data.into())
            );
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
        if let Some(frame_no) = self.store.lock().unwrap().find_frame(page_no) {
            Ok(Response::new(FindFrameResp {
                frame_no: Some(frame_no),
            }))
        } else {
            error!("find_frame() failed for page_no={}", page_no);
            Ok(Response::new(FindFrameResp { frame_no: None }))
        }
    }

    async fn read_frame(
        &self,
        request: tonic::Request<ReadFrameReq>,
    ) -> Result<tonic::Response<ReadFrameResp>, tonic::Status> {
        let frame_no = request.into_inner().frame_no;
        trace!("read_frame(frame_no={})", frame_no);
        if let Some(data) = self.store.lock().unwrap().read_frame(frame_no) {
            Ok(Response::new(ReadFrameResp {
                frame: Some(data.clone().into()),
            }))
        } else {
            error!("read_frame() failed for frame_no={}", frame_no);
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

    async fn frames_in_wal(
        &self,
        request: Request<FramesInWalReq>,
    ) -> std::result::Result<Response<FramesInWalResp>, Status> {
        Ok(Response::new(FramesInWalResp {
            count: self.store.lock().unwrap().frames_in_wal() as u32,
        }))
    }

    async fn frame_page_num(
        &self,
        request: Request<FramePageNumReq>,
    ) -> std::result::Result<Response<FramePageNumResp>, Status> {
        let frame_no = request.into_inner().frame_no;
        if let Some(page_no) = self.store.lock().unwrap().frame_page_no(frame_no) {
            Ok(Response::new(FramePageNumResp { page_no }))
        } else {
            error!("frame_page_num() failed for frame_no={}", frame_no);
            Ok(Response::new(FramePageNumResp { page_no: 0 }))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("libsql_storage_server=trace"));
    tracing::subscriber::set_global_default(
        FmtSubscriber::builder().with_env_filter(filter).finish(),
    )
    .expect("setting default subscriber failed");

    let args = Cli::parse();

    let service = Service::default();

    println!("Starting libSQL storage server on {}", args.listen_addr);
    trace!(
        "(trace) Starting libSQL storage server on {}",
        args.listen_addr
    );
    Server::builder()
        .add_service(StorageServer::new(service))
        .serve(args.listen_addr)
        .await?;

    Ok(())
}
