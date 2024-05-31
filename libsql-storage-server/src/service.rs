use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::memory_store::InMemFrameStore;
use crate::store::FrameStore;
use libsql_storage::rpc;
use libsql_storage::rpc::storage_server::Storage;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::{error, trace};

pub struct Service {
    store: Arc<Mutex<dyn FrameStore + Send + Sync>>,
    db_size: AtomicU32,
}

impl Service {
    pub fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(InMemFrameStore::new())),
            db_size: AtomicU32::new(0),
        }
    }
    pub fn with_store(store: Arc<Mutex<dyn FrameStore + Send + Sync>>) -> Self {
        Self {
            store,
            db_size: AtomicU32::new(0),
        }
    }
}

#[tonic::async_trait]
impl Storage for Service {
    async fn insert_frames(
        &self,
        request: Request<rpc::InsertFramesRequest>,
    ) -> Result<Response<rpc::InsertFramesResponse>, Status> {
        let mut num_frames = 0;
        let mut store = self.store.lock().await;
        let request = request.into_inner();
        let namespace = request.namespace;
        for frame in request.frames.into_iter() {
            trace!(
                "inserted for page {} frame {}",
                frame.page_no,
                store
                    .insert_frame(&namespace, frame.page_no, frame.data.into())
                    .await
            );
            num_frames += 1;
            self.db_size
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        Ok(Response::new(rpc::InsertFramesResponse { num_frames }))
    }

    async fn find_frame(
        &self,
        request: Request<rpc::FindFrameRequest>,
    ) -> Result<Response<rpc::FindFrameResponse>, Status> {
        let request = request.into_inner();
        let page_no = request.page_no;
        let namespace = request.namespace;
        trace!("find_frame(page_no={})", page_no);
        if let Some(frame_no) = self
            .store
            .lock()
            .await
            .find_frame(&namespace, page_no)
            .await
        {
            Ok(Response::new(rpc::FindFrameResponse {
                frame_no: Some(frame_no),
            }))
        } else {
            error!("find_frame() failed for page_no={}", page_no);
            Ok(Response::new(rpc::FindFrameResponse { frame_no: None }))
        }
    }

    async fn read_frame(
        &self,
        request: Request<rpc::ReadFrameRequest>,
    ) -> Result<Response<rpc::ReadFrameResponse>, Status> {
        let request = request.into_inner();
        let frame_no = request.frame_no;
        let namespace = request.namespace;
        trace!("read_frame(frame_no={})", frame_no);
        if let Some(data) = self
            .store
            .lock()
            .await
            .read_frame(&namespace, frame_no)
            .await
        {
            Ok(Response::new(rpc::ReadFrameResponse {
                frame: Some(data.clone().into()),
            }))
        } else {
            error!("read_frame() failed for frame_no={}", frame_no);
            Ok(Response::new(rpc::ReadFrameResponse { frame: None }))
        }
    }

    async fn db_size(
        &self,
        _request: Request<rpc::DbSizeRequest>,
    ) -> Result<Response<rpc::DbSizeResponse>, Status> {
        let size = self.db_size.load(std::sync::atomic::Ordering::SeqCst) as u64;
        Ok(Response::new(rpc::DbSizeResponse { size }))
    }

    async fn frames_in_wal(
        &self,
        request: Request<rpc::FramesInWalRequest>,
    ) -> Result<Response<rpc::FramesInWalResponse>, Status> {
        let namespace = request.into_inner().namespace;
        Ok(Response::new(rpc::FramesInWalResponse {
            count: self.store.lock().await.frames_in_wal(&namespace).await,
        }))
    }

    async fn frame_page_num(
        &self,
        request: Request<rpc::FramePageNumRequest>,
    ) -> Result<Response<rpc::FramePageNumResponse>, Status> {
        let request = request.into_inner();
        let frame_no = request.frame_no;
        let namespace = request.namespace;
        if let Some(page_no) = self
            .store
            .lock()
            .await
            .frame_page_no(&namespace, frame_no)
            .await
        {
            Ok(Response::new(rpc::FramePageNumResponse { page_no }))
        } else {
            error!("frame_page_num() failed for frame_no={}", frame_no);
            Ok(Response::new(rpc::FramePageNumResponse { page_no: 0 }))
        }
    }

    async fn destroy(
        &self,
        request: Request<rpc::DestroyRequest>,
    ) -> Result<Response<rpc::DestroyResponse>, Status> {
        trace!("destroy()");
        let namespace = request.into_inner().namespace;
        self.store.lock().await.destroy(&namespace).await;
        Ok(Response::new(rpc::DestroyResponse {}))
    }
}
