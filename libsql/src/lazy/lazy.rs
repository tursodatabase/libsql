use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use arc_swap::access::Access;
use arc_swap::ArcSwap;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::HeaderValue;
use hyper::Body;
use serde::{Deserialize, Serialize};
use zstd::bulk::Decompressor;

use crate::local::Connection;
use crate::params::Params;
use crate::sync::atomic_write;
use crate::util::ConnectorService;

use super::vfs::{register_vfs, RegisteredVfs};
use super::vfs_default::{get_default_vfs, Sqlite3Vfs};
use super::vfs_lazy::LazyVfs;

const PULL_PAGES_CHUNK_SIZE: usize = 10;
const PULL_PROTOCOL_RETRIES: usize = 3;
pub const LAZY_VFS_NAME: &[u8] = b"turso-vfs-lazy\0";

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PullPlanReqBody {
    pub start_revision: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PullPlanRespBody {
    pub steps: Vec<PullPlanRespStep>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PullPlanRespStep {
    pub end_revision: String,
    pub pages: Vec<usize>,
    pub size_after_in_pages: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PullPagesReqBody {
    pub start_revision: Option<String>,
    pub end_revision: String,
    pub server_pages: Vec<usize>,
    pub client_pages: Vec<usize>,
    pub accept_encodings: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LazyMetadata {
    pub revision: Option<String>,
}

pub trait PageServer {
    fn get_revision(&self) -> String;
    async fn set_revision(&self, revision: String) -> anyhow::Result<()>;
    async fn pull_plan(&self, request: &PullPlanReqBody) -> anyhow::Result<PullPlanRespBody>;
    async fn pull_pages(&self, request: &PullPagesReqBody) -> anyhow::Result<Vec<(u32, Bytes)>>;
}

pub struct TursoPageServer {
    pub endpoint: String,
    pub auth_token: Option<String>,
    pub client: hyper::Client<ConnectorService, Body>,
    pub revision: ArcSwap<String>,
}

impl TursoPageServer {
    async fn send_request(
        &self,
        mut uri: String,
        body: Bytes,
        max_retries: usize,
    ) -> anyhow::Result<Body> {
        let mut retries = 0;
        loop {
            let mut request = http::Request::post(uri.clone());
            match &self.auth_token {
                Some(auth_token) => {
                    let headers = request.headers_mut().expect("valid http request");
                    headers.insert("Authorization", HeaderValue::from_str(&auth_token)?);
                }
                None => {}
            }
            let request = request.body(body.clone().into()).expect("valid body");
            let response = self.client.request(request).await?;

            if response.status().is_success() {
                return Ok(response.into_body());
            }

            if response.status().is_redirection() {
                uri = match response.headers().get(hyper::header::LOCATION) {
                    Some(loc) => loc.to_str()?.to_string(),
                    None => return Err(anyhow!("unable to parse location redirect header")),
                };
            }

            // If we've retried too many times or the error is not a server error,
            // return the error.
            if retries > max_retries || !response.status().is_server_error() {
                let status = response.status();
                let body = hyper::body::to_bytes(response.into_body()).await?;
                let msg = String::from_utf8_lossy(&body[..]);
                return Err(anyhow!(
                    "request failed: url={}, status={}, body={}",
                    uri,
                    status,
                    msg
                ));
            }

            let delay = std::time::Duration::from_millis(100 * (1 << retries));
            tokio::time::sleep(delay).await;
            retries += 1;
        }
    }
}

const PAGE_BATCH_ENCODING_RAW: u32 = 0;
const PAGE_BATCH_ENCODING_ZSTD: u32 = 1;

impl PageServer for TursoPageServer {
    fn get_revision(&self) -> String {
        let revision = self.revision.load_full().to_string();
        tracing::info!("get_revision: {}", revision);
        revision
    }
    async fn set_revision(&self, revision: String) -> anyhow::Result<()> {
        tracing::info!("set_revision: {}", revision);
        self.revision.store(Arc::new(revision));
        Ok(())
    }
    async fn pull_plan(&self, request: &PullPlanReqBody) -> anyhow::Result<PullPlanRespBody> {
        let body = serde_json::to_vec(&request)?;
        let response = self
            .send_request(
                format!("{}/pull-plan", self.endpoint),
                Bytes::from(body),
                PULL_PROTOCOL_RETRIES,
            )
            .await?;

        let response_bytes = hyper::body::to_bytes(response).await?;
        tracing::info!(
            "pull_plan(start_revision={:?}): response_bytes={}",
            request.start_revision,
            response_bytes.len()
        );
        let response = serde_json::from_slice(&response_bytes)?;
        Ok(response)
    }

    async fn pull_pages(&self, request: &PullPagesReqBody) -> anyhow::Result<Vec<(u32, Bytes)>> {
        let body = serde_json::to_vec(&request)?;
        let response = self
            .send_request(
                format!("{}/pull-pages", self.endpoint),
                Bytes::from(body),
                PULL_PROTOCOL_RETRIES,
            )
            .await?;
        let mut response_bytes = hyper::body::to_bytes(response).await?;
        tracing::info!(
            "pull_pages(start_revision={:?}, end_revision={}): response_bytes={}",
            request.start_revision,
            request.end_revision,
            response_bytes.len()
        );
        let encoded_meta_length = response_bytes.get_u32_le();
        let encoded_pages_length = response_bytes.get_u32_le();
        let pages_count = response_bytes.get_u32_le();
        let page_size = response_bytes.get_u32_le();
        let (mut meta, mut pages) = response_bytes.split_at(encoded_meta_length as usize);
        assert!(pages.len() == encoded_pages_length as usize);
        let encoding_type = meta.get_u32_le();
        let mut result = Vec::new();
        match encoding_type {
            PAGE_BATCH_ENCODING_RAW => {
                while !meta.is_empty() {
                    let page_no = meta.get_u32_le();
                    let page;
                    (page, pages) = pages.split_at(page_size as usize);
                    result.push((page_no, Bytes::from(page.to_vec())));
                }
            }
            PAGE_BATCH_ENCODING_ZSTD => {
                let dictionary_pages_count = meta.get_u32_le();
                assert!(dictionary_pages_count == 0);
                let mut zstd = Decompressor::new()?;
                let pages = zstd.decompress(&pages, (pages_count * page_size) as usize)?;
                for i in 0..pages_count {
                    let page_no = meta.get_u32_le();
                    let page = Bytes::from(
                        pages[(i * page_size) as usize..((i + 1) * page_size) as usize].to_vec(),
                    );
                    result.push((page_no, page));
                }
            }
            _ => return Err(anyhow!("unexpected encoding type: {}", encoding_type)),
        }
        assert!(result.len() == pages_count as usize);
        Ok(result)
    }
}

pub struct LazyContext<P: PageServer + Send + Sync + 'static> {
    db_path: String,
    meta_path: String,
    metadata: LazyMetadata,
    encoding: String,
    page_server: Arc<P>,
    vfs: RegisteredVfs<LazyVfs<Sqlite3Vfs, P>>,
}

async fn read_metadata(meta_path: &String) -> anyhow::Result<Option<LazyMetadata>> {
    let exists = Path::new(&meta_path).try_exists()?;
    if !exists {
        tracing::debug!("no metadata info file found");
        return Ok(None);
    }

    let contents = tokio::fs::read(&meta_path).await?;
    let metadata = serde_json::from_slice::<LazyMetadata>(&contents)?;

    tracing::debug!(
        "read lazy metadata for meta_path={:?}, metadata={:?}",
        meta_path,
        metadata
    );
    Ok(Some(metadata))
}

async fn write_metadata(meta_path: &String, metadata: &LazyMetadata) -> anyhow::Result<()> {
    let contents = serde_json::to_vec(metadata)?;
    atomic_write(&meta_path, &contents).await?;
    Ok(())
}

impl<T: PageServer + Send + Sync + 'static> LazyContext<T> {
    pub async fn new(
        db_path: String,
        meta_path: String,
        encoding: String,
        page_server: Arc<T>,
    ) -> anyhow::Result<Self> {
        let metadata = read_metadata(&meta_path).await?;
        let metadata = metadata.unwrap_or(LazyMetadata { revision: None });

        if let Some(revision) = &metadata.revision {
            page_server.set_revision(revision.clone()).await?;
        }

        let vfs_default = get_default_vfs("turso-vfs-default");
        let vfs_lazy = LazyVfs::new("turso-vfs-lazy", vfs_default, page_server.clone());
        let vfs = register_vfs(vfs_lazy)?;
        let ctx = Self {
            db_path,
            meta_path,
            page_server,
            encoding,
            metadata,
            vfs,
        };
        Ok(ctx)
    }
    async fn pull(&mut self, conn: &Connection) -> anyhow::Result<()> {
        let start_revision = self.metadata.revision.clone();
        let pull_plan_request = PullPlanReqBody {
            start_revision: start_revision.clone(),
        };
        let pull_plan = self.page_server.pull_plan(&pull_plan_request).await?;
        tracing::debug!(
            "pull_plan(start_revision={:?}): steps={}",
            pull_plan_request.start_revision,
            pull_plan.steps.len()
        );
        for step in pull_plan.steps {
            tracing::debug!(
                "pull_plan(start_revision={:?}): next step, pages={}",
                pull_plan_request.start_revision,
                step.pages.len()
            );
            let frames_count = conn.wal_frame_count();

            let insert_handle = conn.wal_insert_handle()?;

            let mut frame_buffer = BytesMut::new();
            let mut received_pages = 0;
            for chunk in step.pages.chunks(PULL_PAGES_CHUNK_SIZE) {
                let pull_pages_request = PullPagesReqBody {
                    start_revision: start_revision.clone(),
                    end_revision: step.end_revision.clone(),
                    server_pages: chunk.to_vec(),
                    client_pages: vec![],
                    accept_encodings: vec![self.encoding.clone()],
                };
                let pages = self.page_server.pull_pages(&pull_pages_request).await?;
                assert!(pages.len() == chunk.len());
                for (page_no, page) in pages {
                    received_pages += 1;
                    let size_after = if received_pages == step.pages.len() {
                        step.size_after_in_pages as u32
                    } else {
                        0
                    };

                    tracing::trace!("pull: insert page={}, size_after={}", page_no, size_after);
                    frame_buffer.clear();
                    frame_buffer.put_u32(page_no);
                    frame_buffer.put_u32(size_after);
                    frame_buffer.put_u32(0);
                    frame_buffer.put_u32(0);
                    frame_buffer.put_u32(0);
                    frame_buffer.put_u32(0);
                    frame_buffer.extend_from_slice(&page);

                    // insert_handle.insert(&frame_buffer)?;
                }
            }

            insert_handle.end()?;

            // assert!(conn.wal_frame_count() == frames_count + step.pages.len() as u32);

            let next_revision = Some(step.end_revision.clone());
            let next_metadata = LazyMetadata {
                revision: next_revision,
            };
            write_metadata(&self.meta_path, &next_metadata).await?;
            self.page_server.set_revision(step.end_revision).await?;
            self.metadata = next_metadata;
        }

        // checkpoint local DB just to reduce storage overhead
        conn.wal_checkpoint(true)?;
        let rows = conn.query("SELECT COUNT(*) FROM t", Params::None)?.unwrap();
        let row = rows.next().unwrap().unwrap();
        tracing::info!("rows: {:?}", row.get::<i64>(0));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arc_swap::ArcSwap;
    use std::{sync::Arc, time::Duration};
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    use crate::{
        database::connector,
        lazy::lazy::{LazyContext, TursoPageServer},
        local::{Connection, Database},
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn lazy_pull_test() {
        tracing_subscriber::fmt::init();
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test.db");
        tracing::info!("db_path: {:?}", db_path);
        let db_path = db_path.to_str().unwrap().to_owned();

        let connector = connector().unwrap();
        let svc = connector
            .map_err(|e| e.into())
            .map_response(|s| Box::new(s) as Box<dyn crate::util::Socket>);
        let svc = crate::util::ConnectorService::new(svc);
        use tower::ServiceExt;
        let svc = svc
            .map_err(|e| e.into())
            .map_response(|s| Box::new(s) as Box<dyn crate::util::Socket>);
        let connector = crate::util::ConnectorService::new(svc);
        let client = hyper::client::Client::builder().build::<_, hyper::Body>(connector);

        let page_server = Arc::new(TursoPageServer {
            endpoint: "http://c--ap--b.localhost:8080".into(),
            auth_token: None,
            client,
            revision: ArcSwap::new(Arc::new("".into())),
        });
        let lazy_ctx = LazyContext::new(
            db_path.clone(),
            format!("{}-metadata", db_path),
            "zstd".into(),
            page_server,
        )
        .await
        .unwrap();
        let lazy_ctx = Arc::new(Mutex::new(lazy_ctx));
        let conn = Connection::connect(&Database {
            db_path: db_path.clone(),
            flags: crate::OpenFlags::default(),
            replication_ctx: None,
            sync_ctx: None,
            lazy_ctx: Some(lazy_ctx.clone()),
        })
        .unwrap();
        let mut lazy_ctx = lazy_ctx.lock().await;
        lazy_ctx.pull(&conn).await.unwrap();
        std::thread::sleep(Duration::from_secs(100000));
    }
}
