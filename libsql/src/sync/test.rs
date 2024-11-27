use super::*;
use crate::util::Socket;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{duplex, AsyncRead, AsyncWrite, DuplexStream};
use tower::Service;

#[tokio::test]
async fn test_sync_context_push_frame() {
    let server = MockServer::start();
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let sync_ctx = SyncContext::new(
        server.connector(),
        db_path.to_str().unwrap().to_string(),
        server.url(),
        None,
    )
    .await
    .unwrap();

    let frame = Bytes::from("test frame data");
    let mut sync_ctx = sync_ctx;

    // Push a frame and verify the response
    let durable_frame = sync_ctx.push_one_frame(frame, 1, 0).await.unwrap();
    sync_ctx.write_metadata().await.unwrap();
    assert_eq!(durable_frame, 0); // First frame should return max_frame_no = 0

    // Verify internal state was updated
    assert_eq!(sync_ctx.durable_frame_num(), 0);
    assert_eq!(sync_ctx.generation(), 1);
    assert_eq!(server.frame_count(), 1);
}

#[tokio::test]
async fn test_sync_context_with_auth() {
    let server = MockServer::start();
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let sync_ctx = SyncContext::new(
        server.connector(),
        db_path.to_str().unwrap().to_string(),
        server.url(),
        Some("test_token".to_string()),
    )
    .await
    .unwrap();

    let frame = Bytes::from("test frame with auth");
    let mut sync_ctx = sync_ctx;

    let durable_frame = sync_ctx.push_one_frame(frame, 1, 0).await.unwrap();
    sync_ctx.write_metadata().await.unwrap();
    assert_eq!(durable_frame, 0);
    assert_eq!(server.frame_count(), 1);
}

#[tokio::test]
async fn test_sync_context_multiple_frames() {
    let server = MockServer::start();
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let sync_ctx = SyncContext::new(
        server.connector(),
        db_path.to_str().unwrap().to_string(),
        server.url(),
        None,
    )
    .await
    .unwrap();

    let mut sync_ctx = sync_ctx;

    // Push multiple frames and verify incrementing frame numbers
    for i in 0..3 {
        let frame = Bytes::from(format!("frame data {}", i));
        let durable_frame = sync_ctx.push_one_frame(frame, 1, i).await.unwrap();
        sync_ctx.write_metadata().await.unwrap();
        assert_eq!(durable_frame, i);
        assert_eq!(sync_ctx.durable_frame_num(), i);
        assert_eq!(server.frame_count(), i + 1);
    }
}

#[tokio::test]
async fn test_sync_context_corrupted_metadata() {
    let server = MockServer::start();
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create initial sync context and push a frame
    let sync_ctx = SyncContext::new(
        server.connector(),
        db_path.to_str().unwrap().to_string(),
        server.url(),
        None,
    )
    .await
    .unwrap();

    let mut sync_ctx = sync_ctx;
    let frame = Bytes::from("test frame data");
    let durable_frame = sync_ctx.push_one_frame(frame, 1, 0).await.unwrap();
    sync_ctx.write_metadata().await.unwrap();
    assert_eq!(durable_frame, 0);
    assert_eq!(server.frame_count(), 1);

    // Update metadata path to use -info instead of .meta
    let metadata_path = format!("{}-info", db_path.to_str().unwrap());
    std::fs::write(&metadata_path, b"invalid json data").unwrap();

    // Create new sync context with corrupted metadata
    let sync_ctx = SyncContext::new(
        server.connector(),
        db_path.to_str().unwrap().to_string(),
        server.url(),
        None,
    )
    .await
    .unwrap();

    // Verify that the context was reset to default values
    assert_eq!(sync_ctx.durable_frame_num(), 0);
    assert_eq!(sync_ctx.generation(), 1);
}

#[tokio::test]
async fn test_sync_restarts_with_lower_max_frame_no() {
    let _ = tracing_subscriber::fmt::try_init();

    let server = MockServer::start();
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    // Create initial sync context and push a frame
    let sync_ctx = SyncContext::new(
        server.connector(),
        db_path.to_str().unwrap().to_string(),
        server.url(),
        None,
    )
    .await
    .unwrap();

    let mut sync_ctx = sync_ctx;
    let frame = Bytes::from("test frame data");
    let durable_frame = sync_ctx.push_one_frame(frame.clone(), 1, 0).await.unwrap();
    sync_ctx.write_metadata().await.unwrap();
    assert_eq!(durable_frame, 0);
    assert_eq!(server.frame_count(), 1);

    // Bump the durable frame num so that the next time we call the
    // server we think we are further ahead than the database we are talking to is.
    sync_ctx.durable_frame_num += 3;
    sync_ctx.write_metadata().await.unwrap();

    // Create new sync context with corrupted metadata
    let mut sync_ctx = SyncContext::new(
        server.connector(),
        db_path.to_str().unwrap().to_string(),
        server.url(),
        None,
    )
    .await
    .unwrap();

    // Verify that the context was set to new fake values.
    assert_eq!(sync_ctx.durable_frame_num(), 3);
    assert_eq!(sync_ctx.generation(), 1);

    let frame_no = sync_ctx.durable_frame_num() + 1;
    // This push should fail because we are ahead of the server and thus should get an invalid
    // frame no error.
    sync_ctx
        .push_one_frame(frame.clone(), 1, frame_no)
        .await
        .unwrap_err();

    let frame_no = sync_ctx.durable_frame_num() + 1;
    // This then should work because when the last one failed it updated our state of the server
    // durable_frame_num and we should then start writing from there.
    sync_ctx.push_one_frame(frame, 1, frame_no).await.unwrap();
}

#[tokio::test]
async fn test_sync_context_retry_on_error() {
    // Pause time to control it manually
    tokio::time::pause();

    let server = MockServer::start();
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("test.db");

    let sync_ctx = SyncContext::new(
        server.connector(),
        db_path.to_str().unwrap().to_string(),
        server.url(),
        None,
    )
    .await
    .unwrap();

    let mut sync_ctx = sync_ctx;
    let frame = Bytes::from("test frame data");

    // Set server to return errors
    server.return_error.store(true, Ordering::SeqCst);

    // First attempt should fail but retry
    let result = sync_ctx.push_one_frame(frame.clone(), 1, 0).await;
    assert!(result.is_err());

    // Advance time to trigger retries faster
    tokio::time::advance(Duration::from_secs(2)).await;

    // Verify multiple requests were made (retries occurred)
    assert!(server.request_count() > 1);

    // Allow the server to succeed
    server.return_error.store(false, Ordering::SeqCst);

    // Next attempt should succeed
    let durable_frame = sync_ctx.push_one_frame(frame, 1, 0).await.unwrap();
    sync_ctx.write_metadata().await.unwrap();
    assert_eq!(durable_frame, 0);
    assert_eq!(server.frame_count(), 1);
}

#[test]
fn test_hash_verification() {
    let mut metadata = MetadataJson {
        hash: 0,
        version: 1,
        durable_frame_num: 100,
        generation: 5,
    };

    assert!(metadata.verify_hash().is_err());

    metadata.set_hash();

    assert!(metadata.verify_hash().is_ok());
}

#[test]
fn test_hash_tampering() {
    let mut metadata = MetadataJson {
        hash: 0,
        version: 1,
        durable_frame_num: 100,
        generation: 5,
    };

    // Create metadata with hash
    metadata.set_hash();

    // Tamper with a field
    metadata.version = 2;

    // Verify should fail
    assert!(metadata.verify_hash().is_err());

    metadata.version = 1;
    metadata.generation = 42;

    assert!(metadata.verify_hash().is_err());

    metadata.generation = 5;
    metadata.durable_frame_num = 42;

    assert!(metadata.verify_hash().is_err());

    metadata.durable_frame_num = 100;

    assert!(metadata.verify_hash().is_ok());
}

// Mock connector service that implements tower::Service
#[derive(Clone)]
struct MockConnector {
    tx: tokio::sync::mpsc::Sender<DuplexStream>,
}

impl Service<http::Uri> for MockConnector {
    type Response = Box<dyn Socket>;
    type Error = Box<dyn std::error::Error + Send + Sync>;
    type Future = Pin<
        Box<
            dyn std::future::Future<Output = std::result::Result<Self::Response, Self::Error>>
                + Send,
        >,
    >;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        let (stream, server_stream) = duplex(1024);
        let _ = self.tx.try_send(server_stream);
        let conn = MockConnection { stream };
        Box::pin(std::future::ready(Ok(Box::new(conn) as Box<dyn Socket>)))
    }
}

struct MockServer {
    url: String,
    frame_count: Arc<AtomicU32>,
    connector: ConnectorService,
    return_error: Arc<AtomicBool>,
    request_count: Arc<AtomicU32>,
}

impl MockServer {
    fn start() -> Self {
        let frame_count = Arc::new(AtomicU32::new(0));
        let return_error = Arc::new(AtomicBool::new(false));
        let request_count = Arc::new(AtomicU32::new(0));

        // Create the mock connector with Some(client_stream)
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mock_connector = MockConnector { tx };
        let connector = ConnectorService::new(mock_connector);

        let server = Self {
            url: "http://mock.server".to_string(),
            frame_count: frame_count.clone(),
            connector,
            return_error: return_error.clone(),
            request_count: request_count.clone(),
        };

        // Spawn the server handler
        let frame_count_clone = frame_count.clone();
        let return_error_clone = return_error.clone();
        let request_count_clone = request_count.clone();

        tokio::spawn(async move {
            while let Some(server_stream) = rx.recv().await {
                let frame_count_clone = frame_count_clone.clone();
                let return_error_clone = return_error_clone.clone();
                let request_count_clone = request_count_clone.clone();

                tokio::spawn(async move {
                    use hyper::server::conn::Http;
                    use hyper::service::service_fn;

                    let frame_count_clone = frame_count_clone.clone();
                    let return_error_clone = return_error_clone.clone();
                    let request_count_clone = request_count_clone.clone();
                    let service = service_fn(move |req: http::Request<Body>| {
                        let frame_count = frame_count_clone.clone();
                        let return_error = return_error_clone.clone();
                        let request_count = request_count_clone.clone();
                        async move {
                            request_count.fetch_add(1, Ordering::SeqCst);
                            if return_error.load(Ordering::SeqCst) {
                                return Ok::<_, hyper::Error>(
                                    http::Response::builder()
                                        .status(500)
                                        .body(Body::from("Internal Server Error"))
                                        .unwrap(),
                                );
                            }

                            let current_count = frame_count.fetch_add(1, Ordering::SeqCst);

                            if req.uri().path().contains("/sync/") {
                                // Return the max_frame_no that has been accepted
                                let response = serde_json::json!({
                                    "max_frame_no": current_count
                                });

                                Ok::<_, hyper::Error>(
                                    http::Response::builder()
                                        .status(200)
                                        .body(Body::from(response.to_string()))
                                        .unwrap(),
                                )
                            } else {
                                Ok(http::Response::builder()
                                    .status(404)
                                    .body(Body::empty())
                                    .unwrap())
                            }
                        }
                    });

                    if let Err(e) = Http::new().serve_connection(server_stream, service).await {
                        eprintln!("Error serving connection: {}", e);
                    }
                });
            }
        });

        server
    }

    fn connector(&self) -> ConnectorService {
        self.connector.clone()
    }

    fn url(&self) -> String {
        self.url.clone()
    }

    fn frame_count(&self) -> u32 {
        self.frame_count.load(Ordering::SeqCst)
    }

    fn request_count(&self) -> u32 {
        self.request_count.load(Ordering::SeqCst)
    }
}

// Mock connection that implements the Socket trait
struct MockConnection {
    stream: DuplexStream,
}

impl AsyncRead for MockConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for MockConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

impl hyper::client::connect::Connection for MockConnection {
    fn connected(&self) -> hyper::client::connect::Connected {
        hyper::client::connect::Connected::new()
    }
}
