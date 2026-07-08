//! Simple S3-compatible mock server for testing
//!
//! This is a minimal S3 implementation that supports the operations needed
//! for bottomless backup/restore tests. Uses hyper 1.0 for compatibility.

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::{Method, Request, Response, StatusCode};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct S3MockServer {
    #[allow(dead_code)]
    root: PathBuf,
    buckets: Arc<Mutex<HashMap<String, Bucket>>>,
}

#[derive(Clone, Default)]
struct Bucket {
    objects: HashMap<String, Bytes>,
}

impl S3MockServer {
    pub async fn new() -> std::io::Result<Self> {
        let tmp = std::env::temp_dir().join(format!("s3-mock-{}", Uuid::new_v4().as_simple()));
        fs::create_dir_all(&tmp).await?;
        Ok(Self {
            root: tmp,
            buckets: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn handle(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let query = req.uri().query().unwrap_or("").to_string();

        // Parse path: /bucket-name/key
        let parts: Vec<&str> = path.trim_start_matches('/').splitn(2, '/').collect();
        let bucket_name = parts.first().copied().unwrap_or("").to_string();
        let object_key = parts.get(1).copied().unwrap_or("").to_string();

        // Collect body
        let body_bytes = match req.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                tracing::error!("Error reading body: {}", e);
                return Ok(Self::error_response("Error reading body"));
            }
        };

        let response = match (method.clone(), bucket_name, object_key, query) {
            // Create bucket (PUT /bucket-name/)
            (Method::PUT, bucket, ref key, _) if key.is_empty() => {
                self.create_bucket(&bucket).await
            }
            // Put object (PUT /bucket-name/key)
            (Method::PUT, bucket, key, _) if !key.is_empty() => {
                self.put_object(&bucket, &key, body_bytes).await
            }
            // Get object (GET /bucket-name/key)
            (Method::GET, bucket, key, _) if !key.is_empty() => {
                self.get_object(&bucket, &key).await
            }
            // List objects (GET /bucket-name/ or GET /)
            (Method::GET, ref bucket, ref key, ref q)
                if key.is_empty() && (bucket.is_empty() || q.contains("list")) =>
            {
                if bucket.is_empty() {
                    self.list_buckets().await
                } else {
                    self.list_objects(bucket).await
                }
            }
            // List objects without query
            (Method::GET, bucket, key, _) if key.is_empty() => self.list_objects(&bucket).await,
            // Delete object (DELETE /bucket-name/key)
            (Method::DELETE, bucket, key, _) if !key.is_empty() => {
                self.delete_object(&bucket, &key).await
            }
            // Delete multiple objects (POST /?delete)
            (Method::POST, _, _, q) if q.contains("delete") => {
                self.delete_objects(&body_bytes).await
            }
            // Head bucket (HEAD /bucket-name/)
            (Method::HEAD, bucket, key, _) if key.is_empty() => self.head_bucket(&bucket).await,
            _ => {
                tracing::warn!("Unhandled request: {} {}", method, path);
                Self::not_found()
            }
        };

        Ok(response)
    }

    async fn create_bucket(&self, name: &str) -> Response<Full<Bytes>> {
        let mut buckets = self.buckets.lock().await;
        buckets.entry(name.to_string()).or_default();

        Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::new()))
            .unwrap()
    }

    async fn put_object(&self, bucket: &str, key: &str, data: Bytes) -> Response<Full<Bytes>> {
        let mut buckets = self.buckets.lock().await;
        let bucket = buckets.entry(bucket.to_string()).or_default();
        bucket.objects.insert(key.to_string(), data);

        Response::builder()
            .status(StatusCode::OK)
            .body(Full::new(Bytes::new()))
            .unwrap()
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Response<Full<Bytes>> {
        let buckets = self.buckets.lock().await;
        match buckets.get(bucket).and_then(|b| b.objects.get(key)) {
            Some(data) => Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(data.clone()))
                .unwrap(),
            None => Self::not_found(),
        }
    }

    async fn list_objects(&self, bucket: &str) -> Response<Full<Bytes>> {
        let buckets = self.buckets.lock().await;
        let bucket = match buckets.get(bucket) {
            Some(b) => b,
            None => return Self::not_found(),
        };

        // Simple XML response
        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
        for (key, data) in &bucket.objects {
            xml.push_str(&format!(
                "<Contents><Key>{}</Key><Size>{}</Size></Contents>",
                key,
                data.len()
            ));
        }
        xml.push_str("</ListBucketResult>");

        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/xml")
            .body(Full::new(Bytes::from(xml)))
            .unwrap()
    }

    async fn list_buckets(&self) -> Response<Full<Bytes>> {
        let buckets = self.buckets.lock().await;

        let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Buckets>");
        for name in buckets.keys() {
            xml.push_str(&format!("<Bucket><Name>{}</Name></Bucket>", name));
        }
        xml.push_str("</Buckets></ListAllMyBucketsResult>");

        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/xml")
            .body(Full::new(Bytes::from(xml)))
            .unwrap()
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Response<Full<Bytes>> {
        let mut buckets = self.buckets.lock().await;
        if let Some(bucket) = buckets.get_mut(bucket) {
            bucket.objects.remove(key);
        }

        Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .unwrap()
    }

    async fn delete_objects(&self, _body: &Bytes) -> Response<Full<Bytes>> {
        // Simplified - just return success
        let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult></DeleteResult>";
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/xml")
            .body(Full::new(Bytes::from(xml)))
            .unwrap()
    }

    async fn head_bucket(&self, bucket: &str) -> Response<Full<Bytes>> {
        let buckets = self.buckets.lock().await;
        if buckets.contains_key(bucket) {
            Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::new()))
                .unwrap()
        } else {
            Self::not_found()
        }
    }

    fn not_found() -> Response<Full<Bytes>> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap()
    }

    fn error_response(msg: &str) -> Response<Full<Bytes>> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Full::new(Bytes::from(msg.to_string())))
            .unwrap()
    }
}

/// Start the S3 mock server on the given address
pub async fn start_mock_server(addr: std::net::SocketAddr) -> std::io::Result<S3MockServer> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let server = S3MockServer::new().await?;
    let server_clone = server.clone();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    tracing::error!("Accept error: {}", e);
                    continue;
                }
            };

            let server = server_clone.clone();
            tokio::spawn(async move {
                let service = hyper::service::service_fn(move |req| {
                    let server = server.clone();
                    async move { server.handle(req).await }
                });

                let io = hyper_util::rt::tokio::TokioIo::new(stream);
                let builder = hyper_util::server::conn::auto::Builder::new(
                    hyper_util::rt::TokioExecutor::new(),
                );

                if let Err(e) = builder.serve_connection(io, service).await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    });

    Ok(server)
}
