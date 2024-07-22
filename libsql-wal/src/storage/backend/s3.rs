//! S3 implementation of storage backend

use std::fmt;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;

use aws_config::SdkConfig;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::CreateBucketConfiguration;
use aws_sdk_s3::Client;
use bytes::{Bytes, BytesMut};
use http_body::{Frame as HttpFrame, SizeHint};
use libsql_sys::name::NamespaceName;
use roaring::RoaringBitmap;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_util::sync::ReusableBoxFuture;
use zerocopy::{AsBytes, FromZeroes};

use super::{Backend, SegmentMeta};
use crate::io::buf::ZeroCopyBuf;
use crate::io::compat::copy_to_file;
use crate::io::{FileExt, Io, StdIO};
use crate::segment::compacted::CompactedSegmentDataHeader;
use crate::segment::Frame;
use crate::storage::{Error, RestoreOptions, Result};
use crate::LIBSQL_MAGIC;

pub struct S3Backend<IO> {
    client: Client,
    default_config: Arc<S3Config>,
    io: IO,
}

impl S3Backend<StdIO> {
    pub async fn from_sdk_config(
        aws_config: SdkConfig,
        bucket: String,
        cluster_id: String,
    ) -> Result<S3Backend<StdIO>> {
        Self::from_sdk_config_with_io(aws_config, bucket, cluster_id, StdIO(())).await
    }
}


/// Header for segment index stored into s3
#[repr(C)]
#[derive(Copy, Clone, Debug, AsBytes, FromZeroes, FromBytes)]
struct SegmentIndexHeader {
    magic: lu64,
    version: lu16,
    len: lu64,
    checksum: lu32,
}

impl<IO: Io> S3Backend<IO> {
    #[doc(hidden)]
    pub async fn from_sdk_config_with_io(
        aws_config: SdkConfig,
        bucket: String,
        cluster_id: String,
        io: IO,
    ) -> Result<Self> {
        let client = Client::new(&aws_config);
        let config = S3Config {
            bucket,
            cluster_id,
            aws_config,
        };

        let bucket_config = CreateBucketConfiguration::builder()
            // TODO: get location from config
            .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::UsWest2)
            .build();

        // TODO: we may need to create the bucket for config overrides. Maybe try lazy bucket
        // creation? or assume that the bucket exists?
        let create_bucket_ret = client
            .create_bucket()
            .create_bucket_configuration(bucket_config)
            .bucket(&config.bucket)
            .send()
            .await;

        match create_bucket_ret {
            Ok(_) => (),
            Err(e) => {
                if let Some(service_error) = e.as_service_error() {
                    match service_error {
                        CreateBucketError::BucketAlreadyExists(_)
                        | CreateBucketError::BucketAlreadyOwnedByYou(_) => {
                            tracing::debug!("bucket already exist");
                        }
                        _ => return Err(Error::unhandled(e, "failed to create bucket")),
                    }
                } else {
                    return Err(Error::unhandled(e, "failed to create bucket"));
                }
            }
        }

        Ok(Self {
            client,
            default_config: config.into(),
            io,
        })
    }

    async fn fetch_segment_data_reader(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        segment_key: &SegmentKey,
    ) -> Result<impl AsyncRead> {
        let key = s3_segment_data_key(folder_key, segment_key);
        let stream = self.s3_get(config, key).await?;
        Ok(stream.into_async_read())
    }

    async fn fetch_segment_data(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        segment_key: &SegmentKey,
        file: &impl FileExt,
    ) -> Result<()> {
        let reader = self
            .fetch_segment_data_reader(config, folder_key, segment_key)
            .await?;
        copy_to_file(reader, file).await?;
        Ok(())
    }

    async fn s3_get(&self, config: &S3Config, key: String) -> Result<ByteStream> {
        Ok(self
            .client
            .get_object()
            .bucket(&config.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Error::unhandled(e, "error sending s3 GET request"))?
            .body)
    }

    async fn s3_put(&self, config: &S3Config, key: String, body: ByteStream) -> Result<()> {
        self.client
            .put_object()
            .bucket(&config.bucket)
            .body(body)
            .key(key)
            .send()
            .await
            .map_err(|e| Error::unhandled(e, "error sending s3 PUT request"))?;
        Ok(())
    }

    async fn fetch_segment_index(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        segment_key: &SegmentKey,
    ) -> Result<fst::Map<Vec<u8>>> {
        let s3_index_key = s3_segment_index_key(folder_key, segment_key);
        let stream = self.s3_get(config, s3_index_key).await?;
        // TODO: parse header, check if too large to fit memory
        let bytes = stream
            .collect()
            .await
            .map_err(|e| Error::unhandled(e, ""))?
            .to_vec();
        let index = fst::Map::new(data).map_err(|_| Error::InvalidIndex("invalid index bytes"))?;
        Ok(index)
    }

    /// Find the most recent, and biggest segment that may contain `frame_no`
    async fn find_segment(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        frame_no: u64,
    ) -> Result<Option<SegmentKey>> {
        let lookup_key = s3_segment_index_lookup_key(&folder_key, frame_no);

        let objects = self
            .client
            .list_objects_v2()
            .bucket(&config.bucket)
            .start_after(lookup_key)
            .send()
            .await
            .map_err(|e| Error::unhandled(e, "failed to list bucket"))?;

        let Some(contents) = objects.contents().first() else {
            return Ok(None);
        };
        let key = contents.key().expect("misssing key?");
        let key_path: &Path = key.as_ref();
        let segment_key: SegmentKey = key_path
            .file_stem()
            .expect("invalid key")
            .to_str()
            .expect("invalid key")
            .parse()
            .expect("invalid key");

        Ok(Some(segment_key))
    }

    // This method could probably be optimized a lot by using indexes and only downloading useful
    // segments
    async fn restore_latest(
        &self,
        config: &S3Config,
        namespace: &NamespaceName,
        dest: impl FileExt,
    ) -> Result<()> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace,
        };
        let Some(latest_key) = self.find_segment(config, &folder_key, u64::MAX).await? else {
            tracing::info!("nothing to restore for {namespace}");
            return Ok(());
        };

        let reader = self
            .fetch_segment_data_reader(config, &folder_key, &latest_key)
            .await?;
        let mut reader = BufReader::new(reader);
        let mut header: CompactedSegmentDataHeader = CompactedSegmentDataHeader::new_zeroed();
        reader.read_exact(header.as_bytes_mut()).await?;
        let db_size = header.size_after.get();
        let mut seen = RoaringBitmap::new();
        let mut frame: Frame = Frame::new_zeroed();
        loop {
            for _ in 0..header.frame_count.get() {
                reader.read_exact(frame.as_bytes_mut()).await?;
                let page_no = frame.header().page_no();
                if !seen.contains(page_no) {
                    seen.insert(page_no);
                    let offset = (page_no as u64 - 1) * 4096;
                    let buf = ZeroCopyBuf::new_init(frame).map_slice(|f| f.get_ref().data());
                    let (buf, ret) = dest.write_all_at_async(buf, offset).await;
                    ret?;
                    frame = buf.into_inner().into_inner();
                }
            }

            // db is restored
            if seen.len() == db_size as u64 {
                break;
            }

            let next_frame_no = header.start_frame_no.get() - 1;
            let Some(key) = self
                .find_segment(config, &folder_key, next_frame_no)
                .await?
            else {
                todo!("there should be a segment!");
            };
            let r = self
                .fetch_segment_data_reader(config, &folder_key, &key)
                .await?;
            reader = BufReader::new(r);
            reader.read_exact(header.as_bytes_mut()).await?;
        }

        Ok(())
    }

    async fn fetch_segment_from_key(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        segment_key: &SegmentKey,
        dest_file: &impl FileExt,
    ) -> Result<fst::Map<Vec<u8>>> {
        let (_, index) = tokio::try_join!(
            self.fetch_segment_data(config, &folder_key, &segment_key, dest_file),
            self.fetch_segment_index(config, &folder_key, &segment_key),
        )?;

        Ok(index)
    }
}

pub struct S3Config {
    bucket: String,
    aws_config: SdkConfig,
    cluster_id: String,
}

/// SegmentKey is used to index segment data, where keys a lexicographically ordered.
/// The scheme is `{u64::MAX - start_frame_no}-{u64::MAX - end_frame_no}`. With that naming convention, when looking for
/// the segment containing 'n', we can perform a prefix search with "{u64::MAX - n}". The first
/// element of the range will be the biggest segment that contains n if it exists.
/// Beware that if no segments contain n, either the smallest segment not containing n, if n < argmin
/// {start_frame_no}, or the largest segment if n > argmax {end_frame_no} will be returned.
/// e.g:
/// ```ignore
/// let mut map = BTreeMap::new();
///
/// let meta = SegmentMeta { start_frame_no: 1, end_frame_no: 100 };
/// map.insert(SegmentKey(&meta).to_string(), meta);
///
/// let meta = SegmentMeta { start_frame_no: 101, end_frame_no: 500 };
/// map.insert(SegmentKey(&meta).to_string(), meta);
///
/// let meta = SegmentMeta { start_frame_no: 101, end_frame_no: 1000 };
/// map.insert(SegmentKey(&meta).to_string(), meta);
///
/// map.range(format!("{:019}", u64::MAX - 50)..).next();
/// map.range(format!("{:019}", u64::MAX - 0)..).next();
/// map.range(format!("{:019}", u64::MAX - 1)..).next();
/// map.range(format!("{:019}", u64::MAX - 100)..).next();
/// map.range(format!("{:019}", u64::MAX - 101)..).next();
/// map.range(format!("{:019}", u64::MAX - 5000)..).next();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct SegmentKey {
    start_frame_no: u64,
    end_frame_no: u64,
}

impl SegmentKey {
    fn includes(&self, frame_no: u64) -> bool {
        (self.start_frame_no..self.end_frame_no).contains(&frame_no)
    }
}

impl From<&SegmentMeta> for SegmentKey {
    fn from(value: &SegmentMeta) -> Self {
        Self {
            start_frame_no: value.start_frame_no,
            end_frame_no: value.end_frame_no,
        }
    }
}

impl FromStr for SegmentKey {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (rev_start_fno, s) = s.split_at(20);
        let start_frame_no = u64::MAX - rev_start_fno.parse::<u64>().map_err(|_| ())?;
        let (_, rev_end_fno) = s.split_at(1);
        let end_frame_no = u64::MAX - rev_end_fno.parse::<u64>().map_err(|_| ())?;
        Ok(Self {
            start_frame_no,
            end_frame_no,
        })
    }
}

impl fmt::Display for SegmentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:019}-{:019}",
            u64::MAX - self.start_frame_no,
            u64::MAX - self.end_frame_no,
        )
    }
}

struct FolderKey<'a> {
    cluster_id: &'a str,
    namespace: &'a NamespaceName,
}

impl fmt::Display for FolderKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ns-{}:{}-v2", self.cluster_id, self.namespace)
    }
}

fn s3_segment_data_key(folder_key: &FolderKey, segment_key: &SegmentKey) -> String {
    format!("{folder_key}/segments/{segment_key}")
}

fn s3_segment_index_key(folder_key: &FolderKey, segment_key: &SegmentKey) -> String {
    format!("{folder_key}/indexes/{segment_key}")
}

fn s3_segment_index_lookup_key(folder_key: &FolderKey, frame_no: u64) -> String {
    format!("{folder_key}/indexes/{:019}", u64::MAX - frame_no)
}

fn s3_folder_key(cluster_id: &str, ns: &NamespaceName) -> String {
    format!("ns-{}:{}-v2", cluster_id, ns)
}

impl<IO> Backend for S3Backend<IO>
where
    IO: Io,
{
    type Config = S3Config;

    async fn store(
        &self,
        config: &Self::Config,
        meta: SegmentMeta,
        segment_data: impl FileExt,
        segment_index: Vec<u8>,
    ) -> Result<()> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace: &meta.namespace,
        };
        let segment_key = SegmentKey::from(&meta);
        let s3_data_key = s3_segment_data_key(&folder_key, &segment_key);

        let body = FileStreamBody::new(segment_data).into_byte_stream();

        self.s3_put(config, s3_data_key, body).await?;

        let s3_index_key = s3_segment_index_key(&folder_key, &segment_key);

        let checksum = crc32fast::hash(&segment_index);
        let header = SegmentIndexHeader {
            version: 1.into(),
            len: (segment_index.len() as u64).into(),
            checksum: checksum.into(),
            magic: LIBSQL_MAGIC.into(),
        };

        let mut bytes = BytesMut::with_capacity(size_of::<SegmentIndexHeader>() + segment_index.len());
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(&segment_index);

        let body = ByteStream::from(bytes.freeze());

        self.s3_put(config, s3_index_key, body).await?;

        Ok(())
    }

    async fn fetch_segment(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        frame_no: u64,
        dest_path: &Path,
    ) -> Result<fst::Map<Vec<u8>>> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace: &namespace,
        };

        let Some(segment_key) = self.find_segment(config, &folder_key, frame_no).await? else {
            return Err(Error::FrameNotFound(frame_no));
        };

        if segment_key.includes(frame_no) {
            // TODO: make open async
            let file = self.io.open(false, false, true, dest_path)?;
            self.fetch_segment_from_key(config, &folder_key, &segment_key, &file)
                .await
        } else {
            return Err(Error::FrameNotFound(frame_no));
        }
    }

    async fn meta(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
    ) -> Result<super::DbMeta> {
        // request a key bigger than any other to get the last segment
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace: &namespace,
        };

        let max_segment_key = self.find_segment(config, &folder_key, u64::MAX).await?;

        Ok(super::DbMeta {
            max_frame_no: max_segment_key.map(|s| s.end_frame_no).unwrap_or(0),
        })
    }

    fn default_config(&self) -> Arc<Self::Config> {
        self.default_config.clone()
    }

    async fn restore(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        restore_options: RestoreOptions,
        dest: impl FileExt,
    ) -> Result<()> {
        match restore_options {
            RestoreOptions::Latest => self.restore_latest(config, &namespace, dest).await,
            RestoreOptions::Timestamp(_) => todo!(),
        }
    }
}

#[derive(Clone, Copy)]
enum StreamState {
    Init,
    WaitingChunk,
    Done,
}

struct FileStreamBody<F> {
    inner: Arc<F>,
    current_offset: u64,
    chunk_size: usize,
    state: StreamState,
    fut: ReusableBoxFuture<'static, std::io::Result<Bytes>>,
}

impl<F> FileStreamBody<F> {
    fn new(inner: F) -> Self {
        Self::new_inner(inner.into())
    }

    fn new_inner(inner: Arc<F>) -> Self {
        Self {
            inner,
            current_offset: 0,
            chunk_size: 4096,
            state: StreamState::Init,
            fut: ReusableBoxFuture::new(std::future::pending()),
        }
    }

    fn into_byte_stream(self) -> ByteStream
    where
        F: FileExt,
    {
        let body = SdkBody::retryable(move || {
            let s = Self::new_inner(self.inner.clone());
            SdkBody::from_body_1_x(s)
        });

        ByteStream::new(body)
    }
}

impl<F> http_body::Body for FileStreamBody<F>
where
    F: FileExt,
{
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<HttpFrame<Self::Data>, Self::Error>>> {
        loop {
            match self.state {
                StreamState::Init => {
                    let f = self.inner.clone();
                    let chunk_size = self.chunk_size;
                    let current_offset = self.current_offset;
                    let fut = async move {
                        let buf = BytesMut::with_capacity(chunk_size);
                        let (buf, ret) = f.read_at_async(buf, current_offset).await;
                        ret.map(|_| buf.freeze())
                    };
                    self.fut.set(fut);
                    self.state = StreamState::WaitingChunk;
                }
                StreamState::WaitingChunk => match self.fut.poll(cx) {
                    Poll::Ready(Ok(buf)) => {
                        // TODO: we perform one too many read,
                        if buf.is_empty() {
                            self.state = StreamState::Done;
                            return Poll::Ready(None);
                        } else {
                            self.state = StreamState::Init;
                            self.current_offset += buf.len() as u64;
                            return Poll::Ready(Some(Ok(HttpFrame::data(buf))));
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = StreamState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                StreamState::Done => return Poll::Ready(None),
            }
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self.inner.len() {
            Ok(n) => SizeHint::with_exact(n),
            Err(_) => SizeHint::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use aws_config::{BehaviorVersion, Region, SdkConfig};
    use aws_sdk_s3::config::SharedCredentialsProvider;
    use chrono::Utc;
    use fst::MapBuilder;
    use s3s::auth::SimpleAuth;
    use s3s::service::{S3ServiceBuilder, SharedS3Service};
    use tempfile::NamedTempFile;
    use uuid::Uuid;

    use crate::io::StdIO;

    use super::*;

    #[track_caller]
    fn setup(dir: impl AsRef<Path>) -> (SdkConfig, SharedS3Service) {
        std::fs::create_dir_all(&dir).unwrap();
        let s3_impl = s3s_fs::FileSystem::new(dir).unwrap();

        let cred = aws_credential_types::Credentials::for_tests();

        let mut s3 = S3ServiceBuilder::new(s3_impl);
        s3.set_auth(SimpleAuth::from_single(
            cred.access_key_id(),
            cred.secret_access_key(),
        ));
        s3.set_base_domain("localhost:8014");
        let s3 = s3.build().into_shared();

        let client = s3s_aws::Client::from(s3.clone());

        let config = aws_config::SdkConfig::builder()
            .http_client(client)
            .behavior_version(BehaviorVersion::latest())
            .region(Region::from_static("us-west-2"))
            .credentials_provider(SharedCredentialsProvider::new(cred))
            .endpoint_url("http://localhost:8014")
            .build();

        (config, s3)
    }

    #[tokio::test]
    async fn s3_basic() {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = tempfile::tempdir().unwrap();
        let (aws_config, _s3) = setup(&dir);

        let s3_config = S3Config {
            bucket: "testbucket".into(),
            aws_config: aws_config.clone(),
            cluster_id: "123456789".into(),
        };

        let storage = S3Backend::from_sdk_config_with_io(
            aws_config,
            "testbucket".into(),
            "123456789".into(),
            StdIO(()),
        )
        .await
        .unwrap();

        let f_path = dir.path().join("fs-segments");
        std::fs::write(&f_path, vec![123; 8092]).unwrap();

        let ns = NamespaceName::from_string("foobarbaz".into());

        let mut builder = MapBuilder::memory();
        builder.insert(42u32.to_be_bytes(), 42).unwrap();
        let index = builder.into_inner().unwrap();
        storage
            .store(
                &s3_config,
                SegmentMeta {
                    namespace: ns.clone(),
                    segment_id: Uuid::new_v4(),
                    start_frame_no: 0u64.into(),
                    end_frame_no: 64u64.into(),
                    created_at: Utc::now(),
                },
                std::fs::File::open(&f_path).unwrap(),
                index,
            )
            .await
            .unwrap();

        let db_meta = storage.meta(&s3_config, &ns).await.unwrap();
        assert_eq!(db_meta.max_frame_no, 64);

        let mut builder = MapBuilder::memory();
        builder.insert(44u32.to_be_bytes(), 44).unwrap();
        let index = builder.into_inner().unwrap();
        storage
            .store(
                &s3_config,
                SegmentMeta {
                    namespace: ns.clone(),
                    segment_id: Uuid::new_v4(),
                    start_frame_no: 64u64.into(),
                    end_frame_no: 128u64.into(),
                    created_at: Utc::now(),
                },
                std::fs::File::open(&f_path).unwrap(),
                index,
            )
            .await
            .unwrap();

        let db_meta = storage.meta(&s3_config, &ns).await.unwrap();
        assert_eq!(db_meta.max_frame_no, 128);

        let tmp = NamedTempFile::new().unwrap();

        let index = storage
            .fetch_segment(&s3_config, &ns, 1, tmp.path())
            .await
            .unwrap();
        assert_eq!(index.get(42u32.to_be_bytes()).unwrap(), 42);

        let index = storage
            .fetch_segment(&s3_config, &ns, 63, tmp.path())
            .await
            .unwrap();
        assert_eq!(index.get(42u32.to_be_bytes()).unwrap(), 42);

        let index = storage
            .fetch_segment(&s3_config, &ns, 64, tmp.path())
            .await
            .unwrap();
        assert_eq!(index.get(44u32.to_be_bytes()).unwrap(), 44);

        let index = storage
            .fetch_segment(&s3_config, &ns, 65, tmp.path())
            .await
            .unwrap();
        assert_eq!(index.get(44u32.to_be_bytes()).unwrap(), 44);
    }
}
