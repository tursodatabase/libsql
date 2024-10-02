//! S3 implementation of storage backend

use std::fmt::{self, Formatter};
use std::mem::size_of;
use std::path::Path;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Poll;

use aws_config::SdkConfig;
use aws_sdk_s3::operation::create_bucket::CreateBucketError;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::{ByteStream, SdkBody};
use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, CreateBucketConfiguration, Object,
};
use aws_sdk_s3::Client;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::future::poll_fn;
use futures::{StreamExt, TryFutureExt as _, TryStreamExt};
use http_body::{Body, Frame as HttpFrame, SizeHint};
use libsql_sys::name::NamespaceName;
use pin_project_lite::pin_project;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, BufReader};
use tokio_stream::Stream;
use tokio_util::codec::Decoder;
use tokio_util::sync::ReusableBoxFuture;
use zerocopy::byteorder::little_endian::{U16 as lu16, U32 as lu32, U64 as lu64};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use super::{Backend, FindSegmentReq, SegmentMeta};
use crate::io::compat::copy_to_file;
use crate::io::{FileExt, Io, StdIO};
use crate::segment::compacted::{CompactedFrame, CompactedFrameHeader, CompactedSegmentHeader};
use crate::storage::{Error, Result, SegmentInfo, SegmentKey};
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
        let config = aws_sdk_s3::Config::new(&aws_config)
            .to_builder()
            .force_path_style(true)
            .build();

        let region = config.region().expect("region must be configured").clone();

        let client = Client::from_conf(config);
        let config = S3Config {
            bucket,
            cluster_id,
            aws_config,
        };

        let bucket_config = CreateBucketConfiguration::builder()
            .location_constraint(
                aws_sdk_s3::types::BucketLocationConstraint::from_str(&region.to_string()).unwrap(),
            )
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
        Ok(stream.body.into_async_read())
    }

    async fn fetch_segment_data_inner(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        segment_key: &SegmentKey,
        file: &impl FileExt,
    ) -> Result<CompactedSegmentHeader> {
        let reader = self
            .fetch_segment_data_reader(config, folder_key, segment_key)
            .await?;
        let mut reader = tokio::io::BufReader::with_capacity(8196, reader);
        while reader.fill_buf().await?.len() < size_of::<CompactedSegmentHeader>() {}
        let header = CompactedSegmentHeader::read_from_prefix(reader.buffer()).unwrap();

        copy_to_file(reader, file).await?;

        Ok(header)
    }

    /// returns a stream over raw CompactedFrame bytes
    async fn fetch_segment_data_stream_inner(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        segment_key: &SegmentKey,
    ) -> Result<(
        CompactedSegmentHeader,
        impl Stream<Item = Result<(CompactedFrameHeader, Bytes)>>,
    )> {
        let reader = self
            .fetch_segment_data_reader(config, folder_key, segment_key)
            .await?;

        let mut reader = BufReader::new(reader);
        let mut header = CompactedSegmentHeader::new_zeroed();
        reader.read_exact(header.as_bytes_mut()).await?;

        struct FrameDecoder {
            verify: bool,
            finished: bool,
            current_crc: u32,
        }

        impl FrameDecoder {
            fn new(verify: bool, header: &CompactedSegmentHeader) -> Self {
                let current_crc = if verify {
                    crc32fast::hash(header.as_bytes())
                } else {
                    0
                };

                Self {
                    finished: false,
                    verify,
                    current_crc,
                }
            }
        }

        impl Decoder for FrameDecoder {
            type Item = (CompactedFrameHeader, Bytes);
            type Error = Error;

            fn decode(
                &mut self,
                src: &mut BytesMut,
            ) -> std::result::Result<Option<Self::Item>, Self::Error> {
                if self.finished {
                    return Ok(None);
                }

                if src.len() >= size_of::<CompactedFrame>() {
                    let mut data = src.split_to(size_of::<CompactedFrame>());
                    let header_bytes = data.split_to(size_of::<CompactedFrameHeader>());
                    let header = CompactedFrameHeader::read_from(&header_bytes).unwrap();

                    if header.is_last() {
                        self.finished = true;
                    }

                    if self.verify {
                        let checksum = header.compute_checksum(self.current_crc, &data);
                        if checksum != header.checksum() {
                            todo!("invalid frame checksum")
                        }
                        self.current_crc = checksum;
                    }

                    return Ok(Some((header, data.freeze())));
                } else {
                    if src.capacity() < size_of::<CompactedFrame>() {
                        src.reserve(size_of::<CompactedFrame>() - src.capacity())
                    }
                    Ok(None)
                }
            }

            fn decode_eof(
                &mut self,
                buf: &mut BytesMut,
            ) -> std::result::Result<Option<Self::Item>, Self::Error> {
                match self.decode(buf)? {
                    Some(frame) => Ok(Some(frame)),
                    None => {
                        if buf.is_empty() {
                            Ok(None)
                        } else {
                            Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "bytes remaining on stream",
                            )
                            .into())
                        }
                    }
                }
            }
        }

        let stream = tokio_util::codec::FramedRead::new(reader, FrameDecoder::new(true, &header));

        Ok((header, stream))
    }

    async fn s3_get(&self, config: &S3Config, key: impl ToString) -> Result<GetObjectOutput> {
        Ok(self
            .client
            .get_object()
            .bucket(&config.bucket)
            .key(key.to_string())
            .send()
            .await
            .map_err(|e| Error::unhandled(e, "error sending s3 GET request"))?)
    }

    async fn s3_put(&self, config: &S3Config, key: impl ToString, body: ByteStream) -> Result<()> {
        self.client
            .put_object()
            .bucket(&config.bucket)
            .body(body)
            .key(key.to_string())
            .send()
            .await
            .map_err(|e| Error::unhandled(e, "error sending s3 PUT request"))?;
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(key))]
    async fn s3_put_multipart<B>(
        &self,
        config: &S3Config,
        key: impl ToString,
        data: B,
    ) -> Result<()>
    where
        B: Body<Data = Bytes, Error = crate::storage::Error>,
    {
        const MAX_CHUNK_SIZE: u64 = 50 * 1024 * 1024; // 50MB
        let (s_chunks, r_chunks) = tokio::sync::mpsc::channel(8);
        let key = key.to_string();

        let upload = self
            .client
            .create_multipart_upload()
            .bucket(&config.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| Error::unhandled(e, "creating multipart upload"))?;
        let upload_id = upload.upload_id();

        let make_chunk_fut = async {
            let mut current_chunk_file = self.io.tempfile()?;
            let mut current_chunk_len = 0;
            tokio::pin!(data);
            loop {
                let Some(frame) = poll_fn(|cx| data.as_mut().poll_frame(cx)).await else {
                    break;
                };
                let frame = frame?;
                assert!(frame.is_data());
                let data = frame.into_data().unwrap();
                let offset = current_chunk_len;
                current_chunk_len += data.len() as u64;
                let (_, ret) = current_chunk_file.write_all_at_async(data, offset).await;
                ret?;
                if current_chunk_len >= MAX_CHUNK_SIZE {
                    let new_chunk_file = self.io.tempfile()?;
                    current_chunk_len = 0;
                    let old_chunk_file = std::mem::replace(&mut current_chunk_file, new_chunk_file);
                    if s_chunks.send(old_chunk_file).await.is_err() {
                        break;
                    }
                }
            }

            // make sure we move the sender in the future so the chunk sender eventually exits.
            drop(s_chunks);

            Ok(())
        };

        let send_chunks_fut = async {
            let builder = tokio_stream::wrappers::ReceiverStream::new(r_chunks)
                .enumerate()
                .map(|(i, chunk)| {
                    let i = i;
                    self.client
                        .upload_part()
                        .bucket(&config.bucket)
                        .key(&key)
                        .part_number(i as i32 + 1) // part number must be between 1-10000
                        .set_upload_id(upload_id.map(ToString::to_string))
                        .body(FileStreamBody::new(chunk).into_byte_stream())
                        .send()
                        .map_err(|e| Error::unhandled(e, format!("sending chunk")))
                        .map_ok(move |resp| {
                            CompletedPart::builder()
                                .set_e_tag(resp.e_tag)
                                .set_part_number(Some(i as i32 + 1))
                                .build()
                        })
                })
                .buffered(8)
                .try_fold(CompletedMultipartUpload::builder(), |builder, completed| {
                    std::future::ready(Ok(builder.parts(completed)))
                })
                .await?;

            Ok(builder.build())
        };

        let ret = tokio::try_join!(send_chunks_fut, make_chunk_fut);

        match ret {
            Ok((parts, _)) => {
                self.client
                    .complete_multipart_upload()
                    .bucket(&config.bucket)
                    .set_upload_id(upload_id.map(ToString::to_string))
                    .multipart_upload(parts)
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| Error::unhandled(e, format!("completing multipart upload")))?;

                Ok(())
            }
            Err(e) => {
                self.client
                    .abort_multipart_upload()
                    .bucket(&config.bucket)
                    .set_upload_id(upload_id.map(ToString::to_string))
                    .key(&key)
                    .send()
                    .await
                    .map_err(|e| Error::unhandled(e, format!("aborting multipart upload")))?;
                Err(e)
            }
        }
    }

    async fn fetch_segment_index_inner(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        segment_key: &SegmentKey,
    ) -> Result<fst::Map<Arc<[u8]>>> {
        let s3_index_key = s3_segment_index_key(folder_key, segment_key);
        let mut stream = self
            .s3_get(config, s3_index_key)
            .await?
            .body
            .into_async_read();
        let mut header: SegmentIndexHeader = SegmentIndexHeader::new_zeroed();
        stream.read_exact(header.as_bytes_mut()).await?;
        if header.magic.get() != LIBSQL_MAGIC && header.version.get() != 1 {
            return Err(Error::InvalidIndex("index header magic or version invalid"));
        }
        let mut data = Vec::with_capacity(header.len.get() as _);
        while stream.read_buf(&mut data).await? != 0 {}
        let checksum = crc32fast::hash(&data);
        if checksum != header.checksum.get() {
            return Err(Error::InvalidIndex("invalid index data checksum"));
        }
        let index =
            fst::Map::new(data.into()).map_err(|_| Error::InvalidIndex("invalid index bytes"))?;
        Ok(index)
    }

    /// Find the most recent, and biggest segment that may contain `frame_no`
    async fn find_segment_by_frame_no(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        frame_no: u64,
    ) -> Result<Option<SegmentKey>> {
        let lookup_key_prefix = s3_segment_index_lookup_key_prefix(&folder_key);
        let lookup_key = s3_segment_index_ends_before_lookup_key(&folder_key, frame_no);

        let objects = self
            .client
            .list_objects_v2()
            .bucket(&config.bucket)
            .prefix(lookup_key_prefix.to_string())
            .start_after(lookup_key.to_string())
            .send()
            .await
            .map_err(|e| Error::unhandled(e, "failed to list bucket"))?;

        let Some(contents) = objects.contents().first() else {
            return Ok(None);
        };
        let key = contents.key().expect("misssing key?");
        let key_path: &Path = key.as_ref();

        let key = SegmentKey::validate_from_path(key_path, &folder_key.namespace);

        Ok(key)
    }

    /// We are kinda bruteforcing out way into finding a segment that fits the bill, this can very
    /// probably be optimized
    #[tracing::instrument(skip(self, config, folder_key))]
    async fn find_segment_by_timestamp(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<SegmentKey>> {
        let object_to_key = |o: &Object| {
            let key_path = o.key().unwrap();
            SegmentKey::validate_from_path(key_path.as_ref(), &folder_key.namespace)
        };

        let lookup_key_prefix = s3_segment_index_lookup_key_prefix(&folder_key);

        let mut continuation_token = None;
        loop {
            let objects = self
                .client
                .list_objects_v2()
                .set_continuation_token(continuation_token.take())
                .bucket(&config.bucket)
                .prefix(lookup_key_prefix.to_string())
                .send()
                .await
                .map_err(|e| Error::unhandled(e, "failed to list bucket"))?;

            // there is noting to restore
            if objects.contents().is_empty() {
                return Ok(None);
            }

            let ts = timestamp.timestamp_millis() as u64;
            let search_result =
                objects
                    .contents()
                    .binary_search_by_key(&std::cmp::Reverse(ts), |o| {
                        let key = object_to_key(o).unwrap();
                        std::cmp::Reverse(key.timestamp)
                    });

            match search_result {
                Ok(i) => {
                    let key = object_to_key(&objects.contents()[i]).unwrap();
                    tracing::trace!("found perfect match for `{timestamp}`: {key}");
                    return Ok(Some(key));
                }
                Err(i) if i == 0 => {
                    // this is caught by the first iteration of the loop, anything that's more
                    // recent than the most recent should be interpret as most recent
                    let key = object_to_key(&objects.contents()[i]).unwrap();
                    tracing::trace!("best match for `{timestamp}` is most recent segment: {key}");
                    return Ok(Some(key));
                }
                Err(i) if i == objects.contents().len() => {
                    // there are two scenarios. Either there are more pages with the request, and
                    // we fetch older entries, or there aren't. If there are older segment, search
                    // in those, otherwise, just take the oldest segment and return that
                    if objects.continuation_token().is_some() {
                        // nothing to do; fetch next page
                    } else {
                        let key = object_to_key(&objects.contents().last().unwrap()).unwrap();
                        return Ok(Some(key));
                    }
                }
                // This is the index where timestamp would be inserted, we look left and right of that
                // key and pick the closest one.
                Err(i) => {
                    // i - 1 is well defined since we already catch the case where i == 0 above
                    let left_key = object_to_key(&objects.contents()[i - 1]).unwrap();
                    let right_key = object_to_key(&objects.contents()[i]).unwrap();
                    let time_to_left = left_key.timestamp().signed_duration_since(timestamp).abs();
                    let time_to_right =
                        right_key.timestamp().signed_duration_since(timestamp).abs();

                    if time_to_left < time_to_right {
                        return Ok(Some(left_key));
                    } else {
                        return Ok(Some(right_key));
                    }
                }
            }

            match objects.continuation_token {
                Some(token) => {
                    continuation_token = Some(token);
                }
                None => {
                    unreachable!("the absence of continuation token should be dealt with earlier");
                }
            }
        }
    }

    async fn fetch_segment_from_key(
        &self,
        config: &S3Config,
        folder_key: &FolderKey<'_>,
        segment_key: &SegmentKey,
        dest_file: &impl FileExt,
    ) -> Result<fst::Map<Arc<[u8]>>> {
        let (_, index) = tokio::try_join!(
            self.fetch_segment_data_inner(config, &folder_key, &segment_key, dest_file),
            self.fetch_segment_index_inner(config, &folder_key, &segment_key),
        )?;

        Ok(index)
    }

    fn list_segments_inner<'a>(
        &'a self,
        config: Arc<S3Config>,
        namespace: &'a NamespaceName,
        _until: u64,
    ) -> impl Stream<Item = Result<SegmentInfo>> + 'a {
        async_stream::try_stream! {
            let folder_key = FolderKey { cluster_id: &config.cluster_id, namespace };
            let lookup_key_prefix = s3_segment_index_lookup_key_prefix(&folder_key);

            let mut continuation_token = None;
            loop {
                let objects = self
                    .client
                    .list_objects_v2()
                    .bucket(&config.bucket)
                    .prefix(lookup_key_prefix.to_string())
                    .set_continuation_token(continuation_token.take())
                    .send()
                    .await
                    .map_err(|e| Error::unhandled(e, "failed to list bucket"))?;

                for entry in objects.contents() {
                    let key = entry.key().expect("misssing key?");
                    let key_path: &Path = key.as_ref();
                    let Some(key) = SegmentKey::validate_from_path(key_path, &folder_key.namespace) else { continue };

                    let infos = SegmentInfo {
                        key,
                        size: entry.size().unwrap_or(0) as usize,
                    };

                    yield infos;
                }

                if objects.is_truncated().unwrap_or(false) {
                    assert!(objects.next_continuation_token.is_some());
                    continuation_token = objects.next_continuation_token;
                } else {
                    break
                }
            }
        }
    }

    async fn store_segment_data_inner<B>(
        &self,
        config: &S3Config,
        namespace: &NamespaceName,
        body: B,
        segment_key: &SegmentKey,
    ) -> Result<()>
    where
        B: Body<Data = Bytes, Error = crate::storage::Error>,
    {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace,
        };
        let s3_data_key = s3_segment_data_key(&folder_key, segment_key);

        self.s3_put_multipart(config, s3_data_key, body).await
    }

    async fn store_segment_index_inner(
        &self,
        config: &S3Config,
        namespace: &NamespaceName,
        index: Vec<u8>,
        segment_key: &SegmentKey,
    ) -> Result<()> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace,
        };
        let s3_index_key = s3_segment_index_key(&folder_key, segment_key);

        let checksum = crc32fast::hash(&index);
        let header = SegmentIndexHeader {
            version: 1.into(),
            len: (index.len() as u64).into(),
            checksum: checksum.into(),
            magic: LIBSQL_MAGIC.into(),
        };

        let mut bytes = BytesMut::with_capacity(size_of::<SegmentIndexHeader>() + index.len());
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(&index);

        let body = ByteStream::from(bytes.freeze());

        self.s3_put(config, s3_index_key, body).await?;

        Ok(())
    }
}

pub struct S3Config {
    bucket: String,
    aws_config: SdkConfig,
    cluster_id: String,
}

struct FolderKey<'a> {
    cluster_id: &'a str,
    namespace: &'a NamespaceName,
}

impl fmt::Display for FolderKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "v2/clusters/{}/namespaces/{}",
            self.cluster_id, self.namespace
        )
    }
}

pub struct SegmentDataKey<'a>(&'a FolderKey<'a>, &'a SegmentKey);

impl fmt::Display for SegmentDataKey<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/segments/{}", self.0, self.1)
    }
}

fn s3_segment_data_key<'a>(
    folder_key: &'a FolderKey,
    segment_key: &'a SegmentKey,
) -> SegmentDataKey<'a> {
    SegmentDataKey(folder_key, segment_key)
}

pub struct SegmentIndexKey<'a>(&'a FolderKey<'a>, &'a SegmentKey);

impl fmt::Display for SegmentIndexKey<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/indexes/{}", self.0, self.1)
    }
}

fn s3_segment_index_key<'a>(
    folder_key: &'a FolderKey,
    segment_key: &'a SegmentKey,
) -> SegmentIndexKey<'a> {
    SegmentIndexKey(folder_key, segment_key)
}

pub struct SegmentIndexLookupKeyPrefix<'a>(&'a FolderKey<'a>);

impl fmt::Display for SegmentIndexLookupKeyPrefix<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/indexes/", self.0)
    }
}

fn s3_segment_index_lookup_key_prefix<'a>(
    folder_key: &'a FolderKey,
) -> SegmentIndexLookupKeyPrefix<'a> {
    SegmentIndexLookupKeyPrefix(folder_key)
}

pub struct SegmentIndexLookupKey<'a>(&'a FolderKey<'a>, u64);

impl fmt::Display for SegmentIndexLookupKey<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}/indexes/{:020}", self.0, u64::MAX - self.1)
    }
}

/// return the biggest segment whose end frame number is less than frame_no
fn s3_segment_index_ends_before_lookup_key<'a>(
    folder_key: &'a FolderKey,
    frame_no: u64,
) -> SegmentIndexLookupKey<'a> {
    SegmentIndexLookupKey(folder_key, frame_no)
}

impl<IO> Backend for S3Backend<IO>
where
    IO: Io,
{
    type Config = Arc<S3Config>;

    async fn store(
        &self,
        config: &Self::Config,
        meta: SegmentMeta,
        segment_data: impl FileExt,
        segment_index: Vec<u8>,
    ) -> Result<()> {
        let segment_key = SegmentKey::from(&meta);
        let body = FileStreamBody::new(segment_data);
        self.store_segment_data_inner(config, &meta.namespace, body, &segment_key)
            .await?;
        self.store_segment_index(config, &meta.namespace, &segment_key, segment_index)
            .await?;
        Ok(())
    }

    async fn meta(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
    ) -> Result<super::DbMeta> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace: &namespace,
        };

        // request a key bigger than any other to get the last segment
        let max_segment_key = self
            .find_segment_by_frame_no(config, &folder_key, u64::MAX)
            .await?;

        Ok(super::DbMeta {
            max_frame_no: max_segment_key.map(|s| s.end_frame_no).unwrap_or(0),
        })
    }

    fn default_config(&self) -> Self::Config {
        self.default_config.clone()
    }

    async fn find_segment(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        req: FindSegmentReq,
    ) -> Result<SegmentKey> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace: &namespace,
        };

        match req {
            FindSegmentReq::EndFrameNoLessThan(frame_no) => self
                .find_segment_by_frame_no(config, &folder_key, frame_no)
                .await?
                .ok_or_else(|| Error::SegmentNotFound(req)),
            FindSegmentReq::Timestamp(ts) => self
                .find_segment_by_timestamp(config, &folder_key, ts)
                .await?
                .ok_or_else(|| Error::SegmentNotFound(req)),
        }
    }

    async fn fetch_segment_index(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        key: &SegmentKey,
    ) -> Result<fst::Map<Arc<[u8]>>> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace: &namespace,
        };
        self.fetch_segment_index_inner(config, &folder_key, key)
            .await
    }

    async fn fetch_segment_data_to_file(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        key: &SegmentKey,
        file: &impl FileExt,
    ) -> Result<CompactedSegmentHeader> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace: &namespace,
        };
        let header = self
            .fetch_segment_data_inner(config, &folder_key, key, file)
            .await?;
        Ok(header)
    }

    async fn fetch_segment_data(
        self: Arc<Self>,
        config: Self::Config,
        namespace: NamespaceName,
        key: SegmentKey,
    ) -> Result<impl FileExt> {
        let file = self.io.tempfile()?;
        self.fetch_segment_data_to_file(&config, &namespace, &key, &file)
            .await?;
        Ok(file)
    }

    fn list_segments<'a>(
        &'a self,
        config: Self::Config,
        namespace: &'a NamespaceName,
        until: u64,
    ) -> impl Stream<Item = Result<SegmentInfo>> + 'a {
        self.list_segments_inner(config, namespace, until)
    }

    async fn fetch_segment_data_stream(
        &self,
        config: Self::Config,
        namespace: &NamespaceName,
        key: SegmentKey,
    ) -> Result<(
        CompactedSegmentHeader,
        impl Stream<Item = Result<(CompactedFrameHeader, Bytes)>>,
    )> {
        let folder_key = FolderKey {
            cluster_id: &config.cluster_id,
            namespace: &namespace,
        };
        self.fetch_segment_data_stream_inner(&config, &folder_key, &key)
            .await
    }

    async fn store_segment_data(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        segment_key: &SegmentKey,
        segment_data: impl Stream<Item = Result<Bytes>> + Send + 'static,
    ) -> Result<()> {
        let byte_stream = StreamBody::new(segment_data);
        self.store_segment_data_inner(config, namespace, byte_stream, &segment_key)
            .await?;

        Ok(())
    }

    async fn store_segment_index(
        &self,
        config: &Self::Config,
        namespace: &NamespaceName,
        segment_key: &SegmentKey,
        index: Vec<u8>,
    ) -> Result<()> {
        self.store_segment_index_inner(config, namespace, index, segment_key)
            .await?;

        Ok(())
    }
}

pin_project! {
    struct StreamBody<S> {
        #[pin]
        s: S,
    }
}

impl<S> StreamBody<S> {
    fn new(s: S) -> Self {
        Self { s }
    }
}

impl<S> http_body::Body for StreamBody<S>
where
    S: Stream<Item = Result<Bytes>>,
{
    type Data = bytes::Bytes;
    type Error = crate::storage::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<std::result::Result<HttpFrame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match std::task::ready!(this.s.poll_next(cx)) {
            Some(Ok(data)) => Poll::Ready(Some(Ok(HttpFrame::data(data)))),
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
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
    type Error = crate::storage::Error;

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
                        return Poll::Ready(Some(Err(e.into())));
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

        let s3_config = Arc::new(S3Config {
            bucket: "testbucket".into(),
            aws_config: aws_config.clone(),
            cluster_id: "123456789".into(),
        });

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
                    start_frame_no: 1u64.into(),
                    end_frame_no: 64u64.into(),
                    segment_timestamp: Utc::now(),
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
                    start_frame_no: 64u64.into(),
                    end_frame_no: 128u64.into(),
                    segment_timestamp: Utc::now(),
                },
                std::fs::File::open(&f_path).unwrap(),
                index,
            )
            .await
            .unwrap();

        let db_meta = storage.meta(&s3_config, &ns).await.unwrap();
        assert_eq!(db_meta.max_frame_no, 128);

        let key = storage
            .find_segment(&s3_config, &ns, FindSegmentReq::EndFrameNoLessThan(65))
            .await
            .unwrap();
        assert_eq!(key.start_frame_no, 1);
        assert_eq!(key.end_frame_no, 64);

        let index = storage
            .fetch_segment_index(&s3_config, &ns, &key)
            .await
            .unwrap();
        assert_eq!(index.get(42u32.to_be_bytes()).unwrap(), 42);
    }
}
