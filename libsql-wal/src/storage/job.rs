use std::ops::Deref;

use super::backend::Backend;
use super::backend::SegmentMeta;
use super::Result;
use super::StoreSegmentRequest;
use crate::io::Io;
use crate::segment::Segment;

/// A request, with an id
#[derive(Debug)]
pub(crate) struct IndexedRequest<T, C> {
    pub(crate) request: StoreSegmentRequest<T, C>,
    pub(crate) id: u64,
}

impl<T, C> Deref for IndexedRequest<T, C> {
    type Target = StoreSegmentRequest<T, C>;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

/// A storage Job to be performed
#[derive(Debug)]
pub(crate) struct Job<T, C> {
    /// Segment to store.
    // TODO: implement request batching (merge segment and send).
    pub(crate) request: IndexedRequest<T, C>,
}

impl<Seg, C> Job<Seg, C>
where
    Seg: Segment,
    C: Clone,
{
    /// Perform the job and return the JobResult. This is not allowed to panic.
    pub(crate) async fn perform<B, IO>(self, backend: B, io: IO) -> JobResult<Seg, C>
    where
        B: Backend<Config = C>,
        IO: Io,
    {
        let result = self.try_perform(backend, io).await;
        JobResult { job: self, result }
    }

    async fn try_perform<B, IO>(&self, backend: B, io: IO) -> Result<u64>
    where
        B: Backend<Config = C>,
        IO: Io,
    {
        let segment = &self.request.segment;
        let segment_id = io.uuid();
        let tmp = io.tempfile()?;

        tracing::debug!(
            namespace = self.request.namespace.as_str(),
            "sending segment to durable storage"
        );

        let new_index = segment
            .compact(&tmp, segment_id)
            .await
            .map_err(super::Error::Compact)?;

        let meta = SegmentMeta {
            segment_id,
            namespace: self.request.namespace.clone(),
            start_frame_no: segment.start_frame_no(),
            end_frame_no: segment.last_committed(),
            segment_timestamp: segment.timestamp(),
        };
        let config = self
            .request
            .storage_config_override
            .clone()
            .unwrap_or_else(|| backend.default_config());

        backend.store(&config, meta, tmp, new_index).await?;

        tracing::info!(
            namespace = self.request.namespace.as_str(),
            start_frame_no = segment.start_frame_no(),
            end_frame_no = segment.last_committed(),
            "stored segment"
        );

        Ok(segment.last_committed())
    }
}

#[derive(Debug)]
pub(crate) struct JobResult<S, C> {
    /// The job that was performed
    pub(crate) job: Job<S, C>,
    /// The outcome of the job: the new durable index, or an error.
    pub(crate) result: Result<u64>,
}

#[cfg(test)]
mod test {
    use std::future::ready;
    use std::str::FromStr;
    use std::sync::Arc;

    use chrono::prelude::DateTime;
    use chrono::Utc;
    use uuid::Uuid;

    use crate::io::file::FileExt;
    use crate::io::StdIO;
    use crate::segment::compacted::CompactedSegmentDataHeader;
    use crate::storage::backend::FindSegmentReq;
    use crate::storage::{RestoreOptions, SegmentKey};
    use libsql_sys::name::NamespaceName;

    use super::*;

    #[tokio::test]
    async fn perform_job() {
        #[derive(Debug)]
        struct TestSegment;

        impl Segment for TestSegment {
            async fn compact(
                &self,
                out_file: &impl FileExt,
                id: uuid::Uuid,
            ) -> crate::error::Result<Vec<u8>> {
                out_file.write_all_at(id.to_string().as_bytes(), 0).unwrap();
                Ok(b"test_index".into())
            }

            fn start_frame_no(&self) -> u64 {
                1
            }

            fn last_committed(&self) -> u64 {
                10
            }

            fn index(&self) -> &fst::Map<Arc<[u8]>> {
                todo!()
            }

            fn read_page(
                &self,
                _page_no: u32,
                _max_frame_no: u64,
                _buf: &mut [u8],
            ) -> std::io::Result<bool> {
                todo!()
            }

            fn is_checkpointable(&self) -> bool {
                todo!()
            }

            fn size_after(&self) -> u32 {
                todo!()
            }

            async fn read_frame_offset_async<B>(
                &self,
                _offset: u32,
                _buf: B,
            ) -> (B, crate::error::Result<()>)
            where
                B: crate::io::buf::IoBufMut + Send + 'static,
            {
                todo!()
            }

            fn destroy<IO: Io>(&self, _io: &IO) -> impl std::future::Future<Output = ()> {
                async move { todo!() }
            }

            fn is_storable(&self) -> bool {
                true
            }

            fn timestamp(&self) -> DateTime<Utc> {
                Utc::now()
            }
        }

        struct TestBackend;

        impl Backend for TestBackend {
            type Config = ();

            async fn store(
                &self,
                _config: &Self::Config,
                meta: SegmentMeta,
                segment_data: impl FileExt,
                segment_index: Vec<u8>,
            ) -> Result<()> {
                // verify that the stored segment is the same as the one we compacted
                assert_eq!(segment_index, b"test_index");
                let mut buf = vec![0; Uuid::new_v4().to_string().len()];
                segment_data.read_exact_at(&mut buf, 0).unwrap();
                let id = Uuid::from_str(std::str::from_utf8(&buf).unwrap()).unwrap();
                assert_eq!(meta.segment_id, id);

                Ok(())
            }

            async fn meta(
                &self,
                _config: &Self::Config,
                _namespace: &NamespaceName,
            ) -> Result<crate::storage::backend::DbMeta> {
                todo!()
            }

            fn default_config(&self) -> Self::Config {
                ()
            }

            async fn restore(
                &self,
                _config: &Self::Config,
                _namespace: &NamespaceName,
                _restore_options: RestoreOptions,
                _dest: impl FileExt,
            ) -> Result<()> {
                todo!()
            }

            async fn find_segment(
                &self,
                _config: &Self::Config,
                _namespace: &NamespaceName,
                _frame_no: FindSegmentReq,
            ) -> Result<SegmentKey> {
                todo!()
            }

            async fn fetch_segment_index(
                &self,
                _config: &Self::Config,
                _namespace: &NamespaceName,
                _key: &SegmentKey,
            ) -> Result<fst::Map<Arc<[u8]>>> {
                todo!()
            }

            async fn fetch_segment_data_to_file(
                &self,
                _config: &Self::Config,
                _namespace: &NamespaceName,
                _key: &SegmentKey,
                _file: &impl FileExt,
            ) -> Result<CompactedSegmentDataHeader> {
                todo!()
            }

            async fn fetch_segment_data(
                self: Arc<Self>,
                _config: Self::Config,
                _namespace: NamespaceName,
                _key: SegmentKey,
            ) -> Result<impl FileExt> {
                Ok(std::fs::File::open("").unwrap())
            }

            fn list_segments<'a>(
                &'a self,
                _config: Self::Config,
                _namespace: &'a NamespaceName,
                _until: u64,
            ) -> impl tokio_stream::Stream<Item = Result<crate::storage::SegmentInfo>> + 'a
            {
                tokio_stream::iter(std::iter::from_fn(|| todo!()))
            }
        }

        let job = Job {
            request: IndexedRequest {
                request: StoreSegmentRequest {
                    namespace: "test".into(),
                    segment: TestSegment,
                    created_at: Utc::now(),
                    storage_config_override: None,
                    on_store_callback: Box::new(|_| Box::pin(ready(()))),
                },
                id: 0,
            },
        };

        let result = job.perform(TestBackend, StdIO(())).await;
        assert_eq!(result.result.unwrap(), 10);
    }
}
