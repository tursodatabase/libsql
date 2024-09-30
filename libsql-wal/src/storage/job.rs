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
        let tmp = io.tempfile()?;

        tracing::debug!(
            namespace = self.request.namespace.as_str(),
            "sending segment to durable storage"
        );

        let new_index = segment.compact(&tmp).await.map_err(super::Error::Compact)?;

        let meta = SegmentMeta {
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
