use tokio::sync::mpsc;

use crate::{
    job::{Job, JobResult},
    storage::Storage,
    StoreSegmentRequest,
};

/// When segments are received, they are enqueued in the `SegmentQueue`, stored by namespace. each
/// request is associated with a request id, so that when a request is popped from the queue, the
/// one with the smallest id is processed first. If there are multiple requests for the same
/// namespace, the segments can be merged together, for faster processing.
/// A new segment can not be enqueued until the previous segment for the same namespace has been
/// processed, because only the most recent segment is checked for durability. This property
/// ensures that all segments are present up to the max durable index.
pub(crate) struct Scheduler<S> {
    capacity: usize,
    /// notify new durability index for namespace
    durable_notifier: mpsc::Sender<(libsql_wal::name::NamespaceName, u64)>,
    storage: S,
}

impl<S: Storage> Scheduler<S> {
    /// Register a new request with the scheduler
    pub fn register(&mut self, request: StoreSegmentRequest<S::Config>) {
        // invariant: new segment comes immediately after the latest segment for that namespace. This means:
        // - immediately after the last registered segment, if there is any
        // - immediately after the last durable index
        todo!()
    }

    /// Get the next job to be executed. Gather as much work as possible from the next namespace to
    /// be scheduled, and returns description of the job to be performed. No other job for this
    /// namespace will be scheduled, until the `JobResult` is reported
    pub fn schedule(&mut self) -> Option<Job<S>> {
        todo!()
    }

    /// Report the job result to the scheduler. If the job result was a success, the request as
    /// removed from the queue, else, the job is rescheduled
    pub fn report(&mut self, _job: JobResult<S>) {
        // re-schedule, or report new max durable frame_no for segment
        todo!()
    }

    /// Returns true if the scheduler is empty, that is, there are no scheduler requests, and
    /// no not-scheduled request: iow, it's empty.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// Scheduler has work to do. Calling `schedule` after this method must always return some job
    pub fn has_work(&self) -> bool {
        todo!()
    }
}
