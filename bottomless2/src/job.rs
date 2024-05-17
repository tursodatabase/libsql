use std::sync::Arc;

use crate::storage::Storage;
use crate::Result;
use crate::StoreSegmentRequest;

/// A request, with an id
#[derive(Debug)]
pub(crate) struct IndexedRequest<C> {
    pub(crate) request: StoreSegmentRequest<C>,
    pub(crate) id: u64,
}

/// A storage Job to be performed
#[derive(Debug)]
pub(crate) struct Job<S: Storage> {
    pub(crate) storage: Arc<S>,
    /// Segment to store.
    // TODO: implement request batching (merge segment and send).
    pub(crate) request: IndexedRequest<S::Config>,
}

impl<S: Storage> Job<S> {
    /// Perform the job and return the JobResult. This is not allowed to panic.
    pub(crate) async fn perform(self) -> JobResult<S> {
        let result = self.try_perform().await;
        JobResult { job: self, result }
    }

    async fn try_perform(&self) -> Result<u64> {
        todo!()
    }
}

pub(crate) struct JobResult<S: Storage> {
    /// The job that was performed
    pub(crate) job: Job<S>,
    /// The outcome of the job: the new durable index, or an error.
    pub(crate) result: Result<u64>,
}
