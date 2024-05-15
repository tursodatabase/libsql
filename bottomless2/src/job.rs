use crate::storage::Storage;
use crate::Result;
use crate::{NamespaceName, StoreSegmentRequest};

/// A storage Job to be performed
pub(crate) struct Job<S: Storage> {
    storage: S,
    namespace: NamespaceName,
    /// Segment to store.
    /// TODO: implement request batching (merge segment and send).
    request: StoreSegmentRequest<S::Config>,
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
    job: Job<S>,
    /// The outcome of the job: the new durable index, or an error.
    result: Result<u64>,
}
