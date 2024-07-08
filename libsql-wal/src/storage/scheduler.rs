use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};

use tokio::sync::mpsc;

use super::job::{IndexedRequest, Job, JobResult};
use super::StoreSegmentRequest;
use libsql_sys::name::NamespaceName;

struct NamespaceRequests<C, F> {
    requests: VecDeque<IndexedRequest<C, F>>,
    /// there's work in flight for this namespace
    in_flight: bool,
}

impl<C, F> Default for NamespaceRequests<C, F> {
    fn default() -> Self {
        Self {
            requests: Default::default(),
            in_flight: false,
        }
    }
}

/// When segments are received, they are enqueued in the `SegmentQueue`, stored by namespace. each
/// request is associated with a request id, so that when a request is popped from the queue, the
/// one with the smallest id is processed first. If there are multiple requests for the same
/// namespace, the segments can be merged together, for faster processing.
/// A new segment can not be enqueued until the previous segment for the same namespace has been
/// processed, because only the most recent segment is checked for durability. This property
/// ensures that all segments are present up to the max durable index.
/// It is generic over C: the storage config type (for config overrides), and T, the segment type
pub(crate) struct Scheduler<C, T> {
    /// notify new durability index for namespace
    durable_notifier: mpsc::Sender<(NamespaceName, u64)>,
    requests: HashMap<NamespaceName, NamespaceRequests<C, T>>,
    queue: priority_queue::PriorityQueue<NamespaceName, Reverse<u64>>,
    next_request_id: u64,
}

impl<C, T> Scheduler<C, T> {
    pub fn new(durable_notifier: mpsc::Sender<(NamespaceName, u64)>) -> Self {
        Self {
            durable_notifier,
            requests: Default::default(),
            queue: Default::default(),
            next_request_id: Default::default(),
        }
    }

    /// Register a new request with the scheduler
    #[tracing::instrument(skip_all)]
    pub fn register(&mut self, request: StoreSegmentRequest<C, T>) {
        // invariant: new segment comes immediately after the latest segment for that namespace. This means:
        // - immediately after the last registered segment, if there is any
        // - immediately after the last durable index
        let id = self.next_request_id;
        self.next_request_id += 1;
        let name = request.namespace.clone();
        let slot = IndexedRequest { request, id };
        let requests = self.requests.entry(name.clone()).or_default();
        requests.requests.push_back(slot);

        tracing::debug!(job_id = id, "job registered");

        // if there is a priority for this namespace already, it must be higher than ours, because
        // it was registered earlier
        if !requests.in_flight && self.queue.get_priority(&name).is_none() {
            tracing::debug!(job_id = id, "job queued");
            self.queue.push(name, Reverse(id));
        }
    }

    /// Get the next job to be executed. Gather as much work as possible from the next namespace to
    /// be scheduled, and returns description of the job to be performed. No other job for this
    /// namespace will be scheduled, until the `JobResult` is reported
    #[tracing::instrument(skip_all)]
    pub fn schedule(&mut self) -> Option<Job<C, T>> {
        let (name, _) = self.queue.pop()?;
        let requests = self
            .requests
            .get_mut(&name)
            .expect("work scheduled but not requests?");
        requests.in_flight = true;
        let request = requests.requests.pop_front().unwrap();

        tracing::debug!(job_id = request.id, "scheduled job");

        let job = Job { request };

        Some(job)
    }

    /// Report the job result to the scheduler. If the job result was a success, the request as
    /// removed from the queue, else, the job is rescheduled
    #[tracing::instrument(skip_all, fields(req_id = result.job.request.id))]
    pub async fn report(&mut self, result: JobResult<C, T>) {
        // re-schedule, or report new max durable frame_no for segment
        let name = result.job.request.request.namespace.clone();
        let requests = self
            .requests
            .get_mut(&name)
            .expect("request slot must exist");

        requests.in_flight = false;

        match result.result {
            Ok(durable_index) => {
                tracing::debug!("job success registered");
                if self
                    .durable_notifier
                    .send((name.clone(), durable_index))
                    .await
                    .is_err()
                {
                    tracing::warn!("durability notifier was closed, proceeding anyway");
                }
            }
            Err(e) => {
                tracing::error!("error processing request, re-enqueuing: {e}");
                // put it back at the front of the queue
                requests.requests.push_front(result.job.request);
            }
        }

        assert!(
            self.queue.get_priority(&name).is_none(),
            "there should be not enqueued jobs at this point"
        );

        if !requests.requests.is_empty() {
            let first_id = requests.requests.front().unwrap().id;
            self.queue.push(name, Reverse(first_id));
        } else {
            self.requests.remove(&name);
        }
    }

    /// Returns true if the scheduler is empty, that is, there are no scheduler requests, and
    /// no not-scheduled request: iow, it's empty.
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    /// Scheduler has work to do. Calling `schedule` after this method must always return some job
    pub fn has_work(&self) -> bool {
        !self.queue.is_empty()
    }
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use crate::storage::Error;
    use libsql_sys::name::NamespaceName;

    use super::*;

    #[tokio::test]
    async fn schedule_simple() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
        let mut scheduler = Scheduler::<(), ()>::new(sender);

        let ns1 = NamespaceName::from("test1");
        let ns2 = NamespaceName::from("test2");

        scheduler.register(StoreSegmentRequest {
            namespace: ns1.clone(),
            segment: (),
            created_at: Utc::now(),
            storage_config_override: None,
        });

        scheduler.register(StoreSegmentRequest {
            namespace: ns2.clone(),
            segment: (),
            created_at: Utc::now(),
            storage_config_override: None,
        });

        scheduler.register(StoreSegmentRequest {
            namespace: ns1.clone(),
            segment: (),
            created_at: Utc::now(),
            storage_config_override: None,
        });

        let job1 = scheduler.schedule().unwrap();
        assert_eq!(job1.request.request.namespace, ns1);
        // assert_eq!(job1.request.request.start_frame_no, 0);

        let job2 = scheduler.schedule().unwrap();
        assert_eq!(job2.request.request.namespace, ns2);

        assert!(scheduler.schedule().is_none());

        scheduler
            .report(JobResult {
                job: job2,
                result: Ok(42),
            })
            .await;

        assert!(scheduler.schedule().is_none());
        assert_eq!(receiver.recv().await.unwrap(), (ns2.clone(), 42));

        scheduler
            .report(JobResult {
                job: job1,
                result: Ok(10),
            })
            .await;
        assert_eq!(receiver.recv().await.unwrap(), (ns1.clone(), 10));

        let job1 = scheduler.schedule().unwrap();
        assert_eq!(job1.request.request.namespace, ns1);
        assert_eq!(job1.request.id, 2);
    }

    #[tokio::test]
    async fn job_error_reschedule() {
        let (sender, _) = tokio::sync::mpsc::channel(10);
        let mut scheduler = Scheduler::<(), ()>::new(sender);

        let ns1 = NamespaceName::from("test1");
        let ns2 = NamespaceName::from("test2");

        scheduler.register(StoreSegmentRequest {
            namespace: ns1.clone(),
            segment: (),
            created_at: Utc::now(),
            storage_config_override: None,
        });

        scheduler.register(StoreSegmentRequest {
            namespace: ns2.clone(),
            segment: (),
            created_at: Utc::now(),
            storage_config_override: None,
        });

        let job1 = scheduler.schedule().unwrap();
        assert_eq!(job1.request.request.namespace, ns1);
        assert_eq!(job1.request.id, 0);

        scheduler
            .report(JobResult {
                job: job1,
                result: Err(Error::Store("oops".into())),
            })
            .await;

        // the same job  is immediately re-scheduled
        let job1 = scheduler.schedule().unwrap();
        assert_eq!(job1.request.request.namespace, ns1);
        assert_eq!(job1.request.id, 0);
    }

    #[tokio::test]
    async fn schedule_while_in_flight() {
        let (sender, _) = tokio::sync::mpsc::channel(10);
        let mut scheduler = Scheduler::<(), ()>::new(sender);

        let ns1 = NamespaceName::from("test1");

        scheduler.register(StoreSegmentRequest {
            namespace: ns1.clone(),
            segment: (),
            created_at: Utc::now(),
            storage_config_override: None,
        });

        let job = scheduler.schedule().unwrap();
        assert_eq!(job.request.request.namespace, ns1);
        assert_eq!(job.request.id, 0);

        scheduler.register(StoreSegmentRequest {
            namespace: ns1.clone(),
            segment: (),
            created_at: Utc::now(),
            storage_config_override: None,
        });

        assert!(scheduler.schedule().is_none());

        scheduler
            .report(JobResult {
                job,
                result: Err(Error::Store("oops".into())),
            })
            .await;

        let job = scheduler.schedule().unwrap();
        assert_eq!(job.request.request.namespace, ns1);
        assert_eq!(job.request.id, 0);

        assert!(scheduler.schedule().is_none());

        scheduler
            .report(JobResult {
                job,
                result: Ok(10),
            })
            .await;

        let job = scheduler.schedule().unwrap();
        assert_eq!(job.request.request.namespace, ns1);
        assert_eq!(job.request.id, 1);
    }
}
