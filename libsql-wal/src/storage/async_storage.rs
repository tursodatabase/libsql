//! `AsyncStorage` is a `Storage` implementation that defer storage to a background thread. The
//! durable frame_no is notified asynchronously.

use std::sync::Arc;

use chrono::Utc;
use libsql_sys::name::NamespaceName;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use crate::io::Io;
use crate::segment::Segment;

use super::backend::Backend;
use super::scheduler::Scheduler;
use super::{Storage, StoreSegmentRequest};

/// Background loop task state.
///
/// The background loop task is not allowed to exit, unless it was notified for shutdown.
///
/// On shutdown, attempts to empty the queue, and flush the receiver. When the last handle of the
/// receiver is dropped, and the queue is empty, exit.
pub struct AsyncStorageLoop<B: Backend, IO: Io, S> {
    receiver: mpsc::UnboundedReceiver<StoreSegmentRequest<B::Config, S>>,
    scheduler: Scheduler<B::Config, S>,
    backend: Arc<B>,
    io: Arc<IO>,
    max_in_flight: usize,
    force_shutdown: oneshot::Receiver<()>,
}

impl<B, FS, S> AsyncStorageLoop<B, FS, S>
where
    FS: Io,
    B: Backend + 'static,
    S: Segment,
{
    /// Schedules durability jobs. This loop is not allowed to fail, or lose jobs.
    /// A job is prepared by calling `Scheduler::prepare(..)`. The job is spawned, and it returns a
    /// `JobResult`, which is then returned to the scheduler by calling `Scheduler::report(..)`.
    /// When a request is received, it is immediately scheduled by calling `Scheduler::register`
    /// with it.
    ///
    /// The loop is only allowed to shutdown if the receiver is closed, and the scheduler is empty,
    /// or if `force_shutdown` is called, in which case everything is dropped in place.
    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        let mut shutting_down = false;
        let mut in_flight_futs = JoinSet::new();
        // run the loop until shutdown.
        loop {
            if shutting_down && self.scheduler.is_empty() {
                break;
            }

            // schedule as much work as possible
            while self.scheduler.has_work() && in_flight_futs.len() < self.max_in_flight {
                let job = self
                    .scheduler
                    .schedule()
                    .expect("scheduler has work, but didn't return a job");
                in_flight_futs.spawn(job.perform(self.backend.clone(), self.io.clone()));
            }

            tokio::select! {
                biased;
                Some(join_result) = in_flight_futs.join_next(), if !in_flight_futs.is_empty() => {
                    match join_result {
                        Ok(job_result) => {
                            // if shutting down, log progess:
                            if shutting_down {
                                tracing::info!("processed job, {} jobs remaining", in_flight_futs.len());
                            }
                            self.scheduler.report(job_result).await;
                        }
                        Err(e) => {
                            // job panicked. report and exit process. The program is crippled, from
                            // now on, so we just exit, and hope to restart on a fresh state.
                            tracing::error!("fatal error: bottomless job panicked: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                msg = self.receiver.recv(), if !shutting_down => {
                    match msg {
                        Some(req) => {
                            self.scheduler.register(req);
                        }
                        None => {
                            shutting_down = true;
                        }
                    }
                }
                shutdown = &mut self.force_shutdown => {
                    if shutdown.is_ok() {
                        break
                    } else {
                        // force_shutdown sender was dropped without sending a message (likely a
                        // bug). Log and default to graceful shutdown.
                        // tracing::error!("bottomless force shutdown handle dropped without notifying; shutting down gracefully");
                    }
                }
            }
        }
    }
}

pub struct BottomlessConfig<C> {
    /// The maximum number of store jobs that can be processed conccurently
    pub max_jobs_conccurency: usize,
    /// The maximum number of jobs that can be enqueued before throttling
    pub max_enqueued_jobs: usize,
    pub config: C,
}

pub struct AsyncStorage<C, S> {
    /// send request to the main loop
    job_sender: mpsc::UnboundedSender<StoreSegmentRequest<C, S>>,
    /// receiver for the current max durable index
    durable_notifier: mpsc::Receiver<(NamespaceName, u64)>,
    force_shutdown: oneshot::Sender<()>,
}

impl<C, S> Storage for AsyncStorage<C, S>
where
    C: Send + Sync + 'static,
    S: Segment,
{
    type Segment = S;
    fn store(&self, namespace: &NamespaceName, segment: Self::Segment) {
        let req = StoreSegmentRequest {
            namespace: namespace.clone(),
            segment,
            created_at: Utc::now(),
            storage_config_override: None,
        };

        self.job_sender
            .send(req)
            .expect("bottomless loop was closed before the handle was dropped");
    }

    fn durable_frame_no(&self, _namespace: &NamespaceName) -> u64 {
        todo!()
    }
}

pub struct AsyncStorageInitConfig<S> {
    storage: S,
    max_in_flight_jobs: usize,
}

impl<C, S> AsyncStorage<C, S> {
    pub async fn new<B, IO>(
        config: AsyncStorageInitConfig<B>,
        io: Arc<IO>,
    ) -> (AsyncStorage<C, S>, AsyncStorageLoop<B, IO, S>)
    where
        B: Backend<Config = C>,
        IO: Io,
        S: Segment,
        C: Send + Sync + 'static,
    {
        let (job_snd, job_rcv) = tokio::sync::mpsc::unbounded_channel();
        let (durable_notifier_snd, durable_notifier_rcv) = tokio::sync::mpsc::channel(16);
        let (shutdown_snd, shutdown_rcv) = tokio::sync::oneshot::channel();
        let scheduler = Scheduler::new(durable_notifier_snd);
        let storage_loop = AsyncStorageLoop {
            receiver: job_rcv,
            scheduler,
            backend: Arc::new(config.storage),
            io,
            max_in_flight: config.max_in_flight_jobs,
            force_shutdown: shutdown_rcv,
        };

        let this = Self {
            job_sender: job_snd,
            durable_notifier: durable_notifier_rcv,
            force_shutdown: shutdown_snd,
        };

        (this, storage_loop)
    }

    /// send shutdown signal to bottomless.
    /// return a function that can be called to force shutdown, if necessary
    pub fn send_shutdown(self) -> impl FnOnce() {
        let force_shutdown = {
            // we drop the sender, the loop will finish processing scheduled job and exit
            // gracefully.
            let Self { force_shutdown, .. } = self;
            force_shutdown
        };

        || {
            let _ = force_shutdown.send(());
        }
    }
}
