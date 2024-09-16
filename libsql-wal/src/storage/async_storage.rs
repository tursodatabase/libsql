//! `AsyncStorage` is a `Storage` implementation that defer storage to a background thread. The
//! durable frame_no is notified asynchronously.

use std::sync::Arc;

use chrono::Utc;
use libsql_sys::name::NamespaceName;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_stream::Stream;

use crate::io::{FileExt, Io, StdIO};
use crate::segment::compacted::CompactedSegment;
use crate::segment::Segment;

use super::backend::{Backend, FindSegmentReq};
use super::scheduler::Scheduler;
use super::{OnStoreCallback, RestoreOptions, Storage, StoreSegmentRequest};

/// Background loop task state.
///
/// The background loop task is not allowed to exit, unless it was notified for shutdown.
///
/// On shutdown, attempts to empty the queue, and flush the receiver. When the last handle of the
/// receiver is dropped, and the queue is empty, exit.
pub struct AsyncStorageLoop<B: Backend, IO: Io, S> {
    receiver: mpsc::UnboundedReceiver<StorageLoopMessage<S, B::Config>>,
    scheduler: Scheduler<S, B::Config>,
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
        let mut notify_shutdown = None;
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
                        Some(StorageLoopMessage::StoreReq(req)) => {
                            self.scheduler.register(req);
                        }
                        Some(StorageLoopMessage::DurableFrameNoReq { namespace, ret, config_override }) => {
                            self.fetch_durable_frame_no_async(namespace, ret, config_override);
                        }
                        Some(StorageLoopMessage::Shutdown(ret)) => {
                            notify_shutdown.replace(ret);
                            shutting_down = true;
                            tracing::info!("Storage shutting down");
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

        tracing::info!("Storage shutdown");
        if let Some(notify) = notify_shutdown {
            let _ = notify.send(());
        }
    }

    fn fetch_durable_frame_no_async(
        &self,
        namespace: NamespaceName,
        ret: oneshot::Sender<super::Result<u64>>,
        config_override: Option<B::Config>,
    ) {
        let backend = self.backend.clone();
        let config = match config_override {
            Some(config) => config,
            None => backend.default_config(),
        };

        tokio::spawn(async move {
            let res = backend
                .meta(&config, &namespace)
                .await
                .map(|meta| meta.max_frame_no);
            let _ = ret.send(res);
        });
    }
}

pub struct BottomlessConfig<C> {
    /// The maximum number of store jobs that can be processed conccurently
    pub max_jobs_conccurency: usize,
    /// The maximum number of jobs that can be enqueued before throttling
    pub max_enqueued_jobs: usize,
    pub config: C,
}

enum StorageLoopMessage<S, C> {
    StoreReq(StoreSegmentRequest<S, C>),
    DurableFrameNoReq {
        namespace: NamespaceName,
        config_override: Option<C>,
        ret: oneshot::Sender<super::Result<u64>>,
    },
    Shutdown(oneshot::Sender<()>),
}

pub struct AsyncStorage<B: Backend, S> {
    /// send request to the main loop
    job_sender: mpsc::UnboundedSender<StorageLoopMessage<S, B::Config>>,
    force_shutdown: oneshot::Sender<()>,
    backend: Arc<B>,
}

impl<B, S> Storage for AsyncStorage<B, S>
where
    B: Backend,
    S: Segment,
{
    type Segment = S;
    type Config = B::Config;

    async fn shutdown(&self) {
        let (snd, rcv) = oneshot::channel();
        let _ = self.job_sender.send(StorageLoopMessage::Shutdown(snd));
        let _ = rcv.await;
    }

    fn store(
        &self,
        namespace: &NamespaceName,
        segment: Self::Segment,
        config_override: Option<Self::Config>,
        on_store_callback: OnStoreCallback,
    ) {
        let req = StoreSegmentRequest {
            namespace: namespace.clone(),
            segment,
            created_at: Utc::now(),
            storage_config_override: config_override,
            on_store_callback,
        };

        self.job_sender
            .send(StorageLoopMessage::StoreReq(req))
            .expect("bottomless loop was closed before the handle was dropped");
    }

    async fn durable_frame_no(
        &self,
        namespace: &NamespaceName,
        config_override: Option<Self::Config>,
    ) -> super::Result<u64> {
        let config = config_override.unwrap_or_else(|| self.backend.default_config());
        let meta = self.backend.meta(&config, namespace).await?;
        Ok(meta.max_frame_no)
    }

    async fn restore(
        &self,
        file: impl crate::io::FileExt,
        namespace: &NamespaceName,
        restore_options: RestoreOptions,
        config_override: Option<Self::Config>,
    ) -> super::Result<()> {
        let config = config_override.unwrap_or_else(|| self.backend.default_config());
        self.backend
            .restore(&config, &namespace, restore_options, file)
            .await
    }

    async fn find_segment(
        &self,
        namespace: &NamespaceName,
        req: FindSegmentReq,
        config_override: Option<Self::Config>,
    ) -> super::Result<super::SegmentKey> {
        let config = config_override.unwrap_or_else(|| self.backend.default_config());
        let key = self.backend.find_segment(&config, namespace, req).await?;
        Ok(key)
    }

    async fn fetch_segment_index(
        &self,
        namespace: &NamespaceName,
        key: &super::SegmentKey,
        config_override: Option<Self::Config>,
    ) -> super::Result<fst::Map<Arc<[u8]>>> {
        let config = config_override.unwrap_or_else(|| self.backend.default_config());
        let index = self
            .backend
            .fetch_segment_index(&config, namespace, key)
            .await?;
        Ok(index)
    }

    async fn fetch_segment_data(
        &self,
        namespace: &NamespaceName,
        key: &super::SegmentKey,
        config_override: Option<Self::Config>,
    ) -> super::Result<CompactedSegment<impl FileExt>> {
        // TODO: make async
        let config = config_override.unwrap_or_else(|| self.backend.default_config());
        let backend = self.backend.clone();
        let file = backend
            .fetch_segment_data(config, namespace.clone(), *key)
            .await?;
        let segment = CompactedSegment::open(file).await?;
        Ok(segment)
    }

    fn list_segments<'a>(
        &'a self,
        namespace: &'a NamespaceName,
        until: u64,
        config_override: Option<Self::Config>,
    ) -> impl Stream<Item = super::Result<super::SegmentInfo>> + 'a {
        let config = config_override.unwrap_or_else(|| self.backend.default_config());
        self.backend.list_segments(config, namespace, until)
    }
}

pub struct AsyncStorageInitConfig<B> {
    pub backend: Arc<B>,
    pub max_in_flight_jobs: usize,
}

impl<B: Backend, S> AsyncStorage<B, S> {
    pub async fn new(
        config: AsyncStorageInitConfig<B>,
    ) -> (AsyncStorage<B, S>, AsyncStorageLoop<B, StdIO, S>)
    where
        B: Backend,
        S: Segment,
    {
        Self::new_with_io(config, Arc::new(StdIO(()))).await
    }

    pub async fn new_with_io<IO>(
        config: AsyncStorageInitConfig<B>,
        io: Arc<IO>,
    ) -> (AsyncStorage<B, S>, AsyncStorageLoop<B, IO, S>)
    where
        B: Backend,
        IO: Io,
        S: Segment,
    {
        let (job_snd, job_rcv) = tokio::sync::mpsc::unbounded_channel();
        let (shutdown_snd, shutdown_rcv) = tokio::sync::oneshot::channel();
        let scheduler = Scheduler::new();
        let storage_loop = AsyncStorageLoop {
            receiver: job_rcv,
            scheduler,
            backend: config.backend.clone(),
            io,
            max_in_flight: config.max_in_flight_jobs,
            force_shutdown: shutdown_rcv,
        };

        let this = Self {
            job_sender: job_snd,
            force_shutdown: shutdown_snd,
            backend: config.backend,
        };

        (this, storage_loop)
    }

    pub fn backend(&self) -> &B {
        &self.backend
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
