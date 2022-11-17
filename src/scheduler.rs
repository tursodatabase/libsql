use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use anyhow::Result;
use crossbeam::channel::{Receiver, Sender, TryRecvError, TrySendError};
use message_io::network::Endpoint;
use message_io::node::NodeHandler;

use crate::job::Job;
use crate::statements::Statements;
use crate::worker_pool::WorkerPool;

#[derive(Debug, PartialEq, Default)]
enum QueueState {
    /// Ready to do some work
    #[default]
    Ready,
    /// Performing some oneshot job, and waiting for status.
    Working,
}

#[derive(Default)]
struct EndpointQueue {
    queue: VecDeque<Job>,
    state: QueueState,
    /// Sender to the active transaction for this endpoint.
    /// On ready state, jobs for this endpoint should be sent to this channel instead of the global queue.
    active_txn: Option<Sender<Job>>,
    /// The client for this queue has disconnected
    should_close: bool,
}

impl EndpointQueue {
    fn is_ready(&self) -> bool {
        self.state == QueueState::Ready
    }
}

pub enum UpdateStateMessage {
    Ready(Endpoint),
    TxnBegin(Endpoint, Sender<Job>),
    TxnEnded(Endpoint),
}

pub enum Action {
    Disconnect,
    Execute(Statements),
}

pub struct ServerMessage {
    pub endpoint: Endpoint,
    pub handler: NodeHandler<()>,
    pub action: Action,
}

pub struct Scheduler {
    pool: WorkerPool,
    queues: HashMap<Endpoint, EndpointQueue>,
    /// The receiving end of the channel the pool uses to notify the scheduler of the state
    /// updates for its queues
    update_state_receiver: Receiver<UpdateStateMessage>,
    update_state_sender: Sender<UpdateStateMessage>,
    /// Receiver from the server with new statements to run
    job_receiver: Receiver<ServerMessage>,

    should_exit: bool,

    /// Number of unprocessed inbound jobs remaining
    inbound_jobs: usize,
    /// Number of currently processing jobs
    inflight_jobs: usize,
}

pub struct SchedulerConfig {
    /// Number of desired workers in the threadpool.
    /// A value of 0 will pick a value automatically.
    pub num_workers: usize,
    /// Creates a new database connection on each call.
    /// Connections have to be fresh instances each time!
    pub db_conn_factory: Box<dyn Fn() -> sqlite::Connection + Send + Sync>,
}

impl Scheduler {
    pub fn new(config: &SchedulerConfig, job_receiver: Receiver<ServerMessage>) -> Result<Self> {
        let pool = WorkerPool::new(config.num_workers, &config.db_conn_factory)?;
        let (update_state_sender, update_state_receiver) = crossbeam::channel::unbounded();
        Ok(Self {
            pool,
            queues: Default::default(),
            update_state_receiver,
            update_state_sender,
            job_receiver,
            should_exit: false,
            inbound_jobs: 0,
            inflight_jobs: 0,
        })
    }

    /// push some work to the gobal queue
    fn schedule_work(&mut self) {
        // FIXME: this is not very fair scheduling...
        self.queues.retain(|_, queue| {
            // skip jobs that are not ready
            if !queue.is_ready() {
                return true;
            }

            // take the first job in the queue, else pass, or remove the queue from the queues if
            // the connection was closed.
            let Some(mut job) = queue.queue.pop_front() else { return !queue.should_close };
            self.inbound_jobs -= 1;
            self.inflight_jobs += 1;
            queue.state = QueueState::Working;

            // there is an active transaction, so we should send it there
            if let Some(ref sender) = queue.active_txn {
                job = match sender.try_send(job) {
                    Ok(_) => {
                        return true;
                    }
                    // the transaction channel was closed before we were notified, we'll send
                    // to the main queue instead
                    Err(TrySendError::Disconnected(job)) => {
                        queue.active_txn.take();
                        job
                    }
                    Err(TrySendError::Full(_)) => {
                        unreachable!("txn channel should never be full")
                    }
                };
            }

            // submit job to the main queue:
            self.pool.schedule(job);

            true
        })
    }

    /// Update the queue with new status, and return whether there might be more work ready to do;
    fn update_queue_status(&mut self) -> bool {
        let mut maybe_ready = false;
        while let Ok(update) = self.update_state_receiver.try_recv() {
            match update {
                UpdateStateMessage::Ready(e) => {
                    self.inflight_jobs -= 1;
                    // It's OK if the queue was already removed
                    if let Some(queue) = self.queues.get_mut(&e) {
                        assert_eq!(queue.state, QueueState::Working);
                        maybe_ready |= !queue.queue.is_empty();
                        queue.state = QueueState::Ready;
                    }
                }
                UpdateStateMessage::TxnBegin(e, sender) => {
                    if let Some(queue) = self.queues.get_mut(&e) {
                        assert!(queue.active_txn.is_none());
                        queue.active_txn.replace(sender);
                    }
                }
                UpdateStateMessage::TxnEnded(e) => {
                    if let Some(queue) = self.queues.get_mut(&e) {
                        // it's ok if the txn was already removed
                        queue.active_txn.take();
                        queue.state = QueueState::Ready;
                    }
                }
            }
        }

        maybe_ready
    }

    /// Update queues with new incoming tasks from server.
    fn update_queues(&mut self) {
        loop {
            match self.job_receiver.try_recv() {
                Ok(msg) => match msg.action {
                    Action::Disconnect => {
                        self.queues
                            .get_mut(&msg.endpoint)
                            .map(|q| q.should_close = true);
                    }
                    Action::Execute(statements) => {
                        self.inbound_jobs += 1;
                        let job = Job {
                            scheduler_sender: self.update_state_sender.clone(),
                            statements,
                            endpoint: msg.endpoint,
                            handler: msg.handler,
                        };
                        self.queues
                            .entry(msg.endpoint)
                            .or_default()
                            .queue
                            .push_back(job);
                    }
                },
                Err(TryRecvError::Disconnected) => {
                    self.should_exit = true;
                    break;
                }
                _ => break,
            }
        }
    }

    pub fn start(mut self) {
        loop {
            // 1. find some work to perform
            self.schedule_work();
            // 2. check the state update queue and update the status of the queues
            let maybe_ready = self.update_queue_status();
            // 3. handle new incoming jobs
            if !self.should_exit {
                self.update_queues();
            }

            if self.should_exit && self.inbound_jobs == 0 && self.inflight_jobs == 0 {
                break;
            }

            // just sleep for a bit before checking for more work.
            // FIXME: make this reactive, and put thread to sleep until we know for sure more work
            // need to be done. This may be a bit tricky in practice.
            if !maybe_ready {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
