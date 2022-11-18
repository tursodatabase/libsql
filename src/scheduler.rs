use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

use anyhow::Result;
use crossbeam::channel::{Sender, TrySendError};
use smallvec::SmallVec;
use tokio::sync::mpsc::{UnboundedReceiver as TokioReceiver, UnboundedSender as TokioSender};

use crate::job::Job;
use crate::messages::Responder;
use crate::statements::Statements;

#[derive(Default)]
struct EndpointQueue {
    queue: VecDeque<Job>,
    /// Sender to the active transaction for this endpoint.
    /// On ready state, jobs for this endpoint should be sent to this channel instead of the global queue.
    active_txn: Option<Sender<Job>>,
    /// The client for this queue has disconnected
    should_close: bool,
}

#[derive(Debug)]
pub enum UpdateStateMessage {
    Ready(u32),
    TxnBegin(u32, Sender<Job>),
    TxnEnded(u32),
}

#[derive(Debug)]
pub enum Action {
    Disconnect,
    Execute(Statements),
}

pub struct ServerMessage {
    pub client_id: u32,
    pub action: Action,
    pub responder: Box<dyn Responder>,
}

impl fmt::Debug for ServerMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerMessage")
            .field("endpoint", &self.client_id)
            .field("action", &self.action)
            .finish()
    }
}

pub struct Scheduler {
    worker_pool_sender: Sender<Job>,
    queues: HashMap<u32, EndpointQueue>,
    /// The receiving end of the channel the pool uses to notify the scheduler of the state
    /// updates for its queues
    update_state_receiver: TokioReceiver<UpdateStateMessage>,
    update_state_sender: TokioSender<UpdateStateMessage>,
    /// Receiver from the server with new statements to run
    job_receiver: TokioReceiver<ServerMessage>,

    /// Set of endpoints that are ready to give some work, i.e that have no inflight work
    ready_set: HashSet<u32>,
    /// Set of endpoints that have some work in their queue
    has_work_set: HashSet<u32>,
}

impl Scheduler {
    pub fn new(
        worker_pool_sender: Sender<Job>,
        job_receiver: TokioReceiver<ServerMessage>,
    ) -> Result<Self> {
        let (update_state_sender, update_state_receiver) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            worker_pool_sender,
            queues: Default::default(),
            update_state_receiver,
            update_state_sender,
            job_receiver,
            ready_set: Default::default(),
            has_work_set: Default::default(),
        })
    }

    /// push some work to the gobal queue
    fn schedule_work(&mut self) {
        let mut not_waiting = SmallVec::<[u32; 16]>::new();
        let mut not_ready = SmallVec::<[u32; 16]>::new();

        for client_id in self.ready_set.intersection(&self.has_work_set).copied() {
            let Some(queue) = self.queues.get_mut(&client_id) else {
                not_ready.push(client_id);
                not_waiting.push(client_id);
                continue
            };

            let Some(mut job) = queue.queue.pop_front() else {
                not_waiting.push(client_id);
                continue
            };

            not_ready.push(client_id);

            // there is an active transaction, so we should send it there
            if let Some(ref sender) = queue.active_txn {
                job = match sender.try_send(job) {
                    Ok(_) => {
                        continue;
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
            self.worker_pool_sender
                .send(job)
                .expect("worker pool crashed");

            if queue.queue.is_empty() {
                not_waiting.push(client_id);
                if queue.should_close {
                    self.queues.remove(&client_id);
                }
            }
        }

        for e in &not_ready {
            self.ready_set.remove(e);
        }

        for e in &not_waiting {
            self.has_work_set.remove(e);
        }
    }

    /// Update the queue with new status, and return whether there might be more work ready to do;
    fn update_queue_status(&mut self, update: UpdateStateMessage) {
        match update {
            UpdateStateMessage::Ready(e) => {
                self.ready_set.insert(e);
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
                    self.ready_set.insert(e);
                }
            }
        }
    }

    /// Update queues with new incoming tasks from server.
    fn update_queues(&mut self, msg: ServerMessage) {
        match msg.action {
            Action::Disconnect => {
                self.queues
                    .get_mut(&msg.client_id)
                    .map(|q| q.should_close = true);
            }
            Action::Execute(statements) => {
                let job = Job {
                    scheduler_sender: self.update_state_sender.clone(),
                    statements,
                    client_id: msg.client_id,
                    responder: msg.responder,
                };

                self.queues
                    .entry(msg.client_id)
                    .or_insert_with(|| {
                        // This is the first time we see this endpoint, so it's ready by default
                        self.ready_set.insert(msg.client_id);
                        Default::default()
                    })
                    .queue
                    .push_back(job);

                self.has_work_set.insert(msg.client_id);
            }
        }
    }

    pub async fn start(mut self) {
        let mut should_exit = false;
        loop {
            tokio::select! {
                msg = self.update_state_receiver.recv() => {
                    match msg {
                        Some(msg) => {
                            self.update_queue_status(msg);
                        }
                        None => unreachable!("Scheduler still owns a sender"),
                    }
                },
                msg = self.job_receiver.recv(), if !should_exit => {
                    match msg {
                        Some(msg) => self.update_queues(msg),
                        None => should_exit = true,
                    }
                }
            }

            self.schedule_work();

            if should_exit
                // no queue has work left
                && self.has_work_set.is_empty()
                // no queue has inflight work
                && self.ready_set.len() == self.queues.len()
            {
                break;
            }
        }
    }
}
