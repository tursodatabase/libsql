mod scheduler_impl;
pub mod service;

use tokio::sync::oneshot;

use crate::job::Job;
use crate::query::{QueryRequest, QueryResult};

pub use scheduler_impl::Scheduler;

pub type ClientId = usize;
type SchedulerQuery = (QueryRequest, oneshot::Sender<QueryResult>);

#[derive(Debug)]
pub enum UpdateStateMessage {
    Ready(ClientId),
    TxnBegin(ClientId, crossbeam::channel::Sender<Job>),
    TxnEnded(ClientId),
    TxnTimeout(ClientId),
}
