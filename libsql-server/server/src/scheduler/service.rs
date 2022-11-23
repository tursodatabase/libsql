use std::convert::Infallible;
use std::future::{ready, Ready};
use std::pin::Pin;
use std::task::Poll;

use futures::Future;
use tokio::sync::{mpsc, oneshot};
use tower::Service;

use crate::query::{ErrorCode, Query, QueryError, QueryRequest, QueryResponse, QueryResult};

use super::{ClientId, SchedulerQuery};

pub struct SchedulerServiceFactory {
    next_client_id: ClientId,
    sender: mpsc::UnboundedSender<SchedulerQuery>,
}

impl SchedulerServiceFactory {
    pub fn new(sender: mpsc::UnboundedSender<SchedulerQuery>) -> Self {
        Self {
            next_client_id: 0,
            sender,
        }
    }
}

impl Service<()> for SchedulerServiceFactory {
    type Response = SchedulerService;
    type Error = Infallible;
    type Future = Ready<std::result::Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> Poll<Result<(), Infallible>> {
        Ok(()).into()
    }

    fn call(&mut self, _: ()) -> Self::Future {
        let client_id = self.next_client_id;
        self.next_client_id += 1;
        let svc = SchedulerService {
            client_id,
            scheduler: self.sender.clone(),
        };

        ready(Ok(svc))
    }
}

pub struct SchedulerService {
    client_id: ClientId,
    scheduler: mpsc::UnboundedSender<SchedulerQuery>,
}

impl Service<Query> for SchedulerService {
    type Response = QueryResponse;
    type Error = QueryError;
    type Future = Pin<Box<dyn Future<Output = QueryResult>>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, query: Query) -> Self::Future {
        let (sender, receiver) = oneshot::channel();
        let request = QueryRequest {
            client_id: self.client_id,
            query,
        };

        let scheduler = self.scheduler.clone();
        let fut = async move {
            if scheduler.send((request, sender)).is_err() {
                return Err(QueryError::new(ErrorCode::Internal, "scheduler crashed"));
            }
            match receiver.await {
                Ok(msg) => msg,
                Err(_) => Err(QueryError::new(ErrorCode::Internal, "scheduler crashed")),
            }
        };

        Box::pin(fut)
    }
}
