use std::pin::Pin;
use std::task::{ready, Poll};
use std::time::Instant;
use std::{future::Future, task::Context};

use hyper::{Method, Request, Uri};
use tower::{Layer, Service};

pub struct LoggerLayer;

impl<S> Layer<S> for LoggerLayer {
    type Service = LoggerService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LoggerService { inner }
    }
}

#[derive(Clone)]
pub struct LoggerService<S> {
    inner: S,
}

pin_project_lite::pin_project! {
    pub struct LogFuture<F> {
        method: Method,
        uri: Uri,
        start_time: Instant,
        #[pin]
        f: F,
    }
}

impl<F> Future for LogFuture<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.f.poll(cx));

        tracing::info!(
            "new request: {} {}; completed in {:.3?}",
            this.method,
            this.uri,
            this.start_time.elapsed()
        );

        Poll::Ready(res)
    }
}

impl<B, S> Service<Request<B>> for LoggerService<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = LogFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        LogFuture {
            method: req.method().clone(),
            uri: req.uri().clone(),
            start_time: Instant::now(),
            f: self.inner.call(req),
        }
    }
}
