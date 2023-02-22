use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{watch, Notify};
use tokio::time::timeout;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct IdleShutdownLayer {
    watcher: Arc<watch::Sender<()>>,
}

impl IdleShutdownLayer {
    pub fn new(idle_timeout: Duration, shutdown_notifier: Arc<Notify>) -> Self {
        let (sender, mut receiver) = watch::channel(());
        tokio::spawn(async move {
            loop {
                // FIXME: if we measure that this is causing performance issues, we may want to
                // implement some debouncing.
                let timeout_fut = timeout(idle_timeout, receiver.changed());
                match timeout_fut.await {
                    Ok(Ok(_)) => continue,
                    Ok(Err(_)) => break,
                    Err(_) => {
                        tracing::info!(
                            "Idle timeout, no new connection in {idle_timeout:.0?}. Shutting down.",
                        );
                        shutdown_notifier.notify_waiters();
                    }
                }
            }

            tracing::debug!("idle shutdown loop exited");
        });

        Self {
            watcher: Arc::new(sender),
        }
    }
}

impl<S> Layer<S> for IdleShutdownLayer {
    type Service = IdleShutdownService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IdleShutdownService {
            inner,
            watcher: self.watcher.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IdleShutdownService<S> {
    inner: S,
    watcher: Arc<watch::Sender<()>>,
}

impl<Req, S> Service<Req> for IdleShutdownService<S>
where
    S: Service<Req>,
{
    type Response = S::Response;

    type Error = S::Error;

    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let _ = self.watcher.send(());
        self.inner.call(req)
    }
}
