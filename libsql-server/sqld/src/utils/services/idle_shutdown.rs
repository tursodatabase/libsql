use std::sync::Arc;
use std::time::Duration;

use hyper::http;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct IdleShutdownLayer {
    watcher: Arc<watch::Sender<()>>,
}

impl IdleShutdownLayer {
    pub fn new(idle_timeout: Duration, shutdown_notifier: mpsc::Sender<()>) -> Self {
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
                        shutdown_notifier
                            .send(())
                            .await
                            .expect("failed to shutdown gracefully");
                    }
                }
            }

            tracing::debug!("idle shutdown loop exited");
        });

        Self {
            watcher: Arc::new(sender),
        }
    }

    pub fn into_kicker(self) -> IdleKicker {
        IdleKicker {
            sender: self.watcher,
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
pub struct IdleKicker {
    sender: Arc<watch::Sender<()>>,
}

impl IdleKicker {
    pub fn kick(&self) {
        let _: Result<_, _> = self.sender.send(());
    }
}

#[derive(Clone)]
pub struct IdleShutdownService<S> {
    inner: S,
    watcher: Arc<watch::Sender<()>>,
}

impl<B, S> Service<http::request::Request<B>> for IdleShutdownService<S>
where
    S: Service<http::request::Request<B>>,
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

    fn call(&mut self, req: http::request::Request<B>) -> Self::Future {
        if should_extend_lifetime(req.uri().path()) {
            let _ = self.watcher.send(());
        }
        self.inner.call(req)
    }
}

fn should_extend_lifetime(path: &str) -> bool {
    path != "/health"
}
