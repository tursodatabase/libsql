use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use hyper::http;
use tokio::sync::{watch, Notify};
use tokio::time::timeout;
use tokio::time::Duration;
use tower::{Layer, Service};

#[derive(Clone)]
pub struct IdleShutdownKicker {
    watcher: Arc<watch::Sender<()>>,
    connected_replicas: Arc<AtomicUsize>,
}

impl IdleShutdownKicker {
    pub fn new(
        idle_timeout: Duration,
        initial_idle_timeout: Option<Duration>,
        shutdown_notifier: Arc<Notify>,
    ) -> Self {
        let (sender, mut receiver) = watch::channel(());
        let connected_replicas = Arc::new(AtomicUsize::new(0));
        let connected_replicas_clone = connected_replicas.clone();
        let mut sleep_time = initial_idle_timeout.unwrap_or(idle_timeout);
        tokio::spawn(async move {
            loop {
                // FIXME: if we measure that this is causing performance issues, we may want to
                // implement some debouncing.
                let timeout_res = timeout(sleep_time, receiver.changed()).await;
                if let Ok(Err(_)) = timeout_res {
                    break;
                }
                if timeout_res.is_err() && connected_replicas_clone.load(Ordering::SeqCst) == 0 {
                    tracing::info!(
                        "Idle timeout, no new connection in {sleep_time:.0?}. Shutting down.",
                    );
                    shutdown_notifier.notify_waiters();
                }
                sleep_time = idle_timeout;
            }

            tracing::debug!("idle shutdown loop exited");
        });

        Self {
            watcher: Arc::new(sender),
            connected_replicas,
        }
    }

    pub fn add_connected_replica(&mut self) {
        self.connected_replicas.fetch_add(1, Ordering::SeqCst);
    }

    pub fn remove_connected_replica(&mut self) {
        self.connected_replicas.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn into_kicker(self) -> IdleKicker {
        IdleKicker {
            sender: self.watcher,
        }
    }
}

impl<S> Layer<S> for IdleShutdownKicker {
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
