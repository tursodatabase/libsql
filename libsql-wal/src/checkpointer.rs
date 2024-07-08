use std::future::Future;
use std::sync::Arc;

use hashbrown::HashSet;
use libsql_sys::name::NamespaceName;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::io::Io;
use crate::registry::WalRegistry;

pub(crate) type NotifyCheckpointer = mpsc::Sender<NamespaceName>;

type LibsqlCheckpointer<IO, S> = Checkpointer<WalRegistry<IO, S>>;

impl<IO, S> LibsqlCheckpointer<IO, S>
where
    IO: Io,
    S: Sync + Send + 'static,
{
    pub fn new(
        registry: WalRegistry<IO, S>,
        notifier: mpsc::Receiver<NamespaceName>,
        max_checkpointing_conccurency: usize,
    ) -> Self {
        Self::new_with_performer(registry, notifier, max_checkpointing_conccurency)
    }
}

trait PerformCheckpoint {
    fn checkpoint(
        &self,
        namespace: &NamespaceName,
    ) -> impl Future<Output = crate::error::Result<()>> + Send;
}

impl<IO, S> PerformCheckpoint for WalRegistry<IO, S>
where
    IO: Io,
    S: Sync + Send + 'static,
{
    fn checkpoint(
        &self,
        namespace: &NamespaceName,
    ) -> impl Future<Output = crate::error::Result<()>> + Send {
        let namespace = namespace.clone();
        async move {
            let registry = self
                .get_async(&namespace)
                .await
                .expect("namespace not openned");
            registry.checkpoint().await?;
            Ok(())
        }
    }
}

const CHECKPOINTER_ERROR_THRES: usize = 16;

/// The checkpointer checkpoint wal segments in the main db file, and deletes checkpointed
/// segments.
/// For simplicity of implementation, we only delete segments when they are checkpointed, and only checkpoint when
/// they are reported as durable.
#[derive(Debug)]
pub struct Checkpointer<P> {
    perform_checkpoint: Arc<P>,
    /// Namespaces scheduled for checkpointing, but not currently checkpointing
    scheduled: HashSet<NamespaceName>,
    /// currently checkpointing databases
    checkpointing: HashSet<NamespaceName>,
    /// the checkpointer is notifier whenever there is a change to a namespage that could trigger a
    /// checkpoint
    recv: mpsc::Receiver<NamespaceName>,
    max_checkpointing_conccurency: usize,
    shutting_down: bool,
    join_set: JoinSet<(NamespaceName, crate::error::Result<()>)>,
    processing: Vec<NamespaceName>,
    errors: usize,
}

#[allow(private_bounds)]
impl<P> Checkpointer<P>
where
    P: PerformCheckpoint + Send + Sync + 'static,
{
    fn new_with_performer(
        perform_checkpoint: P,
        notifier: mpsc::Receiver<NamespaceName>,
        max_checkpointing_conccurency: usize,
    ) -> Self {
        Self {
            perform_checkpoint: Arc::new(perform_checkpoint),
            scheduled: Default::default(),
            checkpointing: Default::default(),
            recv: notifier,
            max_checkpointing_conccurency,
            shutting_down: false,
            join_set: JoinSet::new(),
            processing: Vec::new(),
            errors: 0,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        loop {
            if self.should_exit() {
                tracing::info!("checkpointer exited cleanly.");
                return;
            }

            if self.errors > CHECKPOINTER_ERROR_THRES {
                todo!("handle too many consecutive errors");
            }

            self.step().await;
        }
    }

    fn should_exit(&self) -> bool {
        self.shutting_down
            && self.scheduled.is_empty()
            && self.checkpointing.is_empty()
            && self.join_set.is_empty()
    }

    async fn step(&mut self) {
        tokio::select! {
            biased;
            // fixme: we should probably handle a panic in the checkpointing task somehow
            Some(Ok((namespace, result))) = self.join_set.join_next(), if !self.join_set.is_empty() => {
                self.checkpointing.remove(&namespace);
                if let Err(e) = result {
                    self.errors += 1;
                    tracing::error!("error checkpointing ns {namespace}: {e}, rescheduling");
                    // reschedule
                    self.scheduled.insert(namespace);
                } else {
                    self.errors = 0;
                }
            }
            notified = self.recv.recv(), if !self.shutting_down => {
                match notified {
                    Some(namespace) => {
                        self.scheduled.insert(namespace);
                    }
                    None => {
                        self.shutting_down = true;
                    }
                }
            }
            // don't wait if there is stuff to enqueue
            _ = std::future::ready(()), if !self.scheduled.is_empty()
                && self.join_set.len() < self.max_checkpointing_conccurency => (),
        }

        let n_available = self.max_checkpointing_conccurency - self.join_set.len();
        if n_available > 0 {
            for namespace in self
                .scheduled
                .difference(&self.checkpointing)
                .take(n_available)
                .cloned()
            {
                self.processing.push(namespace.clone());
                let perform_checkpoint = self.perform_checkpoint.clone();
                self.join_set.spawn(async move {
                    let ret = perform_checkpoint.checkpoint(&namespace).await;
                    (namespace, ret)
                });
            }

            for namespace in self.processing.drain(..) {
                self.scheduled.remove(&namespace);
                self.checkpointing.insert(namespace);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

    use tokio::time::Duration;

    use super::*;

    #[tokio::test]
    async fn process_checkpoint() {
        static CALLED: AtomicBool = AtomicBool::new(false);

        #[derive(Debug)]
        struct TestPerformCheckoint;

        impl PerformCheckpoint for TestPerformCheckoint {
            async fn checkpoint(&self, _namespace: &NamespaceName) -> crate::error::Result<()> {
                CALLED.store(true, Relaxed);
                Ok(())
            }
        }

        let (sender, receiver) = mpsc::channel(8);
        let mut checkpointer = Checkpointer::new_with_performer(TestPerformCheckoint, receiver, 5);
        let ns = NamespaceName::from("test");

        sender.send(ns.clone()).await.unwrap();

        checkpointer.step().await;

        assert!(checkpointer.checkpointing.contains(&ns));

        checkpointer.step().await;

        assert!(checkpointer.checkpointing.is_empty());
        assert!(checkpointer.scheduled.is_empty());
        assert!(CALLED.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn checkpoint_error() {
        static CALLED: AtomicBool = AtomicBool::new(false);

        #[derive(Debug)]
        struct TestPerformCheckoint;

        impl PerformCheckpoint for TestPerformCheckoint {
            async fn checkpoint(&self, _namespace: &NamespaceName) -> crate::error::Result<()> {
                CALLED.store(true, Relaxed);
                // random error
                Err(crate::error::Error::BusySnapshot)
            }
        }

        let (sender, receiver) = mpsc::channel(8);
        let mut checkpointer = Checkpointer::new_with_performer(TestPerformCheckoint, receiver, 5);
        let ns = NamespaceName::from("test");

        sender.send(ns.clone()).await.unwrap();

        checkpointer.step().await;
        assert_eq!(checkpointer.errors, 0);

        assert!(checkpointer.checkpointing.contains(&ns));

        checkpointer.step().await;

        // job is re-enqueued
        assert!(CALLED.load(std::sync::atomic::Ordering::Relaxed));
        assert!(checkpointer.checkpointing.contains(&ns));
        assert!(checkpointer.scheduled.is_empty());
        assert_eq!(checkpointer.errors, 1);
    }

    #[tokio::test]
    async fn checkpointer_shutdown() {
        #[derive(Debug)]
        struct TestPerformCheckoint;

        impl PerformCheckpoint for TestPerformCheckoint {
            async fn checkpoint(&self, _namespace: &NamespaceName) -> crate::error::Result<()> {
                Ok(())
            }
        }

        let (sender, receiver) = mpsc::channel(8);
        let mut checkpointer = Checkpointer::new_with_performer(TestPerformCheckoint, receiver, 5);

        drop(sender);

        assert!(!checkpointer.should_exit());

        checkpointer.step().await;

        assert!(checkpointer.should_exit());

        // should return immediately.
        checkpointer.run().await;
    }

    #[tokio::test]
    async fn cant_exit_until_all_processed() {
        #[derive(Debug)]
        struct TestPerformCheckoint;

        impl PerformCheckpoint for TestPerformCheckoint {
            async fn checkpoint(&self, _namespace: &NamespaceName) -> crate::error::Result<()> {
                Ok(())
            }
        }

        let (sender, receiver) = mpsc::channel(8);
        let mut checkpointer = Checkpointer::new_with_performer(TestPerformCheckoint, receiver, 5);

        drop(sender);

        checkpointer.step().await;

        let ns: NamespaceName = "test".into();
        checkpointer.scheduled.insert(ns.clone());
        assert!(!checkpointer.should_exit());
        checkpointer.scheduled.remove(&ns);

        checkpointer.checkpointing.insert(ns.clone());
        assert!(!checkpointer.should_exit());
        checkpointer.checkpointing.remove(&ns);

        assert!(checkpointer.should_exit());
        // should return immediately.
        checkpointer.run().await;
    }

    #[tokio::test]
    async fn dont_schedule_already_scheduled() {
        #[derive(Debug)]
        struct TestPerformCheckoint;

        impl PerformCheckpoint for TestPerformCheckoint {
            async fn checkpoint(&self, _namespace: &NamespaceName) -> crate::error::Result<()> {
                tokio::time::sleep(Duration::from_secs(1000)).await;
                Ok(())
            }
        }

        let (sender, receiver) = mpsc::channel(8);
        let mut checkpointer = Checkpointer::new_with_performer(TestPerformCheckoint, receiver, 5);

        let ns: NamespaceName = "test".into();

        sender.send(ns.clone()).await.unwrap();
        sender.send(ns.clone()).await.unwrap();

        checkpointer.step().await;

        assert!(checkpointer.scheduled.is_empty());
        assert!(checkpointer.checkpointing.contains(&ns));

        checkpointer.step().await;

        assert!(checkpointer.scheduled.contains(&ns));
        assert!(checkpointer.checkpointing.contains(&ns));
        assert_eq!(checkpointer.join_set.len(), 1);
    }

    #[tokio::test]
    async fn schedule_conccurently_for_different_namespaces() {
        #[derive(Debug)]
        struct TestPerformCheckoint;

        impl PerformCheckpoint for TestPerformCheckoint {
            async fn checkpoint(&self, _namespace: &NamespaceName) -> crate::error::Result<()> {
                tokio::time::sleep(Duration::from_secs(1000)).await;
                Ok(())
            }
        }

        let (sender, receiver) = mpsc::channel(8);
        let mut checkpointer = Checkpointer::new_with_performer(TestPerformCheckoint, receiver, 5);

        let ns1: NamespaceName = "test1".into();
        let ns2: NamespaceName = "test2".into();

        sender.send(ns1.clone()).await.unwrap();
        sender.send(ns2.clone()).await.unwrap();

        checkpointer.step().await;

        assert!(checkpointer.scheduled.is_empty());
        assert!(checkpointer.checkpointing.contains(&ns1));
        assert_eq!(checkpointer.checkpointing.len(), 1);

        checkpointer.step().await;

        assert!(checkpointer.scheduled.is_empty());
        assert!(checkpointer.checkpointing.contains(&ns2));
        assert_eq!(checkpointer.checkpointing.len(), 2);
        assert_eq!(checkpointer.join_set.len(), 2);
    }

    #[tokio::test]
    async fn checkpointer_limited_conccurency() {
        #[derive(Debug)]
        struct TestPerformCheckoint;

        impl PerformCheckpoint for TestPerformCheckoint {
            async fn checkpoint(&self, _namespace: &NamespaceName) -> crate::error::Result<()> {
                tokio::time::sleep(Duration::from_secs(1000)).await;
                Ok(())
            }
        }

        let (sender, receiver) = mpsc::channel(8);
        let mut checkpointer = Checkpointer::new_with_performer(TestPerformCheckoint, receiver, 2);

        let ns1: NamespaceName = "test1".into();
        let ns2: NamespaceName = "test2".into();
        let ns3: NamespaceName = "test3".into();

        sender.send(ns1.clone()).await.unwrap();
        sender.send(ns2.clone()).await.unwrap();
        sender.send(ns3.clone()).await.unwrap();

        checkpointer.step().await;
        checkpointer.step().await;
        checkpointer.step().await;

        assert_eq!(checkpointer.scheduled.len(), 1);
        assert!(checkpointer.scheduled.contains(&ns3));

        assert!(checkpointer.checkpointing.contains(&ns1));
        assert!(checkpointer.checkpointing.contains(&ns2));
        assert_eq!(checkpointer.checkpointing.len(), 2);
        assert_eq!(checkpointer.join_set.len(), 2);

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2000)).await;

        checkpointer.step().await;
        checkpointer.step().await;

        assert!(checkpointer.scheduled.is_empty());
        assert!(checkpointer.checkpointing.contains(&ns3));
        assert_eq!(checkpointer.checkpointing.len(), 1);
    }
}
