use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use regex::Regex;
use tokio::sync::Notify;
use tokio::time::Duration;

use crate::namespace::NamespaceName;

use super::FrameNo;

const MAX_RETRIES_THRESHOLD: u32 = 64;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Snaphot handler failure.\nstderr:\n{stderr}\nstdout:\n{stdout}")]
    HandlerFailure { stderr: String, stdout: String },
    #[error("Could not parse snapshot path: {0}")]
    InvalidSnapshotPath(PathBuf),
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone)]
pub(crate) struct SnapshotEntry {
    namespace: NamespaceName,
    start_frame_no: FrameNo,
    end_frame_no: FrameNo,
    path: PathBuf,
    retries: u32,
}

pub(crate) trait Handler {
    async fn handle(&mut self, entry: &SnapshotEntry) -> Result<()>;
}

impl PartialEq for SnapshotEntry {
    fn eq(&self, other: &Self) -> bool {
        self.namespace == other.namespace
            && self.start_frame_no == other.start_frame_no
            && self.end_frame_no == other.end_frame_no
    }
}

impl Eq for SnapshotEntry {}

impl PartialOrd for SnapshotEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SnapshotEntry {
    // the ordering are reversed because we use a max queue, so entry a ordered by priority
    fn cmp(&self, other: &Self) -> Ordering {
        // it doesn't matter the order in which we process snapshots for different namespaces
        if self.namespace != other.namespace {
            Ordering::Equal
        } else {
            match self.start_frame_no.cmp(&other.start_frame_no) {
                Ordering::Equal => {
                    // if the two snapshot have the same start frame_no, then we process first
                    // whichever has the greated end frame no. That way the script can decide to
                    // drop the following
                    self.end_frame_no.cmp(&other.end_frame_no)
                }
                // we process first a snapshot that has a lower start_frame_no
                Ordering::Less => Ordering::Greater,
                Ordering::Greater => Ordering::Less,
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct ScriptBackupManager {
    path: PathBuf,
    queue: Arc<Mutex<BinaryHeap<SnapshotEntry>>>,
    notifier: Arc<Notify>,
}

pub(crate) struct ScriptBackupTask<H> {
    queue: Arc<Mutex<BinaryHeap<SnapshotEntry>>>,
    notifier: Arc<Notify>,
    handler: H,
}

pub struct CommandHandler {
    command: String,
}

impl CommandHandler {
    pub fn new(command: String) -> Self {
        Self { command }
    }
}

impl Handler for CommandHandler {
    async fn handle(&mut self, entry: &SnapshotEntry) -> Result<()> {
        let output = tokio::process::Command::new(&self.command)
            .arg(&entry.path)
            .arg(entry.namespace.as_str())
            .arg(entry.start_frame_no.to_string())
            .arg(entry.end_frame_no.to_string())
            .output()
            .await?;
        if !output.status.success() {
            return Err(Error::HandlerFailure {
                stderr: String::from_utf8(output.stderr).unwrap_or_default(),
                stdout: String::from_utf8(output.stdout).unwrap_or_default(),
            });
        }

        Ok(())
    }
}

impl<H: Handler> ScriptBackupTask<H> {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            self.process_one().await?;
        }
    }

    async fn process_one(&mut self) -> crate::Result<()> {
        loop {
            let entry = self.queue.lock().pop();
            match entry {
                Some(mut entry) => {
                    match self.handler.handle(&entry).await {
                        Ok(_) => {
                            assert!(!entry.path.try_exists()?, "snapshot handler returned success, yet snapshot file is still present.");
                        }
                        Err(e) => {
                            tracing::error!(
                                "failed to process scripted snapshot backup for {entry:?}: {e}"
                            );
                            assert!(entry.path.try_exists()?, "snapshot file was removed, but script returned an error. Can't ensure consistency");
                            // exponential backoff
                            tokio::time::sleep(
                                Duration::from_millis(500) * 2u32.pow(entry.retries),
                            )
                            .await;

                            entry.retries += 1;
                            if entry.retries > MAX_RETRIES_THRESHOLD {
                                todo!("failure to make any progress, what do we do?");
                            }
                            self.queue.lock().push(entry);
                        }
                    }

                    return Ok(());
                }
                None => {
                    self.notifier.notified().await;
                }
            }
        }
    }
}

fn make_snapshot_path(
    base_path: impl AsRef<Path>,
    namespace: &NamespaceName,
    start_frame_no: FrameNo,
    end_frame_no: FrameNo,
) -> PathBuf {
    base_path.as_ref().join(format!(
        "{namespace}:{start_frame_no:020x}-{end_frame_no:020x}.snap"
    ))
}

fn parse_snapshot_path(path: PathBuf) -> Result<SnapshotEntry> {
    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r#"([\w-]+?):([0-9a-f]{20})-([0-9a-f]{20}).snap"#).unwrap());

    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| Error::InvalidSnapshotPath(path.clone()))?;

    let captures = RE
        .captures(name)
        .ok_or_else(|| Error::InvalidSnapshotPath(path.clone()))?;
    let namespace = NamespaceName::from_string(captures[1].to_string()).unwrap();
    let start_frame_no = FrameNo::from_str_radix(&captures[2], 16).unwrap();
    let end_frame_no = FrameNo::from_str_radix(&captures[3], 16).unwrap();

    Ok(SnapshotEntry {
        namespace,
        start_frame_no,
        end_frame_no,
        path,
        retries: 0,
    })
}

async fn seed_queue(queue_dir: &Path) -> Result<BinaryHeap<SnapshotEntry>> {
    let mut dir = tokio::fs::read_dir(queue_dir).await?;
    let mut queue = BinaryHeap::new();
    while let Some(entry) = dir.next_entry().await? {
        let entry = parse_snapshot_path(entry.path())?;
        queue.push(entry);
    }

    Ok(queue)
}

impl ScriptBackupManager {
    pub async fn new<H: Handler>(
        base_path: &Path,
        handler: H,
    ) -> Result<(Self, ScriptBackupTask<H>)> {
        let script_backup_path = base_path.join("script_backup");

        tokio::fs::create_dir_all(&script_backup_path).await?;

        let notifier = Arc::new(Notify::new());
        // on startup we recover missing snapshots
        let queue = Arc::new(Mutex::new(seed_queue(&script_backup_path).await?));
        let task = ScriptBackupTask {
            queue: queue.clone(),
            notifier: notifier.clone(),
            handler,
        };
        let this = Self {
            path: script_backup_path,
            queue,
            notifier,
        };

        Ok((this, task))
    }

    pub async fn register(
        &self,
        namespace: NamespaceName,
        start_frame_no: FrameNo,
        end_frame_no: FrameNo,
        src_path: &Path,
    ) -> crate::Result<()> {
        let dst_path = make_snapshot_path(&self.path, &namespace, start_frame_no, end_frame_no);
        tokio::fs::hard_link(src_path, &dst_path).await?;
        let entry = SnapshotEntry {
            namespace,
            start_frame_no,
            end_frame_no,
            path: dst_path,
            retries: 0,
        };
        self.queue.lock().push(entry);
        self.notifier.notify_waiters();

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;
    use tempfile::tempdir;
    use uuid::Uuid;

    proptest! {
        #[test]
        fn parse_rountrip_snapshot_path(
            snapshot_name in r#"[\w-]+"#,
            start_frame_no in 0u64..u64::MAX,
            end_frame_no in 0u64..u64::MAX,
            ){
            let namespace = NamespaceName::from_string(snapshot_name.to_string()).unwrap();
            let path = make_snapshot_path("/test", &namespace, start_frame_no, end_frame_no);
            let entry = parse_snapshot_path(path.clone()).unwrap();
            assert_eq!(entry.end_frame_no, end_frame_no);
            assert_eq!(entry.start_frame_no, start_frame_no);
            assert_eq!(entry.namespace, namespace);
            assert_eq!(entry.path, path);
        }
    }

    fn dummy_entry(name: &str, start: FrameNo, end: FrameNo) -> SnapshotEntry {
        SnapshotEntry {
            namespace: NamespaceName::from_string(name.to_string()).unwrap(),
            start_frame_no: start,
            end_frame_no: end,
            path: PathBuf::new(),
            retries: 0,
        }
    }

    #[test]
    fn compare_entries() {
        // different namespace name can be processed in any order
        assert!(dummy_entry("test2", 1, 50)
            .cmp(&dummy_entry("test1", 30, 50))
            .is_eq());
        // the relation is reflexive
        assert!(dummy_entry("test1", 1, 50)
            .cmp(&dummy_entry("test2", 30, 50))
            .is_eq());

        // snapshot with lower frameno has priority
        assert!(dummy_entry("test1", 1, 50)
            .cmp(&dummy_entry("test1", 30, 50))
            .is_gt());
        assert!(dummy_entry("test1", 30, 50)
            .cmp(&dummy_entry("test1", 1, 50))
            .is_lt());

        // same start point, the largest has a higher priority
        assert!(dummy_entry("test1", 1, 50)
            .cmp(&dummy_entry("test1", 1, 100))
            .is_lt());
        assert!(dummy_entry("test1", 1, 100)
            .cmp(&dummy_entry("test1", 1, 50))
            .is_gt());
    }

    #[test]
    fn queue() {
        let mut queue = BinaryHeap::new();
        queue.push(dummy_entry("test1", 1, 12));
        queue.push(dummy_entry("test1", 29, 31));
        queue.push(dummy_entry("test1", 1, 52));

        assert!(matches!(
            queue.pop().unwrap(),
            SnapshotEntry {
                start_frame_no: 1,
                end_frame_no: 52,
                ..
            }
        ));
        assert!(matches!(
            queue.pop().unwrap(),
            SnapshotEntry {
                start_frame_no: 1,
                end_frame_no: 12,
                ..
            }
        ));
        assert!(matches!(
            queue.pop().unwrap(),
            SnapshotEntry {
                start_frame_no: 29,
                end_frame_no: 31,
                ..
            }
        ));
        assert!(queue.pop().is_none());
    }

    async fn dummy_entry_in(
        path: &Path,
        name: &str,
        start: FrameNo,
        end: FrameNo,
    ) -> SnapshotEntry {
        let dummy_path = path.join(Uuid::new_v4().to_string());
        tokio::fs::File::create(&dummy_path).await.unwrap();
        let mut entry = dummy_entry(name, start, end);
        entry.path = dummy_path;
        entry
    }

    #[tokio::test]
    async fn retry_failed_entry() {
        #[derive(Default)]
        struct FailHandler {
            last_entry: Option<SnapshotEntry>,
        }

        impl Handler for FailHandler {
            async fn handle(&mut self, entry: &SnapshotEntry) -> Result<()> {
                self.last_entry = Some(entry.clone());
                Err(Error::HandlerFailure {
                    stdout: String::new(),
                    stderr: String::new(),
                })
            }
        }

        let tmp = tempdir().unwrap();
        let (manager, mut task) = ScriptBackupManager::new(tmp.path(), FailHandler::default())
            .await
            .unwrap();

        let entry = dummy_entry_in(tmp.path(), "test1", 1, 10).await;
        manager
            .register(
                entry.namespace.clone(),
                entry.start_frame_no,
                entry.end_frame_no,
                &entry.path,
            )
            .await
            .unwrap();

        let entry = dummy_entry_in(tmp.path(), "test1", 11, 21).await;
        manager
            .register(
                entry.namespace.clone(),
                entry.start_frame_no,
                entry.end_frame_no,
                &entry.path,
            )
            .await
            .unwrap();

        task.process_one().await.unwrap();
        assert_eq!(task.handler.last_entry.as_ref().unwrap().start_frame_no, 1);
        assert_eq!(task.handler.last_entry.as_ref().unwrap().retries, 0);
        assert_eq!(task.queue.lock().len(), 2);

        // next step, we retry
        task.process_one().await.unwrap();
        assert_eq!(task.handler.last_entry.as_ref().unwrap().start_frame_no, 1);
        assert_eq!(task.handler.last_entry.as_ref().unwrap().retries, 1);
        assert_eq!(task.queue.lock().len(), 2);
    }

    #[should_panic]
    #[tokio::test]
    async fn panic_if_snapshot_no_removed_on_success() {
        struct OkHandler;
        impl Handler for OkHandler {
            async fn handle(&mut self, _entry: &SnapshotEntry) -> Result<()> {
                Ok(())
            }
        }

        let tmp = tempdir().unwrap();
        let (manager, mut task) = ScriptBackupManager::new(tmp.path(), OkHandler)
            .await
            .unwrap();

        let entry = dummy_entry_in(tmp.path(), "test1", 1, 10).await;
        manager
            .register(
                entry.namespace.clone(),
                entry.start_frame_no,
                entry.end_frame_no,
                &entry.path,
            )
            .await
            .unwrap();

        task.process_one().await.unwrap();
    }

    #[should_panic]
    #[tokio::test]
    async fn panic_if_snapshot_is_removed_on_failure() {
        struct FailHandler;
        impl Handler for FailHandler {
            async fn handle(&mut self, entry: &SnapshotEntry) -> Result<()> {
                tokio::fs::remove_file(&entry.path).await.unwrap();
                Err(Error::HandlerFailure {
                    stdout: String::new(),
                    stderr: String::new(),
                })
            }
        }

        let tmp = tempdir().unwrap();
        let (manager, mut task) = ScriptBackupManager::new(tmp.path(), FailHandler)
            .await
            .unwrap();

        let entry = dummy_entry_in(tmp.path(), "test1", 1, 10).await;
        manager
            .register(
                entry.namespace.clone(),
                entry.start_frame_no,
                entry.end_frame_no,
                &entry.path,
            )
            .await
            .unwrap();

        task.process_one().await.unwrap();
    }

    #[tokio::test]
    async fn normal_operation() {
        struct OkHandler;
        impl Handler for OkHandler {
            async fn handle(&mut self, entry: &SnapshotEntry) -> Result<()> {
                tokio::fs::remove_file(&entry.path).await.unwrap();
                Ok(())
            }
        }

        let tmp = tempdir().unwrap();
        let (manager, mut task) = ScriptBackupManager::new(tmp.path(), OkHandler)
            .await
            .unwrap();

        let entry = dummy_entry_in(tmp.path(), "test1", 1, 10).await;
        manager
            .register(
                entry.namespace.clone(),
                entry.start_frame_no,
                entry.end_frame_no,
                &entry.path,
            )
            .await
            .unwrap();

        let entry = dummy_entry_in(tmp.path(), "test1", 11, 50).await;
        manager
            .register(
                entry.namespace.clone(),
                entry.start_frame_no,
                entry.end_frame_no,
                &entry.path,
            )
            .await
            .unwrap();

        task.process_one().await.unwrap();
        assert_eq!(task.queue.lock().len(), 1);
        task.process_one().await.unwrap();
        assert_eq!(task.queue.lock().len(), 0);
    }

    #[tokio::test]
    async fn new_snapshot_notified() {
        let tmp = tempdir().unwrap();
        struct OkHandler;
        impl Handler for OkHandler {
            async fn handle(&mut self, entry: &SnapshotEntry) -> Result<()> {
                tokio::fs::remove_file(&entry.path).await.unwrap();
                Ok(())
            }
        }

        let (manager, mut task) = ScriptBackupManager::new(tmp.path(), OkHandler)
            .await
            .unwrap();

        let step_fut = task.process_one();
        tokio::pin!(step_fut);

        // nothing to do, waiting
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut step_fut)
                .await
                .is_err()
        );

        let entry = dummy_entry_in(tmp.path(), "test1", 1, 10).await;
        manager
            .register(
                entry.namespace.clone(),
                entry.start_frame_no,
                entry.end_frame_no,
                &entry.path,
            )
            .await
            .unwrap();

        assert!(step_fut.await.is_ok());
    }
}
