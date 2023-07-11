pub mod frame;
pub mod replica;

pub const WAL_PAGE_SIZE: i32 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");

/// The frame uniquely identifying, monotonically increasing number
pub type FrameNo = u64;
pub use frame::{Frame, FrameHeader};
pub use replica::hook::{Frames, InjectorHookCtx};
use replica::snapshot::SnapshotFileHeader;
pub use replica::snapshot::TempSnapshot;

use std::sync::Arc;
use tokio::sync::mpsc::Sender;

pub struct Replicator {
    pub frames_sender: Sender<Frames>,
    pub current_frame_no_notifier: tokio::sync::watch::Receiver<FrameNo>,
    // The hook context needs to live as long as the injector and have a stable memory address.
    // Safety: it must never ever be used directly! Ever. Really.
    _hook_ctx: Arc<parking_lot::Mutex<InjectorHookCtx>>,
    pub meta: Arc<parking_lot::Mutex<Option<replica::meta::WalIndexMeta>>>,
    pub injector: replica::injector::FrameInjector<'static>,
}

impl Replicator {
    pub fn new(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let (applied_frame_notifier, current_frame_no_notifier) =
            tokio::sync::watch::channel(FrameNo::MAX);
        let meta = Arc::new(parking_lot::Mutex::new(None));
        let (frames_sender, receiver) = tokio::sync::mpsc::channel(1);

        let pre_commit = {
            let meta = meta.clone();
            move |fno| {
                let mut lock = meta.lock();
                let meta = match lock.as_mut() {
                    Some(meta) => meta,
                    None => {
                        tracing::warn!(
                            "sync called before meta inialization; filling with dummy metadata"
                        );
                        *lock = Some(replica::meta::WalIndexMeta {
                            pre_commit_frame_no: 1,
                            post_commit_frame_no: 1,
                            generation_id: 1,
                            database_id: 1,
                        });
                        lock.as_mut().unwrap()
                    }
                };
                meta.pre_commit_frame_no = fno;
                // FIXME: consider how we want to enable storing metadata - in a file, like below? Or in an internal table?
                //meta_file.write_all_at(bytes_of(meta), 0)?;

                Ok(())
            }
        };

        let post_commit = {
            let meta = meta.clone();
            let notifier = applied_frame_notifier;
            move |fno| {
                let mut lock = meta.lock();
                let meta = match lock.as_mut() {
                    Some(meta) => meta,
                    None => {
                        tracing::warn!(
                            "sync called before meta inialization; filling with dummy metadata"
                        );
                        *lock = Some(replica::meta::WalIndexMeta {
                            pre_commit_frame_no: 1,
                            post_commit_frame_no: 1,
                            generation_id: 1,
                            database_id: 1,
                        });
                        lock.as_mut().unwrap()
                    }
                };
                assert_eq!(meta.pre_commit_frame_no, fno);
                meta.post_commit_frame_no = fno;
                // FIXME: consider how we want to enable storing metadata - in a file, like below? Or in an internal table?
                //meta_file.write_all_at(bytes_of(meta), 0)?;
                let _ = notifier.send(fno);

                Ok(())
            }
        };

        let hook_ctx = Arc::new(parking_lot::Mutex::new(
            replica::hook::InjectorHookCtx::new(receiver, pre_commit, post_commit),
        ));
        // Safety: hook ctx reference is kept alive by the Arc<>, and is never used directly.
        let hook_ctx_ref = unsafe {
            std::mem::transmute::<
                &mut replica::hook::InjectorHookCtx,
                &'static mut replica::hook::InjectorHookCtx,
            >(&mut *hook_ctx.lock())
        };
        let injector = replica::injector::FrameInjector::new(path.as_ref(), hook_ctx_ref)?;

        Ok(Self {
            frames_sender,
            current_frame_no_notifier,
            _hook_ctx: hook_ctx,
            meta,
            injector,
        })
    }

    pub fn update_metadata_from_snapshot_header(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<()> {
        // FIXME: I guess we should consider allowing async reads here
        use std::io::Read;
        let path = path.as_ref();
        let mut file = std::fs::File::open(path)?;
        let mut buf: [u8; std::mem::size_of::<SnapshotFileHeader>()] =
            [0; std::mem::size_of::<SnapshotFileHeader>()];
        file.read_exact(&mut buf)?;
        let snapshot_header: SnapshotFileHeader = bytemuck::pod_read_unaligned(&buf);

        // Metadata is loaded straight from the snapshot header and overwrites any previous values
        *self.meta.lock() = Some(replica::meta::WalIndexMeta {
            pre_commit_frame_no: snapshot_header.start_frame_no,
            post_commit_frame_no: snapshot_header.start_frame_no,
            generation_id: 1, // FIXME: where to obtain generation id from? Do we need it?
            database_id: snapshot_header.db_id,
        });
        Ok(())
    }

    pub fn sync(&mut self, frames: Frames) -> anyhow::Result<()> {
        if let Frames::Snapshot(snapshot) = &frames {
            tracing::debug!(
                "Updating metadata from snapshot header {}",
                snapshot.path().display()
            );
            self.update_metadata_from_snapshot_header(snapshot.path())?;
        }
        let _ = self.frames_sender.blocking_send(frames);
        self.injector.step()?;
        Ok(())
    }
}
