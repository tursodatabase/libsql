pub mod frame;
pub mod replica;

pub const WAL_PAGE_SIZE: i32 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");

/// The frame uniquely identifying, monotonically increasing number
pub type FrameNo = u64;
pub use frame::{Frame, FrameHeader};
pub use replica::hook::{Frames, InjectorHookCtx};

use bytemuck::bytes_of;
use std::os::unix::prelude::FileExt;
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
    pub fn new(db_path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let db_path = db_path.as_ref();
        let (meta, meta_file) = replica::meta::WalIndexMeta::read_from_path(db_path)?;
        let meta_file = Arc::new(meta_file);
        let (applied_frame_notifier, current_frame_no_notifier) = tokio::sync::watch::channel(
            meta.map(|m| m.post_commit_frame_no).unwrap_or(FrameNo::MAX),
        );
        let meta = Arc::new(parking_lot::Mutex::new(meta));
        let (frames_sender, receiver) = tokio::sync::mpsc::channel(1);

        let pre_commit = {
            let meta = meta.clone();
            let meta_file = meta_file.clone();
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
                meta_file.write_all_at(bytes_of(meta), 0)?;

                Ok(())
            }
        };

        let post_commit = {
            let meta = meta.clone();
            let meta_file = meta_file;
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
                meta_file.write_all_at(bytes_of(meta), 0)?;
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
        let injector = replica::injector::FrameInjector::new(db_path, hook_ctx_ref)?;

        Ok(Self {
            frames_sender,
            current_frame_no_notifier,
            _hook_ctx: hook_ctx,
            meta,
            injector,
        })
    }

    pub fn sync(&mut self, frames: Frames) -> anyhow::Result<()> {
        let _ = self.frames_sender.blocking_send(frames);
        self.injector.step()?;
        Ok(())
    }
}
