use std::fs::File;
use std::path::{Path, PathBuf};

use rusqlite::OpenFlags;
use sqld_libsql_bindings::open_with_regular_wal;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::{replication::FrameNo, rpc::replication_log::rpc::HelloResponse, HARD_RESET};

use super::error::ReplicationError;
use super::hook::{Frames, InjectorHook};
use super::meta::WalIndexMeta;

#[derive(Debug)]
struct FrameApplyOp {
    frames: Frames,
    ret: oneshot::Sender<anyhow::Result<FrameNo>>,
}

pub struct FrameInjectorHandle {
    handle: JoinHandle<anyhow::Result<()>>,
    sender: mpsc::Sender<FrameApplyOp>,
}

impl FrameInjectorHandle {
    pub async fn new(db_path: PathBuf, hello: HelloResponse) -> anyhow::Result<(Self, FrameNo)> {
        let (sender, mut receiver) = mpsc::channel(16);
        let (ret, init_ok) = oneshot::channel();
        let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let mut applicator = match FrameInjector::new_from_hello(&db_path, hello) {
                Ok((hook, last_applied_frame_no)) => {
                    ret.send(Ok(last_applied_frame_no)).unwrap();
                    hook
                }
                Err(e) => {
                    ret.send(Err(e)).unwrap();
                    return Ok(());
                }
            };

            while let Some(FrameApplyOp { frames, ret }) = receiver.blocking_recv() {
                let res = applicator.apply_frames(frames);
                if ret.send(res).is_err() {
                    anyhow::bail!("frame application result must not be ignored.");
                }
            }

            Ok(())
        });

        let last_applied_frame_no = init_ok.await??;

        Ok((Self { handle, sender }, last_applied_frame_no))
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.sender);
        self.handle.await?
    }

    pub async fn apply_frames(&mut self, frames: Frames) -> anyhow::Result<FrameNo> {
        let (ret, rcv) = oneshot::channel();
        self.sender.send(FrameApplyOp { frames, ret }).await?;
        rcv.await?
    }
}

pub struct FrameInjector {
    conn: rusqlite::Connection,
    hook: InjectorHook,
}

impl FrameInjector {
    /// returns the replication hook and the currently applied frame_no
    pub fn new_from_hello(db_path: &Path, hello: HelloResponse) -> anyhow::Result<(Self, FrameNo)> {
        let (meta, file) = WalIndexMeta::read_from_path(db_path)?;
        let meta = match meta {
            Some(meta) => match meta.merge_from_hello(hello) {
                Ok(meta) => meta,
                Err(e @ ReplicationError::Lagging) => {
                    tracing::error!("Replica ahead of primary: hard-reseting replica");
                    HARD_RESET.notify_waiters();

                    anyhow::bail!(e);
                }
                Err(e) => anyhow::bail!(e),
            },
            None => WalIndexMeta::new_from_hello(hello)?,
        };

        Ok((Self::init(db_path, file, meta)?, meta.current_frame_no()))
    }

    fn init(db_path: &Path, meta_file: File, meta: WalIndexMeta) -> anyhow::Result<Self> {
        let hook = InjectorHook::new(meta_file, meta);
        let conn = open_with_regular_wal(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            hook.clone(),
            false, // bottomless replication is not enabled for replicas
        )?;

        Ok(Self { conn, hook })
    }

    /// sets the injector's frames to the provided frames, trigger a dummy write, and collect the
    /// injection result.
    fn apply_frames(&mut self, frames: Frames) -> anyhow::Result<FrameNo> {
        self.hook.set_frames(frames);

        let _ = self.conn.execute(
            "create table if not exists __dummy__ (dummy); insert into __dummy__ values (1);",
            (),
        );

        self.hook.take_result()
    }
}
