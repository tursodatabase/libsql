use std::path::{Path, PathBuf};

use rusqlite::OpenFlags;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::{replication::FrameNo, rpc::replication_log::rpc::HelloResponse, HARD_RESET};

use super::error::ReplicationError;
use super::hook::{Frames, InjectorHookCtx, INJECTOR_METHODS};
use super::meta::WalIndexMeta;

#[derive(Debug)]
struct FrameApplyOp {
    frames: Frames,
    ret: oneshot::Sender<anyhow::Result<FrameNo>>,
}

pub struct FrameInjectorHandle {
    handle: JoinHandle<()>,
    sender: mpsc::Sender<FrameApplyOp>,
}

fn injector_loop(
    db_path: &Path,
    hello: HelloResponse,
    mut receiver: mpsc::Receiver<FrameApplyOp>,
    init_ret: mpsc::Sender<anyhow::Result<FrameNo>>,
    allow_replica_overwrite: bool,
) -> anyhow::Result<()> {
    let mut ctx = InjectorHookCtx::new_from_hello(db_path, hello, allow_replica_overwrite)?;
    let mut injector = FrameInjector::init(db_path, &mut ctx)?;

    init_ret.try_send(Ok(injector.ctx.inner.borrow().meta.current_frame_no()))?;

    while let Some(FrameApplyOp { frames, ret }) = receiver.blocking_recv() {
        let res = injector.inject_frames(frames);
        if ret.send(res).is_err() {
            anyhow::bail!("frame application result must not be ignored.");
        }
    }

    Ok(())
}

impl FrameInjectorHandle {
    pub async fn new(
        db_path: PathBuf,
        hello: HelloResponse,
        allow_replica_overwrite: bool,
    ) -> anyhow::Result<(Self, FrameNo)> {
        let (sender, receiver) = mpsc::channel(16);
        // this ret thing is a bit convoluted: we want to collect the initialization result and
        // then run the loop. This channel with only ever receive one message, but we collect the
        // error outside of `injector_loop`, and the frame_no inside, so a oneshot doesn't cut it.
        // If someone has a nicer solution that does not involve many matches, go ahead :)
        let (ret, mut init_ok) = mpsc::channel(1);
        let handle = tokio::task::spawn_blocking(move || {
            if let Err(e) = injector_loop(
                &db_path,
                hello,
                receiver,
                ret.clone(),
                allow_replica_overwrite,
            ) {
                let _ = ret.try_send(Err(e));
            }
        });

        // there should always be a single message coming from this channel
        let last_applied_frame_no = init_ok.recv().await.unwrap()?;

        Ok((Self { handle, sender }, last_applied_frame_no))
    }

    pub async fn shutdown(self) -> anyhow::Result<()> {
        drop(self.sender);
        if let Err(e) = self.handle.await {
            // propagate panic
            if e.is_panic() {
                std::panic::resume_unwind(e.into_panic());
            } else {
                return Err(e)?;
            }
        }

        Ok(())
    }

    pub async fn apply_frames(&mut self, frames: Frames) -> anyhow::Result<FrameNo> {
        let (ret, rcv) = oneshot::channel();
        self.sender.send(FrameApplyOp { frames, ret }).await?;
        rcv.await?
    }
}

pub struct FrameInjector<'a> {
    conn: sqld_libsql_bindings::Connection<'a>,
    ctx: InjectorHookCtx,
}

impl InjectorHookCtx {
    pub fn new_from_hello(
        db_path: &Path,
        hello: HelloResponse,
        allow_replica_overwrite: bool,
    ) -> anyhow::Result<Self> {
        let (meta, file) = WalIndexMeta::read_from_path(db_path)?;
        let meta = match meta {
            Some(meta) => match meta.merge_from_hello(hello) {
                Ok(meta) => meta,
                Err(e @ ReplicationError::Lagging) => {
                    tracing::error!("Replica ahead of primary: hard-reseting replica");
                    HARD_RESET.notify_waiters();

                    anyhow::bail!(e);
                }
                Err(e @ ReplicationError::DbIncompatible) if allow_replica_overwrite => {
                    tracing::error!("Primary is attempting to replicate a different database, overwriting replica.");
                    HARD_RESET.notify_waiters();

                    anyhow::bail!(e);
                }
                Err(e) => anyhow::bail!(e),
            },
            None => WalIndexMeta::new_from_hello(hello)?,
        };

        Ok(Self::new(file, meta))
    }
}

impl<'a> FrameInjector<'a> {
    fn init(db_path: &Path, hook_ctx: &'a mut InjectorHookCtx) -> anyhow::Result<Self> {
        let ctx = hook_ctx.clone();
        let conn = sqld_libsql_bindings::Connection::open(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            &INJECTOR_METHODS,
            hook_ctx,
        )?;

        Ok(Self { conn, ctx })
    }

    /// sets the injector's frames to the provided frames, trigger a dummy write, and collect the
    /// injection result.
    fn inject_frames(&mut self, frames: Frames) -> anyhow::Result<FrameNo> {
        self.ctx.set_frames(frames);

        let _ = self
            .conn
            .execute("create table if not exists __dummy__ (dummy)", ());

        self.ctx.take_result()
    }
}
