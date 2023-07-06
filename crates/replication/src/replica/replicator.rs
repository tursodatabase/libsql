use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
use bytemuck::bytes_of;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tonic::transport::Channel;

use crate::frame::Frame;
use crate::replica::error::ReplicationError;
use crate::replica::snapshot::TempSnapshot;
use crate::replication_log::rpc::{
    replication_log_client::ReplicationLogClient, HelloRequest, LogOffset,
};
use crate::replication_log::NEED_SNAPSHOT_ERROR_MSG;
use crate::FrameNo;
use crate::HARD_RESET;

use super::hook::{Frames, InjectorHookCtx};
use super::injector::FrameInjector;
use super::meta::WalIndexMeta;

const HANDSHAKE_MAX_RETRIES: usize = 100;

type Client = ReplicationLogClient<Channel>;

/// The `Replicator` duty is to download frames from the primary, and pass them to the injector at
/// transaction boundaries.
pub struct Replicator {
    client: Client,
    db_path: PathBuf,
    meta: Arc<Mutex<Option<WalIndexMeta>>>,
    pub current_frame_no_notifier: watch::Receiver<FrameNo>,
    allow_replica_overwrite: bool,
    frames_sender: mpsc::Sender<Frames>,
}

impl Replicator {
    pub fn new(
        db_path: PathBuf,
        channel: Channel,
        uri: tonic::transport::Uri,
        allow_replica_overwrite: bool,
    ) -> anyhow::Result<Self> {
        let client = Client::with_origin(channel, uri);
        let (meta, meta_file) = WalIndexMeta::read_from_path(&db_path)?;
        let meta_file = Arc::new(meta_file);
        let (applied_frame_notifier, current_frame_no_notifier) =
            watch::channel(meta.map(|m| m.post_commit_frame_no).unwrap_or(FrameNo::MAX));
        let meta = Arc::new(Mutex::new(meta));
        let (frames_sender, receiver) = tokio::sync::mpsc::channel(1);

        let pre_commit = {
            let meta = meta.clone();
            let meta_file = meta_file.clone();
            move |fno| {
                let mut lock = meta.lock();
                let meta = lock
                    .as_mut()
                    .expect("commit called before meta inialization");
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
                let meta = lock
                    .as_mut()
                    .expect("commit called before meta inialization");
                assert_eq!(meta.pre_commit_frame_no, fno);
                meta.post_commit_frame_no = fno;
                meta_file.write_all_at(bytes_of(meta), 0)?;
                let _ = notifier.send(fno);

                Ok(())
            }
        };

        tokio::task::spawn_blocking({
            let db_path = db_path.clone();
            move || -> anyhow::Result<()> {
                let mut ctx = InjectorHookCtx::new(receiver, pre_commit, post_commit);
                let mut injector = FrameInjector::new(&db_path, &mut ctx)?;
                while injector.step()? {}
                Ok(())
            }
        });

        Ok(Self {
            client,
            db_path,
            current_frame_no_notifier,
            allow_replica_overwrite,
            meta,
            frames_sender,
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            self.try_perform_handshake().await?;

            if let Err(e) = self.replicate().await {
                // Replication encountered an error. We log the error, and then shut down the
                // injector and propagate a potential panic from there.
                tracing::warn!("replication error: {e}");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn try_perform_handshake(&mut self) -> anyhow::Result<()> {
        let mut error_printed = false;
        for _ in 0..HANDSHAKE_MAX_RETRIES {
            tracing::info!("Attempting to perform handshake with primary.");
            match self.client.hello(HelloRequest {}).await {
                Ok(resp) => {
                    let hello = resp.into_inner();
                    return tokio::task::block_in_place(|| {
                        let mut lock = self.meta.lock();
                        let meta = match *lock {
                            Some(meta) => match meta.merge_from_hello(hello) {
                                Ok(meta) => meta,
                                Err(e @ ReplicationError::Lagging) => {
                                    tracing::error!(
                                        "Replica ahead of primary: hard-reseting replica"
                                    );
                                    HARD_RESET.notify_waiters();

                                    anyhow::bail!(e);
                                }
                                Err(e @ ReplicationError::DbIncompatible)
                                    if self.allow_replica_overwrite =>
                                {
                                    tracing::error!("Primary is attempting to replicate a different database, overwriting replica.");
                                    HARD_RESET.notify_waiters();

                                    anyhow::bail!(e);
                                }
                                Err(e) => anyhow::bail!(e),
                            },
                            None => WalIndexMeta::new_from_hello(hello)?,
                        };

                        *lock = Some(meta);

                        Ok(())
                    });
                }
                Err(e) if !error_printed => {
                    tracing::error!("error connecting to primary. retrying. error: {e}");
                    error_printed = true;
                }
                _ => (),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        bail!("couldn't connect to primary after {HANDSHAKE_MAX_RETRIES} tries.");
    }

    async fn replicate(&mut self) -> anyhow::Result<()> {
        const MAX_REPLICA_REPLICATION_BUFFER_LEN: usize = 10_000_000 / 4096; // ~10MB
        let offset = LogOffset {
            // if current == FrameNo::Max then it means that we're starting fresh
            next_offset: self.next_offset(),
        };
        let mut stream = self.client.log_entries(offset).await?.into_inner();

        let mut buffer = Vec::new();
        loop {
            match stream.next().await {
                Some(Ok(frame)) => {
                    let frame = Frame::try_from_bytes(frame.data)?;
                    buffer.push(frame.clone());
                    if frame.header().size_after != 0
                        || buffer.len() > MAX_REPLICA_REPLICATION_BUFFER_LEN
                    {
                        let _ = self
                            .frames_sender
                            .send(Frames::Vec(std::mem::take(&mut buffer)))
                            .await;
                    }
                }
                Some(Err(err))
                    if err.code() == tonic::Code::FailedPrecondition
                        && err.message() == NEED_SNAPSHOT_ERROR_MSG =>
                {
                    tracing::debug!("loading snapshot");
                    // remove any outstanding frames in the buffer that are not part of a
                    // transaction: they are now part of the snapshot.
                    buffer.clear();
                    self.load_snapshot().await?;
                }
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            }
        }
    }

    async fn load_snapshot(&mut self) -> anyhow::Result<()> {
        let next_offset = self.next_offset();
        let frames = self
            .client
            .snapshot(LogOffset { next_offset })
            .await?
            .into_inner();

        let stream = frames.map(|data| match data {
            Ok(frame) => Frame::try_from_bytes(frame.data),
            Err(e) => anyhow::bail!(e),
        });
        let snap = TempSnapshot::from_stream(&self.db_path, stream).await?;

        let _ = self.frames_sender.send(Frames::Snapshot(snap)).await;

        Ok(())
    }

    fn next_offset(&mut self) -> FrameNo {
        self.current_frame_no().map(|x| x + 1).unwrap_or(0)
    }

    fn current_frame_no(&mut self) -> Option<FrameNo> {
        let current = *self.current_frame_no_notifier.borrow_and_update();
        (current != FrameNo::MAX).then_some(current)
    }
}
