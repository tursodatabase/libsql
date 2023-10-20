use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt};

use crate::frame::{Frame, FrameNo};
use crate::injector::Injector;

const HANDSHAKE_MAX_RETRIES: usize = 100;

type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Internal error: {0}")]
    Internal(BoxError),
    #[error("Injector error: {0}")]
    Injector(#[from] crate::injector::Error),
    #[error("Replicator client error: {0}")]
    Client(BoxError),
    #[error("Fatal replicator error: {0}")]
    Fatal(BoxError),
    #[error("Timeout performing handshake with primary")]
    PrimaryHandshakeTimeout,
    #[error("Replicator needs to load from snapshot")]
    NeedSnapshot,
}

impl From<tokio::task::JoinError> for Error {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::Internal(value.into())
    }
}

#[async_trait::async_trait]
pub trait ReplicatorClient {
    type FrameStream: Stream<Item = Result<Frame, Error>> + Unpin + Send;

    /// Perform handshake with remote
    async fn handshake(&mut self) -> Result<(), Error>;
    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self) -> Result<Self::FrameStream, Error>;
    /// Return a snapshot for the current replication index. Called after next_frame has returned a
    /// NeedSnapshot error
    async fn snapshot(&mut self) -> Result<Self::FrameStream, Error>;
    /// set the new commit frame_no
    async fn commit_frame_no(&mut self, frame_no: FrameNo) -> Result<(), Error>;
}

/// The `Replicator`'s duty is to download frames from the primary, and pass them to the injector at
/// transaction boundaries.
pub struct Replicator<C> {
    client: C,
    injector: Arc<Mutex<Injector>>,
    has_handshake: bool,
}

const INJECTOR_BUFFER_CAPACITY: usize = 10;

impl<C: ReplicatorClient> Replicator<C> {
    pub async fn new(client: C, db_path: PathBuf, auto_checkpoint: u32) -> Result<Self, Error> {
        let injector = {
            let db_path = db_path.clone();
            spawn_blocking(move || {
                Injector::new(&db_path, INJECTOR_BUFFER_CAPACITY, auto_checkpoint)
            })
            .await??
        };

        Ok(Self {
            client,
            injector: Arc::new(Mutex::new(injector)),
            has_handshake: false,
        })
    }

    pub async fn run(&mut self) -> Error {
        loop {
            if let Err(e) = self.replicate().await {
                // Replication encountered an error. We log the error, and then shut down the
                // injector and propagate a potential panic from there.
                self.has_handshake = false;
                return e;
            }
        }
    }

    async fn try_perform_handshake(&mut self) -> Result<(), Error> {
        let mut error_printed = false;
        for _ in 0..HANDSHAKE_MAX_RETRIES {
            tracing::info!("Attempting to perform handshake with primary.");
            match self.client.handshake().await {
                Ok(_) => {
                    self.has_handshake = true;
                    return Ok(());
                }
                Err(e @ Error::Fatal(_)) => return Err(e),
                Err(e) if !error_printed => {
                    tracing::error!("error connecting to primary. retrying. error: {e}");
                    error_printed = true;
                }
                _ => (),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Err(Error::PrimaryHandshakeTimeout)
    }

    async fn replicate(&mut self) -> Result<(), Error> {
        if !self.has_handshake {
            self.try_perform_handshake().await?;
        }
        let mut stream = self.client.next_frames().await?;
        loop {
            match stream.next().await {
                Some(Ok(frame)) => {
                    self.inject_frame(frame).await?;
                }
                Some(Err(Error::NeedSnapshot)) => {
                    tracing::debug!("loading snapshot");
                    // remove any outstanding frames in the buffer that are not part of a
                    // transaction: they are now part of the snapshot.
                    self.load_snapshot().await?;
                }
                Some(Err(e)) => return Err(e.into()),
                None => return Ok(()),
            }
        }
    }

    async fn load_snapshot(&mut self) -> Result<(), Error> {
        self.injector.lock().clear_buffer();
        let mut stream = self.client.snapshot().await?;
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            self.inject_frame(frame).await?;
        }

        Ok(())
    }

    async fn inject_frame(&mut self, frame: Frame) -> Result<(), Error> {
        let injector = self.injector.clone();
        match spawn_blocking(move || injector.lock().inject_frame(frame)).await? {
            Ok(Some(commit_fno)) => {
                self.client.commit_frame_no(commit_fno).await?;
            }
            Ok(None) => (),
            Err(e) => Err(e)?,
        }

        Ok(())
    }
}
