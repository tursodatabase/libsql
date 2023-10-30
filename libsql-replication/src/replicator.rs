use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt};

use crate::frame::{Frame, FrameNo};
use crate::injector::Injector;
use crate::rpc::replication::{Frame as RpcFrame, NEED_SNAPSHOT_ERROR_MSG};

pub use tokio_util::either::Either;

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
    #[error("Replication meta error: {0}")]
    Meta(#[from] super::meta::Error),
    #[error("Hanshake required")]
    NoHandshake,
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
    /// Returns the currently committed replication index
    fn committed_frame_no(&self) -> Option<FrameNo>;
}

#[async_trait::async_trait]
impl<A, B> ReplicatorClient for Either<A, B>
where
    A: ReplicatorClient + Send,
    B: ReplicatorClient + Send,
{
    type FrameStream = Either<A::FrameStream, B::FrameStream>;

    async fn handshake(&mut self) -> Result<(), Error> {
        match self {
            Either::Left(a) => a.handshake().await,
            Either::Right(b) => b.handshake().await,
        }
    }
    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
        match self {
            Either::Left(a) => a.next_frames().await.map(Either::Left),
            Either::Right(b) => b.next_frames().await.map(Either::Right),
        }
    }
    /// Return a snapshot for the current replication index. Called after next_frame has returned a
    /// NeedSnapshot error
    async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
        match self {
            Either::Left(a) => a.snapshot().await.map(Either::Left),
            Either::Right(b) => b.snapshot().await.map(Either::Right),
        }
    }
    /// set the new commit frame_no
    async fn commit_frame_no(&mut self, frame_no: FrameNo) -> Result<(), Error> {
        match self {
            Either::Left(a) => a.commit_frame_no(frame_no).await,
            Either::Right(b) => b.commit_frame_no(frame_no).await,
        }
    }

    fn committed_frame_no(&self) -> Option<FrameNo> {
        match self {
            Either::Left(a) => a.committed_frame_no(),
            Either::Right(b) => b.committed_frame_no(),
        }
    }
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
    /// Creates a repicator for the db file pointed at by `db_path`
    pub async fn new(client: C, db_path: PathBuf, auto_checkpoint: u32) -> Result<Self, Error> {
        let injector = {
            let db_path = db_path.clone();
            spawn_blocking(move || {
                Injector::new(db_path, INJECTOR_BUFFER_CAPACITY, auto_checkpoint)
            })
            .await??
        };

        Ok(Self {
            client,
            injector: Arc::new(Mutex::new(injector)),
            has_handshake: false,
        })
    }

    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }

    /// Runs replicate in a loop until an error is returned
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

    pub async fn replicate(&mut self) -> Result<(), Error> {
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
                    match self.load_snapshot().await {
                        Ok(()) => (),
                        Err(Error::NoHandshake) => {
                            self.has_handshake = false;
                            self.try_perform_handshake().await?;
                        }
                        Err(e) => return Err(e),
                    }
                }
                Some(Err(Error::NoHandshake)) => {
                    tracing::debug!("session expired, new handshake required");
                    self.has_handshake = false;
                    self.try_perform_handshake().await?;
                }
                Some(Err(e)) => return Err(e),
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

    pub async fn flush(&mut self) -> Result<(), Error> {
        let injector = self.injector.clone();
        match spawn_blocking(move || injector.lock().flush()).await? {
            Ok(Some(commit_fno)) => {
                self.client.commit_frame_no(commit_fno).await?;
            }
            Ok(None) => (),
            Err(e) => Err(e)?,
        }

        Ok(())
    }
}

/// Helper function to convert rpc frames results to replicator frames
pub fn map_frame_err(f: Result<RpcFrame, tonic::Status>) -> Result<Frame, Error> {
    match f {
        Ok(frame) => Ok(Frame::try_from(&*frame.data).map_err(|e| Error::Client(e.into()))?),
        Err(err)
            if err.code() == tonic::Code::FailedPrecondition
                && err.message() == NEED_SNAPSHOT_ERROR_MSG =>
        {
            Err(Error::NeedSnapshot)
        }
        Err(err) => Err(Error::Client(err.into()))?,
    }
}
