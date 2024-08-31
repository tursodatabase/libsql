use std::path::PathBuf;

use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Status};

use crate::frame::{Frame, FrameNo};
use crate::injector::{Injector, SqliteInjector};
use crate::rpc::replication::{
    Frame as RpcFrame, NAMESPACE_DOESNT_EXIST, NEED_SNAPSHOT_ERROR_MSG, NO_HELLO_ERROR_MSG,
};

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
    #[error("Snapshot not ready yet")]
    SnapshotPending,
    #[error("Replication meta error: {0}")]
    Meta(#[from] super::meta::Error),
    #[error("Handshake required")]
    NoHandshake,
    #[error("Requested namespace doesn't exist")]
    NamespaceDoesntExist,
}

impl From<Status> for Error {
    fn from(status: Status) -> Self {
        if status.code() == Code::FailedPrecondition {
            match status.message() {
                NEED_SNAPSHOT_ERROR_MSG => Error::NeedSnapshot,
                NO_HELLO_ERROR_MSG => Error::NoHandshake,
                NAMESPACE_DOESNT_EXIST => Error::NamespaceDoesntExist,
                _ => Error::Client(status.into()),
            }
        } else {
            Error::Client(status.into())
        }
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::Internal(value.into())
    }
}

#[async_trait::async_trait]
pub trait ReplicatorClient {
    type FrameStream: Stream<Item = Result<RpcFrame, Error>> + Unpin + Send;

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
    /// rollback the client to previously committed index.
    fn rollback(&mut self);
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

    fn rollback(&mut self) {
        match self {
            Either::Left(a) => a.rollback(),
            Either::Right(b) => b.rollback(),
        }
    }
}

/// The `Replicator`'s duty is to download frames from the primary, and pass them to the injector at
/// transaction boundaries.
pub struct Replicator<C, I> {
    client: C,
    injector: I,
    state: ReplicatorState,
    frames_synced: usize,
    max_handshake_retries: usize,
}

const INJECTOR_BUFFER_CAPACITY: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicatorState {
    NeedFrames,
    NeedHandshake,
    NeedSnapshot,
    Exit,
}

impl<C> Replicator<C, SqliteInjector>
where
    C: ReplicatorClient,
{
    /// Creates a replicator for the db file pointed at by `db_path`
    pub async fn new_sqlite(
        client: C,
        db_path: PathBuf,
        auto_checkpoint: u32,
        encryption_config: Option<libsql_sys::EncryptionConfig>,
    ) -> Result<Self, Error> {
        let injector = SqliteInjector::new(
            db_path.clone(),
            INJECTOR_BUFFER_CAPACITY,
            auto_checkpoint,
            encryption_config,
        )
        .await?;

        Ok(Self::new(client, injector))
    }
}

impl<C, I> Replicator<C, I>
where
    C: ReplicatorClient,
    I: Injector,
{
    pub fn new(client: C, injector: I) -> Self {
        Self {
            client,
            injector,
            state: ReplicatorState::NeedHandshake,
            frames_synced: 0,
            max_handshake_retries: HANDSHAKE_MAX_RETRIES,
        }
    }

    /// force a handshake on next call to replicate.
    pub fn force_handshake(&mut self) {
        self.state = ReplicatorState::NeedHandshake;
    }

    /// configure number of handshake retries.
    pub fn set_primary_handshake_retries(&mut self, retries: usize) {
        self.max_handshake_retries = retries;
    }

    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }

    /// Runs replicate in a loop until an error is returned
    pub async fn run(&mut self) -> Error {
        loop {
            if let Err(e) = self.replicate().await {
                return e;
            }
        }
    }

    pub async fn try_perform_handshake(&mut self) -> Result<(), Error> {
        let mut error_printed = false;
        for _ in 0..self.max_handshake_retries {
            tracing::debug!("Attempting to perform handshake with primary.");
            match self.client.handshake().await {
                Ok(_) => {
                    self.state = ReplicatorState::NeedFrames;
                    return Ok(());
                }
                Err(Error::Client(e)) if !error_printed => {
                    if e.downcast_ref::<uuid::Error>().is_some() {
                        tracing::error!("error connecting to primary. retrying. Verify that the libsql server version is `>=0.22` error: {e}");
                    } else {
                        tracing::error!("error connecting to primary. retrying. error: {e}");
                    }

                    error_printed = true;
                }
                Err(Error::Client(_)) if error_printed => (),
                Err(e) => return Err(e),
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Err(Error::PrimaryHandshakeTimeout)
    }

    pub async fn replicate(&mut self) -> Result<(), Error> {
        loop {
            self.try_replicate_step().await?;
            if self.state == ReplicatorState::Exit {
                self.state = ReplicatorState::NeedFrames;
                return Ok(());
            }
        }
    }

    async fn try_replicate_step(&mut self) -> Result<(), Error> {
        let state = self.state;
        let ret = match state {
            ReplicatorState::NeedHandshake => self.try_perform_handshake().await,
            ReplicatorState::NeedFrames => self.try_replicate().await,
            ReplicatorState::NeedSnapshot => self.load_snapshot().await,
            ReplicatorState::Exit => unreachable!("trying to step replicator on exit"),
        };

        // in case of error we rollback the current injector transaction, and start over.
        if ret.is_err() {
            self.client.rollback();
            self.injector.rollback().await;
        }

        self.state = match ret {
            // perform normal operation state transition
            Ok(()) => match state {
                ReplicatorState::Exit => unreachable!(),
                ReplicatorState::NeedFrames => ReplicatorState::Exit,
                ReplicatorState::NeedSnapshot | ReplicatorState::NeedHandshake => {
                    ReplicatorState::NeedFrames
                }
            },
            Err(Error::NoHandshake) => {
                if state == ReplicatorState::NeedHandshake {
                    return Err(Error::Fatal(
                        "Received handshake error while performing handshake".into(),
                    ));
                }
                ReplicatorState::NeedHandshake
            }
            Err(Error::NeedSnapshot) => ReplicatorState::NeedSnapshot,
            Err(e) => {
                // an error here could be due to a disconnection, it's safe to rollback to a
                // NeedHandshake state again, to avoid entering a busy loop.
                self.state = ReplicatorState::NeedHandshake;
                return Err(e);
            }
        };

        Ok(())
    }

    async fn try_replicate(&mut self) -> Result<(), Error> {
        let mut stream = self.client.next_frames().await?;

        while let Some(frame) = stream.next().await.transpose()? {
            self.inject_frame(frame).await?;
        }

        Ok(())
    }

    async fn load_snapshot(&mut self) -> Result<(), Error> {
        self.client.rollback();
        self.injector.rollback().await;
        loop {
            match self.client.snapshot().await {
                Ok(mut stream) => {
                    while let Some(frame) = stream.next().await {
                        let frame = frame?;
                        self.inject_frame(frame).await?;
                    }
                    return Ok(());
                }
                Err(Error::SnapshotPending) => {
                    tracing::info!("snapshot not ready yet, waiting 1s...");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn inject_frame(&mut self, frame: RpcFrame) -> Result<(), Error> {
        self.frames_synced += 1;

        if let Some(frame_no) = frame.durable_frame_no {
            self.injector.durable_frame_no(frame_no);
        }

        match self.injector.inject_frame(frame).await? {
            Some(commit_fno) => {
                self.client.commit_frame_no(commit_fno).await?;
            }
            None => (),
        }

        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        match self.injector.flush().await? {
            Some(commit_fno) => {
                self.client.commit_frame_no(commit_fno).await?;
            }
            None => (),
        }

        Ok(())
    }

    pub fn frames_synced(&self) -> usize {
        self.frames_synced
    }
}

/// Helper function to convert rpc frames results to replicator frames
pub fn map_frame_err(f: Result<RpcFrame, Status>) -> Result<Frame, Error> {
    let frame = f?;
    Frame::try_from(&*frame.data).map_err(|e| Error::Client(e.into()))
}

#[cfg(test)]
mod test {
    use std::{mem::size_of, pin::Pin};

    use async_stream::stream;

    use crate::frame::{FrameBorrowed, FrameMut};
    use crate::rpc::replication::Frame as RpcFrame;

    use super::*;

    #[tokio::test]
    async fn handshake_error_namespace_doesnt_exist() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                Err(Error::NamespaceDoesntExist)
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, _frame_no: FrameNo) -> Result<(), Error> {
                unreachable!()
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unreachable!()
            }

            fn rollback(&mut self) {}
        }

        let mut replicator = Replicator::new_sqlite(Client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();

        assert!(matches!(
            replicator.try_replicate_step().await.unwrap_err(),
            Error::NamespaceDoesntExist
        ));
    }

    #[tokio::test]
    async fn no_handshake_error_in_next_frame() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                Err(Error::NoHandshake)
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, _frame_no: FrameNo) -> Result<(), Error> {
                unreachable!()
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unreachable!()
            }
            fn rollback(&mut self) {}
        }

        let mut replicator = Replicator::new_sqlite(Client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();
        // we assume that we already received the handshake and the handshake is not valid anymore
        replicator.state = ReplicatorState::NeedFrames;
        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedHandshake);
    }

    #[tokio::test]
    async fn stream_frame_returns_handshake_error() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                Ok(Box::pin(stream! {
                    yield Err(Error::NoHandshake);
                }))
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, _frame_no: FrameNo) -> Result<(), Error> {
                unreachable!()
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unreachable!()
            }
            fn rollback(&mut self) {}
        }

        let mut replicator = Replicator::new_sqlite(Client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();
        // we assume that we already received the handshake and the handshake is not valid anymore
        replicator.state = ReplicatorState::NeedFrames;
        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedHandshake);
    }

    #[tokio::test]
    async fn stream_frame_returns_need_snapshot() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                Ok(Box::pin(stream! {
                    yield Err(Error::NeedSnapshot);
                }))
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, _frame_no: FrameNo) -> Result<(), Error> {
                unreachable!()
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unreachable!()
            }
            fn rollback(&mut self) {}
        }

        let mut replicator = Replicator::new_sqlite(Client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();
        // we assume that we already received the handshake and the handshake is not valid anymore
        replicator.state = ReplicatorState::NeedFrames;
        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedSnapshot);
    }

    #[tokio::test]
    async fn next_frames_returns_need_snapshot() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                Err(Error::NeedSnapshot)
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, _frame_no: FrameNo) -> Result<(), Error> {
                unreachable!()
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unreachable!()
            }
            fn rollback(&mut self) {}
        }

        let mut replicator = Replicator::new_sqlite(Client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();
        // we assume that we already received the handshake and the handshake is not valid anymore
        replicator.state = ReplicatorState::NeedFrames;
        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedSnapshot);
    }

    #[tokio::test]
    async fn load_snapshot_returns_need_handshake() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                Err(Error::NoHandshake)
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, _frame_no: FrameNo) -> Result<(), Error> {
                unreachable!()
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unreachable!()
            }
            fn rollback(&mut self) {}
        }

        let mut replicator = Replicator::new_sqlite(Client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();
        replicator.state = ReplicatorState::NeedSnapshot;
        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedHandshake);
    }

    #[tokio::test]
    async fn load_snapshot_stream_returns_need_handshake() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                Ok(Box::pin(stream! {
                    yield Err(Error::NoHandshake)
                }))
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, _frame_no: FrameNo) -> Result<(), Error> {
                unreachable!()
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unreachable!()
            }
            fn rollback(&mut self) {}
        }

        let mut replicator = Replicator::new_sqlite(Client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();
        // we assume that we already received the handshake and the handshake is not valid anymore
        replicator.state = ReplicatorState::NeedSnapshot;
        replicator.try_replicate_step().await.unwrap();

        assert_eq!(replicator.state, ReplicatorState::NeedHandshake);
    }

    #[tokio::test]
    async fn receive_handshake_error_while_handshaking() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                Err(Error::NoHandshake)
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, _frame_no: FrameNo) -> Result<(), Error> {
                unreachable!()
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unreachable!()
            }
            fn rollback(&mut self) {}
        }

        let mut replicator = Replicator::new_sqlite(Client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();
        replicator.state = ReplicatorState::NeedHandshake;
        assert!(matches!(
            replicator.try_replicate_step().await.unwrap_err(),
            Error::Fatal(_)
        ));
    }

    #[tokio::test]
    async fn transaction_interupted_by_error_and_resumed() {
        /// this this is generated by creating a table test, inserting 5 rows into it, and then
        /// truncating the wal file of it's header.
        const WAL: &[u8] = include_bytes!("../assets/test/test_wallog");

        fn make_wal_log() -> Vec<Frame> {
            let mut frames = WAL
                .chunks(size_of::<FrameBorrowed>())
                .map(|b| FrameMut::try_from(b).unwrap())
                .map(|mut f| {
                    f.header_mut().size_after.set(0);
                    f
                })
                .collect::<Vec<_>>();

            let size_after = frames.len();
            frames.last_mut().unwrap().header_mut().size_after = (size_after as u32).into();

            frames.into_iter().map(Into::into).collect()
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();

        struct Client {
            frames: Vec<Frame>,
            should_error: bool,
            committed_frame_no: Option<FrameNo>,
        }

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream =
                Pin<Box<dyn Stream<Item = Result<RpcFrame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<(), Error> {
                Ok(())
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self) -> Result<Self::FrameStream, Error> {
                if self.should_error {
                    let frames = self
                        .frames
                        .iter()
                        .map(|f| RpcFrame {
                            data: f.bytes(),
                            timestamp: None,
                            durable_frame_no: None,
                        })
                        .take(2)
                        .map(Ok)
                        .chain(Some(Err(Error::Client("some client error".into()))))
                        .collect::<Vec<_>>();
                    Ok(Box::pin(tokio_stream::iter(frames)))
                } else {
                    let iter = self
                        .frames
                        .iter()
                        .map(|f| RpcFrame {
                            data: f.bytes(),
                            timestamp: None,
                            durable_frame_no: None,
                        })
                        .map(Ok)
                        .collect::<Vec<_>>();
                    Ok(Box::pin(tokio_stream::iter(iter)))
                }
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
            /// set the new commit frame_no
            async fn commit_frame_no(&mut self, frame_no: FrameNo) -> Result<(), Error> {
                self.committed_frame_no = Some(frame_no);
                Ok(())
            }
            /// Returns the currently committed replication index
            fn committed_frame_no(&self) -> Option<FrameNo> {
                unimplemented!()
            }
            fn rollback(&mut self) {}
        }

        let client = Client {
            frames: make_wal_log(),
            should_error: true,
            committed_frame_no: None,
        };

        let mut replicator = Replicator::new_sqlite(client, tmp.path().to_path_buf(), 10000, None)
            .await
            .unwrap();

        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedFrames);

        assert!(matches!(
            replicator.try_replicate_step().await.unwrap_err(),
            Error::Client(_)
        ));
        assert!(!replicator.injector.inner.lock().is_txn());
        assert!(replicator.client_mut().committed_frame_no.is_none());
        assert_eq!(replicator.state, ReplicatorState::NeedHandshake);

        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedFrames);

        replicator.client_mut().should_error = false;

        replicator.try_replicate_step().await.unwrap();
        assert!(!replicator.injector.inner.lock().is_txn());
        assert_eq!(replicator.state, ReplicatorState::Exit);
        assert_eq!(replicator.client_mut().committed_frame_no, Some(6));
    }
}
