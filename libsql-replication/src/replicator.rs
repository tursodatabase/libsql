use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::Duration;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Status};

use crate::frame::{Frame, FrameNo};
use crate::injector::Injector;
use crate::meta::WalIndexMeta;
use crate::rpc::replication::{
    Frame as RpcFrame, HelloResponse, NAMESPACE_DOESNT_EXIST, NEED_SNAPSHOT_ERROR_MSG,
    NO_HELLO_ERROR_MSG,
};

pub use tokio_util::either::Either;

const HANDSHAKE_MAX_RETRIES: usize = 100;

pub static USE_REPLICATION_V2: Lazy<bool> =
    Lazy::new(|| std::env::var("LIBSQL_REPLICATION_V2").is_ok());

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
    type FrameStream: Stream<Item = Result<Frame, Error>> + Unpin + Send;

    /// Perform handshake with remote
    async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error>;
    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self, next_frame_no: FrameNo) -> Result<Self::FrameStream, Error>;
    /// Return a snapshot for the current replication index. Called after next_frame has returned a
    /// NeedSnapshot error
    async fn snapshot(&mut self, next_frame_no: FrameNo) -> Result<Self::FrameStream, Error>;
}

#[async_trait::async_trait]
impl<A, B> ReplicatorClient for Either<A, B>
where
    A: ReplicatorClient + Send,
    B: ReplicatorClient + Send,
{
    type FrameStream = Either<A::FrameStream, B::FrameStream>;

    async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
        match self {
            Either::Left(a) => a.handshake().await,
            Either::Right(b) => b.handshake().await,
        }
    }
    /// Return a stream of frames to apply to the database
    async fn next_frames(&mut self, next_frame: FrameNo) -> Result<Self::FrameStream, Error> {
        match self {
            Either::Left(a) => a.next_frames(next_frame).await.map(Either::Left),
            Either::Right(b) => b.next_frames(next_frame).await.map(Either::Right),
        }
    }
    /// Return a snapshot for the current replication index. Called after next_frame has returned a
    /// NeedSnapshot error
    async fn snapshot(&mut self, next_frame: FrameNo) -> Result<Self::FrameStream, Error> {
        match self {
            Either::Left(a) => a.snapshot(next_frame).await.map(Either::Left),
            Either::Right(b) => b.snapshot(next_frame).await.map(Either::Right),
        }
    }
}

/// The `Replicator`'s duty is to download frames from the primary, and pass them to the injector at
/// transaction boundaries.
pub struct Replicator<C> {
    client: C,
    injector: Arc<Mutex<Injector>>,
    state: ReplicatorState,
    errors_in_a_row: usize,
    commit_index: FrameNo,
    last_injected: FrameNo,
    meta: Option<WalIndexMeta>,
    on_commit: Box<dyn Fn(FrameNo) + Sync + Send + 'static>,
}

impl<C: fmt::Debug> fmt::Debug for Replicator<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Replicator")
            .field("client", &self.client)
            .field("injector", &self.injector)
            .field("state", &self.state)
            .field("errors_in_a_row", &self.errors_in_a_row)
            .field("commit_index", &self.commit_index)
            .field("last_injected", &self.last_injected)
            .field("meta", &self.meta)
            .field("on_commit", &"<fn>")
            .finish()
    }
}

const INJECTOR_BUFFER_CAPACITY: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicatorState {
    NeedFrames,
    NeedHandshake,
    NeedSnapshot,
    Exit,
}

impl<C: ReplicatorClient> Replicator<C> {
    /// Creates a repicator for the db file pointed at by `db_path`
    pub async fn new(
        client: C,
        db_path: PathBuf,
        auto_checkpoint: u32,
        on_commit: impl Fn(FrameNo) + Sync + Send + 'static,
        encryption_key: Option<bytes::Bytes>,
    ) -> Result<Self, Error> {
        dbg!();
        let (injector, commit_index, meta) = {
            let db_path = db_path.clone();
            spawn_blocking(move || -> Result<_, Error> {
                dbg!();
                let injector = Injector::new(&db_path, INJECTOR_BUFFER_CAPACITY, auto_checkpoint, encryption_key)?;
                if *USE_REPLICATION_V2 {
                    dbg!();
                    let commit_index = injector.get_replication_index()?;
                    dbg!(commit_index);
                    Ok((injector, commit_index, None))
                } else {
                    dbg!();
                    let meta = tokio::runtime::Handle::current()
                        .block_on(WalIndexMeta::open(db_path.parent().unwrap()))?;
                    let commit_index = meta.current_frame_no().unwrap_or(0);
                    dbg!();
                    Ok((injector, commit_index, Some(meta)))
                }
            })
            .await??
        };

        (on_commit)(commit_index);

        Ok(Self {
            client,
            injector: Arc::new(Mutex::new(injector)),
            state: ReplicatorState::NeedHandshake,
            errors_in_a_row: 0,
            commit_index,
            last_injected: commit_index,
            meta,
            on_commit: Box::new(on_commit),
        })
    }

    async fn update_commit_index(&mut self, commit_index: FrameNo) -> Result<(), Error> {
        if let Some(meta) = self.meta.as_mut() {
            meta.set_commit_frame_no(commit_index).await?;
        }

        self.commit_index = commit_index;
        (self.on_commit)(self.commit_index);

        Ok(())
    }

    pub fn current_commit_index(&self) -> FrameNo {
        self.commit_index
    }

    /// for a handshake on next call to replicate.
    pub fn force_handshake(&mut self) {
        self.state = ReplicatorState::NeedHandshake;
    }

    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }

    /// Runs replicate in a loop until an error is returned
    pub async fn run(&mut self) -> Error {
        loop {
            if let Err(e) = self.replicate().await {
                self.errors_in_a_row += 1;
                // If too many error occur, upgrade the error to a fatal error
                if self.errors_in_a_row > 10 {
                    return Error::Fatal(e.into());
                }
                return e;
            } else {
                self.errors_in_a_row = 0;
            }
        }
    }

    async fn handle_hello_response(&mut self, hello: HelloResponse) -> Result<(), Error> {
        match self.meta.as_mut() {
            Some(meta) => {
                match meta.init_from_hello(hello) {
                    Ok(()) => {
                        meta.flush().await?;
                    }
                    Err(crate::meta::Error::LogIncompatible) => {
                        // The logs are incompatible; start replicating from scratch again
                        self.commit_index = 0;
                        self.last_injected = self.commit_index;
                    }
                    Err(e) => return Err(Error::Meta(e)),
                }
            }
            None => {
                // do nothing?
            }
        }

        self.state = ReplicatorState::NeedFrames;

        Ok(())
    }

    pub async fn try_perform_handshake(&mut self) -> Result<usize, Error> {
        let mut error_printed = false;
        for _ in 0..HANDSHAKE_MAX_RETRIES {
            tracing::info!("Attempting to perform handshake with primary.");
            match self.client.handshake().await {
                Ok(Some(hello)) => {
                    self.handle_hello_response(hello).await?;
                    return Ok(0);
                }
                Ok(None) => {
                    if let Some(ref mut meta) = self.meta {
                        // init dummy meta
                        meta.init_default();
                        meta.flush().await?;
                    }
                    // yolo
                    self.state = ReplicatorState::NeedFrames;
                    return Ok(0);
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

    pub async fn replicate(&mut self) -> Result<usize, Error> {
        let mut count_frames = 0;
        loop {
            count_frames += self.try_replicate_step().await?;
            if self.state == ReplicatorState::Exit {
                self.state = ReplicatorState::NeedFrames;
                return Ok(count_frames);
            }
        }
    }

    async fn try_replicate_step(&mut self) -> Result<usize, Error> {
        let state = self.state;
        let ret = match state {
            ReplicatorState::NeedHandshake => self.try_perform_handshake().await,
            ReplicatorState::NeedFrames => self.try_replicate().await,
            ReplicatorState::NeedSnapshot => self.load_snapshot().await,
            ReplicatorState::Exit => unreachable!("trying to step replicator on exit"),
        };

        // in case of error we rollback the current injector transaction, and start over from last
        // commit
        if ret.is_err() {
            self.last_injected = self.commit_index;
            self.injector.lock().rollback();
        }

        let mut count_frames = 0;
        self.state = match ret {
            // perform normal operation state transition
            Ok(n) => {
                count_frames += n;
                match state {
                    ReplicatorState::Exit => unreachable!(),
                    ReplicatorState::NeedFrames => ReplicatorState::Exit,
                    ReplicatorState::NeedSnapshot | ReplicatorState::NeedHandshake => {
                        ReplicatorState::NeedFrames
                    }
                }
            }
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

        Ok(count_frames)
    }

    async fn try_replicate(&mut self) -> Result<usize, Error> {
        let mut stream = self.client.next_frames(&self.last_injected + 1).await?;
        let mut count_frames = 0;

        while let Some(frame) = stream.next().await.transpose()? {
            self.inject_frame(frame).await?;
            count_frames += 1;
        }

        Ok(count_frames)
    }

    async fn load_snapshot(&mut self) -> Result<usize, Error> {
        {
            // we load the snapshot from the last commit index.
            let mut injector = self.injector.lock();
            injector.clear_buffer();
            injector.rollback();
            self.last_injected = self.commit_index;
        }

        let mut stream = self.client.snapshot(self.commit_index + 1).await?;
        let mut count_frames = 0;
        while let Some(frame) = stream.next().await {
            let frame = frame?;
            self.inject_frame(frame).await?;
            count_frames += 1;
        }

        Ok(count_frames)
    }

    async fn inject_frame(&mut self, frame: Frame) -> Result<Option<FrameNo>, Error> {
        let injector = self.injector.clone();
        let frame_no = frame.header().frame_no.get();

        let ret = match spawn_blocking(move || injector.lock().inject_frame(frame)).await? {
            Ok(Some(commit_fno)) => {
                self.update_commit_index(commit_fno).await?;
                Ok(Some(commit_fno))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e)?,
        };

        self.last_injected = self.last_injected.max(frame_no);

        ret
    }

    pub async fn flush(&mut self) -> Result<(), Error> {
        let injector = self.injector.clone();
        match spawn_blocking(move || injector.lock().flush()).await? {
            Ok(Some(commit_index)) => {
                self.update_commit_index(commit_index).await?;
            }
            Ok(None) => (),
            Err(e) => Err(e)?,
        }

        Ok(())
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
    use tempfile::tempdir;

    use crate::frame::{FrameBorrowed, FrameMut};

    use super::*;

    #[tokio::test]
    async fn handshake_error_namespace_doesnt_exist() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        struct Client;

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                Err(Error::NamespaceDoesntExist)
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
        }

        let mut replicator = Replicator::new(Client, tmp.path().to_path_buf(), 10000, |_| (), None)
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
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                Err(Error::NoHandshake)
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
        }

        let mut replicator = Replicator::new(Client, tmp.path().to_path_buf(), 10000, |_| (), None)
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
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                Ok(Box::pin(stream! {
                    yield Err(Error::NoHandshake);
                }))
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
        }

        let mut replicator = Replicator::new(Client, tmp.path().to_path_buf(), 10000, |_| (), None)
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
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                Ok(Box::pin(stream! {
                    yield Err(Error::NeedSnapshot);
                }))
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
        }

        let mut replicator = Replicator::new(Client, tmp.path().to_path_buf(), 10000, |_| (), None)
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
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                Err(Error::NeedSnapshot)
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unreachable!()
            }
        }

        let mut replicator = Replicator::new(Client, tmp.path().to_path_buf(), 10000, |_| (), None)
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
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                Err(Error::NoHandshake)
            }
        }

        let mut replicator = Replicator::new(Client, tmp.path().to_path_buf(), 10000, |_| (), None)
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
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                unimplemented!()
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                Ok(Box::pin(stream! {
                    yield Err(Error::NoHandshake)
                }))
            }
        }

        let mut replicator = Replicator::new(Client, tmp.path().to_path_buf(), 10000, |_| (), None)
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
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                Err(Error::NoHandshake)
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
        }

        let mut replicator = Replicator::new(Client, tmp.path().to_path_buf(), 10000, |_| (), None)
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

        let tmp = tempdir().unwrap();

        #[derive(Debug)]
        struct Client {
            frames: Vec<Frame>,
            should_error: bool,
        }

        #[async_trait::async_trait]
        impl ReplicatorClient for Client {
            type FrameStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send + 'static>>;

            /// Perform handshake with remote
            async fn handshake(&mut self) -> Result<Option<HelloResponse>, Error> {
                Ok(None)
            }
            /// Return a stream of frames to apply to the database
            async fn next_frames(
                &mut self,
                _frame_no: FrameNo,
            ) -> Result<Self::FrameStream, Error> {
                if self.should_error {
                    let frames = self
                        .frames
                        .iter()
                        .take(2)
                        .cloned()
                        .map(Ok)
                        .chain(Some(Err(Error::Client("some client error".into()))))
                        .collect::<Vec<_>>();
                    Ok(Box::pin(tokio_stream::iter(frames)))
                } else {
                    let stream = tokio_stream::iter(self.frames.clone().into_iter().map(Ok));
                    Ok(Box::pin(stream))
                }
            }
            /// Return a snapshot for the current replication index. Called after next_frame has returned a
            /// NeedSnapshot error
            async fn snapshot(&mut self, _: FrameNo) -> Result<Self::FrameStream, Error> {
                unimplemented!()
            }
        }

        let client = Client {
            frames: make_wal_log(),
            should_error: true,
        };

        let mut replicator = Replicator::new(client, tmp.path().join("data"), 10000, |_| (), None)
            .await
            .unwrap();

        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedFrames);

        assert!(matches!(
            replicator.try_replicate_step().await.unwrap_err(),
            Error::Client(_)
        ));
        assert!(!replicator.injector.lock().is_txn());
        assert_eq!(replicator.current_commit_index(), 0);
        assert_eq!(replicator.state, ReplicatorState::NeedHandshake);

        replicator.try_replicate_step().await.unwrap();
        assert_eq!(replicator.state, ReplicatorState::NeedFrames);

        replicator.client_mut().should_error = false;

        replicator.try_replicate_step().await.unwrap();
        assert!(!replicator.injector.lock().is_txn());
        assert_eq!(replicator.state, ReplicatorState::Exit);
        assert_eq!(replicator.current_commit_index(), 6);
    }
}
