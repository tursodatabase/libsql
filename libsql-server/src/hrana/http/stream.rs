use anyhow::{anyhow, Context, Result};
use base64::prelude::{Engine as _, BASE64_STANDARD_NO_PAD};
use hmac::Mac as _;
use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::collections::{HashMap, VecDeque};
use std::future::Future as _;
use std::pin::Pin;
use std::sync::Arc;
use std::{future, mem, task};
use tokio::time::{Duration, Instant};

use crate::connection::MakeConnection;
use crate::database::Connection;

use super::super::ProtocolError;
use super::Server;

/// Mutable state related to streams, owned by [`Server`] and protected with a mutex.
pub struct ServerStreamState {
    /// Map from stream ids to stream handles. The stream ids are random integers.
    handles: HashMap<u64, Handle>,
    /// Queue of streams ordered by the instant when they should expire. All these stream ids
    /// should refer to handles in the [`Handle::Available`] variant.
    expire_queue: PriorityQueue<u64, Reverse<Instant>>,
    /// Queue of expired streams that are still stored as [`Handle::Expired`], together with the
    /// instant when we should remove them completely.
    cleanup_queue: VecDeque<(u64, Instant)>,
    /// The timer that we use to wait for the next item in `expire_queue`.
    expire_sleep: Pin<Box<tokio::time::Sleep>>,
    /// A waker to wake up the task that expires streams from the `expire_queue`.
    expire_waker: Option<task::Waker>,
    /// See [`roundup_instant()`].
    expire_round_base: Instant,
}

/// Handle to a stream, owned by the [`ServerStreamState`].
#[derive(Debug)]
pub(crate) enum Handle {
    /// A stream that is open and ready to be used by requests. [`Stream::db`] should always be
    /// `Some`.
    Available(Box<Stream>),
    /// A stream that has been acquired by a request that hasn't finished processing. This will be
    /// replaced with `Available` when the request completes and releases the stream.
    Acquired,
    /// A stream that has been expired. This stream behaves as closed, but we keep this around for
    /// some time to provide a nicer error messages (i.e., if the stream is expired, we return a
    /// "stream expired" error rather than "invalid baton" error).
    Expired,
}

/// State of a Hrana-over-HTTP stream.
///
/// The stream is either owned by [`Handle::Available`] (when it's not in use) or by [`Guard`]
/// (when it's being used by a request).
#[derive(Debug)]
pub(crate) struct Stream {
    /// The database connection that corresponds to this stream. This is `None` after the `"close"`
    /// request was executed.
    pub(crate) db: Option<Arc<Connection>>,
    /// The cache of SQL texts stored on the server with `"store_sql"` requests.
    sqls: HashMap<i32, String>,
    /// Stream id of this stream. The id is generated randomly (it should be unguessable).
    stream_id: u64,
    /// Sequence number that is expected in the next baton. To make sure that clients issue stream
    /// requests sequentially, the baton returned from each HTTP request includes this sequence
    /// number, and the following HTTP request must show a baton with the same sequence number.
    baton_seq: u64,
}

/// Guard object that is used to access a stream from the outside. The guard makes sure that the
/// stream's entry in [`ServerStreamState::handles`] is either removed or replaced with
/// [`Handle::Available`] after the guard goes out of scope.
pub struct Guard<'srv> {
    server: &'srv Server,
    /// The guarded stream. This is only set to `None` in the destructor.
    stream: Option<Box<Stream>>,
    /// If set to `true`, the destructor will release the stream for further use (saving it as
    /// [`Handle::Available`] in [`ServerStreamState::handles`]. If false, the stream is removed on
    /// drop.
    release: bool,
}

/// An unrecoverable error that should close the stream. The difference from [`ProtocolError`] is
/// that a correct client may trigger this error, it does not mean that the protocol has been
/// violated.
#[derive(thiserror::Error, Debug)]
pub enum StreamError {
    #[error("The stream has expired due to inactivity")]
    StreamExpired,
}

impl ServerStreamState {
    pub fn new() -> Self {
        Self {
            handles: HashMap::new(),
            expire_queue: PriorityQueue::new(),
            cleanup_queue: VecDeque::new(),
            expire_sleep: Box::pin(tokio::time::sleep(Duration::ZERO)),
            expire_waker: None,
            expire_round_base: Instant::now(),
        }
    }

    pub(crate) fn handles(&self) -> &HashMap<u64, Handle> {
        &self.handles
    }
}

/// Acquire a guard to a new or existing stream. If baton is `Some`, we try to look up the stream,
/// otherwise we create a new stream.
pub async fn acquire<'srv>(
    server: &'srv Server,
    connection_maker: Arc<dyn MakeConnection<Connection = Connection>>,
    baton: Option<&str>,
) -> Result<Guard<'srv>> {
    let stream = match baton {
        Some(baton) => {
            let (stream_id, baton_seq) = decode_baton(server, baton)?;

            let mut state = server.stream_state.lock();
            let handle = state.handles.get_mut(&stream_id);

            match handle {
                None => {
                    return Err(ProtocolError::BatonInvalid)
                        .context(format!("Stream handle for {stream_id} was not found"));
                }
                Some(Handle::Acquired) => {
                    return Err(ProtocolError::BatonReused)
                        .context(format!("Stream handle for {stream_id} is acquired"));
                }
                Some(Handle::Expired) => {
                    return Err(StreamError::StreamExpired)
                        .context(format!("Stream handle for {stream_id} is expired"));
                }
                Some(Handle::Available(stream)) => {
                    if stream.baton_seq != baton_seq {
                        return Err(ProtocolError::BatonReused).context(format!(
                            "Expected baton seq {}, received {baton_seq}",
                            stream.baton_seq
                        ));
                    }
                }
            };

            let Handle::Available(mut stream) = mem::replace(handle.unwrap(), Handle::Acquired)
            else {
                unreachable!()
            };

            tracing::debug!("Stream {stream_id} was acquired with baton seq {baton_seq}");
            // incrementing the sequence number forces the next HTTP request to use a different
            // baton
            stream.baton_seq = stream.baton_seq.wrapping_add(1);
            unmark_expire(&mut state, stream.stream_id);
            stream
        }
        None => {
            let db = connection_maker.create().await?;

            let mut state = server.stream_state.lock();
            let stream = Box::new(Stream {
                db: Some(Arc::new(db)),
                sqls: HashMap::new(),
                stream_id: gen_stream_id(&mut state),
                // initializing the sequence number randomly makes it much harder to exploit
                // collisions in batons
                baton_seq: rand::random(),
            });
            state.handles.insert(stream.stream_id, Handle::Acquired);
            tracing::debug!(
                "Stream {} was created with baton seq {}",
                stream.stream_id,
                stream.baton_seq
            );
            stream
        }
    };
    Ok(Guard {
        server,
        stream: Some(stream),
        release: false,
    })
}

impl<'srv> Guard<'srv> {
    pub fn get_db(&self) -> Result<&Connection, ProtocolError> {
        let stream = self.stream.as_ref().unwrap();
        stream.db.as_deref().ok_or(ProtocolError::BatonStreamClosed)
    }

    pub fn get_db_owned(&self) -> Result<Arc<Connection>, ProtocolError> {
        let stream = self.stream.as_ref().unwrap();
        stream.db.clone().ok_or(ProtocolError::BatonStreamClosed)
    }

    /// Closes the database connection. The next call to [`Guard::release()`] will then remove the
    /// stream.
    pub fn close_db(&mut self) {
        let stream = self.stream.as_mut().unwrap();
        stream.db = None;
    }

    pub fn sqls(&self) -> &HashMap<i32, String> {
        &self.stream.as_ref().unwrap().sqls
    }

    pub fn sqls_mut(&mut self) -> &mut HashMap<i32, String> {
        &mut self.stream.as_mut().unwrap().sqls
    }

    /// Releases the guard and returns the baton that can be used to access this stream in the next
    /// HTTP request. Returns `None` if the stream has been closed (and thus cannot be accessed
    /// again).
    pub fn release(mut self) -> Option<String> {
        let stream = self.stream.as_ref().unwrap();
        if stream.db.is_some() {
            self.release = true; // tell destructor to make the stream available again
            Some(encode_baton(
                self.server,
                stream.stream_id,
                stream.baton_seq,
            ))
        } else {
            None
        }
    }
}

impl<'srv> Drop for Guard<'srv> {
    fn drop(&mut self) {
        let stream = self.stream.take().unwrap();
        let stream_id = stream.stream_id;

        let mut state = self.server.stream_state.lock();
        let Some(handle) = state.handles.remove(&stream_id) else {
            panic!(
                "Dropped a Guard for stream {stream_id}, \
                but Server does not contain a handle to it"
            );
        };
        if !matches!(handle, Handle::Acquired) {
            panic!(
                "Dropped a Guard for stream {stream_id}, \
                but Server contained handle that is not acquired"
            );
        }

        if self.release {
            state.handles.insert(stream_id, Handle::Available(stream));
            mark_expire(&mut state, stream_id);
            tracing::debug!("Stream {stream_id} was released for further use");
        } else {
            tracing::debug!("Stream {stream_id} was closed");
        }
    }
}

fn gen_stream_id(state: &mut ServerStreamState) -> u64 {
    for _ in 0..10 {
        let stream_id = rand::random();
        if !state.handles.contains_key(&stream_id) {
            return stream_id;
        }
    }
    panic!("Failed to generate a free stream id with rejection sampling")
}

/// Encodes the baton.
///
/// The baton is base64-encoded byte string that is composed from:
///
/// - payload (16 bytes):
///     - `stream_id` (8 bytes, big endian)
///     - `baton_seq` (8 bytes, big endian)
/// - MAC (32 bytes): an authentication code generated with HMAC-SHA256
///
/// The MAC is used to cryptographically verify that the baton was generated by this server. It is
/// unlikely that we ever issue the same baton twice, because there are 2^128 possible combinations
/// for payload (note that both `stream_id` and the initial `baton_seq` are generated randomly).
fn encode_baton(server: &Server, stream_id: u64, baton_seq: u64) -> String {
    let mut payload = [0; 16];
    payload[0..8].copy_from_slice(&stream_id.to_be_bytes());
    payload[8..16].copy_from_slice(&baton_seq.to_be_bytes());

    let mut hmac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&server.baton_key).unwrap();
    hmac.update(&payload);
    let mac = hmac.finalize().into_bytes();

    let mut baton_data = [0; 48];
    baton_data[0..16].copy_from_slice(&payload);
    baton_data[16..48].copy_from_slice(&mac);
    BASE64_STANDARD_NO_PAD.encode(baton_data)
}

/// Decodes a baton encoded with `encode_baton()` and returns `(stream_id, baton_seq)`. Always
/// returns a [`ProtocolError::BatonInvalid`] if the baton is invalid, but it attaches an anyhow
/// context that describes the precise cause.
fn decode_baton(server: &Server, baton_str: &str) -> Result<(u64, u64)> {
    let baton_data = BASE64_STANDARD_NO_PAD.decode(baton_str).map_err(|err| {
        anyhow!(ProtocolError::BatonInvalid)
            .context(format!("Could not base64-decode baton: {err}"))
    })?;

    if baton_data.len() != 48 {
        return Err(ProtocolError::BatonInvalid).context(format!(
            "Baton has invalid size of {} bytes",
            baton_data.len()
        ));
    }

    let payload = &baton_data[0..16];
    let received_mac = &baton_data[16..48];

    let mut hmac = hmac::Hmac::<sha2::Sha256>::new_from_slice(&server.baton_key).unwrap();
    hmac.update(payload);
    hmac.verify_slice(received_mac)
        .map_err(|_| anyhow!(ProtocolError::BatonInvalid).context("Invalid MAC on baton"))?;

    let stream_id = u64::from_be_bytes(payload[0..8].try_into().unwrap());
    let baton_seq = u64::from_be_bytes(payload[8..16].try_into().unwrap());
    Ok((stream_id, baton_seq))
}

/// How long do we keep a stream in [`Handle::Available`] state before expiration. Note that every
/// HTTP request resets the timer to beginning, so the client can keep a stream alive for a long
/// time, as long as it pings regularly.
const EXPIRATION: Duration = Duration::from_secs(10);

/// How long do we keep an expired stream in [`Handle::Expired`] state before removing it for good.
const CLEANUP: Duration = Duration::from_secs(300);

fn mark_expire(state: &mut ServerStreamState, stream_id: u64) {
    let expire_at = roundup_instant(state, Instant::now() + EXPIRATION);
    if state.expire_sleep.deadline() > expire_at {
        if let Some(waker) = state.expire_waker.take() {
            waker.wake();
        }
    }
    state.expire_queue.push(stream_id, Reverse(expire_at));
}

fn unmark_expire(state: &mut ServerStreamState, stream_id: u64) {
    state.expire_queue.remove(&stream_id);
}

/// Handles stream expiration (and cleanup). The returned future is never resolved.
pub async fn run_expire(server: &Server) {
    future::poll_fn(|cx| {
        let mut state = server.stream_state.lock();
        pump_expire(&mut state, cx);
        task::Poll::Pending
    })
    .await
}

fn pump_expire(state: &mut ServerStreamState, cx: &mut task::Context) {
    let now = Instant::now();

    // expire all streams in the `expire_queue` that have passed their expiration time
    let wakeup_at = loop {
        let stream_id = match state.expire_queue.peek() {
            Some((&stream_id, &Reverse(expire_at))) => {
                if expire_at <= now {
                    stream_id
                } else {
                    break expire_at;
                }
            }
            None => break now + Duration::from_secs(60),
        };
        state.expire_queue.pop();

        match state.handles.get_mut(&stream_id) {
            Some(handle @ Handle::Available(_)) => {
                *handle = Handle::Expired;
            }
            _ => continue,
        }
        tracing::debug!("Stream {stream_id} was expired");

        let cleanup_at = roundup_instant(state, now + CLEANUP);
        state.cleanup_queue.push_back((stream_id, cleanup_at));
    };

    // completely remove streams that are due in `cleanup_queue`
    loop {
        let stream_id = match state.cleanup_queue.front() {
            Some(&(stream_id, cleanup_at)) if cleanup_at <= now => stream_id,
            _ => break,
        };
        state.cleanup_queue.pop_front();

        let handle = state.handles.remove(&stream_id);
        assert!(matches!(handle, Some(Handle::Expired)));
        tracing::debug!("Stream {stream_id} was cleaned up after expiration");
    }

    // make sure that this function is called again no later than at time `wakeup_at`
    state.expire_sleep.as_mut().reset(wakeup_at);
    state.expire_waker = Some(cx.waker().clone());
    let _: task::Poll<()> = state.expire_sleep.as_mut().poll(cx);
}

/// Rounds the `instant` to the next second. This is used to ensure that streams that expire close
/// together are expired at exactly the same instant, thus reducing the number of times that
/// [`pump_expire()`] is called during periods of high load.
fn roundup_instant(state: &ServerStreamState, instant: Instant) -> Instant {
    let duration_s = (instant - state.expire_round_base).as_secs();
    state.expire_round_base + Duration::from_secs(duration_s + 1)
}

impl StreamError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::StreamExpired => "STREAM_EXPIRED",
        }
    }
}
