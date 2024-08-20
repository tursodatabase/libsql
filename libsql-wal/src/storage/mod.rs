use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use fst::Map;
use hashbrown::HashMap;
use libsql_sys::name::NamespaceName;
use parking_lot::Mutex;
use tempfile::{tempdir, TempDir};

use crate::io::{FileExt, Io, StdIO};
use crate::segment::compacted::CompactedSegment;
use crate::segment::{sealed::SealedSegment, Segment};

use self::backend::SegmentMeta;
pub use self::error::Error;

pub mod async_storage;
pub mod backend;
pub(crate) mod error;
mod job;
mod scheduler;

pub type Result<T, E = self::error::Error> = std::result::Result<T, E>;

pub enum RestoreOptions {
    Latest,
    Timestamp(DateTime<Utc>),
}

/// SegmentKey is used to index segment data, where keys a lexicographically ordered.
/// The scheme is `{u64::MAX - start_frame_no}-{u64::MAX - end_frame_no}`. With that naming convention, when looking for
/// the segment containing 'n', we can perform a prefix search with "{u64::MAX - n}". The first
/// element of the range will be the biggest segment that contains n if it exists.
/// Beware that if no segments contain n, either the smallest segment not containing n, if n < argmin
/// {start_frame_no}, or the largest segment if n > argmax {end_frame_no} will be returned.
/// e.g:
/// ```ignore
/// let mut map = BTreeMap::new();
///
/// let meta = SegmentMeta { start_frame_no: 1, end_frame_no: 100 };
/// map.insert(SegmentKey(&meta).to_string(), meta);
///
/// let meta = SegmentMeta { start_frame_no: 101, end_frame_no: 500 };
/// map.insert(SegmentKey(&meta).to_string(), meta);
///
/// let meta = SegmentMeta { start_frame_no: 101, end_frame_no: 1000 };
/// map.insert(SegmentKey(&meta).to_string(), meta);
///
/// map.range(format!("{:019}", u64::MAX - 50)..).next();
/// map.range(format!("{:019}", u64::MAX - 0)..).next();
/// map.range(format!("{:019}", u64::MAX - 1)..).next();
/// map.range(format!("{:019}", u64::MAX - 100)..).next();
/// map.range(format!("{:019}", u64::MAX - 101)..).next();
/// map.range(format!("{:019}", u64::MAX - 5000)..).next();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentKey {
    pub start_frame_no: u64,
    pub end_frame_no: u64,
}

impl PartialOrd for SegmentKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.start_frame_no.partial_cmp(&other.start_frame_no) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.end_frame_no.partial_cmp(&other.end_frame_no)
    }
}

impl Ord for SegmentKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl SegmentKey {
    pub(crate) fn includes(&self, frame_no: u64) -> bool {
        (self.start_frame_no..=self.end_frame_no).contains(&frame_no)
    }
}

impl From<&SegmentMeta> for SegmentKey {
    fn from(value: &SegmentMeta) -> Self {
        Self {
            start_frame_no: value.start_frame_no,
            end_frame_no: value.end_frame_no,
        }
    }
}

impl FromStr for SegmentKey {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let (rev_start_fno, s) = s.split_at(20);
        let start_frame_no = u64::MAX - rev_start_fno.parse::<u64>().map_err(|_| ())?;
        let (_, rev_end_fno) = s.split_at(1);
        let end_frame_no = u64::MAX - rev_end_fno.parse::<u64>().map_err(|_| ())?;
        Ok(Self {
            start_frame_no,
            end_frame_no,
        })
    }
}

impl fmt::Display for SegmentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:019}-{:019}",
            u64::MAX - self.start_frame_no,
            u64::MAX - self.end_frame_no,
        )
    }
}

/// takes the new durable frame_no and returns a future
pub type OnStoreCallback = Box<
    dyn FnOnce(u64) -> Pin<Box<dyn Future<Output = ()> + Send + Sync + 'static>>
        + Send
        + Sync
        + 'static,
>;

pub trait Storage: Send + Sync + 'static {
    type Segment: Segment;
    type Config;
    /// store the passed segment for `namespace`. This function is called in a context where
    /// blocking is acceptable.
    /// returns a future that resolves when the segment is stored
    /// The segment should be stored whether or not the future is polled.
    fn store(
        &self,
        namespace: &NamespaceName,
        seg: Self::Segment,
        config_override: Option<Arc<Self::Config>>,
        on_store: OnStoreCallback,
    );

    fn durable_frame_no_sync(
        &self,
        namespace: &NamespaceName,
        config_override: Option<Arc<Self::Config>>,
    ) -> u64;

    async fn durable_frame_no(
        &self,
        namespace: &NamespaceName,
        config_override: Option<Arc<Self::Config>>,
    ) -> u64;

    async fn restore(
        &self,
        file: impl FileExt,
        namespace: &NamespaceName,
        restore_options: RestoreOptions,
        config_override: Option<Arc<Self::Config>>,
    ) -> Result<()>;

    async fn find_segment(
        &self,
        namespace: &NamespaceName,
        frame_no: u64,
        config_override: Option<Arc<Self::Config>>,
    ) -> Result<SegmentKey>;

    async fn fetch_segment_index(
        &self,
        namespace: &NamespaceName,
        key: &SegmentKey,
        config_override: Option<Arc<Self::Config>>,
    ) -> Result<Map<Arc<[u8]>>>;

    async fn fetch_segment_data(
    fn shutdown(&self) -> impl Future<Output = ()> + Send {
        async { () }
    }
        &self,
        namespace: &NamespaceName,
        key: &SegmentKey,
        config_override: Option<Arc<Self::Config>>,
    ) -> Result<CompactedSegment<impl FileExt>>;
}

/// a placeholder storage that doesn't store segment
#[derive(Debug, Clone, Copy)]
pub struct NoStorage;

impl Storage for NoStorage {
    type Config = ();
    type Segment = SealedSegment<std::fs::File>;

    fn store(
        &self,
        _namespace: &NamespaceName,
        _seg: Self::Segment,
        _config: Option<Arc<Self::Config>>,
        _on_store: OnStoreCallback,
    ) {
    }

    async fn durable_frame_no(
        &self,
        namespace: &NamespaceName,
        config: Option<Arc<Self::Config>>,
    ) -> u64 {
        self.durable_frame_no_sync(namespace, config)
    }

    async fn restore(
        &self,
        _file: impl FileExt,
        _namespace: &NamespaceName,
        _restore_options: RestoreOptions,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<()> {
        panic!("can restore from no storage")
    }

    fn durable_frame_no_sync(
        &self,
        _namespace: &NamespaceName,
        _config_override: Option<Arc<Self::Config>>,
    ) -> u64 {
        u64::MAX
    }

    async fn find_segment(
        &self,
        _namespace: &NamespaceName,
        _frame_no: u64,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<SegmentKey> {
        unimplemented!()
    }

    async fn fetch_segment_index(
        &self,
        _namespace: &NamespaceName,
        _key: &SegmentKey,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<Map<Arc<[u8]>>> {
        unimplemented!()
    }

    async fn fetch_segment_data(
        &self,
        _namespace: &NamespaceName,
        _key: &SegmentKey,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<CompactedSegment<impl FileExt>> {
        unimplemented!();
        #[allow(unreachable_code)]
        Result::<CompactedSegment<std::fs::File>>::Err(Error::InvalidIndex(""))
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct TestStorage<IO = StdIO> {
    inner: Arc<Mutex<TestStorageInner<IO>>>,
}

#[derive(Debug)]
struct TestStorageInner<IO> {
    stored: HashMap<NamespaceName, BTreeMap<SegmentKey, (PathBuf, Map<Arc<[u8]>>)>>,
    dir: TempDir,
    io: IO,
    store: bool,
}

impl<F> Clone for TestStorage<F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl TestStorage<StdIO> {
    pub fn new() -> Self {
        Self::new_io(false, StdIO(()))
    }

    pub fn new_store() -> Self {
        Self::new_io(true, StdIO(()))
    }
}

impl<IO: Io> TestStorage<IO> {
    pub fn new_io(store: bool, io: IO) -> Self {
        let dir = tempdir().unwrap();
        Self {
            inner: Arc::new(
                TestStorageInner {
                    dir,
                    stored: Default::default(),
                    io,
                    store,
                }
                .into(),
            ),
        }
    }
}

impl<IO: Io> Storage for TestStorage<IO> {
    type Segment = SealedSegment<IO::File>;
    type Config = ();

    fn store(
        &self,
        namespace: &NamespaceName,
        seg: Self::Segment,
        _config: Option<Arc<Self::Config>>,
        on_store: OnStoreCallback,
    ) {
        let mut inner = self.inner.lock();
        if inner.store {
            let id = uuid::Uuid::new_v4();
            let out_path = inner.dir.path().join(id.to_string());
            let out_file = inner.io.open(true, true, true, &out_path).unwrap();
            let index = tokio::runtime::Handle::current()
                .block_on(seg.compact(&out_file, id))
                .unwrap();
            let end_frame_no = seg.header().last_committed();
            let key = SegmentKey {
                start_frame_no: seg.header().start_frame_no.get(),
                end_frame_no,
            };
            let index = Map::new(index.into()).unwrap();
            inner
                .stored
                .entry(namespace.clone())
                .or_default()
                .insert(key, (out_path, index));
            tokio::runtime::Handle::current().block_on(on_store(end_frame_no));
        }
    }

    async fn durable_frame_no(
        &self,
        namespace: &NamespaceName,
        config: Option<Arc<Self::Config>>,
    ) -> u64 {
        self.durable_frame_no_sync(namespace, config)
    }

    async fn restore(
        &self,
        _file: impl FileExt,
        _namespace: &NamespaceName,
        _restore_options: RestoreOptions,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<()> {
        todo!();
    }

    fn durable_frame_no_sync(
        &self,
        namespace: &NamespaceName,
        _config_override: Option<Arc<Self::Config>>,
    ) -> u64 {
        let inner = self.inner.lock();
        if inner.store {
            let Some(segs) = inner.stored.get(namespace) else {
                return 0;
            };
            segs.keys().map(|k| k.end_frame_no).max().unwrap_or(0)
        } else {
            u64::MAX
        }
    }

    async fn find_segment(
        &self,
        namespace: &NamespaceName,
        frame_no: u64,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<SegmentKey> {
        let inner = self.inner.lock();
        if inner.store {
            if let Some(segs) = inner.stored.get(namespace) {
                let Some((key, _path)) = segs.iter().find(|(k, _)| k.includes(frame_no)) else {
                    return Err(Error::FrameNotFound(frame_no));
                };
                return Ok(*key);
            } else {
                panic!("namespace not found");
            }
        } else {
            panic!("store not enabled")
        }
    }

    async fn fetch_segment_index(
        &self,
        namespace: &NamespaceName,
        key: &SegmentKey,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<Map<Arc<[u8]>>> {
        let inner = self.inner.lock();
        if inner.store {
            match inner.stored.get(namespace) {
                Some(segs) => Ok(segs.get(&key).unwrap().1.clone()),
                None => panic!("unknown namespace"),
            }
        } else {
            panic!("not storing")
        }
    }

    async fn fetch_segment_data(
        &self,
        namespace: &NamespaceName,
        key: &SegmentKey,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<CompactedSegment<impl FileExt>> {
        let inner = self.inner.lock();
        if inner.store {
            match inner.stored.get(namespace) {
                Some(segs) => {
                    let path = &segs.get(&key).unwrap().0;
                    let file = inner.io.open(false, true, false, path).unwrap();
                    Ok(CompactedSegment::open(file).await?)
                }
                None => panic!("unknown namespace"),
            }
        } else {
            panic!("not storing")
        }
    }
}

pub struct StoreSegmentRequest<S> {
    namespace: NamespaceName,
    /// Path to the segment. Read-only for bottomless
    segment: S,
    /// When this segment was created
    created_at: DateTime<Utc>,

    /// alternative configuration to use with the storage layer.
    /// e.g: S3 overrides
    storage_config_override: Option<Arc<dyn Any + Send + Sync>>,
    /// Called after the segment was stored, with the new durable index
    on_store_callback: OnStoreCallback,
}

impl<S> fmt::Debug for StoreSegmentRequest<S>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreSegmentRequest")
            .field("namespace", &self.namespace)
            .field("segment", &self.segment)
            .field("created_at", &self.created_at)
            .finish()
    }
}
