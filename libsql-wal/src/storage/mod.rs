use std::marker::PhantomData;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use libsql_sys::name::NamespaceName;

use crate::io::FileExt;
use crate::segment::{sealed::SealedSegment, Segment};

pub use self::error::Error;

mod job;
pub mod async_storage;
pub mod backend;
pub(crate) mod error;
mod scheduler;

pub type Result<T, E = self::error::Error> = std::result::Result<T, E>;

pub enum RestoreOptions {
    Latest,
    Timestamp(DateTime<Utc>),
}

pub trait Storage: Send + Sync + 'static {
    type Segment: Segment;
    type Config;
    /// store the passed segment for `namespace`. This function is called in a context where
    /// blocking is acceptable.
    fn store(
        &self,
        namespace: &NamespaceName,
        seg: Self::Segment,
        config_override: Option<Arc<Self::Config>>,
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
        key: SegmentKey,
        config_override: Option<Arc<Self::Config>>,
    ) -> Result<Map<Arc<[u8]>>>;

    async fn fetch_segment_data(
        &self,
        namespace: &NamespaceName,
        key: SegmentKey,
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
        _key: SegmentKey,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<Map<Arc<[u8]>>> {
        unimplemented!()
    }

    async fn fetch_segment_data(
        &self,
        _namespace: &NamespaceName,
        _key: SegmentKey,
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
            inner: Arc::new(TestStorageInner {
                dir,
                stored: Default::default(),
                io,
                store,
            }.into()),
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
    ) -> impl Future<Output = u64>  + Send + Sync + 'static{
        let mut inner = self.inner.lock();
        if inner.store {
            let id = uuid::Uuid::new_v4();
            let out_path = inner.dir.path().join(id.to_string());
            let out_file = inner.io.open(true, true, true, &out_path).unwrap();
            let index = tokio::runtime::Handle::current().block_on(seg.compact(&out_file, id)).unwrap();
            let end_frame_no =  seg.header().last_committed();
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
            std::future::ready(end_frame_no)
        } else {
            std::future::ready(u64::MAX)
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
            let Some(segs) = inner.stored.get(namespace) else { return 0 };
            segs
                .keys()
                .map(|k| k.end_frame_no)
                .max()
                .unwrap_or(0)
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
                let Some((key, _path)) = segs.iter().find(|(k, _)| k.includes(frame_no))
                    else { return Err(Error::FrameNotFound(frame_no)) };
                    return Ok(*key)
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
        key: SegmentKey,
        _config_override: Option<Arc<Self::Config>>,
    ) -> Result<Map<Arc<[u8]>>> {
        let inner = self.inner.lock();
        if inner.store {
            match inner.stored.get(namespace) {
                Some(segs) => {
                    Ok(segs.get(&key).unwrap().1.clone())
                }
                None => panic!("unknown namespace"),
            }

        } else {
            panic!("not storing")
        }
    }

    async fn fetch_segment_data(
        &self,
        namespace: &NamespaceName,
        key: SegmentKey,
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

#[derive(Debug)]
pub struct StoreSegmentRequest<C, S> {
    namespace: NamespaceName,
    /// Path to the segment. Read-only for bottomless
    segment: S,
    /// When this segment was created
    created_at: DateTime<Utc>,

    /// alternative configuration to use with the storage layer.
    /// e.g: S3 overrides
    storage_config_override: Option<Arc<C>>,
}
