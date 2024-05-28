use std::mem::size_of;
use std::ops::Deref;
use std::sync::Arc;

use fst::Streamer;
use zerocopy::little_endian::{U128 as lu128, U32 as lu32, U64 as lu64};
use zerocopy::{AsBytes, FromBytes, FromZeroes};

use super::storage::SegmentMeta;
use super::storage::Storage;
use super::Result;
use super::StoreSegmentRequest;
use crate::io::buf::ZeroCopyBuf;
use crate::io::file::FileExt;
use crate::io::Io;
use crate::segment::sealed::SealedSegment;
use crate::segment::Frame;

/// A request, with an id
#[derive(Debug)]
pub(crate) struct IndexedRequest<C, T> {
    pub(crate) request: StoreSegmentRequest<C, T>,
    pub(crate) id: u64,
}

impl<C, T> Deref for IndexedRequest<C, T> {
    type Target = StoreSegmentRequest<C, T>;

    fn deref(&self) -> &Self::Target {
        &self.request
    }
}

/// A storage Job to be performed
#[derive(Debug)]
pub(crate) struct Job<C, T> {
    /// Segment to store.
    // TODO: implement request batching (merge segment and send).
    pub(crate) request: IndexedRequest<C, T>,
}

// todo: Move to segment module
#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentDataHeader {
    frame_count: lu64,
    segment_id: lu128,
    start_frame_no: lu64,
    end_frame_no: lu64,
}

#[derive(Debug, AsBytes, FromZeroes, FromBytes)]
#[repr(C)]
pub struct CompactedSegmentDataFooter {
    checksum: lu32,
}

#[repr(transparent)]
struct BytesLike<T>(pub T);

impl<T> AsRef<[u8]> for BytesLike<T>
where
    T: AsBytes,
{
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl<C, F> Job<C, Arc<SealedSegment<F>>>
where
    F: FileExt,
{
    /// Perform the job and return the JobResult. This is not allowed to panic.
    pub(crate) async fn perform<S, IO>(
        self,
        storage: S,
        io: IO,
    ) -> JobResult<C, Arc<SealedSegment<F>>>
    where
        S: Storage<Config = C>,
        IO: Io,
    {
        let result = self.try_perform(storage, io).await;
        JobResult { job: self, result }
    }

    async fn try_perform<S, IO>(&self, storage: S, io: IO) -> Result<u64>
    where
        S: Storage<Config = C>,
        IO: Io,
    {
        let segment = &self.request.segment;
        let segment_id = io.uuid();
        let tmp = io.tempfile()?;

        let new_index = compact(segment, &tmp, segment_id).await?;

        let meta = SegmentMeta {
            segment_id,
            namespace: self.request.namespace.clone(),
            start_frame_no: segment.header().start_frame_no.get(),
            end_frame_no: segment.header().last_committed(),
            created_at: io.now(),
        };
        let config = self
            .request
            .storage_config_override
            .clone()
            .unwrap_or_else(|| storage.default_config());

        storage.store(&config, meta, tmp, new_index).await?;

        Ok(segment.header().last_committed())
    }
}

/// Compact a sealed segment into out-file with id `segment_id`, and returns the new index.
// todo: move to segment module
async fn compact(
    segment: &SealedSegment<impl FileExt>,
    out_file: &impl FileExt,
    segment_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let mut hasher = crc32fast::Hasher::new();

    let header = CompactedSegmentDataHeader {
        frame_count: (segment.index().len() as u64).into(),
        segment_id: segment_id.as_u128().into(),
        start_frame_no: segment.header().start_frame_no,
        end_frame_no: segment.header().last_commited_frame_no,
    };

    hasher.update(header.as_bytes());
    let (_, ret) = out_file
        .write_all_at_async(ZeroCopyBuf::new_init(header), 0)
        .await;
    ret?;

    let mut pages = segment.index().stream();
    // todo: use Frame::Zeroed somehow, so that header is aligned?
    let mut buffer = Box::new(ZeroCopyBuf::<Frame>::new_uninit());
    let mut out_index = fst::MapBuilder::memory();
    let mut current_offset = 0;

    while let Some((page_no_bytes, offset)) = pages.next() {
        let page_no = u32::from_be_bytes(page_no_bytes.try_into().unwrap());
        let (b, ret) = segment.read_frame_offset_async(offset as _, buffer).await;
        ret.unwrap();
        hasher.update(&b.get_ref().as_bytes());
        let dest_offset =
            size_of::<CompactedSegmentDataHeader>() + current_offset * size_of::<Frame>();
        let (mut b, ret) = out_file.write_all_at_async(b, dest_offset as u64).await;
        ret?;
        out_index
            .insert(page_no_bytes, current_offset as _)
            .unwrap();
        current_offset += 1;
        b.deinit();
        buffer = b;
    }

    let footer = CompactedSegmentDataFooter {
        checksum: hasher.finalize().into(),
    };

    let footer_offset =
        size_of::<CompactedSegmentDataHeader>() + current_offset * size_of::<Frame>();
    let (_, ret) = out_file
        .write_all_at_async(ZeroCopyBuf::new_init(footer), footer_offset as _)
        .await;

    Ok(out_index.into_inner().unwrap())
}

pub(crate) struct JobResult<C, S> {
    /// The job that was performed
    pub(crate) job: Job<C, S>,
    /// The outcome of the job: the new durable index, or an error.
    pub(crate) result: Result<u64>,
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::Write;
    use std::mem::size_of;
    use std::path::Path;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use chrono::{DateTime, Utc};
    use fst::Map;
    use libsql_sys::rusqlite::OpenFlags;
    use tempfile::{tempdir, tempfile, NamedTempFile};
    use uuid::Uuid;

    use crate::io::file::{async_read_all_to_vec, FileExt};
    use crate::io::StdIO;
    use crate::name::NamespaceName;
    use crate::registry::{SegmentSwapHandler, WalRegistry};
    use crate::segment::sealed::SealedSegment;
    use crate::segment::FrameHeader;
    use crate::wal::{LibsqlWal, LibsqlWalManager};

    use super::*;

    fn setup_wal(
        path: &Path,
        swap_handler: impl SegmentSwapHandler<File>,
    ) -> (LibsqlWalManager<StdIO>, Arc<WalRegistry<StdIO>>) {
        let resolver = |path: &Path| {
            NamespaceName::from_string(path.file_name().unwrap().to_str().unwrap().to_string())
        };
        let registry =
            Arc::new(WalRegistry::new(path.join("wals"), resolver, swap_handler).unwrap());
        (LibsqlWalManager::new(registry.clone()), registry)
    }

    fn make_connection(
        path: &Path,
        wal: LibsqlWalManager<StdIO>,
    ) -> libsql_sys::Connection<LibsqlWal<StdIO>> {
        libsql_sys::Connection::open(
            path.join("db"),
            OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
            wal,
            10000,
            None,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn compact_segment() {
        struct SwapHandler;

        impl SegmentSwapHandler<File> for SwapHandler {
            fn handle_segment_swap(
                &self,
                namespace: NamespaceName,
                segment: Arc<SealedSegment<File>>,
            ) {
                tokio::runtime::Handle::current().block_on(async move {
                    let out_file = tempfile().unwrap();
                    let id = Uuid::new_v4();
                    let index_bytes = compact(&segment, &out_file, id).await.unwrap();
                    let index = Map::new(index_bytes).unwrap();

                    // indexes contain the same pages
                    let mut new_stream = index.stream();
                    let mut orig_stream = segment.index().stream();
                    assert_eq!(new_stream.next().unwrap().0, orig_stream.next().unwrap().0);
                    assert_eq!(new_stream.next().unwrap().0, orig_stream.next().unwrap().0);
                    assert!(new_stream.next().is_none());
                    assert!(orig_stream.next().is_none());

                    let mut db_file = NamedTempFile::new().unwrap();
                    let mut stream = index.stream();
                    while let Some((page_bytes, offset)) = stream.next() {
                        let page_no = u32::from_be_bytes(page_bytes.try_into().unwrap());
                        let mut buf = [0u8; 4096];
                        let offset = size_of::<CompactedSegmentDataHeader>()
                            + offset as usize * size_of::<Frame>()
                            + size_of::<FrameHeader>();
                        out_file.read_exact_at(&mut buf, offset as u64).unwrap();
                        db_file
                            .as_file()
                            .write_all_at(&buf, (page_no as u64 - 1) * 4096)
                            .unwrap();
                    }

                    db_file.flush().unwrap();
                    let conn = libsql_sys::rusqlite::Connection::open(db_file.path()).unwrap();
                    conn.query_row("select count(*) from test", (), |r| Ok(()))
                        .unwrap();
                });
            }
        }

        let tmp = tempdir().unwrap();
        let (wal, registry) = setup_wal(tmp.path(), SwapHandler);
        let conn = make_connection(tmp.path(), wal.clone());

        tokio::task::spawn_blocking(move || {
            conn.execute("create table test (x)", ()).unwrap();
            for i in 0..100usize {
                conn.execute("insert into test values (?)", [i]).unwrap();
            }

            registry.shutdown().unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn simple_perform_job() {
        struct TestIO;

        impl Io for TestIO {
            type File = <StdIO as Io>::File;
            type TempFile = <StdIO as Io>::TempFile;

            fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
                StdIO(()).create_dir_all(path)
            }

            fn open(
                &self,
                create_new: bool,
                read: bool,
                write: bool,
                path: &Path,
            ) -> std::io::Result<Self::File> {
                StdIO(()).open(create_new, read, write, path)
            }

            fn tempfile(&self) -> std::io::Result<Self::TempFile> {
                StdIO(()).tempfile()
            }

            fn now(&self) -> DateTime<Utc> {
                DateTime::UNIX_EPOCH
            }

            fn uuid(&self) -> Uuid {
                dbg!();
                Uuid::from_u128(0)
            }
        }

        struct TestStorage {
            called: AtomicBool,
        }

        impl Drop for TestStorage {
            fn drop(&mut self) {
                assert!(self.called.load(std::sync::atomic::Ordering::Relaxed));
            }
        }

        impl Storage for TestStorage {
            type Config = ();

            fn store(
                &self,
                config: &Self::Config,
                meta: SegmentMeta,
                segment_data: impl FileExt,
                segment_index: Vec<u8>,
            ) -> impl std::future::Future<Output = Result<()>> + Send {
                async move {
                    self.called
                        .store(true, std::sync::atomic::Ordering::Relaxed);

                    insta::assert_debug_snapshot!(meta);
                    insta::assert_debug_snapshot!(crc32fast::hash(&segment_index));
                    insta::assert_debug_snapshot!(segment_index.len());
                    let data = async_read_all_to_vec(segment_data).await.unwrap();
                    insta::assert_debug_snapshot!(data.len());
                    insta::assert_debug_snapshot!(crc32fast::hash(&data));

                    Ok(())
                }
            }

            async fn fetch_segment(
                &self,
                _config: &Self::Config,
                _namespace: NamespaceName,
                _frame_no: u64,
                _dest: impl tokio::io::AsyncWrite,
            ) -> Result<()> {
                todo!()
            }

            async fn meta(
                &self,
                _config: &Self::Config,
                _namespace: NamespaceName,
            ) -> Result<crate::bottomless::storage::DbMeta> {
                todo!();
            }

            fn default_config(&self) -> Arc<Self::Config> {
                Arc::new(())
            }
        }

        struct SwapHandler;

        impl SegmentSwapHandler<File> for SwapHandler {
            fn handle_segment_swap(
                &self,
                namespace: NamespaceName,
                segment: Arc<SealedSegment<File>>,
            ) {
                tokio::runtime::Handle::current().block_on(async move {
                    let job = Job {
                        request: IndexedRequest {
                            request: StoreSegmentRequest {
                                namespace,
                                segment,
                                created_at: TestIO.now(),
                                storage_config_override: None,
                            },
                            id: 0,
                        },
                    };

                    let result = job
                        .perform(
                            TestStorage {
                                called: false.into(),
                            },
                            TestIO,
                        )
                        .await;

                    assert_eq!(result.job.request.id, 0);
                    assert!(result.result.is_ok());
                });
            }
        }

        let tmp = tempdir().unwrap();
        let (wal, registry) = setup_wal(tmp.path(), SwapHandler);
        let conn = make_connection(tmp.path(), wal.clone());

        tokio::task::spawn_blocking(move || {
            conn.execute("create table test (x)", ()).unwrap();
            for i in 0..100usize {
                conn.execute("insert into test values (?)", [i]).unwrap();
            }

            registry.shutdown().unwrap();
        })
        .await
        .unwrap();
    }
}
