use std::ops::Deref;

use super::backend::Backend;
use super::backend::SegmentMeta;
use super::Result;
use super::StoreSegmentRequest;
use crate::io::Io;
use crate::segment::Segment;

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

// #[repr(transparent)]
// struct BytesLike<T>(pub T);
//
// impl<T> AsRef<[u8]> for BytesLike<T>
// where
//     T: AsBytes,
// {
//     fn as_ref(&self) -> &[u8] {
//         self.0.as_bytes()
//     }
// }
//
impl<C, Seg> Job<C, Seg>
where
    Seg: Segment,
{
    /// Perform the job and return the JobResult. This is not allowed to panic.
    pub(crate) async fn perform<B, IO>(self, backend: B, io: IO) -> JobResult<C, Seg>
    where
        B: Backend<Config = C>,
        IO: Io,
    {
        let result = self.try_perform(backend, io).await;
        JobResult { job: self, result }
    }

    async fn try_perform<B, IO>(&self, backend: B, io: IO) -> Result<u64>
    where
        B: Backend<Config = C>,
        IO: Io,
    {
        let segment = &self.request.segment;
        let segment_id = io.uuid();
        let tmp = io.tempfile()?;

        let new_index = segment
            .compact(&tmp, segment_id)
            .await
            .map_err(super::Error::Compact)?;

        let meta = SegmentMeta {
            segment_id,
            namespace: self.request.namespace.clone(),
            start_frame_no: segment.start_frame_no(),
            end_frame_no: segment.last_committed(),
            created_at: io.now(),
        };
        let config = self
            .request
            .storage_config_override
            .clone()
            .unwrap_or_else(|| backend.default_config());

        backend.store(&config, meta, tmp, new_index).await?;

        Ok(segment.last_committed())
    }
}

#[derive(Debug)]
pub(crate) struct JobResult<C, S> {
    /// The job that was performed
    pub(crate) job: Job<C, S>,
    /// The outcome of the job: the new durable index, or an error.
    pub(crate) result: Result<u64>,
}

#[cfg(test)]
mod test {
    // use std::fs::File;
    // use std::io::Write;
    // use std::mem::size_of;
    use std::path::Path;
    use std::str::FromStr;
    // use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use chrono::Utc;
    // use fst::{Map, Streamer};
    // use libsql_sys::rusqlite::OpenFlags;
    // use tempfile::{tempdir, tempfile, NamedTempFile};
    use uuid::Uuid;

    use crate::io::file::FileExt;
    use crate::io::StdIO;
    // use crate::registry::WalRegistry;
    // use crate::segment::compacted::CompactedSegmentDataHeader;
    // use crate::segment::sealed::SealedSegment;
    // use crate::segment::{Frame, FrameHeader};
    // use crate::storage::Storage;
    // use crate::wal::{LibsqlWal, LibsqlWalManager};
    use libsql_sys::name::NamespaceName;

    use super::*;

    // fn setup_wal<S: Storage>(
    //     path: &Path,
    //     storage: S,
    // ) -> (LibsqlWalManager<StdIO, S>, Arc<WalRegistry<StdIO, S>>) {
    //     let resolver = |path: &Path| {
    //         NamespaceName::from_string(path.file_name().unwrap().to_str().unwrap().to_string())
    //     };
    //     let registry =
    //         Arc::new(WalRegistry::new(path.join("wals"), storage).unwrap());
    //     (LibsqlWalManager::new(registry.clone()), registry)
    // }
    //
    // fn make_connection(
    //     path: &Path,
    //     wal: LibsqlWalManager<StdIO>,
    // ) -> libsql_sys::Connection<LibsqlWal<StdIO>> {
    //     libsql_sys::Connection::open(
    //         path.join("db"),
    //         OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
    //         wal,
    //         10000,
    //         None,
    //     )
    //     .unwrap()
    // }
    //
    // #[tokio::test]
    // async fn compact_segment() {
    //     struct SwapHandler;
    //
    //     impl SegmentSwapHandler<Arc<SealedSegment<File>>> for SwapHandler {
    //         fn handle_segment_swap(
    //             &self,
    //             namespace: NamespaceName,
    //             segment: Arc<SealedSegment<File>>,
    //         ) {
    //             tokio::runtime::Handle::current().block_on(async move {
    //                 let out_file = tempfile().unwrap();
    //                 let id = Uuid::new_v4();
    //                 let index_bytes = segment.compact(&out_file, id).await.unwrap();
    //                 let index = Map::new(index_bytes).unwrap();
    //
    //                 // indexes contain the same pages
    //                 let mut new_stream = index.stream();
    //                 let mut orig_stream = segment.index().stream();
    //                 assert_eq!(new_stream.next().unwrap().0, orig_stream.next().unwrap().0);
    //                 assert_eq!(new_stream.next().unwrap().0, orig_stream.next().unwrap().0);
    //                 assert!(new_stream.next().is_none());
    //                 assert!(orig_stream.next().is_none());
    //
    //                 let mut db_file = NamedTempFile::new().unwrap();
    //                 let mut stream = index.stream();
    //                 while let Some((page_bytes, offset)) = stream.next() {
    //                     let page_no = u32::from_be_bytes(page_bytes.try_into().unwrap());
    //                     let mut buf = [0u8; 4096];
    //                     let offset = size_of::<CompactedSegmentDataHeader>()
    //                         + offset as usize * size_of::<Frame>()
    //                         + size_of::<FrameHeader>();
    //                     out_file.read_exact_at(&mut buf, offset as u64).unwrap();
    //                     db_file
    //                         .as_file()
    //                         .write_all_at(&buf, (page_no as u64 - 1) * 4096)
    //                         .unwrap();
    //                 }
    //
    //                 db_file.flush().unwrap();
    //                 let conn = libsql_sys::rusqlite::Connection::open(db_file.path()).unwrap();
    //                 conn.query_row("select count(*) from test", (), |r| Ok(()))
    //                     .unwrap();
    //             });
    //         }
    //     }
    //
    //     let tmp = tempdir().unwrap();
    //     let (wal, registry) = setup_wal(tmp.path(), SwapHandler);
    //     let conn = make_connection(tmp.path(), wal.clone());
    //
    //     tokio::task::spawn_blocking(move || {
    //         conn.execute("create table test (x)", ()).unwrap();
    //         for i in 0..100usize {
    //             conn.execute("insert into test values (?)", [i]).unwrap();
    //         }
    //
    //         registry.shutdown().unwrap();
    //     })
    //     .await
    //     .unwrap();
    // }
    //
    // #[tokio::test]
    // async fn simple_perform_job() {
    //     struct TestIO;
    //
    //     impl Io for TestIO {
    //         type File = <StdIO as Io>::File;
    //         type TempFile = <StdIO as Io>::TempFile;
    //
    //         fn create_dir_all(&self, path: &Path) -> std::io::Result<()> {
    //             StdIO(()).create_dir_all(path)
    //         }
    //
    //         fn open(
    //             &self,
    //             create_new: bool,
    //             read: bool,
    //             write: bool,
    //             path: &Path,
    //         ) -> std::io::Result<Self::File> {
    //             StdIO(()).open(create_new, read, write, path)
    //         }
    //
    //         fn tempfile(&self) -> std::io::Result<Self::TempFile> {
    //             StdIO(()).tempfile()
    //         }
    //
    //         fn now(&self) -> DateTime<Utc> {
    //             DateTime::UNIX_EPOCH
    //         }
    //
    //         fn uuid(&self) -> Uuid {
    //             Uuid::from_u128(0)
    //         }
    //
    //         fn hard_link(&self, _src: &Path, _dst: &Path) -> std::io::Result<()> {
    //             unimplemented!()
    //         }
    //     }
    //
    //     struct TestStorage {
    //         called: AtomicBool,
    //     }
    //
    //     impl Drop for TestStorage {
    //         fn drop(&mut self) {
    //             assert!(self.called.load(std::sync::atomic::Ordering::Relaxed));
    //         }
    //     }
    //
    //     impl Backend for TestStorage {
    //         type Config = ();
    //
    //         fn store(
    //             &self,
    //             _config: &Self::Config,
    //             meta: SegmentMeta,
    //             segment_data: impl FileExt,
    //             segment_index: Vec<u8>,
    //         ) -> impl std::future::Future<Output = Result<()>> + Send {
    //             async move {
    //                 self.called
    //                     .store(true, std::sync::atomic::Ordering::Relaxed);
    //
    //                 insta::assert_debug_snapshot!(meta);
    //                 insta::assert_debug_snapshot!(crc32fast::hash(&segment_index));
    //                 insta::assert_debug_snapshot!(segment_index.len());
    //                 let data = async_read_all_to_vec(segment_data).await.unwrap();
    //                 insta::assert_debug_snapshot!(data.len());
    //                 insta::assert_debug_snapshot!(crc32fast::hash(&data));
    //
    //                 Ok(())
    //             }
    //         }
    //
    //         async fn fetch_segment(
    //             &self,
    //             _config: &Self::Config,
    //             _namespace: NamespaceName,
    //             _frame_no: u64,
    //             _dest_path: &Path,
    //         ) -> Result<()> {
    //             todo!()
    //         }
    //
    //         async fn meta(
    //             &self,
    //             _config: &Self::Config,
    //             _namespace: NamespaceName,
    //         ) -> Result<crate::storage::backend::DbMeta> {
    //             todo!();
    //         }
    //
    //         fn default_config(&self) -> Arc<Self::Config> {
    //             Arc::new(())
    //         }
    //     }
    //
    //     struct SwapHandler;
    //
    //     impl SegmentSwapHandler<File> for SwapHandler {
    //         fn handle_segment_swap(
    //             &self,
    //             namespace: NamespaceName,
    //             segment: Arc<SealedSegment<File>>,
    //         ) {
    //             tokio::runtime::Handle::current().block_on(async move {
    //                 let job = Job {
    //                     request: IndexedRequest {
    //                         request: StoreSegmentRequest {
    //                             namespace,
    //                             segment,
    //                             created_at: TestIO.now(),
    //                             storage_config_override: None,
    //                         },
    //                         id: 0,
    //                     },
    //                 };
    //
    //                 let result = job
    //                     .perform(
    //                         TestStorage {
    //                             called: false.into(),
    //                         },
    //                         TestIO,
    //                     )
    //                     .await;
    //
    //                 assert_eq!(result.job.request.id, 0);
    //                 assert!(result.result.is_ok());
    //             });
    //         }
    //     }
    //
    //     let tmp = tempdir().unwrap();
    //     let (wal, registry) = setup_wal(tmp.path(), SwapHandler);
    //     let conn = make_connection(tmp.path(), wal.clone());
    //
    //     tokio::task::spawn_blocking(move || {
    //         conn.execute("create table test (x)", ()).unwrap();
    //         for i in 0..100usize {
    //             conn.execute("insert into test values (?)", [i]).unwrap();
    //         }
    //
    //         registry.shutdown().unwrap();
    //     })
    //     .await
    //     .unwrap();
    // }

    #[tokio::test]
    async fn perform_job() {
        #[derive(Debug)]
        struct TestSegment;

        impl Segment for TestSegment {
            async fn compact(
                &self,
                out_file: &impl FileExt,
                id: uuid::Uuid,
            ) -> crate::error::Result<Vec<u8>> {
                out_file.write_all_at(id.to_string().as_bytes(), 0).unwrap();
                Ok(b"test_index".into())
            }

            fn start_frame_no(&self) -> u64 {
                1
            }

            fn last_committed(&self) -> u64 {
                10
            }

            fn index(&self) -> &fst::Map<Arc<[u8]>> {
                todo!()
            }

            fn read_page(
                &self,
                _page_no: u32,
                _max_frame_no: u64,
                _buf: &mut [u8],
            ) -> std::io::Result<bool> {
                todo!()
            }

            fn is_checkpointable(&self) -> bool {
                todo!()
            }

            fn size_after(&self) -> u32 {
                todo!()
            }

            async fn read_frame_offset_async<B>(
                &self,
                _offset: u32,
                _buf: B,
            ) -> (B, crate::error::Result<()>)
            where
                B: crate::io::buf::IoBufMut + Send + 'static,
            {
                todo!()
            }
        }

        struct TestBackend;

        impl Backend for TestBackend {
            type Config = ();

            async fn store(
                &self,
                _config: &Self::Config,
                meta: SegmentMeta,
                segment_data: impl FileExt,
                segment_index: Vec<u8>,
            ) -> Result<()> {
                // verify that the stored segment is the same as the one we compacted
                assert_eq!(segment_index, b"test_index");
                let mut buf = vec![0; Uuid::new_v4().to_string().len()];
                segment_data.read_exact_at(&mut buf, 0).unwrap();
                let id = Uuid::from_str(std::str::from_utf8(&buf).unwrap()).unwrap();
                assert_eq!(meta.segment_id, id);

                Ok(())
            }

            async fn fetch_segment(
                &self,
                _config: &Self::Config,
                _namespace: NamespaceName,
                _frame_no: u64,
                _dest_path: &Path,
            ) -> Result<()> {
                todo!()
            }

            async fn meta(
                &self,
                _config: &Self::Config,
                _namespace: NamespaceName,
            ) -> Result<crate::storage::backend::DbMeta> {
                todo!()
            }

            fn default_config(&self) -> Arc<Self::Config> {
                Arc::new(())
            }
        }

        let job = Job {
            request: IndexedRequest {
                request: StoreSegmentRequest {
                    namespace: "test".into(),
                    segment: TestSegment,
                    created_at: Utc::now(),
                    storage_config_override: None,
                },
                id: 0,
            },
        };

        let result = job.perform(TestBackend, StdIO(())).await;
        assert_eq!(result.result.unwrap(), 10);
    }
}
