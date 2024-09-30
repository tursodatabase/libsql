use std::io::Write as _;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use fst::raw::IndexedValue;
use fst::MapBuilder;
use fst::Streamer;
use futures::FutureExt as _;
use futures::Stream;
use futures::StreamExt as _;
use futures::TryStreamExt;
use libsql_sys::name::NamespaceName;
use libsql_sys::rusqlite::OptionalExtension;
use libsql_sys::rusqlite::{self, TransactionBehavior};
use roaring::RoaringBitmap;
use tempfile::tempdir;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use zerocopy::AsBytes;

use crate::io::FileExt as _;
use crate::segment::compacted::CompactedFrameHeader;
use crate::segment::compacted::CompactedSegmentHeader;
use crate::LibsqlFooter;
use crate::LIBSQL_MAGIC;
use crate::LIBSQL_PAGE_SIZE;
use crate::LIBSQL_WAL_VERSION;

use super::backend::Backend;
use super::{SegmentInfo, SegmentKey};

pub mod strategy;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("error reading from meta db: {0}")]
    Meta(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::Error),
}

pub struct Compactor<B> {
    backend: Arc<B>,
    conn: rusqlite::Connection,
    path: PathBuf,
}

impl<B> Compactor<B> {
    pub fn new(backend: Arc<B>, compactor_path: &Path) -> Result<Self> {
        let conn = rusqlite::Connection::open(compactor_path.join("meta.db"))?;
        // todo! set pragmas: wal + foreign key check
        conn.pragma_update(None, "journal_mode", "wal")?;
        conn.execute(r#"CREATE TABLE IF NOT EXISTS monitored_namespaces (id INTEGER PRIMARY KEY AUTOINCREMENT, namespace_name BLOB NOT NULL, UNIQUE(namespace_name))"#, ()).unwrap();
        conn.execute(
            r#"CREATE TABLE IF NOT EXISTS segments (
                        start_frame_no INTEGER,
                        end_frame_no INTEGER,
                        timestamp DATE,
                        size INTEGER,
                        namespace_id INTEGER REFERENCES monitored_namespaces(id) ON DELETE CASCADE,
                        PRIMARY KEY (start_frame_no, end_frame_no))
                        "#,
            (),
        )?;

        Ok(Self {
            backend,
            conn,
            path: compactor_path.into(),
        })
    }

    pub async fn monitor(&mut self, namespace: &NamespaceName) -> Result<()>
    where
        B: Backend,
    {
        let tx = self.conn.transaction()?;
        let id = {
            let mut stmt  = tx.prepare_cached("INSERT OR IGNORE INTO monitored_namespaces(namespace_name) VALUES (?) RETURNING id")?;
            stmt.query_row([namespace.as_str()], |r| r.get(0))
                .optional()?
        };

        if let Some(id) = id {
            sync_one(self.backend.as_ref(), namespace, id, &tx, true).await?;
        }

        tx.commit()?;

        Ok(())
    }

    pub fn analyze(&self, namespace: &NamespaceName) -> Result<AnalyzedSegments> {
        let mut stmt = self.conn.prepare_cached(
            r#"
        SELECT start_frame_no, end_frame_no, timestamp
        FROM segments as s
        JOIN monitored_namespaces as m
        ON m.id = s.namespace_id
        WHERE m.namespace_name = ?"#,
        )?;
        let mut rows = stmt.query([namespace.as_str()])?;
        let mut graph = petgraph::graphmap::DiGraphMap::new();
        let mut last_frame_no = 0;
        while let Some(row) = rows.next()? {
            let start_frame_no: u64 = row.get(0)?;
            let end_frame_no: u64 = row.get(1)?;
            let timestamp: u64 = row.get(2)?;
            graph.add_edge(start_frame_no, end_frame_no, timestamp);
            if start_frame_no != 1 {
                graph.add_edge(start_frame_no - 1, start_frame_no, 0);
            }
            last_frame_no = last_frame_no.max(end_frame_no);
        }

        Ok(AnalyzedSegments {
            graph,
            last_frame_no,
            namespace: namespace.clone(),
        })
    }

    pub fn get_segment_range(
        &self,
        namespace: &NamespaceName,
    ) -> Result<Option<(SegmentInfo, SegmentInfo)>> {
        segments_range(&self.conn, namespace)
    }

    /// Polls storage for new frames since last sync
    #[tracing::instrument(skip(self))]
    async fn sync_latest(&self) -> Result<()>
    where
        B: Backend,
    {
        // let tx = self.meta.transaction()?;
        // let stream = self.storage.list_segments();

        Ok(())
    }

    /// sync all segments from storage with local cache
    pub async fn sync_all(&mut self, full: bool) -> Result<()>
    where
        B: Backend,
    {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut stmt = tx.prepare("SELECT namespace_name, id FROM monitored_namespaces")?;
            let mut namespace_rows = stmt.query(())?;
            while let Some(row) = namespace_rows.next()? {
                let namespace = NamespaceName::from_string(row.get::<_, String>(0)?);
                let id = row.get::<_, u64>(1)?;
                sync_one(self.backend.as_ref(), &namespace, id, &tx, full).await?;
            }
        }

        tx.commit()?;

        Ok(())
    }

    pub async fn sync_one(&mut self, namespace: &NamespaceName, full: bool) -> Result<()>
    where
        B: Backend,
    {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut stmt =
                tx.prepare_cached("SELECT id FROM monitored_namespaces WHERE namespace_name = ?")?;
            let id = stmt
                .query_row([namespace.as_str()], |row| row.get(0))
                .optional()?;
            if let Some(id) = id {
                sync_one(self.backend.as_ref(), &namespace, id, &tx, full).await?;
            }
        }

        tx.commit()?;

        Ok(())
    }

    /// `dedup_frames` takes a segment set and returns a stream of deduplicated raw frames,
    /// containing the most recent version of every frame in the segments covered by the set.
    ///
    /// if out_index is passed, the index of the new segement is generated and put there
    ///
    /// the progress callback is called with (count_pages, total_pages), as new pages are found
    ///
    /// returns a stream of the most recent deduplicated frames, and the size_after for that new
    /// segment
    async fn dedup_stream<'a>(
        &'a self,
        set: SegmentSet,
        out_index: Option<&'a mut MapBuilder<Vec<u8>>>,
        mut progress: impl FnMut(u32, u32) + 'a,
    ) -> (
        impl Stream<Item = Result<(CompactedFrameHeader, Bytes)>> + 'a,
        CompactedSegmentHeader,
    )
    where
        B: Backend,
    {
        let (snd, rcv) = tokio::sync::oneshot::channel();
        let mut snd = Some(snd);

        let stream = async_stream::try_stream! {
            assert!(!set.is_empty());
            let tmp = tempdir()?;
            let config = self.backend.default_config();
            // We fetch indexes in reverse order so that the most recent index comes first
            let indexes_stream = futures::stream::iter(set.iter().rev()).map(|k| {
                self
                    .backend
                    .fetch_segment_index(&config, &set.namespace, k)
                    .map(|i| i.map(|i| (i, *k)))
            })
                // we download indexes in the background as we read from their data files to reduce
                // latencies
                .buffered(4);

            tokio::pin!(indexes_stream);

            let mut size_after = u64::MAX;
            let mut seen_pages = RoaringBitmap::new();
            // keep track of the indexes for segments that we took frames from. This is a vec of
            // memory mapped segments, sorted by descending segment timestamp.
            let mut saved_indexes = Vec::new();
            // this map keeps a mapping from index in the saved indexed to the count of frames
            // taken from that segment. It is necessary to rebuild the new index and compute the
            // actual position of a frame in the streamed segement.
            let mut index_offset_mapping = Vec::new();
            let mut page_count = 0;
            let mut current_crc = 0;

            while let Some((index, key)) = indexes_stream.try_next().await? {
                let mut s = index.stream();
                // how many frames to take from that segment
                let mut frames_to_take = 0;
                while let Some((pno, _)) = s.next() {
                    let pno = u32::from_be_bytes(pno.try_into().unwrap());
                    if !seen_pages.contains(pno) {
                        // this segment contains data that we haven't seen before, download that
                        // segment
                        frames_to_take += 1;
                    }
                    tokio::task::consume_budget().await;
                }

                tracing::debug!(key = ?key, "taking {} frames from segment", frames_to_take);

                // no frames to take
                if frames_to_take == 0 { continue }

                // we need to build an index at the end, so we keep the indexes.
                // To reduce the amount of RAM needed to handle all the potential indexes, we write
                // it to disk, and map the file.
                if out_index.is_some() {
                    let mut index_file = std::fs::File::options()
                        .create(true)
                        .read(true)
                        .write(true)
                        .open(tmp.path().join(&format!("{key}")))?;
                    index_file.write_all(index.as_fst().as_bytes())?;
                    let map = unsafe { memmap::Mmap::map(&index_file)? };
                    let index = fst::Map::new(map).unwrap();
                    saved_indexes.push(index);
                    index_offset_mapping.push(page_count);
                }

                let (segment_header, frames) = self.backend.fetch_segment_data_stream(config.clone(), &set.namespace, key).await?;

                if size_after == u64::MAX {
                    size_after = segment_header.size_after() as u64;
                    let key = set.compact_key().expect("we asserted that the segment wasn't empty");
                    let segment_header = CompactedSegmentHeader::new(key.start_frame_no, key.end_frame_no, size_after as u32, key.timestamp(), uuid::Uuid::from_u128(segment_header.log_id.get()));
                    current_crc = crc32fast::hash(segment_header.as_bytes());
                    let _ = snd.take().unwrap().send(segment_header);
                }

                tokio::pin!(frames);

                let mut frames_taken = 0;
                while let Some((mut frame_header, frame_data)) = frames.try_next().await ? {
                    // we took all the frames that we needed from that segment, no need to read the
                    // rest of it
                    if frames_taken == frames_to_take {
                        break
                    }


                    if seen_pages.insert(frame_header.page_no()) {
                        frames_taken += 1;
                        page_count += 1;
                        let is_last = if page_count == size_after {
                            frame_header.set_last();
                            true
                        } else {
                            frame_header.reset_flags();
                            false
                        };

                        current_crc = frame_header.update_checksum(current_crc, &frame_data);
                        progress(page_count as u32, size_after as _);
                        yield (frame_header, frame_data);
                        if is_last {
                            break
                        }
                    }
                }
            }

            // now, we need to construct the index.
            if let Some(out_index) = out_index {
                let op_builder = saved_indexes.iter().collect::<fst::map::OpBuilder>();
                let mut union = op_builder.union();
                while let Some((pno, idxs)) = union.next() {
                    let &IndexedValue { index, .. } = idxs.iter().min_by_key(|idx| idx.index).unwrap();
                    let offset = index_offset_mapping[index];
                    index_offset_mapping[index] += 1;
                    out_index.insert(pno, offset as _).unwrap();
                    tokio::task::consume_budget().await;
                }
            }
        }.peekable();

        let mut stream = Box::pin(stream);
        let header = {
            stream.as_mut().peek().await;
            rcv.await.unwrap()
        };

        (stream, header)
    }

    /// compact the passed segment set to out_path if provided, otherwise, uploads it to the
    /// backend
    pub async fn compact(
        &self,
        set: SegmentSet,
        out_path: Option<&Path>,
        progress: impl FnMut(u32, u32),
    ) -> Result<()>
    where
        B: Backend,
    {
        let Some(new_key) = set.compact_key() else {
            return Ok(());
        };

        let mut builder = MapBuilder::new(Vec::new()).unwrap();

        let (sender, mut receiver) = tokio::sync::mpsc::channel::<crate::storage::Result<Bytes>>(1);
        let handle: JoinHandle<Result<()>> = match out_path {
            Some(path) => {
                let path = path.join(&format!("{new_key}.seg"));
                let mut data_file = tokio::fs::File::create(path).await?;
                tokio::task::spawn(async move {
                    while let Some(data) = receiver.recv().await {
                        let data = data?;
                        data_file.write_all(&data).await?;
                    }

                    data_file.flush().await?;

                    Ok(())
                })
            }
            None => {
                let backend = self.backend.clone();
                let config = self.backend.default_config();
                let ns = set.namespace.clone();
                let key = new_key.clone();
                tokio::task::spawn(async move {
                    backend
                        .store_segment_data(
                            &config,
                            &ns,
                            &key,
                            tokio_stream::wrappers::ReceiverStream::new(receiver),
                        )
                        .await?;
                    Ok(())
                })
            }
        };

        let (stream, segment_header) = self
            .dedup_stream(set.clone(), Some(&mut builder), progress)
            .await;

        sender
            .send(Ok(Bytes::copy_from_slice(segment_header.as_bytes())))
            .await
            .unwrap();

        {
            tokio::pin!(stream);
            loop {
                match stream.next().await {
                    Some(Ok((frame_header, frame_data))) => {
                        sender
                            .send(Ok(Bytes::copy_from_slice(frame_header.as_bytes())))
                            .await
                            .unwrap();
                        sender.send(Ok(frame_data)).await.unwrap();
                    }
                    Some(Err(_e)) => {
                        panic!()
                        // sender.send(Err(e.into())).await.unwrap();
                    }
                    None => break,
                }
            }
            drop(sender);
        }

        handle.await.unwrap()?;

        let index = builder.into_inner().unwrap();
        match out_path {
            Some(path) => {
                let mut index_file =
                    tokio::fs::File::create(path.join(&format!("{new_key}.idx"))).await?;
                index_file.write_all(&index).await?;
                index_file.flush().await?;
            }
            None => {
                self.backend
                    .store_segment_index(
                        &self.backend.default_config(),
                        &set.namespace,
                        &new_key,
                        index,
                    )
                    .await?;
            }
        }

        Ok(())
    }

    /// Restore a datatase file from a segment set
    /// set must start at frame_no 1
    pub async fn restore(
        &self,
        set: SegmentSet,
        to: impl AsRef<Path>,
        progress: impl FnMut(u32, u32),
    ) -> Result<()>
    where
        B: Backend,
    {
        let file = std::fs::File::create(to)?;
        let (stream, header) = self.dedup_stream(set.clone(), None, progress).await;
        let _footer = LibsqlFooter {
            magic: LIBSQL_MAGIC.into(),
            version: LIBSQL_WAL_VERSION.into(),
            replication_index: set.range().unwrap().1.into(),
            log_id: header.log_id.get().into(),
        };

        tokio::pin!(stream);

        while let Some((frame_header, frame_data)) = stream.try_next().await? {
            let (_, ret) = file
                .write_all_at_async(
                    frame_data,
                    LIBSQL_PAGE_SIZE as u64 * (frame_header.page_no() as u64 - 1),
                )
                .await;
            ret?;
        }

        Ok(())
    }

    pub fn list_all_segments(
        &self,
        namespace: &NamespaceName,
        f: impl FnMut(SegmentInfo),
    ) -> Result<()> {
        list_segments(&self.conn, namespace, f)
    }

    pub fn list_monitored_namespaces(&self, f: impl FnMut(NamespaceName)) -> Result<()> {
        list_namespace(&self.conn, f)
    }

    pub fn unmonitor(&self, ns: &NamespaceName) -> Result<()> {
        unmonitor(&self.conn, ns)
    }

    pub fn segment_info(&self, ns: &NamespaceName, key: SegmentKey) -> Result<SegmentInfo> {
        segment_infos(&self.conn, ns, key)
    }
}

pub struct AnalyzedSegments {
    graph: petgraph::graphmap::DiGraphMap<u64, u64>,
    last_frame_no: u64,
    namespace: NamespaceName,
}

impl AnalyzedSegments {
    /// returns a list of keys that covers frame_no 1 to last in the shortest amount of segments
    pub fn shortest_restore_path(&self) -> SegmentSet {
        if self.graph.node_count() == 0 {
            return SegmentSet {
                namespace: self.namespace.clone(),
                segments: Vec::new(),
            };
        }

        let path = petgraph::algo::astar(
            &self.graph,
            1,
            |n| n == self.last_frame_no,
            // it's always free to go from one end of the segment to the other, and it costs us to
            // fetch a new segment. edges between segments are always 0, and edges within segments
            // are the segment timestamp
            |(_, _, &x)| if x == 0 { 1 } else { 0 },
            |n| self.last_frame_no - n,
        );
        let mut segments = Vec::new();
        match path {
            Some((_len, nodes)) => {
                for chunk in nodes.chunks(2) {
                    let start_frame_no = chunk[0];
                    let end_frame_no = chunk[1];
                    let timestamp = *self
                        .graph
                        .edges(start_frame_no)
                        .find_map(|(_, to, ts)| (to == end_frame_no).then_some(ts))
                        .unwrap();
                    let key = SegmentKey {
                        start_frame_no,
                        end_frame_no,
                        timestamp,
                    };
                    segments.push(key);
                }
            }
            None => (),
        }
        SegmentSet {
            segments,
            namespace: self.namespace.clone(),
        }
    }

    pub fn last_frame_no(&self) -> u64 {
        self.last_frame_no
    }

    pub fn segment_count(&self) -> usize {
        self.graph.node_count() / 2
    }
}

/// A set of segments, with the guarantee that segments are non-overlapping and increasing in
/// frameno
#[derive(Clone)]
pub struct SegmentSet {
    namespace: NamespaceName,
    segments: Vec<SegmentKey>,
}

impl SegmentSet {
    pub fn range(&self) -> Option<(u64, u64)> {
        self.segments
            .first()
            .zip(self.segments.last())
            .map(|(f, l)| (f.start_frame_no, l.end_frame_no))
    }

    pub fn compact_key(&self) -> Option<SegmentKey> {
        match self.segments.first().zip(self.segments.last()) {
            Some((f, l)) => Some(SegmentKey {
                start_frame_no: f.start_frame_no,
                end_frame_no: l.end_frame_no,
                timestamp: l.timestamp,
            }),
            None => None,
        }
    }
}

impl Deref for SegmentSet {
    type Target = [SegmentKey];

    fn deref(&self) -> &Self::Target {
        &self.segments
    }
}

async fn sync_one<B: Backend>(
    backend: &B,
    namespace: &NamespaceName,
    id: u64,
    conn: &rusqlite::Connection,
    full: bool,
) -> Result<()> {
    let until = if full {
        get_last_frame_no(conn, id)?
    } else {
        None
    };

    let segs = backend.list_segments(backend.default_config(), &namespace, 0);
    tokio::pin!(segs);

    while let Some(info) = segs.next().await {
        let info = info.unwrap();
        register_segment_info(&conn, &info, id)?;
        if let Some(until) = until {
            if info.key.start_frame_no <= until {
                break;
            }
        }
    }

    Ok(())
}

fn list_segments<'a>(
    conn: &'a rusqlite::Connection,
    namespace: &'a NamespaceName,
    mut f: impl FnMut(SegmentInfo),
) -> Result<()> {
    let mut stmt = conn.prepare_cached(
        r#"
    SELECT timestamp, size, start_frame_no, end_frame_no
    FROM segments as s
    JOIN monitored_namespaces as m
    ON m.id == s.namespace_id
    WHERE m.namespace_name = ?
    ORDER BY end_frame_no, start_frame_no
    "#,
    )?;

    let iter = stmt.query_map([namespace.as_str()], |r| {
        Ok(SegmentInfo {
            key: SegmentKey {
                start_frame_no: r.get(2)?,
                end_frame_no: r.get(3)?,
                timestamp: r.get(0)?,
            },
            size: r.get(1)?,
        })
    })?;

    for info in iter {
        let info = info?;
        f(info);
    }

    Ok(())
}

fn list_namespace<'a>(
    conn: &'a rusqlite::Connection,
    mut f: impl FnMut(NamespaceName),
) -> Result<()> {
    let mut stmt = conn.prepare_cached(r#"SELECT namespace_name FROM monitored_namespaces"#)?;

    stmt.query_map((), |r| {
        let n = NamespaceName::from_string(r.get(0)?);
        f(n);
        Ok(())
    })?
    .try_for_each(|c| c)?;

    Ok(())
}

fn register_segment_info(
    conn: &rusqlite::Connection,
    info: &SegmentInfo,
    namespace_id: u64,
) -> Result<()> {
    let mut stmt = conn.prepare_cached(
        r#"
    INSERT OR IGNORE INTO segments (
        start_frame_no,
        end_frame_no,
        timestamp,
        size,
        namespace_id
    ) 
    VALUES (?, ?, ?, ?, ?)"#,
    )?;
    stmt.execute((
        info.key.start_frame_no,
        info.key.end_frame_no,
        info.key.timestamp,
        info.size,
        namespace_id,
    ))?;
    Ok(())
}

fn segments_range(
    conn: &rusqlite::Connection,
    namespace: &NamespaceName,
) -> Result<Option<(SegmentInfo, SegmentInfo)>> {
    let mut stmt = conn.prepare_cached(
        r#"
    SELECT min(timestamp), size, start_frame_no, end_frame_no
    FROM segments as s
    JOIN monitored_namespaces as m
    ON m.id == s.namespace_id
    WHERE m.namespace_name = ?
    LIMIT 1
"#,
    )?;
    let first = stmt
        .query_row([namespace.as_str()], |r| {
            Ok(SegmentInfo {
                key: SegmentKey {
                    start_frame_no: r.get(2)?,
                    end_frame_no: r.get(3)?,
                    timestamp: r.get(0)?,
                },
                size: r.get(1)?,
            })
        })
        .optional()?;

    let mut stmt = conn.prepare_cached(
        r#"
    SELECT max(timestamp), size, start_frame_no, end_frame_no
    FROM segments as s
    JOIN monitored_namespaces as m
    ON m.id == s.namespace_id
    WHERE m.namespace_name = ?
    LIMIT 1
"#,
    )?;
    let last = stmt
        .query_row([namespace.as_str()], |r| {
            Ok(SegmentInfo {
                key: SegmentKey {
                    start_frame_no: r.get(2)?,
                    end_frame_no: r.get(3)?,
                    timestamp: r.get(0)?,
                },
                size: r.get(1)?,
            })
        })
        .optional()?;

    Ok(first.zip(last))
}

fn get_last_frame_no(conn: &rusqlite::Connection, namespace_id: u64) -> Result<Option<u64>> {
    let mut stmt =
        conn.prepare_cached("SELECT MAX(end_frame_no) FROM segments WHERE namespace_id = ?")?;
    Ok(stmt.query_row([namespace_id], |row| row.get(0))?)
}

fn unmonitor(conn: &rusqlite::Connection, namespace: &NamespaceName) -> Result<()> {
    conn.execute(
        "DELETE FROM monitored_namespaces WHERE namespace_name = ?",
        [namespace.as_str()],
    )?;
    Ok(())
}

fn segment_infos(
    conn: &rusqlite::Connection,
    namespace: &NamespaceName,
    key: SegmentKey,
) -> Result<SegmentInfo> {
    let mut stmt = conn.prepare("SELECT size FROM segments AS s JOIN monitored_namespaces AS ns WHERE s.start_frame_no=? AND s.end_frame_no=? AND ns.namespace_name=? LIMIT 1")?;
    let mut rows = stmt.query((key.start_frame_no, key.end_frame_no, namespace.as_str()))?;

    let row = rows.next()?.unwrap();
    Ok(SegmentInfo {
        key,
        size: row.get(0)?,
    })
}
