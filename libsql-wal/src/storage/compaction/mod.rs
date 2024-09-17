use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::DateTime;
use fst::map::OpBuilder;
use fst::Streamer;
use libsql_sys::name::NamespaceName;
use libsql_sys::rusqlite::OptionalExtension;
use libsql_sys::rusqlite::{self, TransactionBehavior};
use tempfile::tempdir;
use tokio_stream::StreamExt;
use uuid::Uuid;
use zerocopy::AsBytes;

use crate::io::buf::ZeroCopyBuf;
use crate::io::FileExt;
use crate::segment::compacted::CompactedSegment;
use crate::segment::compacted::CompactedSegmentDataFooter;
use crate::segment::compacted::CompactedSegmentDataHeader;
use crate::segment::Frame;
use crate::storage::backend::SegmentMeta;
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
    meta: rusqlite::Connection,
    path: PathBuf,
}

impl<B> Compactor<B> {
    pub fn new(backend: Arc<B>, compactor_path: &Path) -> Result<Self> {
        let meta = rusqlite::Connection::open(compactor_path.join("meta.db"))?;
        // todo! set pragmas: wal + foreign key check
        meta.pragma_update(None, "journal_mode", "wal")?;
        meta.execute(r#"CREATE TABLE IF NOT EXISTS monitored_namespaces (id INTEGER PRIMARY KEY AUTOINCREMENT, namespace_name BLOB NOT NULL, UNIQUE(namespace_name))"#, ()).unwrap();
        meta.execute(
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
            meta,
            path: compactor_path.into(),
        })
    }

    pub async fn monitor(&mut self, namespace: &NamespaceName) -> Result<()>
    where
        B: Backend,
    {
        let tx = self.meta.transaction()?;
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
        let mut stmt = self.meta.prepare_cached(
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
        segments_range(&self.meta, namespace)
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
            .meta
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
            .meta
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

    async fn fetch(
        &self,
        set: &SegmentSet,
        into: &Path,
    ) -> Result<(
        Vec<CompactedSegment<std::fs::File>>,
        Vec<fst::Map<Arc<[u8]>>>,
    )>
    where
        B: Backend,
    {
        let mut indexes = Vec::with_capacity(set.len());
        let mut segments = Vec::with_capacity(set.len());
        for key in set.iter() {
            let file = std::fs::File::options()
                .create_new(true)
                .write(true)
                .read(true)
                .open(into.join(&format!("{key}.data")))
                .unwrap();
            let header = self
                .backend
                .fetch_segment_data_to_file(
                    &self.backend.default_config(),
                    &set.namespace,
                    key,
                    &file,
                )
                .await
                .unwrap();
            let index = self
                .backend
                .fetch_segment_index(&self.backend.default_config(), &set.namespace, key)
                .await
                .unwrap();
            indexes.push(index);
            segments.push(CompactedSegment::from_parts(file, header));
        }

        Ok((segments, indexes))
    }

    pub async fn compact(&self, set: SegmentSet) -> Result<()>
    where
        B: Backend,
    {
        assert!(!set.is_empty());
        let tmp = tempdir().unwrap();
        // FIXME: bruteforce: we don't necessarily need to download all the segments to cover all
        // the changes. Iterating backward over the set items and filling the gaps in the pages
        // range would, in theory, be sufficient
        // another alternative is to download all the indexes, and lazily download the segment data
        // TODO: fetch conccurently
        let (segments, indexes) = self.fetch(&set, tmp.path()).await?;
        let last_header = segments.last().expect("non-empty set").header();

        // It's unfortunate that we need to know the number of frames in the final segment ahead of
        // time, but it's necessary for computing the checksum. This seems like the least costly
        // methods (over recomputing the whole checksum).
        let mut union = OpBuilder::from_iter(indexes.iter()).union();
        let mut count = 0;
        while let Some(_) = union.next() {
            count += 1;
        }

        let mut hasher = crc32fast::Hasher::new();

        let out_file = std::fs::File::options()
            .create_new(true)
            .write(true)
            .read(true)
            .open(tmp.path().join("out"))
            .unwrap();
        let header = CompactedSegmentDataHeader {
            frame_count: (count as u32).into(),
            segment_id: Uuid::new_v4().to_u128_le().into(),
            start_frame_no: set.range().expect("non-empty set").0.into(),
            end_frame_no: set.range().expect("non-empty set").1.into(),
            size_after: last_header.size_after,
            version: LIBSQL_WAL_VERSION.into(),
            magic: LIBSQL_MAGIC.into(),
            page_size: last_header.page_size,
            // the new compacted segment inherit the last segment timestamp: it contains the same
            // logical data.
            timestamp: last_header.timestamp,
        };

        hasher.update(header.as_bytes());
        let (_, ret) = out_file
            .write_all_at_async(ZeroCopyBuf::new_init(header), 0)
            .await;
        ret?;

        let mut union = OpBuilder::from_iter(indexes.iter()).union();
        let mut buffer = Box::new(ZeroCopyBuf::<Frame>::new_uninit());
        let mut out_index = fst::MapBuilder::memory();
        let mut current_offset = 0;

        while let Some((page_no_bytes, indexed_offsets)) = union.next() {
            let (index, offset) = indexed_offsets
                .iter()
                .max_by_key(|v| v.index)
                .map(|v| (v.index, v.value))
                .expect("union returned something, must be non-empty");
            let segment = &segments[index];
            let (frame, ret) = segment.read_frame(buffer, offset as u32).await;
            ret?;
            hasher.update(&frame.get_ref().as_bytes());
            let dest_offset =
                size_of::<CompactedSegmentDataHeader>() + current_offset * size_of::<Frame>();
            let (mut frame, ret) = out_file.write_all_at_async(frame, dest_offset as u64).await;
            ret?;
            out_index
                .insert(page_no_bytes, current_offset as _)
                .unwrap();
            current_offset += 1;
            frame.deinit();
            buffer = frame;
        }

        let footer = CompactedSegmentDataFooter {
            checksum: hasher.finalize().into(),
        };

        let footer_offset =
            size_of::<CompactedSegmentDataHeader>() + current_offset * size_of::<Frame>();
        let (_, ret) = out_file
            .write_all_at_async(ZeroCopyBuf::new_init(footer), footer_offset as _)
            .await;
        ret?;

        let (start, end) = set.range().expect("non-empty set");
        let timestamp = DateTime::from_timestamp_millis(set.last().unwrap().timestamp as _)
            .unwrap()
            .to_utc();
        self.backend
            .store(
                &self.backend.default_config(),
                SegmentMeta {
                    namespace: set.namespace.clone(),
                    segment_id: Uuid::new_v4(),
                    start_frame_no: start,
                    end_frame_no: end,
                    segment_timestamp: timestamp,
                },
                out_file,
                out_index.into_inner().unwrap(),
            )
            .await?;

        Ok(())
    }

    /// Restore a datatase file from a segment set
    /// set must start at frame_no 1
    pub async fn restore(&self, set: SegmentSet, to: impl AsRef<Path>) -> Result<()>
    where
        B: Backend,
    {
        if set.is_empty() {
            return Ok(());
        }
        assert_eq!(set.range().unwrap().0, 1);
        let tmp = tempdir()?;
        let (segments, indexes) = self.fetch(&set, tmp.path()).await?;
        let mut union = OpBuilder::from_iter(indexes.iter()).union();
        let mut buffer = Vec::with_capacity(LIBSQL_PAGE_SIZE as usize);
        let out_file = std::fs::File::create(to)?;

        while let Some((page_no_bytes, indexed_offsets)) = union.next() {
            let page_no = u32::from_be_bytes(page_no_bytes.try_into().unwrap());
            let (index, offset) = indexed_offsets
                .iter()
                .max_by_key(|v| v.index)
                .map(|v| (v.index, v.value as u32))
                .expect("union returned something, must be non-empty");
            let segment = &segments[index];
            let (b, ret) = segment.read_page(buffer, offset).await;
            ret?;
            let offset = (page_no as u64 - 1) * LIBSQL_PAGE_SIZE as u64;
            let (mut b, ret) = out_file.write_all_at_async(b, offset).await;
            ret?;
            b.clear();
            buffer = b;
        }

        Ok(())
    }

    pub fn list_all_segments(
        &self,
        namespace: &NamespaceName,
        f: impl FnMut(SegmentInfo),
    ) -> Result<()> {
        list_segments(&self.meta, namespace, f)
    }

    pub fn list_monitored_namespaces(&self, f: impl FnMut(NamespaceName)) -> Result<()> {
        list_namespace(&self.meta, f)
    }

    pub fn unmonitor(&self, ns: &NamespaceName) -> Result<()> {
        unmonitor(&self.meta, ns)
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
