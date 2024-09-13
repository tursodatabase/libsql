use std::ffi::c_int;
use std::fs::File;
use std::future::ready;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Arc;

use chrono::prelude::{DateTime, Utc};
use libsql_wal::io::{file::FileExt, Io};
use libsql_wal::registry::WalRegistry;
use libsql_wal::storage::TestStorage;
use libsql_wal::wal::LibsqlWalManager;

use libsql_sys::name::NamespaceName;
use libsql_sys::rusqlite::{self, ErrorCode, OpenFlags};

use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tempfile::tempdir;

#[derive(Clone)]
struct FlakyIo {
    p_failure: f32,
    rng: Arc<Mutex<rand_chacha::ChaCha8Rng>>,
    enabled: Arc<AtomicBool>,
}

struct FlakyFile {
    inner: File,
    fs: FlakyIo,
}

impl FileExt for FlakyFile {
    fn write_all_at(&self, buf: &[u8], offset: u64) -> std::io::Result<()> {
        self.fs
            .with_random_failure(|| self.inner.write_all_at(buf, offset))
    }

    fn write_at_vectored(&self, bufs: &[std::io::IoSlice], offset: u64) -> std::io::Result<usize> {
        self.fs
            .with_random_failure(|| self.inner.write_at_vectored(bufs, offset))
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        self.fs
            .with_random_failure(|| self.inner.write_at(buf, offset))
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
        self.fs
            .with_random_failure(|| self.inner.read_exact_at(buf, offset))
    }

    fn sync_all(&self) -> std::io::Result<()> {
        self.fs.with_random_failure(|| self.inner.sync_all())
    }

    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.fs.with_random_failure(|| self.inner.set_len(len))
    }

    fn len(&self) -> std::io::Result<u64> {
        self.inner.len()
    }

    fn read_exact_at_async<B: libsql_wal::io::buf::IoBufMut + Send + 'static>(
        &self,
        _buf: B,
        _offset: u64,
    ) -> impl std::future::Future<Output = (B, std::io::Result<()>)> + Send {
        todo!();
        #[allow(unreachable_code)]
        ready((_buf, Ok(())))
    }

    fn write_all_at_async<B: libsql_wal::io::buf::IoBuf + Send + 'static>(
        &self,
        _buf: B,
        _offset: u64,
    ) -> impl std::future::Future<Output = (B, std::io::Result<()>)> + Send {
        todo!();
        #[allow(unreachable_code)]
        ready((_buf, Ok(())))
    }

    fn read_at(&self, _buf: &mut [u8], _offset: u64) -> std::io::Result<usize> {
        todo!()
    }

    async fn read_at_async<B: libsql_wal::io::buf::IoBufMut + Send + 'static>(
        &self,
        _buf: B,
        _offset: u64,
    ) -> (B, std::io::Result<usize>) {
        todo!()
    }
}

impl FlakyIo {
    fn with_random_failure<R>(&self, f: impl FnOnce() -> std::io::Result<R>) -> std::io::Result<R> {
        let r = self.rng.lock().gen_range(0.0..1.0);
        if self.enabled.load(Relaxed) && r <= self.p_failure {
            Err(std::io::Error::new(std::io::ErrorKind::Other, "failure"))
        } else {
            f()
        }
    }
}

impl Io for FlakyIo {
    type File = FlakyFile;
    type TempFile = FlakyFile;
    type Rng = rand_chacha::ChaCha8Rng;

    fn create_dir_all(&self, path: &std::path::Path) -> std::io::Result<()> {
        self.with_random_failure(|| std::fs::create_dir_all(path))
    }

    fn open(
        &self,
        create_new: bool,
        read: bool,
        write: bool,
        path: &std::path::Path,
    ) -> std::io::Result<Self::File> {
        self.with_random_failure(|| {
            let inner = std::fs::OpenOptions::new()
                .create_new(create_new)
                .read(read)
                .write(write)
                .open(path)?;
            Ok(FlakyFile {
                inner,
                fs: self.clone(),
            })
        })
    }

    fn tempfile(&self) -> std::io::Result<Self::TempFile> {
        todo!()
    }

    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    fn hard_link(&self, _src: &Path, _dst: &Path) -> std::io::Result<()> {
        todo!()
    }

    fn with_rng<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Self::Rng) -> R,
    {
        f(&mut self.rng.lock())
    }

    fn remove_file_async(
        &self,
        path: &Path,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async move { self.with_random_failure(|| std::fs::remove_file(path)) }
    }
}

macro_rules! assert_not_corrupt {
    ($($e:expr,)*) => {
        $(
            match $e {
                Ok(_) => (),
                Err(e) => {
                    match e.sqlite_error() {
                        Some(e) if e.code == ErrorCode::DatabaseCorrupt => panic!("db corrupt"),
                        _ => ()
                    }
                }
            };
        )*
    };
}

fn enable_libsql_logging() {
    use std::sync::Once;
    static ONCE: Once = Once::new();

    fn libsql_log(code: c_int, msg: &str) {
        println!("sqlite error {code}: {msg}");
    }

    ONCE.call_once(|| unsafe {
        rusqlite::trace::config_log(Some(libsql_log)).unwrap();
    });
}

#[tokio::test]
async fn flaky_fs() {
    enable_libsql_logging();
    let seed = rand::thread_rng().gen();
    println!("seed: {seed}");
    let enabled = Arc::new(AtomicBool::new(false));
    let io = FlakyIo {
        p_failure: 0.1,
        rng: Arc::new(Mutex::new(ChaCha8Rng::seed_from_u64(seed))),
        enabled: enabled.clone(),
    };
    let tmp = tempdir().unwrap();
    let resolver = |path: &Path| {
        let name = path.file_name().unwrap().to_str().unwrap();
        NamespaceName::from_string(name.to_string())
    };
    let (sender, _receiver) = tokio::sync::mpsc::channel(64);
    let registry = Arc::new(
        WalRegistry::new_with_io(
            io.clone(),
            tmp.path().join("test/wals"),
            TestStorage::new_io(false, io).into(),
            sender,
        )
        .unwrap(),
    );
    let wal_manager = LibsqlWalManager::new(registry.clone(), Arc::new(resolver));

    let conn = libsql_sys::Connection::open(
        tmp.path().join("test/data").clone(),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        wal_manager.clone(),
        100000,
        None,
    )
    .unwrap();

    let _ = conn.execute(
        "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB(16), c BLOB(16), d BLOB(400));",
        (),
    );
    let _ = conn.execute("CREATE INDEX i1 ON t1(b);", ());
    let _ = conn.execute("CREATE INDEX i2 ON t1(c);", ());

    enabled.store(true, Relaxed);

    for _ in 0..50_000 {
        assert_not_corrupt! {
            conn.execute("REPLACE INTO t1 VALUES(abs(random() % 5000000), randomblob(16), randomblob(16), randomblob(400));", ()),
            conn.execute("REPLACE INTO t1 VALUES(abs(random() % 5000000), randomblob(16), randomblob(16), randomblob(400));", ()),
            conn.execute("REPLACE INTO t1 VALUES(abs(random() % 5000000), randomblob(16), randomblob(16), randomblob(400));", ()),
        }

        let mut stmt = conn
            .prepare("SELECT * FROM t1 WHERE a>abs((random()%5000000)) LIMIT 10;")
            .unwrap();

        assert_not_corrupt! {
            stmt.query(()).map(|r| r.mapped(|_r| Ok(())).count()),
            stmt.query(()).map(|r| r.mapped(|_r| Ok(())).count()),
            stmt.query(()).map(|r| r.mapped(|_r| Ok(())).count()),
        }
    }

    enabled.store(false, Relaxed);

    conn.pragma_query(None, "integrity_check", |_r| Ok(()))
        .unwrap();
    conn.query_row("select count(0) from t1", (), |_r| Ok(()))
        .unwrap();
}
