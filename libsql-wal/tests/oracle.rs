//! This test suite runs about 30k test files against sqlite and our test_suite, compares the
//! results, and then compares the database files.

use std::borrow::Cow;
use std::ffi::{c_char, c_int};
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;

use libsql_sys::ffi::{sqlite3_finalize, sqlite3_prepare};
use libsql_sys::rusqlite::{OpenFlags, self};
use libsql_sys::wal::{Sqlite3WalManager, Wal};
use libsql_sys::Connection;
use libsql_wal::name::NamespaceName;
use libsql_wal::{file::FileExt, registry::WalRegistry, wal::LibsqlWalManager};
use once_cell::sync::Lazy;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use regex::{Regex, Captures};
use tempfile::tempdir;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

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

#[test]
// #[ignore]
fn test_oracle() {
    // enable_libsql_logging();
    let manifest_path: &Path = env!("CARGO_MANIFEST_DIR").as_ref();
    let test_samples_path = manifest_path.join("tests/assets/fixtures");

    let filter = std::env::var("LIBSQL_WAL_FILTER");
    let dir = walkdir::WalkDir::new(test_samples_path);
    for entry in dir {
        let entry = entry.unwrap();
        if let Ok(ref filter) = filter {
            if entry.path().file_name().unwrap().to_str().unwrap() != filter {
                continue;
            }

        }
        if entry.file_type().is_file() {
            run_test_sample(entry.path()).unwrap();
        }
    }
}

fn run_test_sample(path: &Path) -> Result {
    println!("test: {:?}", path.file_name().unwrap());
    let tmp = tempdir()?;

    std::fs::create_dir_all(tmp.path().join("test")).unwrap();
    std::env::set_current_dir(tmp.path().join("test")).unwrap();

    let script = std::fs::read_to_string(path).unwrap();

    let sqlite_conn = libsql_sys::Connection::open(
        tmp.path().join("test/data"),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        Sqlite3WalManager::default(),
        1000,
        None,
    )
    .unwrap();

    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(42);
    let before = std::time::Instant::now();
    let sqlite_results = run_script(&sqlite_conn, &script, &mut rng).collect::<Vec<_>>();
    println!("ran sqlite in {:?}", before.elapsed());
    drop(sqlite_conn);

    std::fs::rename(tmp.path().join("test/data"), tmp.path().join("sqlite-data")).unwrap();
    std::fs::remove_dir_all(tmp.path().join("test")).unwrap();
    std::fs::create_dir_all(tmp.path().join("test")).unwrap();
    std::env::set_current_dir(tmp.path().join("test")).unwrap();

    let resolver = |path: &Path| {
        let name = path
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        NamespaceName::from_string(name.to_string())
    };

    let registry = Arc::new(WalRegistry::new(tmp.path().join("test/wals"), resolver).unwrap());
    let wal_manager = LibsqlWalManager {
        registry: registry.clone(),
        next_conn_id: Default::default(),
    };
    let libsql_conn = libsql_sys::Connection::open(
        tmp.path().join("test/data").clone(),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        wal_manager.clone(),
        100000,
        None,
    )
    .unwrap();

    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(42);
    let before = std::time::Instant::now();
    let libsql_results = run_script(&libsql_conn, &script, &mut rng).collect::<Vec<_>>();
    println!("ran libsql in {:?}", before.elapsed());

    for ((a, _), (b, _)) in sqlite_results.iter().zip(libsql_results.iter()) {
        if a != b {
            panic!("sqlite and libsql output differ:\n{}", PrintScript(sqlite_results, libsql_results));
        }
    }

    drop(libsql_conn);

    // for checkpoint
    registry.shutdown().unwrap();

    match std::panic::catch_unwind(|| {
        compare_db_files(
            &tmp.path().join("sqlite-data"),
            &tmp.path().join("test/data"),
        );
    }) {
        Ok(_) => (),
        Err(e) => {
            let path = tmp.into_path();
            std::fs::rename(path, "failure").unwrap();
            std::panic::resume_unwind(e)
        }
    }

    Ok(())
}

struct PrintScript(Vec<(String, String)>, Vec<(String, String)>);

impl Display for PrintScript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for ((a, cmd), (b, _)) in self.0.iter().zip(self.1.iter()) {
            writeln!(f, "stmt: {}", cmd.trim())?;
            if a != b {
                writeln!(f, "sqlite: {}", a)?;
                writeln!(f, "libsql: {}", b)?;
            } else {
                writeln!(f, "OK")?;
            }
        }

        Ok(())
    }
}

fn compare_db_files(db1: &Path, db2: &Path) {
    let db1 = std::fs::File::open(db1).unwrap();
    let db2 = std::fs::File::open(db2).unwrap();

    let len1 = db1.metadata().unwrap().len();
    let len2 = db2.metadata().unwrap().len();

    assert_eq!(len1, len2);

    let n_pages = len1 % 4096;
    let mut buf1 = [0; 4096];
    let mut buf2 = [0; 4096];

    for i in 0..n_pages {
        db1.read_exact_at(&mut buf1, i * 4096).unwrap();
        db2.read_exact_at(&mut buf2, i * 4096).unwrap();

        assert_eq!(buf1[..4096 - 8], buf2[..4096 - 8], "page {i} differ");
    }
}

fn run_script<'a, T: Wal>(
    conn: &'a Connection<T>,
    script: &'a str,
    rng: &'a mut ChaCha8Rng,
) -> impl Iterator<Item = (String, String)> + 'a {
    let mut stmts = split_statements(conn, script);
    std::iter::from_fn(move || {
        let stmt_str = patch_randomness(stmts.next()?, rng);
        if stmt_str.trim_start().starts_with("ATTACH") {
            patch_attach(&stmt_str);
        }
        let mut stmt = conn.prepare(&stmt_str).unwrap();

        let ret = 
            stmt.query(())
                .unwrap()
                .mapped(|r| Ok(format!("{r:?}")))
                .map(|r| {
                    // let _ = r.as_ref().map_err(|e| assert!(!matches!(e.sqlite_error_code().unwrap(), ErrorCode::DatabaseCorrupt)));
                    r.unwrap_or_else(|e| e.to_string())
                })
                .collect::<String>();

        Some((ret, stmt_str.to_string()))
    })
}

fn patch_attach(s: &str) {
    let mut split = s.split_whitespace();
    let name = split.nth(1).unwrap();
    let name = name.trim_matches('\'');
    let _ = libsql_sys::Connection::open(
        name,
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        Sqlite3WalManager::default(),
        100000,
        None,
    )
    .unwrap();
}

fn patch_randomness<'a>(s: &'a str, rng: &'a mut ChaCha8Rng) -> Cow<'a, str> {
    static RE_RND: Lazy<Regex> = Lazy::new(|| Regex::new(r#"randomblob\((\d+)\)"#).unwrap());
    // static RE_STR: Lazy<Regex> = Lazy::new(|| Regex::new(r#"randomstr\((\d+), (\d+)\)"#).unwrap());
    RE_RND.replace_all(s, |cap: &Captures| { 
        let len = cap[1].parse::<usize>().unwrap();
        let mut data = vec![0; len];
        rng.fill_bytes(&mut data);
        format!("X'{}'", hex::encode(data))
    })
}

// shenanigans to split statments
fn split_statements<'a, T: Wal>(
    conn: &'a Connection<T>,
    script: &'a str,
) -> impl Iterator<Item = &'a str> + 'a {
    let mut tail = script.as_ptr() as *const c_char;
    let mut stmt = std::ptr::null_mut();
    let mut len = script.len();
    std::iter::from_fn(move || unsafe {
        let previous = tail;
        sqlite3_prepare(conn.handle(), previous, len as _, &mut stmt, &mut tail);
        sqlite3_finalize(stmt);
        if stmt.is_null() {
            None
        } else {
            let start = previous as usize - (script.as_ptr() as usize);
            let end = tail as usize - (script.as_ptr() as usize);
            let s = std::str::from_utf8(&script.as_bytes()[start..end]).unwrap();
            len -= s.len();
            Some(s)
        }
    })
}
