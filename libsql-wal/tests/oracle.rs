//! This test suite runs about 30k test files against sqlite and our test_suite, compares the
//! results, and then compares the database files.

use std::ffi::c_char;
use std::fmt::Display;
use std::mem::size_of;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::sync::Arc;

use libsql_sys::ffi::{sqlite3_finalize, sqlite3_prepare, Sqlite3DbHeader};
use libsql_sys::name::NamespaceName;
use libsql_sys::rusqlite::OpenFlags;
use libsql_sys::wal::{Sqlite3WalManager, Wal};
use libsql_sys::Connection;
use libsql_wal::registry::WalRegistry;
use libsql_wal::storage::TestStorage;
use libsql_wal::test::{seal_current_segment, wait_current_durable};
use libsql_wal::wal::LibsqlWalManager;
use once_cell::sync::Lazy;
use rand::Rng;
use rand_chacha::rand_core::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
use regex::{Captures, Regex};
use tempfile::tempdir;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[tokio::test]
async fn test_oracle() {
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
            run_test_sample(entry.path()).await.unwrap();
        }
    }
}

async fn run_test_sample(path: &Path) -> Result {
    println!("test: {:?}", path.file_name().unwrap());
    let curdir = std::env::current_dir().unwrap();
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
        if path.file_name().unwrap() != "data" {
            return NamespaceName::from_string(
                path.file_name().unwrap().to_str().unwrap().to_string(),
            );
        }
        let name = path
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap();
        NamespaceName::from_string(name.to_string())
    };

    let (sender, _receiver) = tokio::sync::mpsc::channel(64);
    let registry = Arc::new(
        WalRegistry::new(
            tmp.path().join("test/wals"),
            TestStorage::new().into(),
            sender,
        )
        .unwrap(),
    );
    let wal_manager = LibsqlWalManager::new(registry.clone(), Arc::new(resolver));
    let db_path = tmp.path().join("test/data").clone();
    let libsql_conn = libsql_sys::Connection::open(
        &db_path,
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
            panic!(
                "sqlite and libsql output differ:\n{}",
                PrintScript(sqlite_results, libsql_results)
            );
        }
    }

    drop(libsql_conn);

    let shared = registry.clone().open(&db_path, &"test".into()).unwrap();
    seal_current_segment(&shared);
    wait_current_durable(&shared).await;
    shared.checkpoint().await.unwrap();

    std::env::set_current_dir(curdir).unwrap();
    match std::panic::catch_unwind(|| {
        compare_db_files(
            &tmp.path().join("sqlite-data"),
            &tmp.path().join("test/data"),
        );
    }) {
        Ok(_) => (),
        Err(e) => {
            let path = tmp.into_path();
            std::fs::rename(path, "./failure").unwrap();
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

fn compare_db_files(sqlite: &Path, libsql: &Path) {
    let db1 = std::fs::File::open(sqlite).unwrap();
    let db2 = std::fs::File::open(libsql).unwrap();

    let len1 = db1.metadata().unwrap().len();
    let len2 = db2.metadata().unwrap().len();

    // sqlite file may contain a footer, compare only the data portions of the db files.
    assert_eq!(len1, 4096 * (len2 / 4096));

    let n_pages = len1 / 4096;
    let mut buf1 = [0; 4096];
    let mut buf2 = [0; 4096];

    for i in 0..n_pages {
        db1.read_exact_at(&mut buf1, i * 4096).unwrap();
        db2.read_exact_at(&mut buf2, i * 4096).unwrap();
        if i == 0 {
            // todo!(page 1 differ)
            assert_eq!(
                buf1[size_of::<Sqlite3DbHeader>()..],
                buf2[size_of::<Sqlite3DbHeader>()..],
                "page 1 differ"
            );
            continue;
        } else {
            assert_eq!(buf1, buf2, "page {} differ", i + 1);
        }
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

        let ret = stmt
            .query(())
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

fn patch_randomness<'a>(s: &'a str, rng: &'a mut ChaCha8Rng) -> String {
    static RE_RANDOM_BLOB: Lazy<Regex> =
        Lazy::new(|| Regex::new(r#"randomblob\((\d+)\)"#).unwrap());
    static RE_RANDOM: Lazy<Regex> = Lazy::new(|| Regex::new(r#"random\(\)"#).unwrap());
    // static RE_STR: Lazy<Regex> = Lazy::new(|| Regex::new(r#"randomstr\((\d+), (\d+)\)"#).unwrap());
    let s = RE_RANDOM_BLOB.replace_all(s, |cap: &Captures| {
        let len = cap[1].parse::<usize>().unwrap();
        let mut data = vec![0; len];
        rng.fill_bytes(&mut data);
        format!("X'{}'", hex::encode(data))
    });

    RE_RANDOM
        .replace_all(&s, |_cap: &Captures| {
            let rand: u64 = rng.gen();
            format!("{rand}")
        })
        .to_string()
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
