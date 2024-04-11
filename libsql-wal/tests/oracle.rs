//! This test suite runs about 30k test files against sqlite and our test_suite, compares the
//! results, and then compares the database files.

use std::{path::Path, sync::Arc, ffi::{CString, c_char}};

use libsql_sys::{
    rusqlite::OpenFlags,
    wal::{Sqlite3WalManager, WalManager, Wal},
    Connection, ffi::{sqlite3_prepare, sqlite3_finalize},
};
use libsql_wal::{registry::WalRegistry, wal::LibsqlWalManager, file::FileExt};
use tempfile::tempdir;

type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[test]
fn test_oracle() {
    let manifest_path: &Path = env!("CARGO_MANIFEST_DIR").as_ref();
    let test_samples_path = manifest_path.join("tests/assets/samples");

    let dir = walkdir::WalkDir::new(test_samples_path);
    for entry in dir {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            run_test_sample(entry.path()).unwrap();
        }
    }
}

fn run_test_sample(path: &Path) -> Result {
    println!("test: {:?}", path.file_name().unwrap());
    let tmp = tempdir()?;

    std::fs::create_dir_all(tmp.path().join("test")).unwrap();

    let script = std::fs::read_to_string(path).unwrap();

    let sqlite_conn = libsql_sys::Connection::open(
        tmp.path().join("test/data"),
        OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE,
        Sqlite3WalManager::default(),
        1000,
        None,
    )
    .unwrap();

    let sqlite_results = run_script(&sqlite_conn, &script).collect::<Vec<_>>();
    let _ = sqlite_conn.execute("ROLLBACK", ());
    let _ = sqlite_conn.execute("VACUUM", ());
    drop(sqlite_conn);
    
    std::fs::rename(tmp.path().join("test/data"), tmp.path().join("sqlite-data")).unwrap();
    std::fs::remove_dir_all(tmp.path().join("test")).unwrap();
    std::fs::create_dir_all(tmp.path().join("test")).unwrap();

    let registry = Arc::new(WalRegistry::new(tmp.path().join("test/wals")));
    let wal_manager = LibsqlWalManager {
        registry: registry.clone(),
        namespace: "test".into(),
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

    let libsql_results = run_script(&libsql_conn, &script).collect::<Vec<_>>();
    let _ = libsql_conn.execute("ROLLBACK", ());
    let _ = libsql_conn.execute("VACUUM", ());
    for (a, b) in sqlite_results.iter().zip(libsql_results.iter()) {
        assert_eq!(a, b)
    }

    drop(libsql_conn);

    // for checkpoint
    registry.shutdown();
    
    match std::panic::catch_unwind(|| {
        compare_db_files(&tmp.path().join("sqlite-data"), &tmp.path().join("test/data"));
    }) {
        Ok(_) => (),
        Err(e) => {
            let path = tmp.into_path();
            std::fs::rename(path, "failure").unwrap();
            std::panic::resume_unwind(e)
        },
    }

    Ok(())
}

fn compare_db_files(db1: &Path, db2: &Path) {
    let db1 = std::fs::File::open(db1).unwrap();
    let db2 = std::fs::File::open(db2).unwrap();

    let len1 = db1.metadata().unwrap().len();
    let len2 = db2.metadata().unwrap().len();

    assert_eq!(len2, len1);

    let n_pages = len1 % 4096;
    let mut buf1 = [0; 4096];
    let mut buf2 = [0; 4096];

    for i in 0..n_pages {
        db1.read_exact_at(&mut buf1, i * 4096).unwrap();
        db2.read_exact_at(&mut buf2, i * 4096).unwrap();

        assert_eq!(
            buf1[..4096 - 8],
            buf2[..4096 - 8],
            "page {i} differ"
        );
    }
}

fn run_script<'a, T: Wal>(conn: &'a Connection<T>, script: &'a str) -> impl Iterator<Item = String> + 'a {
    let mut stmts = split_statements(conn, script);
    std::iter::from_fn(move || {
        let stmt = dbg!(stmts.next()?);
        let mut stmt = conn.prepare(&stmt).unwrap();
        Some(stmt.query(()).unwrap().mapped(|r| Ok(format!("{r:?}"))).map(|r| r.unwrap_or_else(|e| e.to_string())).collect::<String>())
    })
}

// shenanigans to split statments
fn split_statements<'a, T: Wal>(conn: &'a Connection<T>, script: &'a str) -> impl Iterator<Item = &'a str> + 'a {
    let mut tail = script.as_ptr() as *const c_char;
    let mut stmt = std::ptr::null_mut();
    let mut len = script.len();
    std::iter::from_fn(move || {
        unsafe {
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
        }
    })
}
