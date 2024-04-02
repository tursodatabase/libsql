use std::{sync::Arc, path::Path};

use libsql_sys::rusqlite::OpenFlags;
use libsql_wal::{shared_wal::SharedWal, wal::LibsqlWalManager, registry::{self, WalRegistry}};

use tracing_subscriber::{EnvFilter, fmt::{self, format::FmtSpan}, prelude::*};

fn main() {
    tracing_subscriber::registry()
        .with(fmt::layer()
            .with_span_events(FmtSpan::CLOSE))
        .with(EnvFilter::from_default_env())
        .init();

    let path = std::env::args().nth(1).unwrap();
    let path = <str as AsRef<Path>>::as_ref(path.as_str());
    std::fs::create_dir_all(&path).unwrap();
    let registry = Arc::new(WalRegistry::new(path.join("wals")));
    let wal_manager = LibsqlWalManager {
        registry,
        name: "test".into(),
    };
    let conn = libsql_sys::Connection::open(path.join("data"), OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_READ_WRITE, wal_manager, 100000, None).unwrap();

    let lines = std::io::stdin().lines();
    for line in lines {
        let line = line.unwrap();
        if line.trim() == "quit" {
            break;
        }
        let mut stmt = conn.prepare(&line).unwrap();
        let mut rows = stmt.query(()).unwrap();
        while let Ok(Some(row)) = rows.next() {
            dbg!(row);
        }
    }
}
