#![allow(dead_code, unused_variables, unreachable_code)]
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::sync::Arc;
use std::path::PathBuf;

use hashbrown::HashMap;
use parking_lot::RwLock;

use crate::log::{SealedLog, Log};
use crate::name::NamespaceName;
use crate::shared_wal::SharedWal;

/// Wal Registry maintains a set of shared Wal, and their respective set of files.
pub struct WalRegistry {
    path: PathBuf,
    openned: RwLock<HashMap<NamespaceName, Arc<SharedWal>>>,
}

impl WalRegistry {
    pub fn new(path: PathBuf) -> Self {
        std::fs::create_dir_all(&path).unwrap();
        Self {
            path,
            openned: Default::default(),
        }
    }

    pub fn open(&self, namespace: NamespaceName, db_file: &libsql_sys::wal::Sqlite3File) -> Arc<SharedWal> {
        let mut openned = self.openned.upgradable_read();
        if let Some(entry) = openned.get(&namespace) {
            return entry.clone();
        }

        let path = self.path.join(namespace.as_str());
        std::fs::create_dir_all(&path).unwrap();
        let dir = walkdir::WalkDir::new(dbg!(&path))
            .sort_by_file_name()
            .into_iter();

        let mut segments = VecDeque::new();
        for entry in dir {
            let entry = entry.unwrap();
            if entry.path().extension().map(|e| e.to_str().unwrap() != "log").unwrap_or(true) {
                continue
            }
            let file = OpenOptions::new().read(true).open(dbg!(entry.path())).unwrap();
            let sealed = SealedLog::open(&file);
            segments.push_back(sealed);
        }

        dbg!();

        let (db_size, start_frame_no) = dbg!(segments.back().map(|log| { 
                    let header = log.header();
                    (header.db_size.get(), header.last_commited_frame_no.get() + 1)
                }).unwrap_or((0, 0)));
        let current_path = path.join(format!("{namespace}:{start_frame_no:020}.log"));
        let current = Arc::new(Log::create(&current_path, start_frame_no, db_size));
        
        let shared = Arc::new(SharedWal {
            current,
            segments: RwLock::new(segments),
            tx_id: Default::default(),
            next_tx_id: Default::default(),
        });

        openned.with_upgraded(|openned| {
            openned.insert(namespace.clone(), shared.clone());
        });

        shared
    }
}

// fn parse_file_name(s: &str) -> (NamespaceName, u64) {
//     let mut split = s.rsplit('.');
//     match split.next() {
//         Some("log") => {
//             return parse_log_entry(split.next().unwrap())
//         }
//         _ => todo!("handle other entry types"),
//     }
// }
//
// fn parse_log_entry(s: &str) -> (NamespaceName, u64) {
//     let mut split = s.rsplit(':');
//     let Some(start_frame_no) = split.next() else { panic!() };
//     let Some(namespace) = split.next() else { panic!() };
//     (NamespaceName::from_string(namespace.to_string()), start_frame_no.parse().unwrap())
// }
