use std::num::NonZeroU32;

use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::Wal;
use zerocopy::{AsBytes, FromZeroes};

use super::FrameNo;

pub(crate) mod compactor;
pub(crate) mod frame_notifier;
pub(crate) mod record_commit;
pub(crate) mod replication_index_injector;
pub(crate) mod replicator;

fn get_base_frame_no<T: Wal>(wal: &mut T) -> libsql_sys::wal::Result<FrameNo> {
    let mut header = Sqlite3DbHeader::new_zeroed();
    match wal.find_frame(NonZeroU32::new(1).unwrap())? {
        Some(i) => {
            wal.read_frame(i, header.as_bytes_mut())?;
            Ok(header.replication_index.get() - i.get() as u64)
        }
        None => {
            wal.db_file().read_at(header.as_bytes_mut(), 0)?;
            Ok(header.replication_index.get())
        }
    }
}
