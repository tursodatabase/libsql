pub mod frame;
pub mod primary;
pub mod replica;
mod snapshot;

use crc::Crc;
pub use primary::logger::{LogReadError, ReplicationLogger, ReplicationLoggerHook};
pub use snapshot::SnapshotCallback;

pub const WAL_PAGE_SIZE: i32 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");
const CRC_64_GO_ISO: Crc<u64> = Crc::<u64>::new(&crc::CRC_64_GO_ISO);

/// The frame uniquely identifying, monotonically increasing number
pub type FrameNo = u64;
