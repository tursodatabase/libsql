pub mod frame;
pub mod primary;
pub mod replica;
pub mod replication_log;
mod snapshot;

use crc::Crc;
pub use primary::logger::{LogReadError, ReplicationLogger, ReplicationLoggerHook};

pub const WAL_PAGE_SIZE: i32 = 4096;
pub const WAL_MAGIC: u64 = u64::from_le_bytes(*b"SQLDWAL\0");
const CRC_64_GO_ISO: Crc<u64> = Crc::<u64>::new(&crc::CRC_64_GO_ISO);

/// The frame uniquely identifying, monotonically increasing number
pub type FrameNo = u64;

/// Trigger a hard database reset. This cause the database to be wiped, freshly restarted
/// This is used for replicas that are left in an unrecoverabe state and should restart from a
/// fresh state.
///
/// /!\ use with caution.
pub(crate) static HARD_RESET: once_cell::sync::Lazy<std::sync::Arc<tokio::sync::Notify>> =
    once_cell::sync::Lazy::new(|| std::sync::Arc::new(tokio::sync::Notify::new()));
