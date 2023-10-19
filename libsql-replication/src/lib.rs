pub mod frame;
pub mod injector;
pub mod rpc;

mod error;

pub type FrameNo = u64;

pub const LIBSQL_PAGE_SIZE: usize = 4096;
