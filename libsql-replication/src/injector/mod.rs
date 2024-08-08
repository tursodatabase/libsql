use std::future::Future;

pub use libsql_injector::LibsqlInjector;
pub use sqlite_injector::SqliteInjector;

use crate::frame::{Frame, FrameNo};

pub use error::Error;
use error::Result;

mod error;
mod libsql_injector;
mod sqlite_injector;

pub trait Injector {
    /// Inject a singular frame.
    fn inject_frame(
        &mut self,
        frame: Frame,
    ) -> impl Future<Output = Result<Option<FrameNo>>> + Send;

    /// Discard any uncommintted frames.
    fn rollback(&mut self) -> impl Future<Output = ()> + Send;

    /// Flush the buffer to libsql WAL.
    /// Trigger a dummy write, and flush the cache to trigger a call to xFrame. The buffer's frame
    /// are then injected into the wal.
    fn flush(&mut self) -> impl Future<Output = Result<Option<FrameNo>>> + Send;
}
