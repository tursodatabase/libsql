use std::pin::Pin;

use libsql_replication::frame::Frame;
use tokio_stream::Stream;

type BoxError = Box<dyn std::error::Error + Sync + Send + 'static>;
pub enum Frames {
    Vec(Vec<Frame>),
    Snapshot(Pin<Box<dyn Stream<Item = Result<Frame, BoxError>> + Send + Sync + 'static>>),
}
