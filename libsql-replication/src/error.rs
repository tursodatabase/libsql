#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Invalid frame length")]
    InvalidFrameLen,
}
