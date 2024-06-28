use tonic::Status;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Write conflict")]
    WriteConflict,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<Error> for Status {
    fn from(error: Error) -> Self {
        match error {
            Error::WriteConflict => Status::aborted("write conflict"),
            Error::Other(err) => Status::internal(err.to_string()),
        }
    }
}
