use axum::response::IntoResponse;
use hyper::StatusCode;

use crate::error::ResponseError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to register migration job: {0}")]
    Registration(Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("migration scheduler exited")]
    SchedulerExited,
}

impl ResponseError for Error {}

impl IntoResponse for &Error {
    fn into_response(self) -> axum::response::Response {
        match self {
            Error::Registration(_) | Error::SchedulerExited => {
                self.format_err(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }
}
