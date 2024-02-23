pub mod disabled;
pub mod http_basic;
pub mod jwt;

use axum::http::HeaderValue;
pub use disabled::*;
pub use http_basic::*;
pub use jwt::*;

use crate::namespace::NamespaceName;

use super::{AuthError, Authenticated};

pub struct UserAuthContext {
    pub namespace: NamespaceName,
    pub namespace_credential: Option<jsonwebtoken::DecodingKey>,
    pub user_credential: Option<HeaderValue>,
}

pub trait UserAuthStrategy: Sync + Send {
    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError>;
}
