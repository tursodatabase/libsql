pub mod disabled;
pub mod http_basic;
pub mod jwt;

pub use disabled::Disabled;
pub use http_basic::HttpBasic;
pub use jwt::Jwt;

use super::{AuthError, Authenticated};

pub struct UserAuthContext {
    pub scheme: Option<String>,
    pub token: Option<String>,
}

impl TryFrom<&str> for UserAuthContext {
    type Error = AuthError;

    fn try_from(auth_string: &str) -> Result<Self, AuthError> {
        let (scheme, token) = auth_string
            .split_once(' ')
            .ok_or(AuthError::AuthStringMalformed)?; 
        Ok(UserAuthContext {
            scheme: Some(scheme.into()),
            token: Some(token.into()),
        })
    }
}
pub trait UserAuthStrategy: Sync + Send {
    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError>;
}
