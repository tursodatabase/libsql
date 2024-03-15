pub mod disabled;
pub mod http_basic;
pub mod jwt;

pub use disabled::Disabled;
pub use http_basic::HttpBasic;
pub use jwt::Jwt;

use super::{AuthError, Authenticated};

#[derive(Debug)]
pub struct UserAuthContext {
    scheme: Option<String>,
    token: Option<String>,
}

impl UserAuthContext {
    pub fn scheme(&self) -> &Option<String> {
        &self.scheme
    }

    pub fn token(&self) -> &Option<String> {
        &self.token
    }

    pub fn empty() -> UserAuthContext {
        UserAuthContext {
            scheme: None,
            token: None,
        }
    }

    pub fn basic(creds: &str) -> UserAuthContext {
        UserAuthContext {
            scheme: Some("Basic".into()),
            token: Some(creds.into()),
        }
    }

    pub fn bearer(token: &str) -> UserAuthContext {
        UserAuthContext {
            scheme: Some("Bearer".into()),
            token: Some(token.into()),
        }
    }

    pub fn bearer_opt(token: Option<String>) -> UserAuthContext {
        UserAuthContext {
            scheme: Some("Bearer".into()),
            token: token,
        }
    }

    pub fn new(scheme: &str, token: &str) -> UserAuthContext {
        UserAuthContext {
            scheme: Some(scheme.into()),
            token: Some(token.into()),
        }
    }

    pub fn from_auth_str(auth_string: &str) -> Result<Self, AuthError> {
        let (scheme, token) = auth_string
            .split_once(' ')
            .ok_or(AuthError::AuthStringMalformed)?;
        Ok(UserAuthContext::new(scheme, token))
    }
}

pub trait UserAuthStrategy: Sync + Send {
    fn authenticate(
        &self,
        context: Result<UserAuthContext, AuthError>,
    ) -> Result<Authenticated, AuthError>;
}
