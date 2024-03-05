pub mod disabled;
pub mod http_basic;
pub mod jwt;

pub use disabled::Disabled;
pub use http_basic::HttpBasic;
pub use jwt::Jwt;

use anyhow::Context;

use super::{AuthError, Authenticated};

pub struct UserAuthContext {
    pub scheme: Option<String>,
    pub token: Option<String>,
}

impl TryFrom<&str> for UserAuthContext {
    type Error = anyhow::Error;

    fn try_from(auth_string: &str) -> Result<Self, Self::Error> {
        let (scheme, token) = auth_string
            .split_once(' ')
            .context("malformed auth string`")?;
        Ok(UserAuthContext {
            scheme: Some(scheme.into()),
            token: Some(token.into()),
        })
    }
}
pub trait UserAuthStrategy: Sync + Send {
    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError>;
}
