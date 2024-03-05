pub mod disabled;
pub mod http_basic;
pub mod jwt;

use anyhow::Context;
pub use disabled::*;
pub use http_basic::*;
pub use jwt::*;

use super::{AuthError, Authenticated};

pub struct UserAuthContext {
    pub scheme: Option<String>,
    pub token: Option<String>, // token might not be required in some cases
}

impl TryFrom<&str> for UserAuthContext {
    type Error = anyhow::Error;

    fn try_from(auth_string: &str) -> Result<Self, Self::Error> {
        let (scheme, token) = auth_string.split_once(' ').context("malformed auth string`")?;
        Ok(UserAuthContext{scheme: Some(scheme.into()), token: Some(token.into())})    
    }
}
pub trait UserAuthStrategy: Sync + Send {
    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError>;
}
