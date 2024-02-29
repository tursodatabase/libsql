pub mod disabled;
pub mod http_basic;
pub mod jwt;

pub use disabled::*;
pub use http_basic::*;
pub use jwt::*;

use super::{AuthError, Authenticated};

pub struct UserAuthContext {
    pub scheme: Option<String>,
    pub token: Option<String>, // token might not be required in some cases
}

pub trait UserAuthStrategy: Sync + Send {
    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError>;
}
