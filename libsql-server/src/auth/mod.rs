use std::sync::Arc;

pub mod authenticated;
pub mod authorized;
pub mod constants;
pub mod errors;
pub mod parsers;
pub mod permission;
pub mod user_auth_strategies;

pub use authenticated::Authenticated;
pub use authorized::Authorized;
pub use errors::AuthError;
pub use parsers::{parse_http_auth_header, parse_http_basic_auth_arg, parse_jwt_key};
pub use permission::Permission;
pub use user_auth_strategies::{Disabled, HttpBasic, Jwt, UserAuthContext, UserAuthStrategy};

#[derive(Clone)]
pub struct Auth {
    pub user_strategy: Arc<dyn UserAuthStrategy + Send + Sync>,
}

impl Auth {
    pub fn new(user_strategy: impl UserAuthStrategy + Send + Sync + 'static) -> Self {
        Self {
            user_strategy: Arc::new(user_strategy),
        }
    }

    pub fn authenticate(
        &self,
        context: Result<UserAuthContext, AuthError>,
    ) -> Result<Authenticated, AuthError> {
        self.user_strategy.authenticate(context)
    }
}
