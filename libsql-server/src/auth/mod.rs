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

pub type Auth = Arc<dyn UserAuthStrategy>;
