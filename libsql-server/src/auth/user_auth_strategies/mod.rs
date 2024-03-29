pub mod disabled;
pub mod http_basic;
pub mod jwt;
pub mod proxy_grpc;

pub use disabled::Disabled;
use hashbrown::HashMap;
pub use http_basic::HttpBasic;
pub use jwt::Jwt;
pub use proxy_grpc::ProxyGrpc;

use super::{AuthError, Authenticated};

#[derive(Debug)]
pub struct UserAuthContext {
    pub custom_fields: HashMap<Box<str>, String>,
}

impl UserAuthContext {
    pub fn empty() -> UserAuthContext {
        UserAuthContext {
            custom_fields: HashMap::new(),
        }
    }

    pub fn basic(creds: &str) -> UserAuthContext {
        UserAuthContext {
            custom_fields: HashMap::from([("authorization".into(), format!("Basic {creds}"))]),
        }
    }

    pub fn bearer(token: &str) -> UserAuthContext {
        UserAuthContext {
            custom_fields: HashMap::from([("authorization".into(), format!("Bearer {token}"))]),
        }
    }

    pub fn new(scheme: &str, token: &str) -> UserAuthContext {
        UserAuthContext {
            custom_fields: HashMap::from([("authorization".into(), format!("{scheme} {token}"))]),
        }
    }

    pub fn from_auth_str(auth_string: &str) -> Result<Self, AuthError> {
        let (scheme, token) = auth_string
            .split_once(' ')
            .ok_or(AuthError::AuthStringMalformed)?;
        Ok(UserAuthContext::new(scheme, token))
    }

    pub fn add_field(&mut self, key: String, value: String) {
        self.custom_fields.insert(key.into(), value.into());
    }
}

pub trait UserAuthStrategy: Sync + Send {
    fn required_fields(&self) -> Vec<String> {
        vec![]
    }

    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError>;
}
