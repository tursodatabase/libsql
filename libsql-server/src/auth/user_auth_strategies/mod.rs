pub mod disabled;
pub mod http_basic;
pub mod jwt;

pub use disabled::Disabled;
use hashbrown::HashMap;
pub use http_basic::HttpBasic;
pub use jwt::Jwt;

use super::{constants::AUTH_HEADER, AuthError, Authenticated};

#[derive(Debug)]
pub struct UserAuthContext {
    custom_fields: HashMap<&'static str, String>,
}

impl UserAuthContext {
    pub fn empty() -> UserAuthContext {
        UserAuthContext {
            custom_fields: HashMap::new(),
        }
    }

    pub fn basic(creds: &str) -> UserAuthContext {
        UserAuthContext {
            custom_fields: HashMap::from([(AUTH_HEADER, format!("Basic {creds}"))]),
        }
    }

    pub fn bearer(token: &str) -> UserAuthContext {
        UserAuthContext {
            custom_fields: HashMap::from([(AUTH_HEADER, format!("Bearer {token}"))]),
        }
    }

    pub fn new(scheme: &str, token: &str) -> UserAuthContext {
        UserAuthContext {
            custom_fields: HashMap::from([(AUTH_HEADER, format!("{scheme} {token}"))]),
        }
    }

    pub fn from_auth_str(auth_string: &str) -> Result<Self, AuthError> {
        let (scheme, token) = auth_string
            .split_once(' ')
            .ok_or(AuthError::AuthStringMalformed)?;
        Ok(UserAuthContext::new(scheme, token))
    }

    pub fn add_field(&mut self, key: &'static str, value: String) {
        self.custom_fields.insert(key, value.into());
    }

    pub fn get_field(&self, key: &'static str) -> Option<&String> {
        return self.custom_fields.get(key);
    }
}

pub trait UserAuthStrategy: Sync + Send + std::fmt::Debug {
    /// Returns a list of fields required by the stragegy.
    /// Every strategy implementation should override this function if it requires input to work.
    /// Strategy implementations should validate the content of provided fields.
    ///
    /// The caller is responsible for providing at least one of these fields in UserAuthContext.
    /// The caller should assume the strategy will not work if none of the required fields is provided.
    fn required_fields(&self) -> Vec<&'static str> {
        vec![]
    }

    /// Performs authentication of the user and returns Authenticated witness if successful.
    /// Returns respective AuthError communicating the reason for failure.
    /// Assumes the context input contains at least one of the fields specified in required_fields()
    ///
    /// Warning: this function deals with sensitive information.
    /// Implementer should be very careful about what information they chose to log or provide in AuthError message.
    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError>;
}
