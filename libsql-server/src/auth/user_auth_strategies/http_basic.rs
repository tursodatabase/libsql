use crate::auth::{parse_http_auth_header, AuthError, Authenticated};

use super::{UserAuthContext, UserAuthStrategy};

pub struct HttpBasic {
    credential: String,
}

impl UserAuthStrategy for HttpBasic {
    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError> {
        tracing::trace!("executing http basic auth");

        let param = parse_http_auth_header("basic", &context.user_credential)?;

        // NOTE: this naive comparison may leak information about the `expected_value`
        // using a timing attack
        let actual_value = param.trim_end_matches('=');
        let expected_value = self.credential.trim_end_matches('=');

        if actual_value == expected_value {
            return Ok(Authenticated::FullAccess);
        }

        Err(AuthError::BasicRejected)
    }
}

impl HttpBasic {
    pub fn new(credential: String) -> Self {
        Self { credential }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    const CREDENTIAL: &str = "d29qdGVrOnRoZWJlYXI=";

    fn strategy() -> HttpBasic {
        HttpBasic::new(CREDENTIAL.into())
    }

    #[test]
    fn authenticates_with_valid_credential() {
        let context = UserAuthContext {
            user_credential: HeaderValue::from_str(&format!("Basic {CREDENTIAL}")).ok(),
        };

        assert!(matches!(
            strategy().authenticate(context).unwrap(),
            Authenticated::FullAccess
        ))
    }

    #[test]
    fn authenticates_with_valid_trimmed_credential() {
        let credential = CREDENTIAL.trim_end_matches('=');

        let context = UserAuthContext {
            user_credential: HeaderValue::from_str(&format!("Basic {credential}")).ok(),
        };

        assert!(matches!(
            strategy().authenticate(context).unwrap(),
            Authenticated::FullAccess
        ))
    }

    #[test]
    fn errors_when_credentials_do_not_match() {
        let context = UserAuthContext {
            user_credential: HeaderValue::from_str("Basic abc").ok(),
        };

        assert_eq!(
            strategy().authenticate(context).unwrap_err(),
            AuthError::BasicRejected
        )
    }
}
