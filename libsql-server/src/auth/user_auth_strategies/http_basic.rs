use crate::auth::{AuthError, Authenticated};

use super::{UserAuthContext, UserAuthStrategy};

pub struct HttpBasic {
    credential: String,
}

impl UserAuthStrategy for HttpBasic {
    fn authenticate(
        &self,
        context: Result<UserAuthContext, AuthError>,
    ) -> Result<Authenticated, AuthError> {
        tracing::trace!("executing http basic auth");

        // NOTE: this naive comparison may leak information about the `expected_value`
        // using a timing attack
        let expected_value = self.credential.trim_end_matches('=');

        let creds_match = match context?.token {
            Some(s) => s.contains(expected_value),
            None => expected_value.is_empty(),
        };

        if creds_match {
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
    use super::*;

    const CREDENTIAL: &str = "d29qdGVrOnRoZWJlYXI=";

    fn strategy() -> HttpBasic {
        HttpBasic::new(CREDENTIAL.into())
    }

    #[test]
    fn authenticates_with_valid_credential() {
        let context = Ok(UserAuthContext::basic(CREDENTIAL));

        assert!(matches!(
            strategy().authenticate(context).unwrap(),
            Authenticated::FullAccess
        ))
    }

    #[test]
    fn authenticates_with_valid_trimmed_credential() {
        let credential = CREDENTIAL.trim_end_matches('=');
        let context = Ok(UserAuthContext::basic(credential));

        assert!(matches!(
            strategy().authenticate(context).unwrap(),
            Authenticated::FullAccess
        ))
    }

    #[test]
    fn errors_when_credentials_do_not_match() {
        let context = Ok(UserAuthContext::basic("abc"));

        assert_eq!(
            strategy().authenticate(context).unwrap_err(),
            AuthError::BasicRejected
        )
    }
}
