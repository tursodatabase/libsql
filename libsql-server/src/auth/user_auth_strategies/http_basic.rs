use crate::auth::{
    constants::{AUTH_HEADER, GRPC_AUTH_HEADER},
    AuthError, Authenticated,
};

use super::{UserAuthContext, UserAuthStrategy};

#[derive(Debug)]
pub struct HttpBasic {
    credential: String,
}

impl UserAuthStrategy for HttpBasic {
    fn authenticate(&self, ctx: UserAuthContext) -> Result<Authenticated, AuthError> {
        tracing::trace!("executing http basic auth");
        let auth_str = ctx
            .get_field(AUTH_HEADER)
            .or_else(|| ctx.get_field(GRPC_AUTH_HEADER));

        let (_, token) = auth_str
            .ok_or(AuthError::AuthHeaderNotFound)
            .map(|s| s.split_once(' ').ok_or(AuthError::AuthStringMalformed))
            .and_then(|o| o)?;

        // NOTE: this naive comparison may leak information about the `expected_value`
        // using a timing attack
        let expected_value = self.credential.trim_end_matches('=');
        let creds_match = token.contains(expected_value);
        if creds_match {
            return Ok(Authenticated::FullAccess);
        }
        Err(AuthError::BasicRejected)
    }

    fn required_fields(&self) -> Vec<&'static str> {
        vec![AUTH_HEADER, GRPC_AUTH_HEADER]
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
        let context = UserAuthContext::basic(CREDENTIAL);

        assert!(matches!(
            strategy().authenticate(context).unwrap(),
            Authenticated::FullAccess
        ))
    }

    #[test]
    fn authenticates_with_valid_trimmed_credential() {
        let credential = CREDENTIAL.trim_end_matches('=');
        let context = UserAuthContext::basic(credential);

        assert!(matches!(
            strategy().authenticate(context).unwrap(),
            Authenticated::FullAccess
        ))
    }

    #[test]
    fn errors_when_credentials_do_not_match() {
        let context = UserAuthContext::basic("abc");

        assert_eq!(
            strategy().authenticate(context).unwrap_err(),
            AuthError::BasicRejected
        )
    }
}
