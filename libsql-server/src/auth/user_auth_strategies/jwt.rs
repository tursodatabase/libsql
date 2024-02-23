use crate::{
    auth::{parse_http_auth_header, AuthError, Authenticated, Authorized, Permission},
    namespace::NamespaceName,
};

use super::{UserAuthContext, UserAuthStrategy};

pub struct Jwt {
    key: jsonwebtoken::DecodingKey,
}

impl UserAuthStrategy for Jwt {
    fn authenticate(&self, context: UserAuthContext) -> Result<Authenticated, AuthError> {
        tracing::trace!("executing jwt auth");

        let param = parse_http_auth_header("bearer", &context.user_credential)?;

        let jwt_key = match context.namespace_credential.as_ref() {
            Some(jwt_key) => jwt_key,
            None => &self.key,
        };

        validate_jwt(jwt_key, param, context.namespace)
    }
}

impl Jwt {
    pub fn new(key: jsonwebtoken::DecodingKey) -> Self {
        Self { key }
    }
}

fn validate_jwt(
    jwt_key: &jsonwebtoken::DecodingKey,
    jwt: &str,
    namespace: NamespaceName,
) -> Result<Authenticated, AuthError> {
    use jsonwebtoken::errors::ErrorKind;

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
    validation.required_spec_claims.remove("exp");

    match jsonwebtoken::decode::<serde_json::Value>(jwt, jwt_key, &validation).map(|t| t.claims) {
        Ok(serde_json::Value::Object(claims)) => {
            tracing::trace!("Claims: {claims:#?}");
            let namespace = if namespace == NamespaceName::default() {
                None
            } else {
                claims
                    .get("id")
                    .and_then(|ns| NamespaceName::from_string(ns.as_str()?.into()).ok())
            };

            let permission = match claims.get("a").and_then(|s| s.as_str()) {
                Some("ro") => Permission::ReadOnly,
                Some("rw") => Permission::FullAccess,
                Some(_) => return Ok(Authenticated::Anonymous),
                // Backward compatibility - no access claim means full access
                None => Permission::FullAccess,
            };

            Ok(Authenticated::Authorized(Authorized {
                namespace,
                permission,
            }))
        }
        Ok(_) => Err(AuthError::JwtInvalid),
        Err(error) => Err(match error.kind() {
            ErrorKind::InvalidToken
            | ErrorKind::InvalidSignature
            | ErrorKind::InvalidAlgorithm
            | ErrorKind::Base64(_)
            | ErrorKind::Json(_)
            | ErrorKind::Utf8(_) => AuthError::JwtInvalid,
            ErrorKind::ExpiredSignature => AuthError::JwtExpired,
            ErrorKind::ImmatureSignature => AuthError::JwtImmature,
            _ => AuthError::Other,
        }),
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use crate::auth::parse_jwt_key;

    use super::*;

    const KEY: &str = "zaMv-aFGmB7PXkjM4IrMdF6B5zCYEiEGXW3RgMjNAtc";

    fn strategy() -> Jwt {
        Jwt::new(parse_jwt_key(KEY).unwrap())
    }

    #[test]
    fn authenticates_valid_jwt_token_with_full_access() {
        let token = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.\
            eyJleHAiOjc5ODg0ODM4Mjd9.\
            MatB2aLnPFusagqH2RMoVExP37o2GFLmaJbmd52OdLtAehRNeqeJZPrefP1t2GBFidApUTLlaBRL6poKq_s3CQ";

        let context = UserAuthContext {
            namespace: NamespaceName::default(),
            namespace_credential: None,
            user_credential: HeaderValue::from_str(&format!("Bearer {token}")).ok(),
        };

        assert_eq!(
            strategy().authenticate(context).unwrap(),
            Authenticated::Authorized(Authorized {
                namespace: None,
                permission: Permission::FullAccess,
            })
        )
    }

    #[test]
    fn authenticates_valid_jwt_token_with_read_only_access() {
        let token = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.\
            eyJleHAiOjc5ODg0ODM4MjcsImEiOiJybyJ9.\
            _2ZZiO2HC8b3CbCHSCufXXBmwpl-dLCv5O9Owvpy7LZ9aiQhXODpgV-iCdTsLQJ5FVanWhfn3FtJSnmWHn25DQ";

        let context = UserAuthContext {
            namespace: NamespaceName::default(),
            namespace_credential: None,
            user_credential: HeaderValue::from_str(&format!("Bearer {token}")).ok(),
        };

        assert_eq!(
            strategy().authenticate(context).unwrap(),
            Authenticated::Authorized(Authorized {
                namespace: None,
                permission: Permission::ReadOnly,
            })
        )
    }

    #[test]
    fn errors_when_jwt_token_invalid() {
        let context = UserAuthContext {
            namespace: NamespaceName::default(),
            namespace_credential: None,
            user_credential: HeaderValue::from_str("Bearer abc").ok(),
        };

        assert_eq!(
            strategy().authenticate(context).unwrap_err(),
            AuthError::JwtInvalid
        )
    }
}
