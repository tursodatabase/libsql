use anyhow::{bail, Context as _, Result};

/// Authentication that is required to access the server.
#[derive(Default)]
pub struct Auth {
    /// When true, no authentication is required.
    pub disabled: bool,
    /// If `Some`, we accept HTTP basic auth if it matches this value.
    pub http_basic: Option<String>,
    /// If `Some`, we accept all JWTs signed by this key.
    pub jwt_key: Option<jsonwebtoken::DecodingKey>,
}

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("The `Authorization` HTTP header is required but was not specified")]
    HttpAuthHeaderMissing,
    #[error("The `Authorization` HTTP header has invalid value")]
    HttpAuthHeaderInvalid,
    #[error("The authentication scheme in the `Authorization` HTTP header is not supported")]
    HttpAuthHeaderUnsupportedScheme,
    #[error("The `Basic` HTTP authentication scheme is not allowed")]
    BasicNotAllowed,
    #[error("The `Basic` HTTP authentication credentials were rejected")]
    BasicRejected,
    #[error("Authentication is required but no JWT was specified")]
    JwtMissing,
    #[error("Authentication using a JWT is not allowed")]
    JwtNotAllowed,
    #[error("The JWT is invalid")]
    JwtInvalid,
    #[error("The JWT has expired")]
    JwtExpired,
    #[error("The JWT is immature (not valid yet)")]
    JwtImmature,
    #[error("Authentication failed")]
    Other,
}

/// A witness that the user has been authenticated.
pub struct Authenticated(());

impl Auth {
    pub fn authenticate_http(
        &self,
        auth_header: Option<&hyper::header::HeaderValue>,
    ) -> Result<Authenticated, AuthError> {
        if self.disabled {
            return Ok(Authenticated(()));
        }

        let Some(auth_header) = auth_header else {
            return Err(AuthError::HttpAuthHeaderMissing)
        };

        match parse_http_auth_header(auth_header)? {
            HttpAuthHeader::Basic(actual_value) => {
                let Some(expected_value) = self.http_basic.as_ref() else {
                    return Err(AuthError::BasicNotAllowed)
                };
                // NOTE: this naive comparison may leak information about the `expected_value`
                // using a timing attack
                let actual_value = actual_value.trim_end_matches('=');
                let expected_value = expected_value.trim_end_matches('=');
                if actual_value == expected_value {
                    Ok(Authenticated(()))
                } else {
                    Err(AuthError::BasicRejected)
                }
            }
            HttpAuthHeader::Bearer(token) => self.validate_jwt(&token),
        }
    }

    pub fn authenticate_jwt(&self, jwt: Option<&str>) -> Result<Authenticated, AuthError> {
        if self.disabled {
            return Ok(Authenticated(()));
        }

        let Some(jwt) = jwt else {
            return Err(AuthError::JwtMissing)
        };

        self.validate_jwt(jwt)
    }

    fn validate_jwt(&self, jwt: &str) -> Result<Authenticated, AuthError> {
        let Some(jwt_key) = self.jwt_key.as_ref() else {
            return Err(AuthError::JwtNotAllowed)
        };
        validate_jwt(jwt_key, jwt)
    }
}

#[derive(Debug)]
enum HttpAuthHeader {
    Basic(String),
    Bearer(String),
}

fn parse_http_auth_header(
    header: &hyper::header::HeaderValue,
) -> Result<HttpAuthHeader, AuthError> {
    let Ok(header) = header.to_str() else {
        return Err(AuthError::HttpAuthHeaderInvalid)
    };

    let Some((scheme, param)) = header.split_once(' ') else {
        return Err(AuthError::HttpAuthHeaderInvalid)
    };

    if scheme.eq_ignore_ascii_case("basic") {
        Ok(HttpAuthHeader::Basic(param.into()))
    } else if scheme.eq_ignore_ascii_case("bearer") {
        Ok(HttpAuthHeader::Bearer(param.into()))
    } else {
        Err(AuthError::HttpAuthHeaderUnsupportedScheme)
    }
}

fn validate_jwt(
    jwt_key: &jsonwebtoken::DecodingKey,
    jwt: &str,
) -> Result<Authenticated, AuthError> {
    use jsonwebtoken::errors::ErrorKind;

    let validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
    match jsonwebtoken::decode::<serde_json::Value>(jwt, jwt_key, &validation) {
        Ok(_token) => Ok(Authenticated(())),
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

pub fn parse_http_basic_auth_arg(arg: &str) -> Result<String> {
    let Some((scheme, param)) = arg.split_once(':') else {
        bail!("invalid HTTP auth config: {arg}")
    };

    if scheme == "basic" {
        Ok(param.into())
    } else {
        bail!("unsupported HTTP auth scheme: {scheme:?}")
    }
}

pub fn parse_jwt_key(data: &str) -> Result<jsonwebtoken::DecodingKey> {
    if data.starts_with("-----BEGIN PUBLIC KEY-----") {
        jsonwebtoken::DecodingKey::from_ed_pem(data.as_bytes())
            .context("Could not decode Ed25519 public key from PEM")
    } else if data.starts_with("-----BEGIN PRIVATE KEY-----") {
        bail!("Received a private key, but a public key is expected")
    } else if data.starts_with("-----BEGIN") {
        bail!("Key is in unsupported PEM format")
    } else {
        jsonwebtoken::DecodingKey::from_ed_components(data)
            .context("Could not decode Ed25519 public key from base64")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    fn authenticate_http(auth: &Auth, header: &str) -> Result<Authenticated, AuthError> {
        auth.authenticate_http(Some(&HeaderValue::from_str(header).unwrap()))
    }

    const VALID_JWT: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSJ9.\
        eyJleHAiOjE2NzczMzE3NTJ9.\
        k1_9-JjMmGCtCuZb7HshOlRojPQmDgu_88o-t4SAR1r7YZCHr6TPaNtWH1tqZuNFut5P64fZTcE-RpiLd8IWDA";
    const VALID_JWT_KEY: &str = "6Zx1NP27GNsej38CoGCQuUJZYAxGETQ1bIE-Fqxkyjk";

    #[test]
    fn test_default() {
        let auth = Auth::default();
        assert!(auth.authenticate_http(None).is_err());
        assert!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWJlYXI=").is_err());
        assert!(auth.authenticate_jwt(Some(VALID_JWT)).is_err());
    }

    #[test]
    fn test_http_basic() {
        let auth = Auth {
            http_basic: Some(parse_http_basic_auth_arg("basic:d29qdGVrOnRoZWJlYXI=").unwrap()),
            ..Auth::default()
        };
        assert!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWJlYXI=").is_ok());
        assert!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWJlYXI").is_ok());
        assert!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWJlYXI===").is_ok());

        assert!(authenticate_http(&auth, "basic d29qdGVrOnRoZWJlYXI=").is_ok());

        assert!(authenticate_http(&auth, "Basic d29qdgvronrozwjlyxi=").is_err());
        assert!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWZveA==").is_err());

        assert!(auth.authenticate_http(None).is_err());
        assert!(authenticate_http(&auth, "").is_err());
        assert!(authenticate_http(&auth, "foobar").is_err());
        assert!(authenticate_http(&auth, "foo bar").is_err());
        assert!(authenticate_http(&auth, "basic #$%^").is_err());
    }

    #[test]
    fn test_http_bearer() {
        let auth = Auth {
            jwt_key: Some(parse_jwt_key(VALID_JWT_KEY).unwrap()),
            ..Auth::default()
        };
        assert!(authenticate_http(&auth, &format!("Bearer {VALID_JWT}")).is_ok());
        assert!(authenticate_http(&auth, &format!("bearer {VALID_JWT}")).is_ok());

        assert!(authenticate_http(&auth, "Bearer foobar").is_err());
        assert!(authenticate_http(&auth, &format!("Bearer {}", &VALID_JWT[..80])).is_err());
    }

    #[test]
    fn test_jwt() {
        let auth = Auth {
            jwt_key: Some(parse_jwt_key(VALID_JWT_KEY).unwrap()),
            ..Auth::default()
        };
        assert!(auth.authenticate_jwt(Some(VALID_JWT)).is_ok());
        assert!(auth.authenticate_jwt(Some(&VALID_JWT[..80])).is_err());
    }
}
