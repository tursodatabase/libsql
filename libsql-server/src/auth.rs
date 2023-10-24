use anyhow::{bail, Context as _, Result};
use axum::http::HeaderValue;
use libsql_replication::rpc::replication::NAMESPACE_METADATA_KEY;
use tonic::Status;

use crate::namespace::NamespaceName;

static GRPC_AUTH_HEADER: &str = "x-authorization";
static GRPC_PROXY_AUTH_HEADER: &str = "x-proxy-authorization";

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Authorized {
    pub namespace: Option<NamespaceName>,
    pub permission: Permission,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Permission {
    FullAccess,
    ReadOnly,
}

/// A witness that the user has been authenticated.
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Authenticated {
    Anonymous,
    Authorized(Authorized),
}

impl Auth {
    pub fn authenticate_http(
        &self,
        auth_header: Option<&hyper::header::HeaderValue>,
        disable_namespaces: bool,
    ) -> Result<Authenticated, AuthError> {
        if self.disabled {
            return Ok(Authenticated::Authorized(Authorized {
                namespace: None,
                permission: Permission::FullAccess,
            }));
        }

        let Some(auth_header) = auth_header else {
            return Err(AuthError::HttpAuthHeaderMissing);
        };

        match parse_http_auth_header(auth_header)? {
            HttpAuthHeader::Basic(actual_value) => {
                let Some(expected_value) = self.http_basic.as_ref() else {
                    return Err(AuthError::BasicNotAllowed);
                };
                // NOTE: this naive comparison may leak information about the `expected_value`
                // using a timing attack
                let actual_value = actual_value.trim_end_matches('=');
                let expected_value = expected_value.trim_end_matches('=');
                if actual_value == expected_value {
                    Ok(Authenticated::Authorized(Authorized {
                        namespace: None,
                        permission: Permission::FullAccess,
                    }))
                } else {
                    Err(AuthError::BasicRejected)
                }
            }
            HttpAuthHeader::Bearer(token) => self.validate_jwt(&token, disable_namespaces),
        }
    }

    pub fn authenticate_grpc<T>(
        &self,
        req: &tonic::Request<T>,
        disable_namespaces: bool,
    ) -> Result<Authenticated, Status> {
        let metadata = req.metadata();

        let auth = metadata
            .get(GRPC_AUTH_HEADER)
            .map(|v| v.to_bytes().expect("Auth should always be ASCII"))
            .map(|v| HeaderValue::from_maybe_shared(v).expect("Should already be valid header"));

        self.authenticate_http(auth.as_ref(), disable_namespaces)
            .map_err(Into::into)
    }

    pub fn authenticate_jwt(
        &self,
        jwt: Option<&str>,
        disable_namespaces: bool,
    ) -> Result<Authenticated, AuthError> {
        if self.disabled {
            return Ok(Authenticated::Authorized(Authorized {
                namespace: None,
                permission: Permission::FullAccess,
            }));
        }

        let Some(jwt) = jwt else {
            return Err(AuthError::JwtMissing);
        };

        self.validate_jwt(jwt, disable_namespaces)
    }

    fn validate_jwt(
        &self,
        jwt: &str,
        disable_namespaces: bool,
    ) -> Result<Authenticated, AuthError> {
        let Some(jwt_key) = self.jwt_key.as_ref() else {
            return Err(AuthError::JwtNotAllowed);
        };
        validate_jwt(jwt_key, jwt, disable_namespaces)
    }
}

impl Authenticated {
    pub fn from_proxy_grpc_request<T>(
        req: &tonic::Request<T>,
        disable_namespace: bool,
    ) -> Result<Self, Status> {
        let namespace = if disable_namespace {
            None
        } else {
            req.metadata()
                .get_bin(NAMESPACE_METADATA_KEY)
                .map(|c| c.to_bytes())
                .transpose()
                .map_err(|_| Status::invalid_argument("failed to parse namespace header"))?
                .map(NamespaceName::from_bytes)
                .transpose()
                .map_err(|_| Status::invalid_argument("invalid namespace name"))?
        };

        let auth = match req
            .metadata()
            .get(GRPC_PROXY_AUTH_HEADER)
            .map(|v| v.to_str())
            .transpose()
            .map_err(|_| Status::invalid_argument("missing authorization header"))?
        {
            Some("full_access") => Authenticated::Authorized(Authorized {
                namespace,
                permission: Permission::FullAccess,
            }),
            Some("read_only") => Authenticated::Authorized(Authorized {
                namespace,
                permission: Permission::ReadOnly,
            }),
            Some("anonymous") => Authenticated::Anonymous,
            Some(level) => {
                return Err(Status::permission_denied(format!(
                    "invalid authorization level: {}",
                    level
                )))
            }
            None => return Err(Status::invalid_argument("x-proxy-authorization not set")),
        };

        Ok(auth)
    }

    pub fn upgrade_grpc_request<T>(&self, req: &mut tonic::Request<T>) {
        let key = tonic::metadata::AsciiMetadataKey::from_static(GRPC_PROXY_AUTH_HEADER);

        let auth = match self {
            Authenticated::Anonymous => "anonymous",
            Authenticated::Authorized(Authorized {
                permission: Permission::FullAccess,
                ..
            }) => "full_access",
            Authenticated::Authorized(Authorized {
                permission: Permission::ReadOnly,
                ..
            }) => "read_only",
        };

        let value = tonic::metadata::AsciiMetadataValue::try_from(auth).unwrap();

        req.metadata_mut().insert(key, value);
    }

    pub fn is_namespace_authorized(&self, namespace: &NamespaceName) -> bool {
        match self {
            Authenticated::Anonymous => false,
            Authenticated::Authorized(Authorized {
                namespace: Some(ns),
                ..
            }) => ns == namespace,
            // we threat the absence of a specific namespace has a permission to any namespace
            Authenticated::Authorized(Authorized {
                namespace: None, ..
            }) => true,
        }
    }

    /// Returns `true` if the authenticated is [`Anonymous`].
    ///
    /// [`Anonymous`]: Authenticated::Anonymous
    #[must_use]
    pub fn is_anonymous(&self) -> bool {
        matches!(self, Self::Anonymous)
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
        return Err(AuthError::HttpAuthHeaderInvalid);
    };

    let Some((scheme, param)) = header.split_once(' ') else {
        return Err(AuthError::HttpAuthHeaderInvalid);
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
    disable_namespace: bool,
) -> Result<Authenticated, AuthError> {
    use jsonwebtoken::errors::ErrorKind;

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
    validation.required_spec_claims.remove("exp");

    match jsonwebtoken::decode::<serde_json::Value>(jwt, jwt_key, &validation).map(|t| t.claims) {
        Ok(serde_json::Value::Object(claims)) => {
            tracing::trace!("Claims: {claims:#?}");
            let namespace = if disable_namespace {
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

pub fn parse_http_basic_auth_arg(arg: &str) -> Result<Option<String>> {
    if arg == "always" {
        return Ok(None);
    }

    let Some((scheme, param)) = arg.split_once(':') else {
        bail!("invalid HTTP auth config: {arg}")
    };

    if scheme == "basic" {
        Ok(Some(param.into()))
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

impl AuthError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::HttpAuthHeaderMissing => "AUTH_HTTP_HEADER_MISSING",
            Self::HttpAuthHeaderInvalid => "AUTH_HTTP_HEADER_INVALID",
            Self::HttpAuthHeaderUnsupportedScheme => "AUTH_HTTP_HEADER_UNSUPPORTED_SCHEME",
            Self::BasicNotAllowed => "AUTH_BASIC_NOT_ALLOWED",
            Self::BasicRejected => "AUTH_BASIC_REJECTED",
            Self::JwtMissing => "AUTH_JWT_MISSING",
            Self::JwtNotAllowed => "AUTH_JWT_NOT_ALLOWED",
            Self::JwtInvalid => "AUTH_JWT_INVALID",
            Self::JwtExpired => "AUTH_JWT_EXPIRED",
            Self::JwtImmature => "AUTH_JWT_IMMATURE",
            Self::Other => "AUTH_FAILED",
        }
    }
}

impl From<AuthError> for Status {
    fn from(e: AuthError) -> Self {
        Status::unauthenticated(format!("AuthError: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::header::HeaderValue;

    fn authenticate_http(auth: &Auth, header: &str) -> Result<Authenticated, AuthError> {
        auth.authenticate_http(Some(&HeaderValue::from_str(header).unwrap()), false)
    }

    const VALID_JWT_KEY: &str = "zaMv-aFGmB7PXkjM4IrMdF6B5zCYEiEGXW3RgMjNAtc";
    const VALID_JWT: &str = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.\
        eyJleHAiOjc5ODg0ODM4Mjd9.\
        MatB2aLnPFusagqH2RMoVExP37o2GFLmaJbmd52OdLtAehRNeqeJZPrefP1t2GBFidApUTLlaBRL6poKq_s3CQ";
    const VALID_READONLY_JWT: &str = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.\
        eyJleHAiOjc5ODg0ODM4MjcsImEiOiJybyJ9.\
        _2ZZiO2HC8b3CbCHSCufXXBmwpl-dLCv5O9Owvpy7LZ9aiQhXODpgV-iCdTsLQJ5FVanWhfn3FtJSnmWHn25DQ";

    macro_rules! assert_ok {
        ($e:expr) => {
            let res = $e;
            if let Err(err) = res {
                panic!("Expected Ok, got Err({:?})", err)
            }
        };
    }

    macro_rules! assert_err {
        ($e:expr) => {
            let res = $e;
            if let Ok(ok) = res {
                panic!("Expected Err, got Ok({:?})", ok);
            }
        };
    }

    #[test]
    fn test_default() {
        let auth = Auth::default();
        assert_err!(auth.authenticate_http(None, false));
        assert_err!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWJlYXI="));
        assert_err!(auth.authenticate_jwt(Some(VALID_JWT), false));
    }

    #[test]
    fn test_http_basic() {
        let auth = Auth {
            http_basic: parse_http_basic_auth_arg("basic:d29qdGVrOnRoZWJlYXI=").unwrap(),
            ..Auth::default()
        };
        assert_ok!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWJlYXI="));
        assert_ok!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWJlYXI"));
        assert_ok!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWJlYXI==="));

        assert_ok!(authenticate_http(&auth, "basic d29qdGVrOnRoZWJlYXI="));

        assert_err!(authenticate_http(&auth, "Basic d29qdgvronrozwjlyxi="));
        assert_err!(authenticate_http(&auth, "Basic d29qdGVrOnRoZWZveA=="));

        assert_err!(auth.authenticate_http(None, false));
        assert_err!(authenticate_http(&auth, ""));
        assert_err!(authenticate_http(&auth, "foobar"));
        assert_err!(authenticate_http(&auth, "foo bar"));
        assert_err!(authenticate_http(&auth, "basic #$%^"));
    }

    #[test]
    fn test_http_bearer() {
        let auth = Auth {
            jwt_key: Some(parse_jwt_key(VALID_JWT_KEY).unwrap()),
            ..Auth::default()
        };
        assert_ok!(authenticate_http(&auth, &format!("Bearer {VALID_JWT}")));
        assert_ok!(authenticate_http(&auth, &format!("bearer {VALID_JWT}")));

        assert_err!(authenticate_http(&auth, "Bearer foobar"));
        assert_err!(authenticate_http(
            &auth,
            &format!("Bearer {}", &VALID_JWT[..80])
        ));

        assert_eq!(
            authenticate_http(&auth, &format!("Bearer {VALID_READONLY_JWT}")).unwrap(),
            Authenticated::Authorized(Authorized {
                namespace: None,
                permission: Permission::ReadOnly
            })
        );
    }

    #[test]
    fn test_jwt() {
        let auth = Auth {
            jwt_key: Some(parse_jwt_key(VALID_JWT_KEY).unwrap()),
            ..Auth::default()
        };
        assert_ok!(auth.authenticate_jwt(Some(VALID_JWT), false));
        assert_err!(auth.authenticate_jwt(Some(&VALID_JWT[..80]), false));
    }
}
