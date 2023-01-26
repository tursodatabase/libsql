use anyhow::{anyhow, Result};
use hyper::{Body, Request};
use jsonwebtoken::{DecodingKey, Validation};
use serde_json::Value as JsonValue;
use std::sync::Arc;

/// HTTP request authorizer.
pub trait Authorizer {
    fn is_authorized(&self, req: &Request<Body>) -> bool;
}

/// Takes a string representing a RSA PEM with its header and footer trimmed, and all line breaks
/// removed, and created a decoding key for decoding a JWT.
fn decoding_key_from_pem(key: &str) -> Result<DecodingKey> {
    let header = String::from("-----BEGIN PUBLIC KEY-----\n");
    let mut key = key
        .as_bytes()
        .chunks(64)
        .try_fold(header, |mut buf, s| -> Result<String> {
            let line = std::str::from_utf8(s)?;
            buf.push_str(line);
            buf.push('\n');
            Ok(buf)
        })?;
    key.push_str("-----END PUBLIC KEY-----");
    tracing::warn!("Public: {}", key);

    let dkey = DecodingKey::from_rsa_pem(key.as_bytes())?;
    Ok(dkey)
}

pub fn parse_auth(auth: Option<String>) -> Result<Arc<dyn Authorizer + Sync + Send>> {
    match auth {
        Some(auth) => match auth.split_once(':') {
            Some((scheme, param)) => match scheme {
                "basic" => Ok(Arc::new(BasicAuthAuthorizer {
                    expected_auth: format!("Basic {}", param).to_lowercase(),
                })),
                "jwt" => Ok(Arc::new(BearerAuthAuthorizer {
                    decoding_key: decoding_key_from_pem(param)?,
                })),
                _ => Err(anyhow!("unsupported HTTP auth scheme: {}", scheme)),
            },
            None if auth == "always" => Ok(Arc::new(AlwaysAllowAuthorizer {})),
            None => Err(anyhow!("invalid HTTP auth config: {}", auth)),
        },
        None => Ok(Arc::new(AlwaysAllowAuthorizer {})),
    }
}

/// An authorizer that always allows all requests.
pub struct AlwaysAllowAuthorizer {}

impl Authorizer for AlwaysAllowAuthorizer {
    fn is_authorized(&self, _req: &Request<Body>) -> bool {
        true
    }
}

/// Basic authentication authorizer.
pub struct BasicAuthAuthorizer {
    // Expected value in `Authorization` header.
    expected_auth: String,
}

impl Authorizer for BasicAuthAuthorizer {
    fn is_authorized(&self, req: &Request<Body>) -> bool {
        let headers = req.headers();
        let actual_auth = headers.get(hyper::header::AUTHORIZATION);
        if let Some(actual_auth) = actual_auth {
            actual_auth
                .to_str()
                .map(|actual_auth| actual_auth.to_lowercase() == self.expected_auth)
                .unwrap_or(false)
        } else {
            false
        }
    }
}

/// Bearer token authentication authorizer.
pub struct BearerAuthAuthorizer {
    decoding_key: DecodingKey,
}

impl BearerAuthAuthorizer {
    fn validate_token(&self, token: &str) -> Result<bool> {
        // Once we start verifying claims, token will become useful
        let _token = jsonwebtoken::decode::<JsonValue>(
            token,
            &self.decoding_key,
            &Validation::new(jsonwebtoken::Algorithm::RS256),
        )?;

        Ok(true)
    }
}

impl Authorizer for BearerAuthAuthorizer {
    fn is_authorized(&self, req: &Request<Body>) -> bool {
        let headers = req.headers();
        let actual_auth = headers.get(hyper::header::AUTHORIZATION);
        if let Some(Ok(actual_auth)) = actual_auth.map(|a| a.to_str()) {
            if !actual_auth.starts_with("Bearer ") {
                return false;
            }
            let token = &actual_auth[7..];
            self.validate_token(token).unwrap_or(false)
        } else {
            false
        }
    }
}
