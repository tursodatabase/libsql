use anyhow::{anyhow, Result};
use hyper::{Body, Request};
use std::sync::Arc;

/// HTTP request authorizer.
pub trait Authorizer {
    fn is_authorized(&self, req: &Request<Body>) -> bool;
}

pub fn parse_auth(auth: Option<String>) -> Result<Arc<dyn Authorizer + Sync + Send>> {
    match auth {
        Some(auth) => match auth.split_once(':') {
            Some((scheme, param)) => match scheme {
                "basic" => Ok(Arc::new(BasicAuthAuthorizer {
                    expected_auth: format!("Basic {}", param).to_lowercase(),
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
