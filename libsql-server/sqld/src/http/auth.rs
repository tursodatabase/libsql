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
                    expected_auth: format!("Basic {param}"),
                })),
                "jwt" => Ok(Arc::new(BearerAuthAuthorizer {
                    decoding_key: decoding_key_from_pem(param)?,
                })),
                _ => Err(anyhow!("unsupported HTTP auth scheme: {scheme}")),
            },
            None if auth == "always" => Ok(Arc::new(AlwaysAllowAuthorizer {})),
            None => Err(anyhow!("invalid HTTP auth config: {auth}")),
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
                .map(|actual_auth| actual_auth == self.expected_auth)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_auth_request(auth_value: &str) -> Request<Body> {
        Request::builder()
            .uri("http://example.com")
            .header("Authorization", auth_value)
            .body(Body::empty())
            .unwrap()
    }

    #[test]
    fn test_basic() {
        let basic_authorizer = parse_auth(Some("basic:d29qdGVrOnRoZWJlYXI=".to_string())).unwrap();
        let req = make_auth_request("Basic d29qdGVrOnRoZWJlYXI=");
        assert!(basic_authorizer.is_authorized(&req));
        // Similar request, but base64-encoded string is lowercased
        let req = make_auth_request("Basic d29qdgvronrozwjlyxi=");
        assert!(!basic_authorizer.is_authorized(&req));
        let req = make_auth_request("Basic d29qdGVrOnRoZWZveA==");
        assert!(!basic_authorizer.is_authorized(&req));
    }

    // Keys generated with ssh-keygen, token generated with http://jwt.io
    #[test]
    fn test_bearer() {
        let bearer_authorizer = parse_auth(Some(concat!("jwt:",
                "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA1lKV8RjGQkgUPs9P7OgxWIVAgTTKazm2C464SUVRt5rfcsf/",
                "JFA7r6SZAiGRB4BHC41NfKGWLeHz+uayqQDy1c14ly9lS3gQPzBTtk1SuX2WpaO7szXDwXHsjZ2fICkFo3QCuW7VhAC1",
                "bvHU5nodXr2TaYtLGoTARZSGgpwZO0/nedGsS90V6BPTHFtebzDWCsF8SIdiHXrO9oAYpmegWC97ydvbN2bFHD+03yPW",
                "K9qVZ0gj9rcLScpp/o6db2B/7VMQALKet2m3Vcee7fC+LrOV9XX+OOv4833+p4SG17v2r0rkn64OymTb03LPcOBH2LE+",
                "mA6PYCDQy5Qr5q/V7f6YLuVuUczEPxd094mTG8HwZLKetJFC0gttYD1fBpC1PL7p2lz4i56OE4Nn5p46J9hEw5gGwyEp",
                "8BBlcMIoKboKJCIpogwYhzKGJgVeP43M6WGdOIwy532L3H9G/v7skBjsy4nFmb+9mE+40JfZ0ikFhy15x+3isFmxWBHf",
                "O74khtYcebbQj8P3mT6tACEp0EiI8lobFRkwd5s501ZYxF8iFpLQznBMWGlRoLtRNY0RDjxW5AkNjwn3a+Zi3GFjp7Vv",
                "dxCWkNf+KzS5ilN2gEh5ZWJuWoc1cvrbDZYTAkN/+QmG5+mhT62pklAm3stxRXU9sO4gGV+wgmKq8lldO18CAwEAAQ==").to_string())).unwrap();
        let req = make_auth_request(
            concat!("Bearer ",
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImV4cCI6MTIzfQ.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gR",
            "G9lIiwiYWRtaW4iOnRydWUsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxMjMxMjMxMjMxMjMxMjN9.n3fwdgh1AGL05tR9ItPP",
            "xdPHMGhfPOWRHs2-18glGqDYnDE1lKqmkp9JCyVdTog2ToZ3RvNf7sJRBwdmVVPmjSjWYipRki_8dSa6EhaOICEVcRYdjB3-",
            "v4JSdKEF4jo2O1R_rsf09G1LWF-RBVwfflZfwMK7GSTd4kUEcm95jdlyIzQXDGaNbG7Ev6r1gsqn0r_Mll28opOXnFrUwpcV",
            "QRtkZcjwG0az6Ei875Keo4iDMzh4c-b9olwzZzvG-Axr3SXAUC_rq5s71Iu50T-yplLAcK1KIdIC5Dx7hRL-vw6CMl9lh015",
            "PlRFH1y-BgGi8nDbC7Cgstm_wALTFz36c6KXXHzF19SaIyKtZcCxe835YE_OkBWzKOGPgds8lXIUs8d01tbSxKOL791vb-3f",
            "4XYnuhe8dGhq64PHNeI0EFCF4IH_rJ1VjfNKDE9yGaFIxETL7hS2aFLKB4VDkYj0iQ0G4m9mIOvciUKargE750NgEtvMuTZW",
            "e1B7t-Qz-ozqBMUNBWVKHjvKSn5JpMEupE_z3uELCMv0vdEecXlTNMWMJqC2B1Mjni8SSbeBEW8KAOd3bAtVVRUwskkSeuf5",
            "wwjlP-MAXJSnnMGZ0PZkCuNSuHReooGpdWI58Pv2gtEfQBsz7Rd2kJDt7DbMQQ3dNuV5WvC5ZOLaAMOK0SDLrfY")
        );
        assert!(bearer_authorizer.is_authorized(&req));
        let req = make_auth_request(
            concat!("Bearer ",
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4",
            "iOnRydWUsImlhdCI6MTUxNjIzOTAyMn0.NHVaYe26MbtOYhSKkoKYdFVomg4i8ZJd8_-RU8VNbftc4TSMb4bXP3l3YlNWACw",
            "yXPGffz5aXHc6lty1Y2t4SWRqGteragsVdZufDn5BlnJl9pdR_kdVFUsra2rWKEofkZeIC4yWytE58sMIihvo9H1ScmmVwBc",
            "QP6XETqYd0aSHp1gOa9RdUPDvoXQ5oqygTqVtxaDr6wUFKrKItgBMzWIdNZ6y7O9E0DhEPTbE9rfBo6KTFsHAZnMg4k68CDp",
            "2woYIaXbmYTWcvbzIuHO7_37GT79XdIwkm95QJ7hYC9RiwrV7mesbY4PAahERJawntho0my942XheVLmGwLMBkQ")
        );
        assert!(!bearer_authorizer.is_authorized(&req));
    }
}
