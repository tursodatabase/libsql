use crate::auth::{constants::GRPC_AUTH_HEADER, AuthError};

use anyhow::{bail, Context as _, Result};
use axum::http::HeaderValue;
use tonic::metadata::MetadataMap;

use super::UserAuthContext;

pub fn parse_http_basic_auth_arg(arg: &str) -> Result<Option<String>> {
    if arg == "always" {
        return Ok(Some("always".to_string()));
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
            .map_err(|e| anyhow::anyhow!("Could not decode Ed25519 public key from base64: {e}"))
    }
}

pub(crate) fn parse_grpc_auth_header(metadata: &MetadataMap) -> Result<UserAuthContext, AuthError> {
    metadata
        .get(GRPC_AUTH_HEADER)
        .ok_or(AuthError::AuthHeaderNotFound)
        .and_then(|h| h.to_str().map_err(|_| AuthError::AuthHeaderNonAscii))
        .and_then(|t| UserAuthContext::from_auth_str(t))
}

pub fn parse_http_auth_header<'a>(
    expected_scheme: &str,
    auth_header: &'a Option<HeaderValue>,
) -> Result<&'a str, AuthError> {
    let Some(header) = auth_header else {
        return Err(AuthError::HttpAuthHeaderMissing);
    };

    let Ok(header) = header.to_str() else {
        return Err(AuthError::HttpAuthHeaderInvalid);
    };

    let Some((scheme, param)) = header.split_once(' ') else {
        return Err(AuthError::HttpAuthHeaderInvalid);
    };

    if !scheme.eq_ignore_ascii_case(expected_scheme) {
        return Err(AuthError::HttpAuthHeaderUnsupportedScheme);
    }

    Ok(param)
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;
    use hyper::header::AUTHORIZATION;

    use crate::auth::{parse_http_auth_header, AuthError};

    use super::{parse_grpc_auth_header, parse_http_basic_auth_arg};

    #[test]
    fn parse_grpc_auth_header_returns_valid_context() {
        let mut map = tonic::metadata::MetadataMap::new();
        map.insert("x-authorization", "bearer 123".parse().unwrap());
        let context = parse_grpc_auth_header(&map).unwrap();
        assert_eq!(context.scheme().as_ref().unwrap(), "bearer");
        assert_eq!(context.token().as_ref().unwrap(), "123");
    }

    #[test]
    fn parse_grpc_auth_header_error_no_header() {
        let map = tonic::metadata::MetadataMap::new();
        let result = parse_grpc_auth_header(&map);
        assert_eq!(
            result.unwrap_err().to_string(),
            "Expected authorization header but none given"
        );
    }

    #[test]
    fn parse_grpc_auth_header_error_non_ascii() {
        let mut map = tonic::metadata::MetadataMap::new();
        map.insert("x-authorization", "bearer I❤NY".parse().unwrap());
        let result = parse_grpc_auth_header(&map);
        assert_eq!(result.unwrap_err().to_string(), "Non-ASCII auth header")
    }

    #[test]
    fn parse_grpc_auth_header_error_malformed_auth_str() {
        let mut map = tonic::metadata::MetadataMap::new();
        map.insert("x-authorization", "bearer123".parse().unwrap());
        let result = parse_grpc_auth_header(&map);
        assert_eq!(
            result.unwrap_err().to_string(),
            "Auth string does not conform to '<scheme> <token>' form"
        )
    }

    #[test]
    fn parse_http_auth_header_returns_auth_header_param_when_valid() {
        assert_eq!(
            parse_http_auth_header("basic", &HeaderValue::from_str("Basic abc").ok()).unwrap(),
            "abc"
        )
    }

    #[test]
    fn parse_http_auth_header_errors_when_auth_header_missing() {
        assert_eq!(
            parse_http_auth_header("basic", &None).unwrap_err(),
            AuthError::HttpAuthHeaderMissing
        )
    }

    #[test]
    fn parse_http_auth_header_errors_when_auth_header_cannot_be_converted_to_str() {
        assert_eq!(
            parse_http_auth_header("basic", &Some(HeaderValue::from_name(AUTHORIZATION)))
                .unwrap_err(),
            AuthError::HttpAuthHeaderInvalid
        )
    }

    #[test]
    fn parse_http_auth_header_errors_when_auth_header_invalid_format() {
        assert_eq!(
            parse_http_auth_header("basic", &HeaderValue::from_str("invalid").ok()).unwrap_err(),
            AuthError::HttpAuthHeaderInvalid
        )
    }

    #[test]
    fn parse_http_auth_header_errors_when_auth_header_is_unsupported_scheme() {
        assert_eq!(
            parse_http_auth_header("basic", &HeaderValue::from_str("Bearer abc").ok()).unwrap_err(),
            AuthError::HttpAuthHeaderUnsupportedScheme
        )
    }

    #[test]
    fn parse_http_auth_arg_always() {
        let out = parse_http_basic_auth_arg("always").unwrap();
        assert_eq!(out, Some("always".to_string()));
    }
}
