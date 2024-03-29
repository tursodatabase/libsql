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

pub(crate) fn parse_grpc_auth_header(
    metadata: &MetadataMap,
    required_fields: &Vec<String>,
) -> UserAuthContext {
    // let mut context = metadata
    //     .get(GRPC_AUTH_HEADER)
    //     .ok_or(AuthError::AuthHeaderNotFound)
    //     .and_then(|h| h.to_str().map_err(|_| AuthError::AuthHeaderNonAscii))
    //     .and_then(|t| UserAuthContext::from_auth_str(t));
    let mut context = UserAuthContext::empty();
    for field in required_fields.iter() {
        metadata
            .get(field)
            .map(|header| header.to_str().ok())
            .and_then(|r| r)
            .map(|v| context.add_field(field.into(), v.into()));
    }

    context
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
        map.insert(
            crate::auth::constants::GRPC_AUTH_HEADER,
            "bearer 123".parse().unwrap(),
        );
        let required_fields = vec!["x-authorization".into()];
        let context = parse_grpc_auth_header(&map, &required_fields);
        assert_eq!(
            context.custom_fields.get("x-authorization"),
            Some(&"bearer 123".to_string())
        );
    }

    // #[test] TODO rewrite
    // fn parse_grpc_auth_header_error_non_ascii() {
    //     let mut map = tonic::metadata::MetadataMap::new();
    //     map.insert("x-authorization", "bearer I❤NY".parse().unwrap());
    //     let required_fields = Vec::new();
    //     let result = parse_grpc_auth_header(&map, &required_fields);
    //     assert_eq!(result.unwrap_err().to_string(), "Non-ASCII auth header")
    // }

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
