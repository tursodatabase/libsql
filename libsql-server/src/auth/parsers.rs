use crate::auth::AuthError;

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

pub fn parse_jwt_keys(data: &str) -> Result<Vec<jsonwebtoken::DecodingKey>> {
    if data.starts_with("-----BEGIN") {
        let pems = pem::parse_many(data).context("Could not parse many certificates from PEM")?;

        pems.iter()
            .map(|pem| match pem.tag() {
                "PUBLIC KEY" => jsonwebtoken::DecodingKey::from_ed_pem(pem.to_string().as_bytes())
                    .context("Could not decode Ed25519 public key from PEM"),
                "PRIVATE KEY" => bail!("Received a private key, but a public key is expected"),
                _ => bail!("Key is in unsupported PEM format"),
            })
            .collect()
    } else {
        jsonwebtoken::DecodingKey::from_ed_components(data)
            .map(|v| vec![v]) // Only supports a single key
            .map_err(|e| anyhow::anyhow!("Could not decode Ed25519 public key from base64: {e}"))
    }
}

pub(crate) fn parse_grpc_auth_header(
    metadata: &MetadataMap,
    required_fields: &Vec<&'static str>,
) -> Result<UserAuthContext> {
    let mut context = UserAuthContext::empty();

    let mut auth_header_seen = false;

    if required_fields.is_empty() {
        return Ok(context);
    }

    for field in required_fields.iter() {
        if let Some(h) = metadata.get(*field) {
            let v = h.to_str().map_err(|_| AuthError::AuthHeaderNonAscii)?;
            context.add_field(field, v.into());
            auth_header_seen = true;
        }
    }

    if !auth_header_seen {
        return Err(AuthError::AuthHeaderNotFound.into());
    }

    Ok(context)
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

    use crate::auth::authorized::Scopes;
    use crate::auth::constants::GRPC_AUTH_HEADER;
    use crate::auth::user_auth_strategies::jwt::Token;
    use crate::auth::{parse_http_auth_header, parse_jwt_keys, AuthError};

    use super::{parse_grpc_auth_header, parse_http_basic_auth_arg};

    #[test]
    fn parse_grpc_auth_header_returns_valid_context() {
        let mut map = tonic::metadata::MetadataMap::new();
        map.insert(GRPC_AUTH_HEADER, "bearer 123".parse().unwrap());
        let required_fields = vec!["x-authorization".into()];
        let context = parse_grpc_auth_header(&map, &required_fields).unwrap();

        assert_eq!(
            context.get_field("x-authorization"),
            Some(&"bearer 123".to_string())
        );
    }

    #[test]
    fn parse_grpc_auth_header_with_multiple_required_fields() {
        let mut map = tonic::metadata::MetadataMap::new();
        map.insert(GRPC_AUTH_HEADER, "bearer 123".parse().unwrap());
        let required_fields = vec!["authorization".into(), "x-authorization".into()];
        let context = parse_grpc_auth_header(&map, &required_fields).unwrap();

        assert_eq!(
            context.get_field("x-authorization"),
            Some(&"bearer 123".to_string())
        );
    }

    #[test]
    fn parse_grpc_auth_header_error_non_ascii() {
        let mut map = tonic::metadata::MetadataMap::new();
        map.insert("x-authorization", "bearer I❤NY".parse().unwrap());
        let required_fields = vec!["x-authorization".into()];
        let result = parse_grpc_auth_header(&map, &required_fields);
        assert_eq!(result.unwrap_err().to_string(), "Non-ASCII auth header")
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

    // Examples created via libsql-server/scripts/gen_jwt_test_assets.py
    const EXAMPLE_JWT_PUBLIC_KEY: &str = include_str!("../../assets/test/auth/example1.pem");
    const EXAMPLE_JWT_PRIVATE_KEY: &str = include_str!("../../assets/test/auth/example1.key");
    const EXAMPLE_JWT: &str = include_str!("../../assets/test/auth/example1.jwt");
    const EXAMPLE3_JWT: &str = include_str!("../../assets/test/auth/example3.jwt");
    const MULTI_JWT_PUBLIC_KEY: &str = include_str!("../../assets/test/auth/combined123.pem");

    #[test]
    fn parse_jwt_keys_single_pem() {
        let keys = parse_jwt_keys(EXAMPLE_JWT_PUBLIC_KEY);
        assert_eq!(keys.as_ref().map_or(0, |v| v.len()), 1);
        let key = keys.unwrap().into_iter().next().unwrap();

        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
        validation.required_spec_claims.remove("exp");
        let decoded = jsonwebtoken::decode::<Token>(&EXAMPLE_JWT, &key, &validation);

        assert!(matches!(
            decoded,
            Ok(jsonwebtoken::TokenData {
                header: _,
                claims: _
            })
        ));

        let jsonwebtoken::TokenData { header, claims } = decoded.unwrap();
        assert_eq!(header.alg, jsonwebtoken::Algorithm::EdDSA);

        assert!(claims.p.is_some());
        let Some(authorized) = claims.p else {
            panic!("Assertion should have already failed");
        };

        let scopes: Scopes =
            serde_json::from_str(r##"{"ns":["example1a","example1b","example1c"]}"##)
                .expect("JSON failed to parse");
        assert_eq!(authorized.read_only, Some(scopes));
    }

    #[test]
    fn parse_jwt_keys_multiple_pems() {
        let keys = parse_jwt_keys(MULTI_JWT_PUBLIC_KEY);
        assert_eq!(keys.as_ref().map_or(0, |v| v.len()), 3);
        let keys = keys.unwrap();

        let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
        validation.required_spec_claims.remove("exp");
        let decoded = jsonwebtoken::decode::<Token>(&EXAMPLE_JWT, &keys[0], &validation);

        assert!(matches!(
            decoded,
            Ok(jsonwebtoken::TokenData {
                header: _,
                claims: _
            })
        ));

        let jsonwebtoken::TokenData { header, claims } = decoded.unwrap();
        assert_eq!(header.alg, jsonwebtoken::Algorithm::EdDSA);

        assert!(claims.p.is_some());
        let Some(authorized) = claims.p else {
            panic!("Assertion should have already failed");
        };

        let scopes: Scopes =
            serde_json::from_str(r##"{"ns":["example1a","example1b","example1c"]}"##)
                .expect("JSON failed to parse");
        assert_eq!(authorized.read_only, Some(scopes));

        let decoded = jsonwebtoken::decode::<Token>(&EXAMPLE3_JWT, &keys[2], &validation);

        assert!(matches!(
            decoded,
            Ok(jsonwebtoken::TokenData {
                header: _,
                claims: _
            })
        ));

        let jsonwebtoken::TokenData { header, claims } = decoded.unwrap();
        assert_eq!(header.alg, jsonwebtoken::Algorithm::EdDSA);

        assert!(claims.p.is_some());
        let Some(authorized) = claims.p else {
            panic!("Assertion should have already failed");
        };

        let scopes: Scopes = serde_json::from_str(r##"{"ns":["example3e","example3f"]}"##)
            .expect("JSON failed to parse");
        assert_eq!(authorized.read_only, Some(scopes));
    }

    #[test]
    fn parse_jwt_keys_fail_when_multiple_contains_private_key() {
        let keys = parse_jwt_keys(
            format!("{}\n{}", MULTI_JWT_PUBLIC_KEY, EXAMPLE_JWT_PRIVATE_KEY).as_str(),
        );
        assert!(keys.is_err());
        assert_eq!(
            keys.err().unwrap().to_string(),
            "Received a private key, but a public key is expected"
        );
    }

    #[test]
    fn parse_jwt_keys_fail_when_private_key() {
        let keys = parse_jwt_keys(EXAMPLE_JWT_PRIVATE_KEY);
        assert!(keys.is_err());
        assert_eq!(
            keys.err().unwrap().to_string(),
            "Received a private key, but a public key is expected"
        );
    }

    #[test]
    fn parse_jwt_keys_fail_when_non_key_pem() {
        let keys = parse_jwt_keys(
            "-----BEGIN CERTIFICATE-----\nMIIKLwIBAzCCCesGCSqGSIb3DQE\n-----END CERTIFICATE-----\n",
        );

        assert!(keys.is_err());
        assert_eq!(
            keys.err().unwrap().to_string(),
            "Could not parse many certificates from PEM"
        );
    }
}
