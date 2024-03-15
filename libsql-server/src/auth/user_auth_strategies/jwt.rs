use chrono::{DateTime, Utc};

use crate::{
    auth::{authenticated::LegacyAuth, AuthError, Authenticated, Authorized, Permission},
    namespace::NamespaceName,
};

use super::{UserAuthContext, UserAuthStrategy};

pub struct Jwt {
    key: jsonwebtoken::DecodingKey,
}

impl UserAuthStrategy for Jwt {
    fn authenticate(
        &self,
        context: Result<UserAuthContext, AuthError>,
    ) -> Result<Authenticated, AuthError> {
        tracing::trace!("executing jwt auth");

        let ctx = context?;

        let UserAuthContext {
            scheme: Some(scheme),
            token: Some(token),
        } = ctx
        else {
            return Err(AuthError::HttpAuthHeaderInvalid);
        };

        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(AuthError::HttpAuthHeaderUnsupportedScheme);
        }

        return validate_jwt(&self.key, &token);
    }
}

impl Jwt {
    pub fn new(key: jsonwebtoken::DecodingKey) -> Self {
        Self { key }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
struct Token {
    #[serde(default)]
    id: Option<NamespaceName>,
    #[serde(default)]
    a: Option<Permission>,
    #[serde(default)]
    p: Option<Authorized>,
    #[serde(with = "jwt_time", default)]
    exp: Option<DateTime<Utc>>,
}

mod jwt_time {
    use chrono::{DateTime, Utc};
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(date: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match date {
            Some(date) => serializer.serialize_i64(date.timestamp()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<i64>::deserialize(deserializer)?
            .map(|x| {
                DateTime::from_timestamp(x, 0).ok_or_else(|| D::Error::custom("invalid exp claim"))
            })
            .transpose()
    }
}

fn validate_jwt(
    jwt_key: &jsonwebtoken::DecodingKey,
    jwt: &str,
) -> Result<Authenticated, AuthError> {
    use jsonwebtoken::errors::ErrorKind;

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
    validation.required_spec_claims.remove("exp");

    match jsonwebtoken::decode::<Token>(jwt, jwt_key, &validation).map(|t| t.claims) {
        Ok(Token { id, a, p, .. }) => {
            if p.is_some() {
                Ok(Authenticated::Authorized(p.unwrap_or_default().into()))
            } else {
                Ok(Authenticated::Legacy(LegacyAuth {
                    namespace: id,
                    perm: a.unwrap_or(Permission::Write),
                }))
            }
        }
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
    use std::time::Duration;

    use jsonwebtoken::{DecodingKey, EncodingKey};
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use serde::Serialize;

    use crate::auth::authorized::Scope;

    use super::*;

    fn strategy(dec: jsonwebtoken::DecodingKey) -> Jwt {
        Jwt::new(dec)
    }

    fn key_pair() -> (jsonwebtoken::EncodingKey, jsonwebtoken::DecodingKey) {
        let doc = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
        let encoding_key = EncodingKey::from_ed_der(doc.as_ref());
        let pair = Ed25519KeyPair::from_pkcs8(doc.as_ref()).unwrap();
        let decoding_key = DecodingKey::from_ed_der(pair.public_key().as_ref());
        (encoding_key, decoding_key)
    }

    fn encode<T: Serialize>(claims: &T, key: &EncodingKey) -> String {
        let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
        jsonwebtoken::encode(&header, &claims, key).unwrap()
    }

    #[test]
    fn authenticates_valid_jwt_token_with_full_access() {
        // this is a full access token
        let (enc, dec) = key_pair();
        let token = Token {
            id: None,
            a: None,
            p: None,
            exp: None,
        };
        let token = encode(&token, &enc);

        let context = Ok(UserAuthContext::bearer(token.as_str()));

        assert!(matches!(
            strategy(dec).authenticate(context).unwrap(),
            Authenticated::Legacy(LegacyAuth {
                namespace: None,
                perm: Permission::Write
            })
        ))
    }

    #[test]
    fn authenticates_valid_jwt_token_with_read_only_access() {
        let (enc, dec) = key_pair();
        let token = Token {
            id: Some(NamespaceName::default()),
            a: Some(Permission::Read),
            p: None,
            exp: None,
        };
        let token = encode(&token, &enc);

        let context = Ok(UserAuthContext::bearer(token.as_str()));

        let Authenticated::Legacy(a) = strategy(dec).authenticate(context).unwrap() else {
            panic!()
        };

        assert_eq!(a.namespace, Some(NamespaceName::default()));
        assert_eq!(a.perm, Permission::Read);
    }

    #[test]
    fn errors_when_jwt_token_invalid() {
        let (_enc, dec) = key_pair();
        let context = Ok(UserAuthContext::bearer("abc"));

        assert_eq!(
            strategy(dec).authenticate(context).unwrap_err(),
            AuthError::JwtInvalid
        )
    }

    #[test]
    fn expired_token() {
        let (enc, dec) = key_pair();
        let token = Token {
            id: None,
            a: None,
            p: None,
            exp: Some(Utc::now() - Duration::from_secs(5 * 60)),
        };

        let token = encode(&token, &enc);

        let context = Ok(UserAuthContext::bearer(token.as_str()));

        assert_eq!(
            strategy(dec).authenticate(context).unwrap_err(),
            AuthError::JwtExpired
        );
    }

    #[test]
    fn multi_scopes() {
        let (enc, dec) = key_pair();
        let token = serde_json::json!({
            "id": "foobar",
            "a": "ro",
            "p": {
                "rw": { "ns": ["foo"] },
                "roa": { "ns": ["bar"] }
            }
        });

        let token = encode(&token, &enc);

        let context = Ok(UserAuthContext::bearer(token.as_str()));

        let Authenticated::Authorized(a) = strategy(dec).authenticate(context).unwrap() else {
            panic!()
        };

        let mut perms = a.perms_iter();
        assert_eq!(
            perms.next().unwrap(),
            (
                Scope::Namespace(NamespaceName::from_string("foo".into()).unwrap()),
                Permission::Write
            )
        );
        assert_eq!(
            perms.next().unwrap(),
            (
                Scope::Namespace(NamespaceName::from_string("bar".into()).unwrap()),
                Permission::AttachRead
            )
        );
        assert!(perms.next().is_none());
    }
}
