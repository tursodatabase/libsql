use chrono::{DateTime, Utc};

use crate::{
    auth::{authenticated::LegacyAuth, AuthError, Authenticated, Authorized, Permission},
    namespace::NamespaceName,
};

use super::{UserAuthContext, UserAuthStrategy};

pub enum DecodingKeyContainer {
    Single(jsonwebtoken::DecodingKey),
    Multiple(Vec<jsonwebtoken::DecodingKey>),
}

pub struct Jwt {
    key: DecodingKeyContainer,
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

        match &self.key {
            DecodingKeyContainer::Single(key) => validate_jwt(&key, &token),
            DecodingKeyContainer::Multiple(keys) => validate_any_jwt(&keys, &token),
        }
    }
}

impl Jwt {
    pub fn new(key: DecodingKeyContainer) -> Self {
        Self { key: key }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub(crate) struct Token {
    #[serde(default)]
    id: Option<NamespaceName>,
    #[serde(default)]
    a: Option<Permission>,
    #[serde(default)]
    pub(crate) p: Option<Authorized>,
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

fn validate_any_jwt(
    jwt_keys: &Vec<jsonwebtoken::DecodingKey>,
    jwt: &str,
) -> Result<Authenticated, AuthError> {
    for (index, jwt_key) in jwt_keys.iter().enumerate() {
        let result = validate_jwt(&jwt_key, jwt);
        if result.is_ok() || index == jwt_keys.len() - 1 {
            return result;
        }
    }

    Err(AuthError::Other)
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
        Jwt::new(DecodingKeyContainer::Single(dec))
    }

    fn strategy_with_multiple(multi_dec: Vec<jsonwebtoken::DecodingKey>) -> Jwt {
        Jwt::new(DecodingKeyContainer::Multiple(multi_dec))
    }

    fn key_pair() -> (jsonwebtoken::EncodingKey, jsonwebtoken::DecodingKey) {
        let doc = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
        let encoding_key = EncodingKey::from_ed_der(doc.as_ref());
        let pair = Ed25519KeyPair::from_pkcs8(doc.as_ref()).unwrap();
        let decoding_key = DecodingKey::from_ed_der(pair.public_key().as_ref());
        (encoding_key, decoding_key)
    }

    fn key_pairs(
        size: usize,
    ) -> (
        Vec<jsonwebtoken::EncodingKey>,
        Vec<jsonwebtoken::DecodingKey>,
    ) {
        let mut multi_enc = Vec::new();
        let mut multi_dec = Vec::new();

        for _ in 0..size {
            let (enc, dec) = key_pair();
            multi_enc.push(enc);
            multi_dec.push(dec);
        }
        (multi_enc, multi_dec)
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

    #[test]
    fn multi_keys() {
        let (multi_enc, multi_dec) = key_pairs(3);
        let token = serde_json::json!({
            "p": {
                "rw": { "ns": ["foo"] },
            },
        });

        let strategy = strategy_with_multiple(multi_dec);
        for enc in multi_enc.iter() {
            let token = encode(&token, &enc);

            let context = Ok(UserAuthContext::bearer(token.as_str()));

            let Authenticated::Authorized(a) = strategy.authenticate(context).unwrap() else {
                panic!()
            };

            assert_eq!(
                a.perms_iter().next().unwrap(),
                (
                    Scope::Namespace(NamespaceName::from_string("foo".into()).unwrap()),
                    Permission::Write
                )
            );
        }
    }

    #[test]
    fn multi_keys_but_all_fail() {
        let (_, multi_dec) = key_pairs(3);
        let (enc, _) = key_pair();
        let token = serde_json::json!({
            "p": {
                "rw": { "ns": ["foo"] },
            },
        });
        let token = encode(&token, &enc);

        let context = Ok(UserAuthContext::bearer(token.as_str()));

        assert_eq!(
            strategy_with_multiple(multi_dec)
                .authenticate(context)
                .unwrap_err(),
            AuthError::JwtInvalid
        );
    }

    #[test]
    fn multi_keys_but_first_expired() {
        let (multi_enc, multi_dec) = key_pairs(3);
        let token = Token {
            id: None,
            a: None,
            p: None,
            exp: Some(Utc::now() - Duration::from_secs(5 * 60)),
        };
        let token = encode(&token, &multi_enc[0]);

        let context = Ok(UserAuthContext::bearer(token.as_str()));

        assert_eq!(
            strategy_with_multiple(multi_dec)
                .authenticate(context)
                .unwrap_err(),
            AuthError::JwtInvalid
        );
    }

    #[test]
    fn multi_keys_but_last_expired() {
        let (multi_enc, multi_dec) = key_pairs(3);
        let token = Token {
            id: None,
            a: None,
            p: None,
            exp: Some(Utc::now() - Duration::from_secs(5 * 60)),
        };
        let token = encode(&token, &multi_enc[2]);

        let context = Ok(UserAuthContext::bearer(token.as_str()));

        assert_eq!(
            strategy_with_multiple(multi_dec)
                .authenticate(context)
                .unwrap_err(),
            AuthError::JwtExpired
        );
    }
}
