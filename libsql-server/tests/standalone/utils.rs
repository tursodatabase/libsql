use base64::Engine;
use jsonwebtoken::EncodingKey;
use ring::signature::{Ed25519KeyPair, KeyPair};

pub fn key_pair() -> (EncodingKey, String) {
    let doc = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
    let encoding_key = EncodingKey::from_ed_der(doc.as_ref());
    let pair = Ed25519KeyPair::from_pkcs8(doc.as_ref()).unwrap();
    let jwt_key = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(pair.public_key().as_ref());
    (encoding_key, jwt_key)
}

pub fn encode<T: serde::Serialize>(claims: &T, key: &EncodingKey) -> String {
    let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
    jsonwebtoken::encode(&header, &claims, key).unwrap()
}
