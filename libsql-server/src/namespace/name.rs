use std::fmt;

use bytes::Bytes;
use serde::{de::Visitor, Deserialize};

use crate::error::Error;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NamespaceName(Bytes);

impl fmt::Debug for NamespaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Default for NamespaceName {
    fn default() -> Self {
        Self(Bytes::from_static(b"default"))
    }
}

impl AsRef<str> for NamespaceName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<&'static str> for NamespaceName {
    fn from(value: &'static str) -> Self {
        Self::from_bytes(Bytes::from_static(value.as_bytes())).unwrap()
    }
}

impl NamespaceName {
    pub fn from_string(s: String) -> crate::Result<Self> {
        Self::validate(&s)?;
        Ok(Self(Bytes::from(s)))
    }

    fn validate(s: &str) -> crate::Result<()> {
        if s.is_empty() {
            return Err(crate::error::Error::InvalidNamespace);
        }

        Ok(())
    }

    pub fn as_str(&self) -> &str {
        // Safety: the namespace is always valid UTF8
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    pub fn from_bytes(bytes: Bytes) -> crate::Result<Self> {
        let s = std::str::from_utf8(&bytes).map_err(|_| Error::InvalidNamespace)?;
        Self::validate(s)?;
        Ok(Self(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn new_unchecked(s: impl AsRef<str>) -> Self {
        Self(Bytes::copy_from_slice(s.as_ref().as_bytes()))
    }
}

impl fmt::Display for NamespaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl<'de> Deserialize<'de> for NamespaceName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct V;

        impl<'de> Visitor<'de> for V {
            type Value = NamespaceName;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a valid namespace name")
            }

            fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                NamespaceName::from_string(v).map_err(|e| E::custom(e))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                NamespaceName::from_string(v.to_string()).map_err(|e| E::custom(e))
            }
        }

        deserializer.deserialize_string(V)
    }
}

impl serde::Serialize for NamespaceName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}
