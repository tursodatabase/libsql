use bytes::Bytes;
use std::fmt;
use std::path::Path;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NamespaceName(pub Bytes);

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
        Self(Bytes::from_static(value.as_bytes()))
    }
}

impl NamespaceName {
    pub fn from_string(s: String) -> Self {
        Self(Bytes::from(s))
    }

    pub fn as_str(&self) -> &str {
        // Safety: the namespace is always valid UTF8
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for NamespaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

/// Translates a path to a namespace name
pub trait NamespaceResolver: Send + Sync + 'static {
    fn resolve(&self, path: &Path) -> NamespaceName;
}

impl<F: Fn(&Path) -> NamespaceName + Send + Sync + 'static> NamespaceResolver for F {
    fn resolve(&self, path: &Path) -> NamespaceName {
        (self)(path)
    }
}
