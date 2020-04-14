use smallvec::{smallvec, SmallVec};
use std::ffi::{CStr, CString, NulError};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SmallCString(smallvec::SmallVec<[u8; 16]>);

impl SmallCString {
    #[inline]
    pub fn new(s: &str) -> Result<Self, NulError> {
        if s.as_bytes().contains(&0u8) {
            return Err(Self::fabricate_nul_error(s));
        }
        let mut buf = SmallVec::with_capacity(s.len() + 1);
        buf.extend_from_slice(s.as_bytes());
        buf.push(0);
        Ok(Self(buf))
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        debug_assert!(std::str::from_utf8(&self.as_bytes_without_nul()).is_ok());
        // Constructor takes a &str so this is safe.
        unsafe { std::str::from_utf8_unchecked(&self.as_bytes_without_nul()) }
    }

    #[inline]
    pub fn as_bytes_without_nul(&self) -> &[u8] {
        &self.0[..self.0.len() - 1]
    }

    #[inline]
    pub fn len(&self) -> usize {
        debug_assert_ne!(self.0.len(), 0);
        self.0.len() - 1
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn as_cstr(&self) -> &CStr {
        debug_assert!(CStr::from_bytes_with_nul(&self.0).is_ok());
        unsafe { CStr::from_bytes_with_nul_unchecked(&self.0) }
    }

    #[cold]
    fn fabricate_nul_error(b: &str) -> NulError {
        CString::new(b).unwrap_err()
    }
}

impl Default for SmallCString {
    #[inline]
    fn default() -> Self {
        Self(smallvec![0])
    }
}

impl std::fmt::Debug for SmallCString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SmallCString").field(&self.as_str()).finish()
    }
}

impl std::ops::Deref for SmallCString {
    type Target = CStr;
    #[inline]
    fn deref(&self) -> &CStr {
        self.as_cstr()
    }
}

impl PartialEq<SmallCString> for str {
    #[inline]
    fn eq(&self, s: &SmallCString) -> bool {
        s.as_bytes_without_nul() == self.as_bytes()
    }
}

impl PartialEq<str> for SmallCString {
    #[inline]
    fn eq(&self, s: &str) -> bool {
        self.as_bytes_without_nul() == s.as_bytes()
    }
}

impl std::borrow::Borrow<str> for SmallCString {
    #[inline]
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_small_cstring() {
        // We don't go through the normal machinery for default, so make sure
        // things work.
        assert_eq!(SmallCString::default().0, SmallCString::new("").unwrap().0);
        assert_eq!(SmallCString::new("foo").unwrap().len(), 3);
        assert_eq!(SmallCString::new("foo").unwrap().0.as_slice(), b"foo\0");
        assert_eq!(
            SmallCString::new("foo").unwrap().as_bytes_without_nul(),
            b"foo",
        );

        assert_eq!(SmallCString::new("😀").unwrap().len(), 4);
        assert_eq!(
            SmallCString::new("😀").unwrap().0.as_slice(),
            b"\xf0\x9f\x98\x80\0",
        );
        assert_eq!(
            SmallCString::new("😀").unwrap().as_bytes_without_nul(),
            b"\xf0\x9f\x98\x80",
        );

        assert_eq!(SmallCString::new("").unwrap().len(), 0);
        assert!(SmallCString::new("").unwrap().is_empty());

        assert_eq!(SmallCString::new("").unwrap().0.as_slice(), b"\0");
        assert_eq!(SmallCString::new("").unwrap().as_bytes_without_nul(), b"");

        assert!(SmallCString::new("\0").is_err());
        assert!(SmallCString::new("\0abc").is_err());
        assert!(SmallCString::new("abc\0").is_err());
    }
}
