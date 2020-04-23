// This is used when either vtab or modern-sqlite is on. Different methods are
// used in each feature. Avoid having to track this for each function. We will
// still warn for anything that's not used by either, though.
#![cfg_attr(
    not(all(feature = "vtab", feature = "modern-sqlite")),
    allow(dead_code)
)]
use crate::ffi;
use std::marker::PhantomData;
use std::os::raw::{c_char, c_int};
use std::ptr::NonNull;

/// A string we own that's allocated on the SQLite heap. Automatically calls
/// `sqlite3_free` when dropped, unless `into_raw` (or `into_inner`) is called
/// on it. If constructed from a rust string, `sqlite3_malloc` is used.
///
/// It has identical representation to a nonnull `*mut c_char`, so you can use
/// it transparently as one. It's nonnull, so Option<SqliteMallocString> can be
/// used for nullable ones (it's still just one pointer).
///
/// Most strings shouldn't use this! Only places where the string needs to be
/// freed with `sqlite3_free`. This includes `sqlite3_extended_sql` results,
/// some error message pointers... Note that misuse is extremely dangerous!
///
/// Note that this is *not* a lossless interface. Incoming strings with internal
/// NULs are modified, and outgoing strings which are non-UTF8 are modified.
/// This seems unavoidable -- it tries very hard to not panic.
#[repr(transparent)]
pub(crate) struct SqliteMallocString {
    ptr: NonNull<c_char>,
    _boo: PhantomData<Box<[c_char]>>,
}
// This is owned data for a primitive type, and thus it's safe to implement
// these. That said, nothing needs them, and they make things easier to misuse.

// unsafe impl Send for SqliteMallocString {}
// unsafe impl Sync for SqliteMallocString {}

impl SqliteMallocString {
    /// SAFETY: Caller must be certain that `m` a nul-terminated c string
    /// allocated by sqlite3_malloc, and that SQLite expects us to free it!
    #[inline]
    pub(crate) unsafe fn from_raw_nonnull(ptr: NonNull<c_char>) -> Self {
        Self {
            ptr,
            _boo: PhantomData,
        }
    }

    /// SAFETY: Caller must be certain that `m` a nul-terminated c string
    /// allocated by sqlite3_malloc, and that SQLite expects us to free it!
    #[inline]
    pub(crate) unsafe fn from_raw(ptr: *mut c_char) -> Option<Self> {
        NonNull::new(ptr).map(|p| Self::from_raw_nonnull(p))
    }

    /// Get the pointer behind `self`. After this is called, we no longer manage
    /// it.
    #[inline]
    pub(crate) fn into_inner(self) -> NonNull<c_char> {
        let p = self.ptr;
        std::mem::forget(self);
        p
    }

    /// Get the pointer behind `self`. After this is called, we no longer manage
    /// it.
    #[inline]
    pub(crate) fn into_raw(self) -> *mut c_char {
        self.into_inner().as_ptr()
    }

    /// Borrow the pointer behind `self`. We still manage it when this function
    /// returns. If you want to relinquish ownership, use `into_raw`.
    #[inline]
    pub(crate) fn as_ptr(&self) -> *const c_char {
        self.ptr.as_ptr()
    }

    #[inline]
    pub(crate) fn as_cstr(&self) -> &std::ffi::CStr {
        unsafe { std::ffi::CStr::from_ptr(self.as_ptr()) }
    }

    #[inline]
    pub(crate) fn to_string_lossy(&self) -> std::borrow::Cow<'_, str> {
        self.as_cstr().to_string_lossy()
    }

    /// Convert `s` into a SQLite string.
    ///
    /// This should almost never be done except for cases like error messages or
    /// other strings that SQLite frees.
    ///
    /// If `s` contains internal NULs, we'll replace them with
    /// `NUL_REPLACE_CHAR`.
    ///
    /// Except for debug_asserts which may trigger during testing, this function
    /// never panics. If we hit integer overflow or the allocation fails, we
    /// call `handle_alloc_error` which aborts the program after calling a
    /// global hook.
    ///
    /// This means it's safe to use in extern "C" functions even outside of
    /// catch_unwind.
    pub(crate) fn from_str(s: &str) -> Self {
        use std::convert::TryFrom;
        let s = if s.as_bytes().contains(&0) {
            std::borrow::Cow::Owned(make_nonnull(s))
        } else {
            std::borrow::Cow::Borrowed(s)
        };
        debug_assert!(!s.as_bytes().contains(&0));
        let bytes: &[u8] = s.as_ref().as_bytes();
        let src_ptr: *const c_char = bytes.as_ptr().cast();
        let src_len = bytes.len();
        let maybe_len_plus_1 = s.len().checked_add(1).and_then(|v| c_int::try_from(v).ok());
        unsafe {
            let res_ptr = maybe_len_plus_1
                .and_then(|len_to_alloc| {
                    // `>` because we added 1.
                    debug_assert!(len_to_alloc > 0);
                    debug_assert_eq!((len_to_alloc - 1) as usize, src_len);
                    NonNull::new(ffi::sqlite3_malloc(len_to_alloc) as *mut c_char)
                })
                .unwrap_or_else(|| {
                    use std::alloc::{handle_alloc_error, Layout};
                    // Report via handle_alloc_error so that it can be handled with any
                    // other allocation errors and properly diagnosed.
                    //
                    // This is safe:
                    // - `align` is never 0
                    // - `align` is always a power of 2.
                    // - `size` needs no realignment because it's guaranteed to be
                    //   aligned (everything is aligned to 1)
                    // - `size` is also never zero, although this function doesn't actually require it now.
                    let layout = Layout::from_size_align_unchecked(s.len().saturating_add(1), 1);
                    // Note: This call does not return.
                    handle_alloc_error(layout);
                });
            let buf: *mut c_char = res_ptr.as_ptr() as *mut c_char;
            src_ptr.copy_to_nonoverlapping(buf, src_len);
            buf.add(src_len).write(0);
            debug_assert_eq!(std::ffi::CStr::from_ptr(res_ptr.as_ptr()).to_bytes(), bytes);
            Self::from_raw_nonnull(res_ptr)
        }
    }
}

const NUL_REPLACE: &str = "␀";

#[cold]
fn make_nonnull(v: &str) -> String {
    v.replace('\0', NUL_REPLACE)
}

impl Drop for SqliteMallocString {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3_free(self.ptr.as_ptr().cast()) };
    }
}

impl std::fmt::Debug for SqliteMallocString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_string_lossy().fmt(f)
    }
}

impl std::fmt::Display for SqliteMallocString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_string_lossy().fmt(f)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_from_str() {
        let to_check = [
            ("", ""),
            ("\0", "␀"),
            ("␀", "␀"),
            ("\0bar", "␀bar"),
            ("foo\0bar", "foo␀bar"),
            ("foo\0", "foo␀"),
            ("a\0b\0c\0\0d", "a␀b␀c␀␀d"),
            ("foobar0123", "foobar0123"),
        ];

        for &(input, output) in &to_check {
            let s = SqliteMallocString::from_str(input);
            assert_eq!(s.to_string_lossy(), output);
            assert_eq!(s.as_cstr().to_str().unwrap(), output);
        }
    }

    // This will trigger an asan error if into_raw still freed the ptr.
    #[test]
    fn test_lossy() {
        let p = SqliteMallocString::from_str("abcd").into_raw();
        // Make invalid
        let s = unsafe {
            p.cast::<u8>().write(b'\xff');
            SqliteMallocString::from_raw(p).unwrap()
        };
        assert_eq!(s.to_string_lossy().as_ref(), "\u{FFFD}bcd");
    }

    // This will trigger an asan error if into_raw still freed the ptr.
    #[test]
    fn test_into_raw() {
        let mut v = vec![];
        for i in 0..1000 {
            v.push(SqliteMallocString::from_str(&i.to_string()).into_raw());
            v.push(SqliteMallocString::from_str(&format!("abc {} 😀", i)).into_raw());
        }
        unsafe {
            for (i, s) in v.chunks_mut(2).enumerate() {
                let s0 = std::mem::replace(&mut s[0], std::ptr::null_mut());
                let s1 = std::mem::replace(&mut s[1], std::ptr::null_mut());
                assert_eq!(
                    std::ffi::CStr::from_ptr(s0).to_str().unwrap(),
                    &i.to_string()
                );
                assert_eq!(
                    std::ffi::CStr::from_ptr(s1).to_str().unwrap(),
                    &format!("abc {} 😀", i)
                );
                let _ = SqliteMallocString::from_raw(s0).unwrap();
                let _ = SqliteMallocString::from_raw(s1).unwrap();
            }
        }
    }
}
