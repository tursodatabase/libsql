// Internal utilities
pub(crate) mod param_cache;
mod small_cstr;
pub(crate) use param_cache::ParamIndexCache;
pub(crate) use small_cstr::SmallCString;

// Doesn't use any modern features or vtab stuff, but is only used by them.
#[cfg(any(feature = "modern_sqlite", feature = "vtab"))]
mod sqlite_string;
#[cfg(any(feature = "modern_sqlite", feature = "vtab"))]
pub(crate) use sqlite_string::SqliteMallocString;

#[inline]
pub(crate) fn get_cached<T, F>(cache: &std::cell::Cell<Option<T>>, lookup: F) -> T
where
    T: Copy,
    F: FnOnce() -> T,
{
    if let Some(v) = cache.get() {
        v
    } else {
        let cb = lookup();
        cache.set(Some(cb));
        cb
    }
}
