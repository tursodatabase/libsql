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
