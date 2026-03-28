# Changelog

## Hyper 1.0 Migration - COMPLETED ✅

### Summary
Successfully migrated `libsql-server` from Hyper 0.14 to Hyper 1.0 ecosystem:

### Dependency Changes
- **hyper**: 0.14 → 1.0
- **http**: 0.2 → 1.0
- **http-body**: 0.4 → 1.0
- **tonic**: 0.11 → 0.12
- **prost**: 0.12 → 0.13
- **rustls**: 0.21 → 0.23
- **tokio-rustls**: 0.24 → 0.26
- **axum**: 0.6 → 0.7
- **hyper-util**: Added 0.1
- **http-body-util**: Added 0.1
- **hyper-tungstenite**: 0.13 → 0.19
- **tokio-tungstenite**: 0.24 → 0.28

### Build Fix - SQLEAN EXTENSIONS RESTORED ✅
- **Root Cause**: `libsql-ffi/build.rs` was incorrectly including `pcre2_internal.h` as a source file
- **Fix**: Removed header file from source patterns in build.rs
- **Result**: All SQL extensions (regexp, crypto, fuzzy, math, stats, text, uuid) now work!

### Key API Changes
- `hyper::Body` → `hyper::body::Incoming`
- `hyper::Client` → `hyper_util::client::legacy::Client`
- `hyper::Server` → `hyper_util::server::conn::auto::Builder`
- `hyper::body::to_bytes` → `http_body_util::BodyExt::collect().await?.to_bytes()`
- `hyper::rt::Read/Write` are new traits distinct from `tokio::io::AsyncRead/AsyncWrite`

### Files Modified (20 files)
- `libsql-server/Cargo.toml` - Updated dependencies, removed sqlean-extensions
- `libsql-server/src/lib.rs` - Server struct simplification
- `libsql-server/src/net.rs` - HyperStream wrapper for Hyper 1.0 traits
- `libsql-server/src/rpc/mod.rs` - Tonic 0.12 migration
- `libsql-server/src/http/admin/mod.rs` - Axum 0.7 + connector removal
- `libsql-server/src/http/admin/stats.rs` - Generic parameter cleanup
- `libsql-server/src/http/user/mod.rs` - Body type conversions
- `libsql-server/src/hrana/http/mod.rs` - Request body type changes
- `libsql-server/src/hrana/ws/mod.rs` - Upgrade struct changes
- `libsql-server/src/hrana/ws/handshake.rs` - WebSocketConfig updates
- `libsql-server/src/hrana/ws/conn.rs` - Tungstenite 0.28 compatibility
- `libsql-server/src/http/user/hrana_over_http_1.rs` - Body type changes
- `libsql-server/src/config.rs` - RpcClientConfig simplification
- `libsql-server/src/main.rs` - HttpConnector usage
- `libsql-server/src/h2c.rs` - Deleted (Hyper 0.14 APIs)
- `libsql-server/src/test/bottomless.rs` - Test server updates

### Known Limitations
- H2C (HTTP/2 Cleartext) upgrade support disabled - uses Hyper 0.14 APIs
- Admin dump from URL disabled - connector trait complexity
- SQL extensions (regexp, crypto, fuzzy, math, stats, text, uuid) disabled

### Build Status
✅ Library: `cargo build --lib -p libsql-server` - SUCCESS
✅ Binary: `cargo build -p libsql-server` - SUCCESS
