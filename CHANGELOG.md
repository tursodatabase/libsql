# Changelog

## Hyper 1.0 Migration - IN PROGRESS 🔄

### Summary
Migrating `libsql-server` from Hyper 0.14 to Hyper 1.0 ecosystem. This is a major upgrade affecting the entire HTTP stack.

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

### Current CI Status (Latest Run)
| Workflow | Status |
|----------|--------|
| Run Checks | ✅ PASS |
| c-bindings | ✅ PASS |
| c-bundle-validate | ✅ PASS |
| CR SQLite C Tests | ✅ PASS |
| CR SQLite Rust Tests | ✅ PASS |
| Extensions Tests | ✅ PASS |
| Windows checks | ✅ PASS |
| golang-bindings | ❌ FAIL |
| Check features and unused dependencies | ❌ FAIL |

### Critical Issue: gRPC Handshake Timeout
The `golang-bindings` test is failing with:
```
replication error: Timeout performing handshake with primary
```

This indicates the gRPC server (tonic 0.12 + hyper 1.0) is not properly handling HTTP/2 connections from embedded replica clients.

### Build Fix - SQLEAN EXTENSIONS RESTORED ✅
- **Root Cause**: `libsql-ffi/build.rs` was incorrectly including `pcre2_internal.h` as a source file
- **Fix**: Removed header file from source patterns in build.rs
- **Result**: SQL extensions compile successfully

### Key API Changes
- `hyper::Body` → `hyper::body::Incoming`
- `hyper::Client` → `hyper_util::client::legacy::Client`
- `hyper::Server` → `hyper_util::server::conn::auto::Builder`
- `hyper::body::to_bytes` → `http_body_util::BodyExt::collect().await?.to_bytes()`
- `hyper::rt::Read/Write` are new traits distinct from `tokio::io::AsyncRead/AsyncWrite`

### Files Modified (25+ files)
- `libsql-server/Cargo.toml` - Updated dependencies
- `libsql-server/src/lib.rs` - Server struct simplification
- `libsql-server/src/net.rs` - HyperStream wrapper for Hyper 1.0 traits
- `libsql-server/src/rpc/mod.rs` - Tonic 0.12 migration, custom incoming streams
- `libsql-server/src/http/admin/mod.rs` - Axum 0.7 migration
- `libsql-server/src/http/user/mod.rs` - Body type conversions
- `libsql-server/src/hrana/http/mod.rs` - Request body type changes
- `libsql-server/src/hrana/ws/handshake.rs` - WebSocketConfig updates
- `libsql-server/src/test/bottomless.rs` - S3 mock server updates
- `libsql/src/sync.rs` - Fixed private_interfaces warning
- `libsql/src/hrana/hyper.rs` - Removed unused imports
- `bindings/c/Cargo.toml` - hyper-rustls 0.25 → 0.27
- All integration test files migrated to hyper 1.0

### Known Limitations
- H2C (HTTP/2 Cleartext) upgrade support disabled - uses Hyper 0.14 APIs
- Admin dump from URL disabled - connector trait complexity
- 2 bottomless S3 tests ignored - need full S3 protocol mock

### Next Steps
1. Fix gRPC handshake timeout in golang-bindings test
2. Fix cargo-udeps unused dependencies check
3. Complete cleanup of temporary files
4. Final merge preparation

---

## Previous Releases

### v0.24.33
- Original Hyper 0.14 based release
