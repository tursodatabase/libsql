#!/bin/bash -e

SCRIPT_DIR=$(cd "$(dirname "$_")" && pwd)
CUR_DIR=$(pwd -P)
echo "$SCRIPT_DIR"
cd "$SCRIPT_DIR" || { echo "fatal error" >&2; exit 1; }
cargo clean
mkdir -p "$SCRIPT_DIR/../target" "$SCRIPT_DIR/sqlite3" "$SCRIPT_DIR/sqlcipher"
export SQLITE3_LIB_DIR="$SCRIPT_DIR/sqlite3"
export SQLITE3_INCLUDE_DIR="$SQLITE3_LIB_DIR"
export SQLCIPHER_LIB_DIR="$SCRIPT_DIR/sqlcipher"
export SQLCIPHER_INCLUDE_DIR="$SQLCIPHER_LIB_DIR"

# Download and extract amalgamation
SQLITE=sqlite-amalgamation-3360000
curl -O https://sqlite.org/2021/$SQLITE.zip
unzip -p "$SQLITE.zip" "$SQLITE/sqlite3.c" > "$SQLITE3_LIB_DIR/sqlite3.c"
unzip -p "$SQLITE.zip" "$SQLITE/sqlite3.h" > "$SQLITE3_LIB_DIR/sqlite3.h"
unzip -p "$SQLITE.zip" "$SQLITE/sqlite3ext.h" > "$SQLITE3_LIB_DIR/sqlite3ext.h"
rm -f "$SQLITE.zip"

# Regenerate bindgen file for sqlite3
rm -f "$SQLITE3_LIB_DIR/bindgen_bundled_version.rs"
cargo update
# Just to make sure there is only one bindgen.rs file in target dir
find "$SCRIPT_DIR/../target" -type f -name bindgen.rs -exec rm {} \;
env LIBSQLITE3_SYS_BUNDLING=1 cargo build --features "buildtime_bindgen session" --no-default-features
find "$SCRIPT_DIR/../target" -type f -name bindgen.rs -exec mv {} "$SQLITE3_LIB_DIR/bindgen_bundled_version.rs" \;

SQLCIPHER_VERSION="4.4.3"
# Download and generate sqlcipher amalgamation
mkdir -p $SCRIPT_DIR/sqlcipher.src
[ -e "v${SQLCIPHER_VERSION}.tar.gz" ] || curl -sfL -O "https://github.com/sqlcipher/sqlcipher/archive/v${SQLCIPHER_VERSION}.tar.gz"
tar xzf "v${SQLCIPHER_VERSION}.tar.gz" --strip-components=1 -C "$SCRIPT_DIR/sqlcipher.src"
cd "$SCRIPT_DIR/sqlcipher.src"
./configure --with-crypto-lib=none
make sqlite3.c
cp sqlite3.c sqlite3.h sqlite3ext.h "$SCRIPT_DIR/sqlcipher/"
cd "$SCRIPT_DIR"
rm -rf "v${SQLCIPHER_VERSION}.tar.gz" sqlcipher.src

# Regenerate bindgen file for sqlcipher
rm -f "$SQLCIPHER_LIB_DIR/bindgen_bundled_version.rs"
cargo clean
# cargo update
# find "$SCRIPT_DIR/../target" -type f -name bindgen.rs -exec rm {} \;
env LIBSQLITE3_SYS_BUNDLING=1 cargo build --features "sqlcipher buildtime_bindgen session"
find "$SCRIPT_DIR/../target" -type f -name bindgen.rs -exec mv {} "$SQLCIPHER_LIB_DIR/bindgen_bundled_version.rs" \;

# Sanity checks
cd "$SCRIPT_DIR/.." || { echo "fatal error" >&2; exit 1; }
cargo update
cargo test --features "backup blob chrono functions limits load_extension serde_json trace vtab bundled"
printf '    \e[35;1mFinished\e[0m bundled sqlite3 tests\n'
cargo test --features "backup blob chrono functions limits load_extension serde_json trace vtab bundled-sqlcipher-vendored-openssl"
printf '    \e[35;1mFinished\e[0m bundled-sqlcipher-vendored-openssl/sqlcipher tests\n'
echo 'You should increment the version in libsqlite3-sys/Cargo.toml'
