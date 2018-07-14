SCRIPT_DIR=$(cd "$(dirname "$_")" && pwd)
echo $SCRIPT_DIR
cd $SCRIPT_DIR
SQLITE3_LIB_DIR=$SCRIPT_DIR/sqlite3

# Download and extract amalgamation
SQLITE=sqlite-amalgamation-3240000
curl -O http://sqlite.org/2018/$SQLITE.zip
unzip -p $SQLITE.zip $SQLITE/sqlite3.c > $SQLITE3_LIB_DIR/sqlite3.c
unzip -p $SQLITE.zip $SQLITE/sqlite3.h > $SQLITE3_LIB_DIR/sqlite3.h
unzip -p $SQLITE.zip $SQLITE/sqlite3ext.h > $SQLITE3_LIB_DIR/sqlite3ext.h
rm -f $SQLITE.zip

# Regenerate bindgen file
rm -f $SQLITE3_LIB_DIR/bindgen_bundled_version.rs
SQLITE3_INCLUDE_DIR=$SQLITE3_LIB_DIR
cargo update
# Just to make sure there is only one bindgen.rs file in target dir
find $SCRIPT_DIR/target -type f -name bindgen.rs -exec rm {} \;
cargo build --features "buildtime_bindgen" --no-default-features
find $SCRIPT_DIR/target -type f -name bindgen.rs -exec cp {} $SQLITE3_LIB_DIR/bindgen_bundled_version.rs \;
# Sanity check
cd $SCRIPT_DIR/..
cargo update
cargo test --features "backup blob chrono functions limits load_extension serde_json trace bundled"
echo 'You should increment the version in libsqlite3-sys/Cargo.toml'
