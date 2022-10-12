# Usage
First off, make sure `sqlite.o` is in your build directory. How to do it is described [here](https://github.com/libsql/libsql/blob/main/README-SQLite.md).

# Testing in your build directory
```
make rusttest
```

# Testing in `src/rust`
```
cd src/
ar rcs libsqlite3.a <path-to-build-directory>/sqlite3.o
gcc -c -o callback.o callback.c
ar rcs libcallback.a callback.o
cargo test
```
