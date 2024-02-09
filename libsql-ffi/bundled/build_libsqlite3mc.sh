#!/usr/bin/env sh

set -x

git submodule update --init SQLite3MultipleCiphers
mkdir -p SQLite3MultipleCiphers/build
cd SQLite3MultipleCiphers/build
cmake .. -DCMAKE_BUILD_TYPE=Release -DSQLITE3MC_STATIC=ON \
    -DCODEC_TYPE=AES256 -DSQLITE3MC_BUILD_SHELL=OFF \
    -DSQLITE_SHELL_IS_UTF8=OFF -DSQLITE_USER_AUTHENTICATION=OFF \
    -DSQLITE_SECURE_DELETE=OFF -DSQLITE_ENABLE_COLUMN_METADATA=ON \
    -DSQLITE_USE_URI=ON $@
make -j12
