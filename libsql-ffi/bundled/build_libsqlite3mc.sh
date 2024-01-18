#!/usr/bin/env sh

set -x

git submodule update --init SQLite3MultipleCiphers
mkdir -p SQLite3MultipleCiphers/build
cd SQLite3MultipleCiphers/build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DSQLITE3MC_STATIC=ON \
    -DCODEC_TYPE=AES256 -DSQLITE3MC_BUILD_SHELL=OFF \
    -DSQLITE_SHELL_IS_UTF8=OFF -DSQLITE_USER_AUTHENTICATION=OFF \
    -DSQLITE_SECURE_DELETE=OFF
make -j12
