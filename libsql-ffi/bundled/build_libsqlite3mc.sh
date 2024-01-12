#!/usr/bin/env sh

set -x

git submodule update --init SQLite3MultipleCiphers
mkdir -p SQLite3MultipleCiphers/build
cd SQLite3MultipleCiphers/build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DSQLITE3MC_STATIC=ON \
    -DCODEC_TYPE=AES256 -DSQLITE3MC_BUILD_SHELL=OFF
make -j12
