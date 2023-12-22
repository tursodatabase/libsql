#!/usr/bin/env sh

set -x

git submodule update --init SQLite3MultipleCiphers
cd SQLite3MultipleCiphers/build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j12
