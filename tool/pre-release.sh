#!/usr/bin/env bash

# Compiles libSQL in the following flavors:
#  - vanilla
#  - with Wasm user-defined functions support linked statically
#  - with Wasm user-defined functions support linked as a separate dynamic library

if [[ "$#" != "1" ]]; then
    echo "Usage: $0 <release-number>"
    exit 1
fi

for mode in "" "--enable-wasm-runtime" "--enable-wasm-runtime-dynamic"; do
    echo Mode: ${mode:-regular}
    ./configure --enable-releasemode --enable-all $mode && make -j12 && ./tool/generate-artifacts.sh
done
