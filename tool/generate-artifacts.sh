#!/usr/bin/env bash

# Generates artifacts from the current build:
# - .c and .h amalgamation files
# - a precompiled binary package
#
# Assumes that ./configure and make steps were executed and succeeded

LIBSQL_WASM_UDF_SUFFIX=
if grep -s "OPT_WASM_RUNTIME_LIBRARY_TARGET.*wasm" Makefile; then
  LIBSQL_WASM_UDF_SUFFIX="-wasm-udf-dynamic"
fi

set -x

tar czvf libsql-amalgamation-$(<LIBSQL_VERSION)${LIBSQL_WASM_UDF_SUFFIX}.tar.gz sqlite3.c sqlite3.h
tar czvf libsql-$(<LIBSQL_VERSION)${LIBSQL_WASM_UDF_SUFFIX}.tar.gz sqlite3 libsql .libs
