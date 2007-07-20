#!/bin/sh
#
# This script is used to compile SQLite into a shared library on Linux.
#
# Two separate shared libraries are generated.  "sqlite3.so" is the core
# library.  "tclsqlite3.so" contains the TCL bindings and is the
# library that is loaded into TCL in order to run SQLite.
#
CFLAGS=-O2 -Wall
make fts2amal.c
echo gcc $CFLAGS -shared fts2amal.c -o fts2.so
gcc $CFLAGS -shared fts2amal.c -o fts2.so
strip fts2.so
