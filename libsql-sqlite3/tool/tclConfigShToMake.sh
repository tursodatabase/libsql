#!/bin/sh
#
# A level of indirection for use soley by main.mk to extract info
# from tclConfig.sh if it's not provided by the configure script.
#
# Expects to be passed a full path to a tclConfig.sh. It sources it
# and emits TCL code which sets some vars which are exported by
# tclConfig.sh.
#
# This script expects that the caller has already validated that the
# file exists, is not a directory, and is readable.
#
# If passed no filename, or an empty one, then it emits config code
# suitable for the "config not found" case.
if test x = "x$1"; then
  TCL_INCLUDE_SPEC=
  TCL_LIB_SPEC=
  TCL_STUB_LIB_SPEC=
  TCL_EXEC_PREFIX=
  TCL_VERSION=
else
  . "$1"
fi

cat <<EOF
TCL_INCLUDE_SPEC = $TCL_INCLUDE_SPEC
TCL_LIB_SPEC = $TCL_LIB_SPEC
TCL_STUB_LIB_SPEC = $TCL_STUB_LIB_SPEC
TCL_EXEC_PREFIX = $TCL_EXEC_PREFIX
TCL_VERSION = $TCL_VERSION
EOF
