
AM_CFLAGS = @THREADSAFE_FLAGS@ @DYNAMIC_EXTENSION_FLAGS@ @FTS5_FLAGS@ @JSON1_FLAGS@ -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_RTREE

lib_LTLIBRARIES = libsqlite3.la
libsqlite3_la_SOURCES = sqlite3.c
libsqlite3_la_LDFLAGS = -no-undefined -version-info 8:6:8

bin_PROGRAMS = sqlite3
sqlite3_SOURCES = shell.c sqlite3.h
EXTRA_sqlite3_SOURCES = sqlite3.c
sqlite3_LDADD = @EXTRA_SHELL_OBJ@ @READLINE_LIBS@
sqlite3_DEPENDENCIES = @EXTRA_SHELL_OBJ@
sqlite3_CFLAGS = $(AM_CFLAGS)

include_HEADERS = sqlite3.h sqlite3ext.h

EXTRA_DIST = sqlite3.1 tea
pkgconfigdir = ${libdir}/pkgconfig
pkgconfig_DATA = sqlite3.pc

man_MANS = sqlite3.1
