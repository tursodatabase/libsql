# Process this file with automake to create Makefile.in for sqlite3mc library
#
# Copyright (C) 2019-2023 Ulrich Telle <ulrich@telle-online.de>
#
# This file is covered by the same licence as the entire SQLite3 Multiple Ciphers package.

if HOST_X86
X86_FLAGS = -msse4.2 -maes 
else
X86_FLAGS =
endif

if HOST_ARM
#ARM_FLAGS = -march=native 
ARM_FLAGS =
else
ARM_FLAGS =
endif

ACLOCAL_AMFLAGS = -I admin/m4

# Flags used for compiling all the targets and linking all the executables
# (libraries use LIBADD which is set for lib@SQLITE3MC_LIBNAME@.la only).
AM_CXXFLAGS = -I$(top_srcdir)/src
AM_CFLAGS   = -I$(top_srcdir)/src

AM_CFLAGS += -std=c99 -DSQLITE_THREADSAFE=1 -DSQLITE_DQS=0 -DSQLITE_MAX_ATTACHED=10 -DSQLITE_SOUNDEX=1 -DSQLITE_ENABLE_COLUMN_METADATA=1 -DSQLITE_SECURE_DELETE=1 -DSQLITE_ENABLE_DESERIALIZE=1 -DSQLITE_ENABLE_FTS3=1 -DSQLITE_ENABLE_FTS3_PARENTHESIS=1 -DSQLITE_ENABLE_FTS4=1 -DSQLITE_ENABLE_FTS5=1 -DSQLITE_ENABLE_JSON1=1 -DSQLITE_ENABLE_RTREE=1 -DSQLITE_ENABLE_GEOPOLY=1 -DSQLITE_ENABLE_PREUPDATE_HOOK=1 -DSQLITE_ENABLE_SESSION=1 -DSQLITE_CORE=1 -DSQLITE_ENABLE_EXTFUNC=1 -DSQLITE_ENABLE_MATH_FUNCTIONS=1 -DSQLITE_ENABLE_CSV=1 -DSQLITE_ENABLE_SHA3=1 -DSQLITE_ENABLE_CARRAY=1 -DSQLITE_ENABLE_FILEIO=1 -DSQLITE_ENABLE_SERIES=1 -DSQLITE_ENABLE_UUID=1 -DSQLITE_ENABLE_REGEXP=1 -DSQLITE_TEMP_STORE=2 -DSQLITE_USE_URI=1 -DSQLITE_USER_AUTHENTICATION=1 $(X86_FLAGS) $(ARM_FLAGS)

if HOST_WINDOWS
AM_CFLAGS += -DSQLITE_API=__declspec\(dllexport\)
AM_LDFLAGS =
else
AM_LDFLAGS = -pthread -ldl -lm
endif

if HAVE_ZLIB
AM_CFLAGS += -DSQLITE_HAVE_ZLIB=1
AM_LDFLAGS += -lz
else
AM_CFLAGS += -DSQLIT3MC_USE_MINIZ=1
endif

if HOST_OSX
AM_LDFLAGS += -framework Security
endif

LDADD = lib@SQLITE3MC_LIBNAME@.la

if HOST_WINDOWS
# Use old style suffix rule instead of the pattern one as automake only
# recognizes this kind of rules.
#
# Also notice that we use .res_o_o extension for the object files obtained by
# compiling resource files, instead of just the usual .o, because we'd have
# conflicts between object file names for foo.cpp and foo.rc otherwise. Due to
# this we have to add the .res_o files manually to LDADD later, but such is life.
.rc.res_o:
	$(AM_V_GEN) $(WINDRES) -I $(top_srcdir) $(WX_CPPFLAGS) -O coff $< $@
endif

# Define the directory where the library headers are installed.
includemcdir = $(includedir)/sqlite3mc

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = sqlite3mc.pc

# The main library. Notice that it uses dynamic, i.e. determined when running
# configure, name because it depends on the version of wxWidgets used. This is
# rather unusual, but Automake doesn't seem to mind.
lib_LTLIBRARIES = lib@SQLITE3MC_LIBNAME@.la

lib@SQLITE3MC_LIBNAME@_la_SOURCES = \
    src/sqlite3mc.c

if HOST_WINDOWS
lib@SQLITE3MC_LIBNAME@_la_SOURCES += \
    src/sqlite3mc.rc
endif

includemc_HEADERS = \
    src/sqlite3.h \
    src/sqlite3userauth.h \
    src/sqlite3ext.h \
    src/sqlite3mc_version.h \
    src/sqlite3mc_vfs.h \
    src/sqlite3mc.h

noinst_HEADERS = \
    src/carray.c \
    src/chacha20poly1305.c \
    src/cipher_chacha20.c \
    src/cipher_common.c \
    src/cipher_config.c \
    src/cipher_sds_rc4.c \
    src/cipher_sqlcipher.c \
    src/cipher_wxaes128.c \
    src/cipher_wxaes256.c \
    src/codec_algos.c \
    src/codecext.c \
    src/csv.c \
    src/extensionfunctions.c \
    src/fastpbkdf2.c \
    src/fileio.c \
    src/md5.c \
    src/rekeyvacuum.c \
    src/rijndael.c \
    src/series.c \
    src/sha1.c \
    src/sha2.c \
    src/shathree.c \
    src/sqlite3.c \
    src/sqlite3mc_vfs.c \
    src/test_windirent.c \
    src/userauth.c \
    src/uuid.c \
    src/cipher_common.h \
    src/fastpbkdf2.h \
    src/mystdint.h \
    src/rijndael.h \
    src/sha1.h \
    src/sha2.h \
    src/sqlite3mc_vfs.h \
    src/sqlite3userauth.h \
    src/test_windirent.h

lib@SQLITE3MC_LIBNAME@_la_LDFLAGS = -no-undefined $(AM_LDFLAGS)
if HOST_WINDOWS
lib@SQLITE3MC_LIBNAME@_la_LIBADD = src/sqlite3mc.res_o
else
lib@SQLITE3MC_LIBNAME@_la_LIBADD = -ldl -lm
endif


# Samples (don't need to be installed).
noinst_PROGRAMS = sqlite3shell

sqlite3shell_SOURCES = \
    src/sqlite3mc.c \
    src/shell.c

sqlite3shell_CFLAGS = -I$(top_srcdir)/src -std=c99 -D_GNU_SOURCE -DSQLITE_THREADSAFE=1 -DSQLITE_DQS=0 -DSQLITE_MAX_ATTACHED=10 -DSQLITE_SOUNDEX=1 -DSQLITE_ENABLE_COLUMN_METADATA=1 -DSQLITE_SECURE_DELETE=1 -DSQLITE_ENABLE_DESERIALIZE=1 -DSQLITE_ENABLE_FTS3=1 -DSQLITE_ENABLE_FTS3_PARENTHESIS=1 -DSQLITE_ENABLE_FTS4=1 -DSQLITE_ENABLE_FTS5=1 -DSQLITE_ENABLE_JSON1=1 -DSQLITE_ENABLE_RTREE=1 -DSQLITE_ENABLE_GEOPOLY=1 -DSQLITE_ENABLE_PREUPDATE_HOOK=1 -DSQLITE_ENABLE_SESSION=1 -DSQLITE_CORE=1 -DSQLITE_ENABLE_EXTFUNC=1 -DSQLITE_ENABLE_MATH_FUNCTIONS=1 -DSQLITE_ENABLE_CSV=1 -DSQLITE_ENABLE_CARRAY=1 -DSQLITE_ENABLE_UUID=1 -DSQLITE_TEMP_STORE=2 -DSQLITE_USE_URI=1 -DSQLITE_USER_AUTHENTICATION=1 -DSQLITE_ENABLE_DBPAGE_VTAB=1 -DSQLITE_ENABLE_DBSTAT_VTAB=1 -DSQLITE_ENABLE_STMTVTAB=1 -DSQLITE_ENABLE_UNKNOWN_SQL_FUNCTION=1 $(X86_FLAGS) $(ARM_FLAGS)

if HOST_WINDOWS
sqlite3shell_LDADD =
sqlite3shell_LDFLAGS =
else
sqlite3shell_LDADD = -ldl -lm
sqlite3shell_LDFLAGS = -pthread
endif

if HAVE_ZLIB
sqlite3shell_CFLAGS += -DSQLITE_HAVE_ZLIB=1
sqlite3shell_LDADD += -lz
endif

if HOST_WINDOWS
sqlite3shell_SOURCES += src/sqlite3mc_shell.rc
sqlite3shell_LDADD += src/sqlite3mc_shell.res_o
CLEANFILES = src/sqlite3mc_shell.res_o
else
# libtool complains about unknown "-no-install" option when targetting MSW, so
# use it only in the "else" branch.
sqlite3shell_LDFLAGS += -no-install

if HOST_OSX
sqlite3shell_LDFLAGS += -framework Security
endif

endif
