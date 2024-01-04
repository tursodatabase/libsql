dnl Process this script with autoconf to create configure for sqlite3mc library
dnl
dnl Copyright (C) 2019-2023 Ulrich Telle <ulrich@telle-online.de>
dnl
dnl This file is covered by the same licence as the entire SQLite3 Multiple Ciphers package.

AC_INIT([sqlite3mc], [1.8.1], [ulrich@telle-online.de])

dnl This is the version tested with, might work with earlier ones.
AC_PREREQ([2.69])

AC_CONFIG_SRCDIR([src/sqlite3.h])
AC_CONFIG_AUX_DIR([admin/build-aux])
AC_CONFIG_MACRO_DIR([admin/m4])

AM_INIT_AUTOMAKE([1.11 foreign subdir-objects])
AM_MAINTAINER_MODE([enable])
AM_SILENT_RULES([yes])

LT_INIT()
AC_PROG_CXX
AC_LANG(C++)

AC_ARG_ENABLE(sqlite-debug,
    [  --enable-sqlite-debug  Enable SQLite Debug assertions
                         This is a debugging feature
                         which should usually not be enabled],
    [
      AC_DEFINE([SQLITE_DEBUG])
    ])

dnl Options for deselecting ciphers

dnl Cipher wxSQLite3 AES-128-CBC

AC_ARG_WITH([aes128cbc],
    [AS_HELP_STRING([--without-aes128cbc],
        [Disable support for AES 128 Bit CBC Encryption])],
    [],
    [with_aes128cbc=yes])

AS_IF([test "x$with_aes128cbc" = xno],
    [AC_DEFINE([HAVE_CIPHER_AES_128_CBC], [0], [Define if you have AES 128 Bit CBC disabled])])

dnl Cipher wxSQLite3 AES-256-CBC

AC_ARG_WITH([aes256cbc],
    [AS_HELP_STRING([--without-aes256cbc],
        [Disable support for AES 256 Bit CBC Encryption])],
    [],
    [with_aes256cbc=yes])

AS_IF([test "x$with_aes256cbc" = xno],
    [AC_DEFINE([HAVE_CIPHER_AES_256_CBC], [0], [Define if you have AES 256 Bit CBC disabled])])

dnl Cipher ChaCha20-Poly1305 / sqleet

AC_ARG_WITH([chacha20],
    [AS_HELP_STRING([--without-chacha20],
        [Disable support for ChaCha20-Poly1305 Encryption])],
    [],
    [with_chacha20=yes])

AS_IF([test "x$with_chacha20" = xno],
    [AC_DEFINE([HAVE_CIPHER_CHACHA20], [0], [Define if you have ChaCha20-Poly1305 disabled])])

dnl Cipher SQLCipher

AC_ARG_WITH([sqlcipher],
    [AS_HELP_STRING([--without-sqlcipher],
        [Disable support for SQLCipher Encryption])],
    [],
    [with_sqlcipher=yes])

AS_IF([test "x$with_sqlcipher" = xno],
    [AC_DEFINE([HAVE_CIPHER_SQLCIPHER], [0], [Define if you have SQLCipher disabled])])

dnl Cipher System.Data.SQLite RC4

AC_ARG_WITH([rc4],
    [AS_HELP_STRING([--without-rc4],
        [Disable support for System.Data.SQLite RC4 Encryption])],
    [],
    [with_rc4=yes])

AS_IF([test "x$with_rc4" = xno],
    [AC_DEFINE([HAVE_CIPHER_RC4], [0], [Define if you have System.Data.SQLite RC4  disabled])])

AC_ARG_WITH([ascon128],
    [AS_HELP_STRING([--without-ascon128],
        [Disable support for Ascon-128 Encryption])],
    [],
    [with_ascon128=yes])

AS_IF([test "x$with_ascon128" = xno],
    [AC_DEFINE([HAVE_CIPHER_ASCON128], [0], [Define if you have Ascon-128  disabled])])

dnl Enable cipher codec

AC_ARG_ENABLE(codec,
    [  --enable-codec[=<codec type>] Specify the codec type:
                              aes128: AES 128 Bit CBC Encryption
                              aes256: AES 256 Bit CBC Encryption
                              chacha20 [default]: ChaCha20-Poly1305 Encryption
                              sqlcipher: SQLCipher Encryption
                              rc4: System.Data.SQLite RC4 Encryption
                              ascon128: Ascon-128 Encryption],
      [if   test "x$enableval" = "xaes128" && test "x$with_aes128cbc" = xyes ; then
        codec_type=CODEC_TYPE_AES128
      elif test "x$enableval" = "xaes256" && test "x$with_aes256cbc" = xyes ; then
        codec_type=CODEC_TYPE_AES256
      elif test "x$enableval" = "xchacha20" && test "x$with_chacha20" = xyes ; then
        codec_type=CODEC_TYPE_CHACHA20
      elif test "x$enableval" = "xsqlcipher" && test "x$with_sqlcipher" = xyes ; then
        codec_type=CODEC_TYPE_SQLCIPHER
      elif test "x$enableval" = "xrc4" && test "x$with_rc4" = xyes ; then
        codec_type=CODEC_TYPE_RC4
      elif test "x$enableval" = "xascon128" && test "x$with_ascon128" = xyes ; then
        codec_type=CODEC_TYPE_ASCON128
      else
        echo
        echo "Error!"
        echo "Unknown or Unsupported codec type"
        exit -1
     fi
     AC_DEFINE_UNQUOTED([CODEC_TYPE], [$codec_type])
    ])

AS_IF([test "x$with_aes128cbc" = xno &&
       test "x$with_aes256cbc" = xno &&
       test "x$with_chacha20" = xno &&
       test "x$with_sqlcipher" = xno &&
       test "x$with_rc4" = xno &&
       test "x$with_ascon128" = xno],
       [AC_DEFINE([SQLITE_HAS_CODEC], [0], [All ciphers disabled so encryption is disabled])])

dnl Check for zlib
haveZLib=false
AC_CHECK_HEADERS(zlib.h, [
  AC_SEARCH_LIBS(deflate, z, [haveZLib=true], [haveZLib=false])
], [haveZLib=false])

AC_CANONICAL_HOST
hostX86=false
hostARM=false
AS_CASE([$host_cpu],
  [i?86], [hostX86=true],
  [x86_64], [hostX86=true],
  [arm*|aarch64*], [hostARM=true]
)
AM_CONDITIONAL([HOST_X86], [test x$hostX86 = xtrue])
AM_CONDITIONAL([HOST_ARM], [test x$hostARM = xtrue])
AM_CONDITIONAL([HAVE_ZLIB], [test x$haveZLib = xtrue])

dnl Detect the target system

build_linux=no
build_windows=no
build_mac=no

case "${host_os}" in
    linux*)
        build_linux=yes
        ;;
    cygwin*|mingw*)
        build_windows=yes
        ;;
    darwin*)
        build_mac=yes
        ;;
    *)
        AC_MSG_ERROR(["OS $host_os is not supported"])
        ;;
esac

if [test "$build_windows" = "yes"]; then
    AC_CHECK_TOOL([WINDRES], [windres], no)
fi

dnl Pass the conditionals to automake
AM_CONDITIONAL([HOST_LINUX], [test "$build_linux" = "yes"])
AM_CONDITIONAL([HOST_WINDOWS], [test "$build_windows" = "yes"])
AM_CONDITIONAL([HOST_OSX], [test "$build_mac" = "yes"])

AC_SUBST(SQLITE3MC_LIBNAME, "sqlite3mc")
AC_SUBST( LIBDIR, "lib" )

AC_CONFIG_FILES([Makefile sqlite3mc.pc])
AC_OUTPUT
