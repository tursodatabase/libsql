
#-----------------------------------------------------------------------
# Supports the following non-standard switches.
#
#   --enable-threadsafe
#   --enable-readline
#   --enable-editline
#   --enable-static-shell
#   --enable-dynamic-extensions
#

AC_PREREQ(2.61)
AC_INIT(sqlite, --SQLITE-VERSION--, http://www.sqlite.org)
AC_CONFIG_SRCDIR([sqlite3.c])

# Use automake.
AM_INIT_AUTOMAKE([foreign])

AC_SYS_LARGEFILE

# Check for required programs.
AC_PROG_CC
AC_PROG_LIBTOOL
AC_PROG_MKDIR_P

# Check for library functions that SQLite can optionally use.
AC_CHECK_FUNCS([fdatasync usleep fullfsync localtime_r gmtime_r])
AC_FUNC_STRERROR_R

AC_CONFIG_FILES([Makefile sqlite3.pc])
AC_SUBST(BUILD_CFLAGS)

#-----------------------------------------------------------------------
#   --enable-editline
#   --enable-readline
#
AC_ARG_ENABLE(editline, [AS_HELP_STRING(
  [--enable-editline], 
  [use BSD libedit])], 
  [], [enable_editline=yes])
AC_ARG_ENABLE(readline, [AS_HELP_STRING(
  [--enable-readline], 
  [use readline])], 
  [], [enable_readline=no])
if test x"$enable_editline" != xno ; then
  sLIBS=$LIBS
  LIBS=""
  AC_SEARCH_LIBS([readline],[edit],[enable_readline=no],[enable_editline=no])
  READLINE_LIBS=$LIBS
  if test x"$LIBS" != "x"; then
     AC_DEFINE([HAVE_EDITLINE],1,Define to use BSD editline)
  else
    unset ac_cv_search_readline
  fi
  LIBS=$sLIBS
fi
if test x"$enable_readline" != xno ; then
  sLIBS=$LIBS
  LIBS=""
  AC_SEARCH_LIBS(tgetent, curses ncurses ncursesw, [], [])
  AC_SEARCH_LIBS(readline, readline, [], [enable_readline=no])
  AC_CHECK_FUNCS(readline, [], [])
  READLINE_LIBS=$LIBS
  LIBS=$sLIBS
fi
AC_SUBST(READLINE_LIBS)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --enable-threadsafe
#
AC_ARG_ENABLE(threadsafe, [AS_HELP_STRING(
  [--enable-threadsafe], [build a thread-safe library [default=yes]])], 
  [], [enable_threadsafe=yes])
THREADSAFE_FLAGS=-DSQLITE_THREADSAFE=0
if test x"$enable_threadsafe" != "xno"; then
  THREADSAFE_FLAGS="-D_REENTRANT=1 -DSQLITE_THREADSAFE=1"
  AC_SEARCH_LIBS(pthread_create, pthread)
fi
AC_SUBST(THREADSAFE_FLAGS)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --enable-dynamic-extensions
#
AC_ARG_ENABLE(dynamic-extensions, [AS_HELP_STRING(
  [--enable-dynamic-extensions], [support loadable extensions [default=yes]])], 
  [], [enable_dynamic_extensions=yes])
if test x"$enable_dynamic_extensions" != "xno"; then
  AC_SEARCH_LIBS(dlopen, dl)
else
  DYNAMIC_EXTENSION_FLAGS=-DSQLITE_OMIT_LOAD_EXTENSION=1
fi
AC_MSG_CHECKING([for whether to support dynamic extensions])
AC_MSG_RESULT($enable_dynamic_extensions)
AC_SUBST(DYNAMIC_EXTENSION_FLAGS)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --enable-fts5
#
AC_ARG_ENABLE(fts5, [AS_HELP_STRING(
  [--enable-fts5], [include fts5 support [default=no]])], 
  [], [enable_fts5=no])
if test x"$enable_fts5" == "xyes"; then
  AC_SEARCH_LIBS(log, m)
  FTS5_FLAGS=-DSQLITE_ENABLE_FTS5
fi
AC_SUBST(FTS5_FLAGS)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --enable-json1
#
AC_ARG_ENABLE(json1, [AS_HELP_STRING(
  [--enable-json1], [include json1 support [default=no]])], 
  [], [enable_json1=no])
if test x"$enable_json1" == "xyes"; then
  JSON1_FLAGS=-DSQLITE_ENABLE_JSON1
fi
AC_SUBST(JSON1_FLAGS)
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
#   --enable-static-shell
#
AC_ARG_ENABLE(static-shell, [AS_HELP_STRING(
  [--enable-static-shell], 
  [statically link libsqlite3 into shell tool [default=yes]])], 
  [], [enable_static_shell=yes])
if test x"$enable_static_shell" == "xyes"; then
  EXTRA_SHELL_OBJ=sqlite3.$OBJEXT
else
  EXTRA_SHELL_OBJ=libsqlite3.la
fi
AC_SUBST(EXTRA_SHELL_OBJ)
#-----------------------------------------------------------------------

AC_CHECK_FUNCS(posix_fallocate)

#-----------------------------------------------------------------------
# UPDATE: Maybe it's better if users just set CFLAGS before invoking
# configure. This option doesn't really add much...
#
#   --enable-tempstore
#
# AC_ARG_ENABLE(tempstore, [AS_HELP_STRING(
#   [--enable-tempstore], 
#   [in-memory temporary tables (never, no, yes, always) [default=no]])], 
#   [], [enable_tempstore=no])
# AC_MSG_CHECKING([for whether or not to store temp tables in-memory])
# case "$enable_tempstore" in
#   never )  TEMP_STORE=0 ;;
#   no )     TEMP_STORE=1 ;;
#   always ) TEMP_STORE=3 ;;
#   yes )    TEMP_STORE=3 ;;
#   * )
#     TEMP_STORE=1
#     enable_tempstore=yes
#   ;;
# esac
# AC_MSG_RESULT($enable_tempstore)
# AC_SUBST(TEMP_STORE)
#-----------------------------------------------------------------------

AC_OUTPUT
