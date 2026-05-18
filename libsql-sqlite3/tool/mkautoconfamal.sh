#!/bin/sh
# This script is used to build the amalgamation autoconf package.
# It assumes the following:
#
#   1. The files "sqlite3.c", "sqlite3.h", "sqlite3ext.h", "shell.c",
#      and "sqlite3rc.h" are available in the current directory.
#
#   2. Variable $TOP is set to the full path of the root directory
#      of the SQLite source tree.
#
#   3. There is nothing of value in the ./mkpkg_tmp_dir directory.
#      This is important, as the script executes "rm -rf ./mkpkg_tmp_dir".
#


# Bail out of the script if any command returns a non-zero exit
# status. Or if the script tries to use an unset variable. These
# may fail for old /bin/sh interpreters.
#
set -e
set -u

TMPSPACE=./mkpkg_tmp_dir
VERSION=`cat $TOP/VERSION`
HASH=`cut -c1-10 $TOP/manifest.uuid`
DATETIME=`grep '^D' $TOP/manifest | tr -c -d '[0-9]' | cut -c1-12`

# If this script is given an argument of --snapshot, then generate a
# snapshot tarball named for the current checkout SHA hash, rather than
# the version number.
#
if test "$#" -ge 1 -a x$1 != x--snapshot
then
  # Set global variable $ARTIFACT to the "3xxyyzz" string incorporated
  # into artifact filenames. And $VERSION2 to the "3.x.y[.z]" form.
  xx=`echo $VERSION|sed 's/3\.\([0-9]*\)\..*/\1/'`
  yy=`echo $VERSION|sed 's/3\.[^.]*\.\([0-9]*\).*/\1/'`
  zz=0
  set +e
    zz=`echo $VERSION|sed 's/3\.[^.]*\.[^.]*\.\([0-9]*\).*/\1/'|grep -v '\.'`
  set -e
  TARBALLNAME=`printf "sqlite-autoconf-3%.2d%.2d%.2d" $xx $yy $zz`
else
  TARBALLNAME=sqlite-snapshot-$DATETIME
fi

rm -rf $TMPSPACE
cp -R $TOP/autoconf       $TMPSPACE
cp -R $TOP/autosetup      $TMPSPACE
cp -p $TOP/configure      $TMPSPACE
cp sqlite3.c              $TMPSPACE
cp sqlite3.h              $TMPSPACE
cp sqlite3ext.h           $TMPSPACE
cp sqlite3rc.h            $TMPSPACE
cp $TOP/sqlite3.1         $TMPSPACE
cp $TOP/sqlite3.pc.in     $TMPSPACE
cp shell.c                $TMPSPACE
cp $TOP/src/sqlite3.rc    $TMPSPACE
cp $TOP/tool/Replace.cs   $TMPSPACE
cp $TOP/VERSION           $TMPSPACE
cp $TOP/main.mk           $TMPSPACE

cd $TMPSPACE

#if true; then
  # Clean up *~ files (emacs-generated backups).
  # This bit is only for use during development of
  # the autoconf bundle.
#  find . -name '*~' -exec rm \{} \;
#fi

mkdir -p tea/generic
cat <<EOF > tea/generic/tclsqlite3.c
#ifdef USE_SYSTEM_SQLITE
# include <sqlite3.h>
#else
# include "sqlite3.c"
#endif
EOF
cat  $TOP/src/tclsqlite.c           >> tea/generic/tclsqlite3.c

# Clean up some local remnants from the tarball.
rm -f tea/.env-* # autosetup environment overrides
find . -type f -name '*~' -exec rm -f \{} \;  # backup files
find . -type f -name '#*#' -exec rm -f \{} \; # emacs lock files
find . -type f -name '*.o' -exec rm -f \{} \;
find . -type f -name '*.so' -exec rm -f \{} \;

./configure && ${MAKE-make} dist
tar xzf sqlite-$VERSION.tar.gz
mv sqlite-$VERSION $TARBALLNAME
tar czf $TARBALLNAME.tar.gz $TARBALLNAME
mv $TARBALLNAME.tar.gz ..
cd ..
ls -l $TARBALLNAME.tar.gz
