#!/bin/sh
# This script is used to build the amalgamation autoconf package.
# It assumes the following:
#
#   1. The files "sqlite3.c", "sqlite3.h" and "sqlite3ext.h"
#      are available in the current directory.
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

# Set global variable $ARTIFACT to the "3xxyyzz" string incorporated 
# into artifact filenames. And $VERSION2 to the "3.x.y[.z]" form.
xx=`echo $VERSION|sed 's/3\.\([0-9]*\)\..*/\1/'`
yy=`echo $VERSION|sed 's/3\.[^.]*\.\([0-9]*\).*/\1/'`
zz=0
set +e
  zz=`echo $VERSION|sed 's/3\.[^.]*\.[^.]*\.\([0-9]*\).*/\1/'|grep -v '\.'`
set -e
ARTIFACT=`printf "3%.2d%.2d%.2d" $xx $yy $zz`

rm -rf $TMPSPACE
cp -R $TOP/autoconf $TMPSPACE

cp sqlite3.c          $TMPSPACE
cp sqlite3.h          $TMPSPACE
cp sqlite3ext.h       $TMPSPACE
cp $TOP/sqlite3.1     $TMPSPACE
cp $TOP/sqlite3.pc.in $TMPSPACE
cp $TOP/src/shell.c   $TMPSPACE

chmod 755 $TMPSPACE/install-sh
chmod 755 $TMPSPACE/missing
chmod 755 $TMPSPACE/depcomp
chmod 755 $TMPSPACE/config.sub
chmod 755 $TMPSPACE/config.guess

cat $TMPSPACE/configure.ac |
sed "s/AC_INIT(sqlite, .*, http:\/\/www.sqlite.org)/AC_INIT(sqlite, $VERSION, http:\/\/www.sqlite.org)/" > $TMPSPACE/tmp
mv $TMPSPACE/tmp $TMPSPACE/configure.ac

cd $TMPSPACE
aclocal
autoconf
automake

mkdir -p tea/generic
echo "#ifdef USE_SYSTEM_SQLITE"      > tea/generic/tclsqlite3.c 
echo "# include <sqlite3.h>"        >> tea/generic/tclsqlite3.c
echo "#else"                        >> tea/generic/tclsqlite3.c
echo "#include \"sqlite3.c\""       >> tea/generic/tclsqlite3.c
echo "#endif"                       >> tea/generic/tclsqlite3.c
cat  $TOP/src/tclsqlite.c           >> tea/generic/tclsqlite3.c

cat tea/configure.ac | 
  sed "s/AC_INIT(\[sqlite\], .*)/AC_INIT([sqlite], [$VERSION])/" > tmp
mv tmp tea/configure.ac

cd tea
autoconf
rm -rf autom4te.cache

cd ../
./configure && make dist
tar -xzf sqlite-$VERSION.tar.gz
mv sqlite-$VERSION sqlite-autoconf-$ARTIFACT
tar -czf sqlite-autoconf-$ARTIFACT.tar.gz sqlite-autoconf-$ARTIFACT
mv sqlite-autoconf-$ARTIFACT.tar.gz ..
