#!/bin/sh
#
# This script is used to compile SQLite and package everything up
# so that it is ready to move to the SQLite website.
#

# Set srcdir to the name of the directory that contains the publish.sh
# script.
#
srcdir=`echo "$0" | sed 's%\(^.*\)/[^/][^/]*$%\1%'`

# Get the makefile.
#
cp $srcdir/Makefile.linux-gcc ./Makefile
chmod +x $srcdir/install-sh

# Get the current version number - needed to help build filenames
#
VERS=`cat $srcdir/VERSION`
VERSW=`sed 's/\./_/g' $srcdir/VERSION`
echo "VERSIONS: $VERS $VERSW"

# Start by building an sqlite shell for linux.
#
make clean
make sqlite3.c
CFLAGS="-Os -DSQLITE_ENABLE_FTS3=1 -DSQLITE_THREADSAFE=0"
NAME=sqlite3-$VERS-osx-x86.bin
echo '***** '"COMPILING $NAME..."
gcc $CFLAGS -Itsrc sqlite3.c tsrc/shell.c -o $NAME -ldl
strip $NAME
chmod 644 $NAME
gzip $NAME
mkdir -p doc
mv $NAME.gz doc
