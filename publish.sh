#!/bin/sh
#
# This script is used to compile SQLite and all its documentation and
# ship everything up to the SQLite website.  This script will only work
# on the system "zadok" at the Hwaci offices.  But others might find
# the script useful as an example.
#

# Set srcdir to the name of the directory that contains the publish.sh
# script.
#
srcdir=`echo "$0" | sed 's%\(^.*\)/[^/][^/]*$%\1%'`

# Get the makefile.
#
cp $srcdir/Makefile.template ./Makefile

# Start building stuff.
#
make clean
make sqlite
strip sqlite
mv sqlite sqlite.bin
rm -f sqlite.bin.gz
gzip sqlite.bin

# Build the tclsqlite.so shared library for import into tclsh or wish
# under Linux
#
make target_source
cd tsrc
rm shell.c
TCLDIR=/home/drh/tcltk/8.2linux
TCLSTUBLIB=$TCLDIR/libtclstub8.2g.a
OPTS='-DUSE_TCL_STUBS=1 -DNDEBUG=1'
gcc -fPIC $OPTS -O2 -I. -I$TCLDIR -shared *.c $TCLSTUBLIB -o tclsqlite.so
strip tclsqlite.so
mv tclsqlite.so ..
cd ..
rm -f tclsqlite.so.gz
gzip tclsqlite.so

# Build the tclsqlite.dll shared library that can be imported into tclsh
# or wish on windows.
#
make target_source
cd tsrc
rm shell.c
TCLDIR=/home/drh/tcltk/8.2win
TCLSTUBLIB=$TCLDIR/tclstub82.a
PATH=$PATH:/opt/mingw/bin
OPTS='-DUSE_TCL_STUBS=1 -DNDEBUG=1 -DTHREADSAFE=1'
CC="i386-mingw32-gcc -O2 $OPTS -I. -I$TCLDIR"
rm shell.c
for i in *.c; do
  CMD="$CC -c $i"
  echo $CMD
  $CMD
done
echo 'EXPORTS' >tclsqlite.def
echo 'Tclsqlite_Init' >>tclsqlite.def
echo 'Sqlite_Init' >>tclsqlite.def
i386-mingw32-dllwrap \
     --def tclsqlite.def -v --export-all \
     --driver-name i386-mingw32-gcc \
     --dlltool-name i386-mingw32-dlltool \
     --as i386-mingw32-as \
     --target i386-mingw32 \
     -dllname tclsqlite.dll -lmsvcrt *.o $TCLSTUBLIB
i386-mingw32-strip tclsqlite.dll
mv tclsqlite.dll ..
cd ..
rm -f tclsqlite.zip
zip tclsqlite.zip tclsqlite.dll

# Build the sqlite.exe executable for windows.
#
make target_source
cd tsrc
rm tclsqlite.c
OPTS='-DSTATIC_BUILD=1 -DNDEBUG=1'
i386-mingw32-gcc -O2 $OPTS -I. -I$TCLDIR *.c -o sqlite.exe
mv sqlite.exe ..
cd ..
rm -f sqlite.zip
zip sqlite.zip sqlite.exe

# Construct a tarball of the source tree
#
ORIGIN=`pwd`
cd $srcdir
cd ..
EXCLUDE=`find sqlite -print | grep CVS | sed 's,sqlite/, --exclude sqlite/,'`
tar czf $ORIGIN/sqlite.tar.gz $EXCLUDE sqlite
cd $ORIGIN
vers=`cat $srcdir/VERSION`
rm -f sqlite-$vers.tar.gz
ln sqlite.tar.gz sqlite-$vers.tar.gz

# Build the website
#
cp $srcdir/../historical/* .
rm -rf doc
make doc
ln sqlite.bin.gz sqlite.zip sqlite*.tar.gz tclsqlite.so.gz tclsqlite.zip doc
