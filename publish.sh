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
cp $srcdir/Makefile.linux-gcc ./Makefile
cp $srcdir/sqlite3.def ./sqlite3.def
chmod +x $srcdir/install-sh

# Get the current version number - needed to help build filenames
#
VERS=`cat $srcdir/VERSION`
VERSW=`sed 's/\./_/g' $srcdir/VERSION`

# Start by building an sqlite shell for linux.
#
make clean
make sqlite3
strip sqlite3
mv sqlite3 sqlite3-$VERS.bin
gzip sqlite3-$VERS.bin
mv sqlite3-$VERS.bin.gz doc

# Build the tclsqlite.so shared library for import into tclsh or wish
# under Linux
#
make target_source
cd tsrc
zip ../doc/sqlite-source-$VERSW.zip *
rm shell.c
TCLDIR=/home/drh/tcltk/8.2linux
TCLSTUBLIB=$TCLDIR/libtclstub8.2g.a
OPTS='-DUSE_TCL_STUBS=1 -DNDEBUG=1'
gcc -fPIC $OPTS -O2 -I. -I$TCLDIR -shared *.c $TCLSTUBLIB -o tclsqlite.so
strip tclsqlite.so
mv tclsqlite.so tclsqlite-$VERS.so
gzip tclsqlite-$VERS.so
mv tclsqlite-$VERS.so.gz ../doc
rm tclsqlite.c
gcc -fPIC -DNDEBUG=1 -O2 -I. -shared *.c -o sqlite.so
strip sqlite.so
mv sqlite.so sqlite-$VERS.so
gzip sqlite-$VERS.so
mv sqlite-$VERS.so.gz ../doc
cd ..

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
CC="i386-mingw32msvc-gcc -O2 $OPTS -I. -I$TCLDIR"
rm shell.c
for i in *.c; do
  CMD="$CC -c $i"
  echo $CMD
  $CMD
done
echo 'EXPORTS' >tclsqlite3.def
echo 'Tclsqlite3_Init' >>tclsqlite3.def
echo 'Sqlite3_Init' >>tclsqlite3.def
i386-mingw32msvc-dllwrap \
     --def tclsqlite3.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname tclsqlite3.dll -lmsvcrt *.o $TCLSTUBLIB
i386-mingw32msvc-strip tclsqlite3.dll
rm tclsqlite.o
cp ../sqlite3.def .
i386-mingw32msvc-dllwrap \
     --def sqlite3.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname sqlite3.dll -lmsvcrt *.o
i386-mingw32msvc-strip sqlite3.dll
zip ../doc/tclsqlite-$VERSW.zip tclsqlite3.dll
echo zip ../doc/tclsqlite-$VERSW.zip tclsqlite3.dll
zip ../doc/sqlitedll-$VERSW.zip sqlite3.dll sqlite3.def
echo zip ../doc/sqlitedll-$VERSW.zip sqlite3.dll sqlite3.def
cd ..

# Build the sqlite.exe executable for windows.
#
make target_source
cd tsrc
rm tclsqlite.c
OPTS='-DSTATIC_BUILD=1 -DNDEBUG=1'
i386-mingw32msvc-gcc -O2 $OPTS -I. -I$TCLDIR *.c -o sqlite3.exe
zip ../doc/sqlite-$VERSW.zip sqlite3.exe
cd ..

# Construct a tarball of the source tree
#
ORIGIN=`pwd`
cd $srcdir
cd ..
EXCLUDE=`find sqlite -print | grep CVS | sed 's,^, --exclude ,'`
tar czf $ORIGIN/doc/sqlite-$VERS.tar.gz $EXCLUDE sqlite
cd $ORIGIN

#
# Build RPMS (binary) and Source RPM
#

# Make sure we are properly setup to build RPMs
#
echo "%HOME %{expand:%%(cd; pwd)}" > $HOME/.rpmmacros
echo "%_topdir %{HOME}/rpm" >> $HOME/.rpmmacros
mkdir $HOME/rpm
mkdir $HOME/rpm/BUILD
mkdir $HOME/rpm/SOURCES
mkdir $HOME/rpm/RPMS
mkdir $HOME/rpm/SRPMS
mkdir $HOME/rpm/SPECS

# create the spec file from the template
sed s/SQLITE_VERSION/$VERS/g $srcdir/spec.template > $HOME/rpm/SPECS/sqlite.spec

# copy the source tarball to the rpm directory
cp doc/sqlite-$VERS.tar.gz $HOME/rpm/SOURCES/.

# build all the rpms
rpm -ba $HOME/rpm/SPECS/sqlite.spec >& rpm-$vers.log

# copy the RPMs into the build directory.
mv $HOME/rpm/RPMS/i386/sqlite*-$vers*.rpm doc
mv $HOME/rpm/SRPMS/sqlite-$vers*.rpm doc

# Build the website
#
cp $srcdir/../historical/* doc
make doc
