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
cat >sqlite3.def <<\END_OF_FILE
EXPORTS
sqlite3_aggregate_context
sqlite3_aggregate_count
sqlite3_bind_blob
sqlite3_bind_double
sqlite3_bind_int
sqlite3_bind_int64
sqlite3_bind_null
sqlite3_bind_text
sqlite3_bind_text16
sqlite3_busy_handler
sqlite3_busy_timeout
sqlite3_close
sqlite3_column_blob
sqlite3_column_bytes
sqlite3_column_bytes16
sqlite3_column_count
sqlite3_column_decltype
sqlite3_column_decltype16
sqlite3_column_double
sqlite3_column_int
sqlite3_column_int64
sqlite3_column_name
sqlite3_column_name16
sqlite3_column_text
sqlite3_column_text16
sqlite3_column_type
sqlite3_complete
sqlite3_complete16
sqlite3_create_function
sqlite3_create_function16
sqlite3_errcode
sqlite3_errmsg
sqlite3_errmsg16
sqlite3_exec
sqlite3_finalize
sqlite3_free
sqlite3_interrupt
sqlite3_last_insert_rowid
sqlite3_mprintf
sqlite3_open
sqlite3_open16
sqlite3_prepare
sqlite3_prepare16
sqlite3_reset
sqlite3_result_blob
sqlite3_result_double
sqlite3_result_error
sqlite3_result_error16
sqlite3_result_int
sqlite3_result_int64
sqlite3_result_null
sqlite3_result_text
sqlite3_result_text16
sqlite3_result_value
sqlite3_set_authorizer
sqlite3_step
sqlite3_user_data
sqlite3_value_blob
sqlite3_value_bytes
sqlite3_value_bytes16
sqlite3_value_double
sqlite3_value_int
sqlite3_value_int64
sqlite3_value_text
sqlite3_value_text16
sqlite3_value_type
sqlite3_vmprintf
END_OF_FILE
i386-mingw32msvc-dllwrap \
     --def sqlite3.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname sqlite3.dll -lmsvcrt *.o
i386-mingw32msvc-strip sqlite3.dll
zip ../doc/tclsqlite-$VERSW.zip tclsqlite3.dll
zip ../doc/sqlitedll-$VERSW.zip sqlite3.dll sqlite3.def
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
EXCLUDE=`find sqlite -print | grep CVS | sed 's,sqlite/, --exclude sqlite/,'`
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
