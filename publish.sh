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
rm sqlite_source.zip
cd tsrc
zip ../sqlite_source.zip *
rm shell.c
TCLDIR=/home/drh/tcltk/8.2linux
TCLSTUBLIB=$TCLDIR/libtclstub8.2g.a
OPTS='-DUSE_TCL_STUBS=1 -DNDEBUG=1'
gcc -fPIC $OPTS -O2 -I. -I$TCLDIR -shared *.c $TCLSTUBLIB -o tclsqlite.so
strip tclsqlite.so
mv tclsqlite.so ..
rm tclsqlite.c
gcc -fPIC -DNDEBUG=1 -O2 -I. -shared *.c -o sqlite.so
strip sqlite.so
mv sqlite.so ..
cd ..
rm -f tclsqlite.so.gz sqlite.so.gz
gzip tclsqlite.so
gzip sqlite.so

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
echo 'EXPORTS' >tclsqlite.def
echo 'Tclsqlite_Init' >>tclsqlite.def
echo 'Sqlite_Init' >>tclsqlite.def
i386-mingw32msvc-dllwrap \
     --def tclsqlite.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname tclsqlite.dll -lmsvcrt *.o $TCLSTUBLIB
i386-mingw32msvc-strip tclsqlite.dll
mv tclsqlite.dll ..
rm tclsqlite.o
cat >sqlite.def <<\END_OF_FILE
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
     --def sqlite.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname sqlite.dll -lmsvcrt *.o
i386-mingw32msvc-strip sqlite.dll
mv sqlite.dll sqlite.def ..
cd ..
rm -f tclsqlite.zip sqlitedll.zip
zip tclsqlite.zip tclsqlite.dll
zip sqlitedll.zip sqlite.dll sqlite.def

# Build the sqlite.exe executable for windows.
#
make target_source
cd tsrc
rm tclsqlite.c
OPTS='-DSTATIC_BUILD=1 -DNDEBUG=1'
i386-mingw32msvc-gcc -O2 $OPTS -I. -I$TCLDIR *.c -o sqlite.exe
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
sed s/SQLITE_VERSION/$vers/g $srcdir/spec.template > $HOME/rpm/SPECS/sqlite.spec

# copy the source tarball to the rpm directory
cp sqlite-$vers.tar.gz $HOME/rpm/SOURCES/.

# build all the rpms
rpm -ba $HOME/rpm/SPECS/sqlite.spec >& rpm-$vers.log

# copy the RPMs into the build directory.
ln $HOME/rpm/RPMS/i386/sqlite*-$vers*.rpm .
ln $HOME/rpm/SRPMS/sqlite-$vers*.rpm .


# Build the website
#
cp $srcdir/../historical/* .
rm -rf doc
make doc
ln sqlite.bin.gz sqlite.zip sqlite*.tar.gz tclsqlite.so.gz tclsqlite.zip doc
ln sqlitedll.zip sqlite.so.gz sqlite_source.zip doc
ln *.rpm doc
