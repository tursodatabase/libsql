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
sqlite_open
sqlite_close
sqlite_exec
sqlite_last_insert_rowid
sqlite_error_string
sqlite_interrupt
sqlite_complete
sqlite_busy_handler
sqlite_busy_timeout
sqlite_get_table
sqlite_free_table
sqlite_mprintf
sqlite_vmprintf
sqlite_exec_printf
sqlite_exec_vprintf
sqlite_get_table_printf
sqlite_get_table_vprintf
sqlite_freemem
sqlite_libversion
sqlite_libencoding
sqlite_changes
sqlite_create_function
sqlite_create_aggregate
sqlite_function_type
sqlite_user_data
sqlite_aggregate_context
sqlite_aggregate_count
sqlite_set_result_string
sqlite_set_result_int
sqlite_set_result_double
sqlite_set_result_error
sqliteMalloc
sqliteFree
sqliteRealloc
sqlite_set_authorizer
sqlite_trace
sqlite_compile
sqlite_step
sqlite_finalize
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
