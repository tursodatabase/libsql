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

# Clear out the build directory "doc"
#
#rm -rf doc
#make doc

# Get the current version number - needed to help build filenames
#
VERS=`cat $srcdir/VERSION`
VERSW=`sed 's/\./_/g' $srcdir/VERSION`

# Start by building an sqlite shell for linux.
#
make clean
make sqlite
strip sqlite
mv sqlite sqlite-$VERS.bin
rm -f sqlite.bin.gz
gzip sqlite-$VERS.bin
mv sqlite-$VERS.bin.gz doc

# Build the tclsqlite.so shared library for import into tclsh or wish
# under Linux
#
make target_source
rm -f doc/sqlite-source-$VERSW.zip
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
#mv tclsqlite.dll ../tclsqlite-$VERSW.dll
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
sqlite_reset
sqlite_bind
sqlite_last_statement_changes
sqlite_encode_binary
sqlite_decode_binary
END_OF_FILE
i386-mingw32msvc-dllwrap \
     --def sqlite.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname sqlite.dll -lmsvcrt *.o
i386-mingw32msvc-strip sqlite.dll
zip ../doc/tclsqlite-$VERSW.zip tclsqlite.dll
zip ../doc/sqlitedll-$VERSW.zip sqlite.dll sqlite.def
cd ..

# Build the sqlite.exe executable for windows.
#
make target_source
cd tsrc
rm tclsqlite.c
OPTS='-DSTATIC_BUILD=1 -DNDEBUG=1'
i386-mingw32msvc-gcc -O2 $OPTS -I. -I$TCLDIR *.c -o sqlite.exe
zip ../doc/sqlite-$VERSW.zip sqlite.exe
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
mv $HOME/rpm/RPMS/i386/sqlite*-$VERS*.rpm doc
mv $HOME/rpm/SRPMS/sqlite-$VERS*.rpm doc
