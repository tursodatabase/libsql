#!/bin/sh
#
# This script is used to compile SQLite and all its documentation and
# ship everything up to the SQLite website.  This script will only work
# on the system "zadok" at the Hwaci offices.  But others might find
# the script useful as an example.
#

# Build the tclsqlite.dll shared library that can be imported into tclsh
# or wish on windows.
#
make target_source
cd tsrc
PATH=$PATH:/opt/mingw/bin
OPTS='-DUSE_TCL_STUBS=1 -DNDEBUG=1 -DTHREADSAFE=1'
CC="i386-mingw32msvc-gcc -O2 $OPTS -I."
rm shell.c
rm tclsqlite.c
for i in *.c; do
  CMD="$CC -c $i"
  echo $CMD
  $CMD
done
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
mv sqlite3.dll sqlite3.def ..
cd ..
rm -f sqlite3dll.zip
zip sqlite3dll.zip sqlite3.dll sqlite3.def
