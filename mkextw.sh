#!/bin/sh
#
# This script is used to compile SQLite extensions into DLLs.
#
make fts2amal.c
PATH=$PATH:/opt/mingw/bin
OPTS='-DTHREADSAFE=1 -DBUILD_sqlite=1 -DSQLITE_OS_WIN=1'
CC="i386-mingw32msvc-gcc -O2 $OPTS -Itsrc"
NM="i386-mingw32msvc-nm"
CMD="$CC -c fts2amal.c"
echo $CMD
$CMD
echo 'EXPORTS' >fts2.def
echo 'sqlite3_extension_init' >>fts2.def
i386-mingw32msvc-dllwrap \
     --def fts2.def -v --export-all \
     --driver-name i386-mingw32msvc-gcc \
     --dlltool-name i386-mingw32msvc-dlltool \
     --as i386-mingw32msvc-as \
     --target i386-mingw32 \
     -dllname fts2.dll -lmsvcrt fts2amal.o
zip fts2dll.zip fts2.dll fts2.def
