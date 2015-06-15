#/bin/sh
#
# Run this script in a directory with a working makefile to check for 
# compiler warnings in SQLite.
#
rm -f sqlite3.c
make sqlite3.c
echo '********** No optimizations.  Includes FTS4 and RTREE *********'
gcc -c -Wshadow -Wall -Wextra -pedantic-errors -Wno-long-long -std=c89 \
      -ansi -DHAVE_STDINT_H -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_RTREE \
      sqlite3.c
echo '********** Android configuration ******************************'
gcc -c \
  -DHAVE_USLEEP=1 \
  -DSQLITE_HAVE_ISNAN \
  -DSQLITE_DEFAULT_JOURNAL_SIZE_LIMIT=1048576 \
  -DSQLITE_THREADSAFE=2 \
  -DSQLITE_TEMP_STORE=3 \
  -DSQLITE_POWERSAFE_OVERWRITE=1 \
  -DSQLITE_DEFAULT_FILE_FORMAT=4 \
  -DSQLITE_DEFAULT_AUTOVACUUM=1 \
  -DSQLITE_ENABLE_MEMORY_MANAGEMENT=1 \
  -DSQLITE_ENABLE_FTS3 \
  -DSQLITE_ENABLE_FTS3_BACKWARDS \
  -DSQLITE_ENABLE_FTS4 \
  -DSQLITE_OMIT_BUILTIN_TEST \
  -DSQLITE_OMIT_COMPILEOPTION_DIAGS \
  -DSQLITE_OMIT_LOAD_EXTENSION \
  -DSQLITE_DEFAULT_FILE_PERMISSIONS=0600 \
  -DSQLITE_ENABLE_ICU \
  -DUSE_PREAD64 \
  -Wshadow -Wall -Wextra \
  -Os sqlite3.c shell.c
echo '********** No optimizations. ENABLE_STAT4. THREADSAFE=0 *******'
gcc -c -Wshadow -Wall -Wextra -pedantic-errors -Wno-long-long -std=c89 \
      -ansi -DSQLITE_ENABLE_STAT4 -DSQLITE_THREADSAFE=0 \
      sqlite3.c
echo '********** Optimized -O3.  Includes FTS4 and RTREE ************'
gcc -O3 -c -Wshadow -Wall -Wextra -pedantic-errors -Wno-long-long -std=c89 \
      -ansi -DHAVE_STDINT_H -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_RTREE \
      sqlite3.c
