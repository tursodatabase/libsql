#/bin/sh
#
# Run this script in a directory with a working makefile to check for 
# compiler warnings in SQLite.
#
rm -f sqlite3.c
make sqlite3.c-debug
echo '********** No optimizations.  Includes FTS4 and RTREE *********'
gcc -c -Wshadow -Wall -Wextra -pedantic-errors -Wno-long-long -std=c89 \
      -ansi -DHAVE_STDINT_H -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_RTREE \
      sqlite3.c
echo '********** No optimizations. ENABLE_STAT4. THREADSAFE=0 *******'
gcc -c -Wshadow -Wall -Wextra -pedantic-errors -Wno-long-long -std=c89 \
      -ansi -DSQLITE_ENABLE_STAT4 -DSQLITE_THREADSAFE=0 \
      sqlite3.c
echo '********** Optimized -O3.  Includes FTS4 and RTREE ************'
gcc -O3 -c -Wshadow -Wall -Wextra -pedantic-errors -Wno-long-long -std=c89 \
      -ansi -DHAVE_STDINT_H -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_RTREE \
      sqlite3.c
