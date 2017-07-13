#!/bin/bash
#
# This is a template for a script used for day-to-day size and 
# performance monitoring of SQLite.  Typical usage:
#
#     sh run-speed-test.sh trunk  #  Baseline measurement of trunk
#     sh run-speed-test.sh x1     # Measure some experimental change
#     fossil test-diff --tk cout-trunk.txt cout-x1.txt   # View chanages
#
# There are multiple output files, all with a base name given by
# the first argument:
#
#     summary-$BASE.txt           # Copy of standard output
#     cout-$BASE.txt              # cachegrind output
#     explain-$BASE.txt           # EXPLAIN listings (only with --explain)
#
if test "$1" = ""
then
  echo "Usage: $0 OUTPUTFILE [OPTIONS]"
  exit
fi
NAME=$1
shift
#CC_OPTS="-DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_MEMSYS5"
CC_OPTS="-DSQLITE_ENABLE_MEMSYS5"
CC=gcc
SPEEDTEST_OPTS="--shrink-memory --reprepare --stats --heap 10000000 64"
SIZE=5
LEAN_OPTS="-DSQLITE_THREADSAFE=0"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_DEFAULT_MEMSTATUS=0"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_LIKE_DOESNT_MATCH_BLOB"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_MAX_EXPR_DEPTH=0"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_OMIT_DECLTYPE"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_OMIT_DEPRECATED"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_OMIT_PROGRESS_CALLBACK"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_OMIT_SHARED_CACHE"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_USE_ALLOCA"
doExplain=0
doCachegrind=1
while test "$1" != ""; do
  case $1 in
    --reprepare)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS $1"
        ;;
    --autovacuum)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS $1"
        ;;
    --utf16be)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS $1"
        ;;
    --stats)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS $1"
        ;;
    --without-rowid)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS $1"
        ;;
    --nomemstat)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS $1"
        ;;
    --temp)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS --temp 6"
        ;;
    --wal)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS --journal wal"
        ;;
    --size)
        shift; SIZE=$1
        ;;
    --cachesize)
        shift; SPEEDTEST_OPTS="$SPEEDTEST_OPTS --cachesize $1"
        ;;
    --explain)
        doExplain=1
        ;;
    --vdbeprofile)
        rm -f vdbe_profile.out
        CC_OPTS="$CC_OPTS -DVDBE_PROFILE"
        doCachegrind=0
        ;;
    --lean)
        CC_OPTS="$CC_OPTS $LEAN_OPTS"
        ;;
    --clang)
        CC=clang
        ;;
    --icc)
        CC=/home/drh/intel/bin/icc
        ;;
    --gcc7)
        CC=gcc-7
        ;;
    --heap)
        CC_OPTS="$CC_OPTS -DSQLITE_ENABLE_MEMSYS5"
        shift;
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS --heap $1 64"
        ;;
    --lookaside)
        shift;
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS --lookaside $1 $2"
        shift;
        ;;
    --repeat)
        CC_OPTS="$CC_OPTS -DSQLITE_ENABLE_RCACHE"
        shift;
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS --repeat $1"
        ;;
    --mmap)
        shift;
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS --mmap $1"
        ;;
    --rtree)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS --testset rtree"
        CC_OPTS="$CC_OPTS -DSQLITE_ENABLE_RTREE"
        ;;
    --orm)
        SPEEDTEST_OPTS="$SPEEDTEST_OPTS --testset orm"
        ;;
    *)
        CC_OPTS="$CC_OPTS $1"
        ;;
  esac
  shift
done
SPEEDTEST_OPTS="$SPEEDTEST_OPTS --size $SIZE"
echo "NAME           = $NAME" | tee summary-$NAME.txt
echo "SPEEDTEST_OPTS = $SPEEDTEST_OPTS" | tee -a summary-$NAME.txt
echo "CC_OPTS        = $CC_OPTS" | tee -a summary-$NAME.txt
rm -f cachegrind.out.* speedtest1 speedtest1.db sqlite3.o
$CC -g -Os -Wall -I. $CC_OPTS -c sqlite3.c
size sqlite3.o | tee -a summary-$NAME.txt
if test $doExplain -eq 1; then
  $CC -g -Os -Wall -I. $CC_OPTS \
     -DSQLITE_ENABLE_EXPLAIN_COMMENTS \
    ./shell.c ./sqlite3.c -o sqlite3 -ldl -lpthread
fi
SRC=./speedtest1.c
$CC -g -Os -Wall -I. $CC_OPTS $SRC ./sqlite3.o -o speedtest1 -ldl -lpthread
ls -l speedtest1 | tee -a summary-$NAME.txt
if test $doCachegrind -eq 1; then
  valgrind --tool=cachegrind ./speedtest1 speedtest1.db \
      $SPEEDTEST_OPTS 2>&1 | tee -a summary-$NAME.txt
else
  ./speedtest1 speedtest1.db $SPEEDTEST_OPTS 2>&1 | tee -a summary-$NAME.txt
fi
size sqlite3.o | tee -a summary-$NAME.txt
wc sqlite3.c
if test $doCachegrind -eq 1; then
  cg_anno.tcl cachegrind.out.* >cout-$NAME.txt
fi
if test $doExplain -eq 1; then
  ./speedtest1 --explain $SPEEDTEST_OPTS | ./sqlite3 >explain-$NAME.txt
fi
if test "$NAME" != "trunk"; then
  fossil test-diff --tk -c 20 cout-trunk.txt cout-$NAME.txt
fi
