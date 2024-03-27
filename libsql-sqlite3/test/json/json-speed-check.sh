#!/bin/bash
#
# This is a template for a script used for day-to-day size and 
# performance monitoring of SQLite.  Typical usage:
#
#     sh speed-check.sh trunk  #  Baseline measurement of trunk
#     sh speed-check.sh x1     # Measure some experimental change
#     fossil xdiff --tk jout-trunk.txt jout-x1.txt   # View chanages
#
# There are multiple output files, all with a base name given by
# the first argument:
#
#     summary-$BASE.txt           # Copy of standard output
#     jout-$BASE.txt              # cachegrind output
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
LEAN_OPTS="-DSQLITE_THREADSAFE=0"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_DEFAULT_MEMSTATUS=0"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_DEFAULT_WAL_SYNCHRONOUS=1"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_LIKE_DOESNT_MATCH_BLOBS"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_MAX_EXPR_DEPTH=0"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_OMIT_DECLTYPE"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_OMIT_DEPRECATED"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_OMIT_PROGRESS_CALLBACK"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_OMIT_SHARED_CACHE"
LEAN_OPTS="$LEAN_OPTS -DSQLITE_USE_ALLOCA"
BASELINE="trunk"
TYPE="json"
doExplain=0
doCachegrind=1
doVdbeProfile=0
doWal=1
doDiff=1
doJsonB=0
while test "$1" != ""; do
  case $1 in
    --nodiff)
	doDiff=0
        ;;
    --lean)
        CC_OPTS="$CC_OPTS $LEAN_OPTS"
        ;;
    --clang)
        CC=clang
        ;;
    --gcc7)
        CC=gcc-7
        ;;
    --jsonb)
        doJsonB=1
        TYPE="jsonb"
        ;;
    -*)
        CC_OPTS="$CC_OPTS $1"
        ;;
    *)
	BASELINE=$1
        ;;
  esac
  shift
done
echo "NAME           = $NAME" | tee summary-$NAME.txt
echo "CC_OPTS        = $CC_OPTS" | tee -a summary-$NAME.txt
rm -f cachegrind.out.* jsonshell
$CC -g -Os -Wall -I. $CC_OPTS ./shell.c ./sqlite3.c -o jsonshell -ldl -lpthread
ls -l jsonshell | tee -a summary-$NAME.txt
home=`echo $0 | sed -e 's,/[^/]*$,,'`
DB=$TYPE''100mb.db
echo ./jsonshell $DB "<$home/$TYPE-q1.txt"
valgrind --tool=cachegrind ./jsonshell json100mb_b.db <$home/$TYPE-q1.txt \
        2>&1 | tee -a summary-$NAME.txt
cg_anno.tcl cachegrind.out.* >$TYPE-$NAME.txt
echo '*****************************************************' >>$TYPE-$NAME.txt
sed 's/^[0-9=-]\{9\}/==00000==/' summary-$NAME.txt >>$TYPE-$NAME.txt
if test "$NAME" != "$BASELINE" -a $doDiff -ne 0; then
  fossil xdiff --tk -c 20 $TYPE-$BASELINE.txt $TYPE-$NAME.txt
fi
