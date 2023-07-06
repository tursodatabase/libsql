#!/bin/bash
# Expects $1 to be a (speedtest1 --script) output file. Output is a
# series of SQL files extracted from that file.
infile=${1:?arg = speedtest1 --script output file}
testnums=$(grep -e '^-- begin test' "$infile" | cut -d' ' -f4)
if [ x = "x${testnums}" ]; then
  echo "Could not parse any begin/end blocks out of $infile" 1>&2
  exit 1
fi
odir=${infile%%/*}
if [ "$odir" = "$infile" ]; then odir="."; fi
#echo testnums=$testnums
for n in $testnums; do
  ofile=$odir/$(printf "speedtest1-%03d.sql" $n)
  sed -n -e "/^-- begin test $n /,/^-- end test $n\$/p" $infile > $ofile
  echo -e "$n\t$ofile"
done
