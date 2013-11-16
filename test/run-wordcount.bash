#!/bin/bash
#
# This script runs the wordcount program in different ways, comparing
# the output from each.
#


# Run the wordcount command with argument supplied and with --summary.
# Store the results in wc-out.txt and report the run-time.
#
function time_wordcount {
  /usr/bin/time --format='%e %C' ./wordcount --summary $* >wc-out.txt
}

# Compare wc-out.txt against wc-baseline.txt and report any differences.
#
function compare_results {
  if cmp -s wc-out.txt wc-baseline.txt;
  then echo hi >/dev/null;
  else echo ERROR:;
       diff -u wc-baseline.txt wc-out.txt;
  fi
}

# Select the source text to be analyzed.
#
if test "x$1" = "x";
then echo "Usage: $0 FILENAME [ARGS...]"; exit 1;
fi

# Do test runs
#
rm -f wcdb1.db
time_wordcount wcdb1.db $* --insert
mv wc-out.txt wc-baseline.txt
rm -f wcdb2.db
time_wordcount wcdb2.db $* --insert --without-rowid
compare_results

rm -f wcdb1.db
time_wordcount wcdb1.db $* --replace
compare_results
rm -f wcdb2.db
time_wordcount wcdb2.db $* --replace --without-rowid
compare_results

rm -f wcdb1.db
time_wordcount wcdb1.db $* --select
compare_results
rm -f wcdb2.db
time_wordcount wcdb2.db $* --select --without-rowid
compare_results

time_wordcount wcdb1.db $* --query
mv wc-out.txt wc-baseline.txt
time_wordcount wcdb2.db $* --query --without-rowid
compare_results

time_wordcount wcdb1.db $* --delete
mv wc-out.txt wc-baseline.txt
time_wordcount wcdb2.db $* --delete --without-rowid
compare_results

# Clean up temporary files created.
#
rm -rf wcdb1.db wcdb2.db wc-out.txt wc-baseline.txt
