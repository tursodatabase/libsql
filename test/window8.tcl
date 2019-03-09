# 2018 May 19
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
#

source [file join [file dirname $argv0] pg_common.tcl]

#=========================================================================

start_test window8 "2019 March 01"
ifcapable !windowfunc

execsql_test 1.0 {
  DROP TABLE IF EXISTS t3;
  CREATE TABLE t3(a TEXT, b TEXT, c INTEGER);
  INSERT INTO t3 VALUES
    ('HH', 'bb', 355), ('CC', 'aa', 158), ('BB', 'aa', 399), 
    ('FF', 'bb', 938), ('HH', 'aa', 480), ('FF', 'bb', 870), 
    ('JJ', 'aa', 768), ('JJ', 'aa', 899), ('GG', 'bb', 929), 
    ('II', 'bb', 421), ('GG', 'bb', 844), ('FF', 'bb', 574), 
    ('CC', 'bb', 822), ('GG', 'bb', 938), ('BB', 'aa', 660), 
    ('HH', 'aa', 979), ('BB', 'bb', 792), ('DD', 'aa', 845), 
    ('JJ', 'bb', 354), ('FF', 'bb', 295), ('JJ', 'aa', 234), 
    ('BB', 'bb', 840), ('AA', 'aa', 934), ('EE', 'aa', 113), 
    ('AA', 'bb', 309), ('BB', 'aa', 412), ('AA', 'aa', 911), 
    ('AA', 'bb', 572), ('II', 'aa', 398), ('II', 'bb', 250), 
    ('II', 'aa', 652), ('BB', 'bb', 633), ('AA', 'aa', 239), 
    ('FF', 'aa', 670), ('BB', 'bb', 705), ('HH', 'bb', 963), 
    ('CC', 'bb', 346), ('II', 'bb', 671), ('BB', 'aa', 247), 
    ('AA', 'aa', 223), ('GG', 'aa', 480), ('HH', 'aa', 790), 
    ('FF', 'aa', 208), ('BB', 'bb', 711), ('EE', 'aa', 777), 
    ('DD', 'bb', 716), ('CC', 'aa', 759), ('CC', 'aa', 430), 
    ('CC', 'aa', 607), ('DD', 'bb', 794), ('GG', 'aa', 148), 
    ('GG', 'aa', 634), ('JJ', 'bb', 257), ('DD', 'bb', 959), 
    ('FF', 'bb', 726), ('BB', 'aa', 762), ('JJ', 'bb', 336), 
    ('GG', 'aa', 335), ('HH', 'bb', 330), ('GG', 'bb', 160), 
    ('JJ', 'bb', 839), ('FF', 'aa', 618), ('BB', 'aa', 393), 
    ('EE', 'bb', 629), ('FF', 'aa', 667), ('AA', 'bb', 870), 
    ('FF', 'bb', 102), ('JJ', 'aa', 113), ('DD', 'aa', 224), 
    ('AA', 'bb', 627), ('HH', 'bb', 730), ('II', 'bb', 443), 
    ('HH', 'bb', 133), ('EE', 'bb', 252), ('II', 'bb', 805), 
    ('BB', 'bb', 786), ('EE', 'bb', 768), ('HH', 'bb', 683), 
    ('DD', 'bb', 238), ('DD', 'aa', 256);
}

foreach {tn frame} {
  1  { GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING }
  2  { GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW }
  3  { GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING }
  4  { GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING }
  5  { GROUPS BETWEEN 1 PRECEDING         AND 2 PRECEDING }
  6  { GROUPS BETWEEN 2 PRECEDING         AND 1 PRECEDING }
  7  { GROUPS BETWEEN 3 PRECEDING         AND 1 PRECEDING }
  8  { GROUPS BETWEEN 3 PRECEDING         AND 0 PRECEDING }
  9  { GROUPS BETWEEN 2 PRECEDING         AND CURRENT ROW }
  10 { GROUPS BETWEEN 3 PRECEDING         AND 0 FOLLOWING }
  11 { GROUPS BETWEEN 2 PRECEDING         AND UNBOUNDED FOLLOWING }
  12 { GROUPS BETWEEN CURRENT ROW         AND 0 FOLLOWING }
  13 { GROUPS BETWEEN CURRENT ROW         AND 1 FOLLOWING }
  14 { GROUPS BETWEEN CURRENT ROW         AND 100 FOLLOWING }
  15 { GROUPS BETWEEN CURRENT ROW         AND UNBOUNDED FOLLOWING }
  16 { GROUPS BETWEEN 0 FOLLOWING         AND 0 FOLLOWING }
  17 { GROUPS BETWEEN 1 FOLLOWING         AND 0 FOLLOWING }
  18 { GROUPS BETWEEN 1 FOLLOWING         AND 5 FOLLOWING }
  19 { GROUPS BETWEEN 1 FOLLOWING         AND UNBOUNDED FOLLOWING }

} {
  execsql_test 1.$tn.1 "
    SELECT a, b, sum(c) OVER (ORDER BY a $frame) FROM t3 ORDER BY 1, 2, 3;
  "
  execsql_test 1.$tn.2 "
    SELECT a, b, sum(c) OVER (ORDER BY a,b $frame) FROM t3 ORDER BY 1, 2, 3;
  "
  execsql_test 1.$tn.3 "
    SELECT a, b, rank() OVER (ORDER BY a $frame) FROM t3 ORDER BY 1, 2, 3;
  "
  execsql_test 1.$tn.4 "
    SELECT a, b, max(c) OVER (ORDER BY a,b $frame) FROM t3 ORDER BY 1, 2, 3;
  "
  execsql_test 1.$tn.5 "
    SELECT a, b, min(c) OVER (ORDER BY a,b $frame) FROM t3 ORDER BY 1, 2, 3;
  "
}

==========

execsql_test 2.0 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(a INTEGER, b INTEGER);
  INSERT INTO t1 VALUES
      (13, 26), (15, 30);
}

foreach {tn frame} {
  1 { ORDER BY a RANGE BETWEEN 5 PRECEDING AND 5 FOLLOWING }
  2 { ORDER BY a RANGE BETWEEN 10 PRECEDING AND 5 PRECEDING }
  3 { ORDER BY a RANGE BETWEEN 2 FOLLOWING AND 3 FOLLOWING }
} {
  execsql_test 2.$tn "SELECT a, sum(b) OVER win FROM t1 WINDOW win AS ($frame)"
}


finish_test


