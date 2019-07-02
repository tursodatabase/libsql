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


start_test filter2 "2019 July 2"

ifcapable !windowfunc

execsql_test 1.0 {
  DROP TABLE IF EXISTS t1;
  CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER);
  INSERT INTO t1 VALUES
   (1, 7), (2, 3), (3, 5), (4, 30), (5, 26), (6, 23), (7, 27),
   (8, 3), (9, 17), (10, 26), (11, 33), (12, 25), (13, NULL), (14, 47),
   (15, 36), (16, 13), (17, 45), (18, 31), (19, 11), (20, 36), (21, 37),
   (22, 21), (23, 22), (24, 14), (25, 16), (26, 3), (27, 7), (28, 29),
   (29, 50), (30, 38), (31, 3), (32, 36), (33, 12), (34, 4), (35, 46),
   (36, 3), (37, 48), (38, 23), (39, NULL), (40, 24), (41, 5), (42, 46),
   (43, 11), (44, NULL), (45, 18), (46, 25), (47, 15), (48, 18), (49, 23);
}

execsql_test 1.1 { SELECT sum(b) FROM t1 }

execsql_test 1.2 { SELECT sum(b) FILTER (WHERE a<10) FROM t1 }

execsql_test 1.3 { SELECT count(DISTINCT b) FROM t1 }

execsql_test 1.4 { SELECT count(DISTINCT b) FILTER (WHERE a!=19) FROM t1 }

execsql_test 1.5 { 
  SELECT min(b) FILTER (WHERE a>19),
         min(b) FILTER (WHERE a>0),
         max(a+b) FILTER (WHERE a>19),
         max(b+a) FILTER (WHERE a BETWEEN 10 AND 40)
  FROM t1;
}

execsql_test 1.6 { 
  SELECT min(b),
         min(b),
         max(a+b),
         max(b+a)
  FROM t1
  GROUP BY (a%10)
  ORDER BY 1, 2, 3, 4;
}

execsql_test 1.7 { 
  SELECT min(b) FILTER (WHERE a>19),
         min(b) FILTER (WHERE a>0),
         max(a+b) FILTER (WHERE a>19),
         max(b+a) FILTER (WHERE a BETWEEN 10 AND 40)
  FROM t1
  GROUP BY (a%10)
  ORDER BY 1, 2, 3, 4;
}

finish_test


