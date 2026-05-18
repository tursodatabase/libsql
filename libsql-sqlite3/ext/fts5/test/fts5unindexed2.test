# 2024 Sep 13
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
# The tests in this file focus on "unindexed" columns in contentless 
# tables.
#

source [file join [file dirname [info script]] fts5_common.tcl]
set testprefix fts5unindexed2

# If SQLITE_ENABLE_FTS5 is not defined, omit this file.
ifcapable !fts5 {
  finish_test
  return
}


do_execsql_test 1.1 {
  CREATE VIRTUAL TABLE t1 USING fts5(
      a, b UNINDEXED, content=, contentless_unindexed=1
  );
} {}

do_execsql_test 1.2 {
  INSERT INTO t1 VALUES('abc def', 'ghi jkl');
}

do_execsql_test 1.3 {
  SELECT rowid, a, b FROM t1
} {1 {} {ghi jkl}}

do_execsql_test 1.4 {
  INSERT INTO t1(rowid, a, b) VALUES(11, 'hello world', 'one two three');
}

do_execsql_test 1.5 {
  INSERT INTO t1(t1, rowid, a, b) VALUES('delete', 1, 'abc def', 'ghi jkl');
}

do_execsql_test 1.6 {
  SELECT rowid, a, b FROM t1
} {
  11 {} {one two three}
}

do_execsql_test 1.7 {
  PRAGMA integrity_check
} {ok}

do_execsql_test 1.8 {
  INSERT INTO t1(rowid, a, b) VALUES(12, 'abc def', 'ghi jkl');
}

do_execsql_test 1.9 {
  SELECT rowid, a, b FROM t1('def')
} {12 {} {ghi jkl}}

do_execsql_test 1.10 {
  SELECT rowid, a, b FROM t1('def OR hello') ORDER BY rank
} {11 {} {one two three} 12 {} {ghi jkl}}

do_execsql_test 1.11 {
  SELECT rowid, a, b FROM t1 WHERE rowid=11
} {11 {} {one two three}}

do_execsql_test 1.12 {
  SELECT rowid, a, b FROM t1
} {11 {} {one two three} 12 {} {ghi jkl}}


fts5_aux_test_functions db
do_execsql_test 1.12.2 {
  SELECT rowid, fts5_test_columntext(t1) FROM t1('def OR hello')
} {11 {{} {one two three}} 12 {{} {ghi jkl}}}

do_execsql_test 1.13 {
  INSERT INTO t1(t1) VALUES('delete-all');
}

do_execsql_test 1.14 {
  SELECT rowid, a, b FROM t1
} 

do_execsql_test 1.15 {
  PRAGMA integrity_check
} {ok}

do_execsql_test 2.0 {
  CREATE VIRTUAL TABLE t4 USING fts5(
      x, y UNINDEXED, z, columnsize=0, content='', contentless_unindexed=1
  );
}

do_execsql_test 2.1 {
  INSERT INTO t4(rowid, x, y, z) VALUES(1, 'a a', 'b b b', 'c');
}

#-------------------------------------------------------------------------
reset_db

do_execsql_test 3.0 {
  CREATE VIRTUAL TABLE x1 USING fts5(
      a UNINDEXED, b, c UNINDEXED, d, content=, contentless_delete=1,
      contentless_unindexed=1
  );
}

do_execsql_test 3.1 {
  INSERT INTO x1(rowid, a, b, c, d) VALUES(131, 'aaa', 'bbb', 'ccc', 'ddd');
}

do_execsql_test 3.2 {
  SELECT * FROM x1
} {aaa {} ccc {}}

do_execsql_test 3.3 {
  INSERT INTO x1(rowid, a, b, c, d) VALUES(1000, 'AAA', 'BBB', 'CCC', 'DDD');
}

do_execsql_test 3.4 {
  SELECT rowid, * FROM x1
} {
  131 aaa {} ccc {}
  1000 AAA {} CCC {}
}

do_execsql_test 3.5 {
  DELETE FROM x1 WHERE rowid=131;
  SELECT rowid, * FROM x1
} {
  1000 AAA {} CCC {}
}

do_execsql_test 3.6 {
  INSERT INTO x1(rowid, a, b, c, d) VALUES(112, 'aaa', 'bbb', 'ccc', 'ddd');
  SELECT rowid, * FROM x1
} {
  112 aaa {} ccc {}
  1000 AAA {} CCC {}
}

do_execsql_test 3.7 {
  UPDATE x1 SET b='hello', d='world', rowid=1120 WHERE rowid=112
}

do_execsql_test 3.8 {
  SELECT rowid, * FROM x1
} {
  1000 AAA {} CCC {}
  1120 aaa {} ccc {}
}

do_execsql_test 3.9 {
  SELECT rowid, * FROM x1('hello');
} {
  1120 aaa {} ccc {}
}

do_execsql_test 3.9 {
  SELECT rowid, * FROM x1('bbb');
} {
  1000 AAA {} CCC {}
}

fts5_aux_test_functions db
do_execsql_test 3.10 {
  SELECT rowid, fts5_test_columntext(x1) FROM x1('b*')
} {1000 {AAA {} CCC {}}}

#-------------------------------------------------------------------------
# Check that if contentless_unindexed=1 is not specified, the values
# of UNINDEXED columns are not stored in the database.
#
# Also check that contentless_unindexed=1 is not allowed unless the table
# is actually contentless.
#
reset_db
do_execsql_test 4.0 {
  CREATE VIRTUAL TABLE ft USING fts5(a, b, c UNINDEXED, content='');
  INSERT INTO ft VALUES('one', 'two', 'three');
  SELECT rowid, * FROM ft;
} {1 {} {} {}}

do_execsql_test 4.1 {
  SELECT name FROM sqlite_schema ORDER BY 1
} {
  ft ft_config ft_data ft_docsize ft_idx
}

do_catchsql_test 4.2 {
  CREATE VIRTUAL TABLE ft2 USING fts5(
      a, b, c UNINDEXED, contentless_unindexed=1
  );
} {1 {contentless_unindexed=1 requires a contentless table}}

do_catchsql_test 4.3 {
  DELETE FROM ft WHERE rowid=1
} {1 {cannot DELETE from contentless fts5 table: ft}}

#-------------------------------------------------------------------------
# Check that the usual restrictions on contentless tables apply to 
# contentless_unindexed=1 tables.
#
reset_db
do_execsql_test 5.0 {
  CREATE VIRTUAL TABLE ft USING fts5(
      a, b UNINDEXED, c, content='', contentless_unindexed=1
  );
  INSERT INTO ft VALUES('one', 'two', 'three');
  INSERT INTO ft VALUES('four', 'five', 'six');
  INSERT INTO ft VALUES('seven', 'eight', 'nine');
  SELECT rowid, * FROM ft;
} {
  1 {} two {}
  2 {} five {}
  3 {} eight {}
}

do_execsql_test 5.1 {
  PRAGMA integrity_check
} {ok}

do_catchsql_test 5.2 {
  DELETE FROM ft WHERE rowid=2
} {1 {cannot DELETE from contentless fts5 table: ft}}

do_execsql_test 5.3 {
  SELECT rowid, * FROM ft('six')
} {
  2 {} five {}
}

do_catchsql_test 5.4 {
  UPDATE ft SET a='x', b='y', c='z' WHERE rowid=3
} {1 {cannot UPDATE contentless fts5 table: ft}}

fts5_aux_test_functions db

do_execsql_test 5.5 {
  SELECT fts5_test_columntext(ft) FROM ft WHERE rowid=3
} {
  {{} eight {}}
}
do_execsql_test 5.6 {
  SELECT fts5_test_columntext(ft) FROM ft('three');
} {
  {{} two {}}
}

#-------------------------------------------------------------------------
# Check that it is possible to UPDATE a contentless_unindexed=1 table
# if the only columns being modified are UNINDEXED.
#
# If the contentless_unindexed=1 table is also contentless_delete=1, then
# it is also possible to update indexed columns - but only if *all* indexed
# columns are updated.
#
reset_db
do_execsql_test 6.0 {
  CREATE VIRTUAL TABLE ft1 USING fts5(a, b UNINDEXED, c UNINDEXED, d,
      contentless_unindexed=1, content=''
  );

  INSERT INTO ft1(rowid, a, b, c, d) VALUES
      (100, 'x y', 'b1', 'c1', 'a b'),
      (200, 'c d', 'b2', 'c2', 'a b'),
      (300, 'e f', 'b3', 'c3', 'a b');
}

do_execsql_test 6.1 {
  UPDATE ft1 SET b='b1.1', c='c1.1' WHERE rowid=100;
}
do_execsql_test 6.2 {
  UPDATE ft1 SET b='b2.1' WHERE rowid=200;
}
do_execsql_test 6.3 {
  UPDATE ft1 SET c='c3.1' WHERE rowid=300;
}

do_execsql_test 6.4 {
  SELECT rowid, a, b, c, d FROM ft1
} {
  100 {} b1.1 c1.1 {}
  200 {} b2.1 c2 {}
  300 {} b3 c3.1 {}
}

finish_test

