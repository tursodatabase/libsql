# 2023 October 24
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#*************************************************************************
#
# Tests for the fts5 "trigram" tokenizer.
#

source [file join [file dirname [info script]] fts5_common.tcl]
ifcapable !fts5 { finish_test ; return }
set ::testprefix fts5trigram2

do_execsql_test 1.0 "
  CREATE VIRTUAL TABLE t1 USING fts5(y, tokenize='trigram remove_diacritics 1');
  INSERT INTO t1 VALUES('abc\u0303defghijklm');
  INSERT INTO t1 VALUES('a\u0303b\u0303c\u0303defghijklm');
"
do_catchsql_test 1.0.1 {
  CREATE VIRTUAL TABLE t2 USING fts5(z, tokenize='trigram remove_diacritics');
} {1 {error in tokenizer constructor}}

do_execsql_test 1.1 {
  SELECT highlight(t1, 0, '(', ')') FROM t1('abc');
} [list \
  "(abc\u0303)defghijklm"                          \
  "(a\u0303b\u0303c\u0303)defghijklm"              \
]

do_execsql_test 1.2 {
  SELECT highlight(t1, 0, '(', ')') FROM t1('bcde');
} [list \
  "a(bc\u0303de)fghijklm"                          \
  "a\u0303(b\u0303c\u0303de)fghijklm"              \
]

do_execsql_test 1.3 {
  SELECT highlight(t1, 0, '(', ')') FROM t1('cdef');
} [list \
  "ab(c\u0303def)ghijklm"                          \
  "a\u0303b\u0303(c\u0303def)ghijklm"              \
]

do_execsql_test 1.4 {
  SELECT highlight(t1, 0, '(', ')') FROM t1('def');
} [list \
  "abc\u0303(def)ghijklm"                          \
  "a\u0303b\u0303c\u0303(def)ghijklm"              \
]


#-------------------------------------------------------------------------
do_catchsql_test 2.0 {
  CREATE VIRTUAL TABLE t2 USING fts5(
      z, tokenize='trigram case_sensitive 1 remove_diacritics 1'
  );
} {1 {error in tokenizer constructor}}

do_execsql_test 2.1 {
  CREATE VIRTUAL TABLE t2 USING fts5(
      z, tokenize='trigram case_sensitive 0 remove_diacritics 1'
  );
}
do_execsql_test 2.2 "
  INSERT INTO t2 VALUES('\u00E3bcdef');
  INSERT INTO t2 VALUES('b\u00E3cdef');
  INSERT INTO t2 VALUES('bc\u00E3def');
  INSERT INTO t2 VALUES('bcd\u00E3ef');
"

do_execsql_test 2.3 {
  SELECT highlight(t2, 0, '(', ')') FROM t2('abc');
} "(\u00E3bc)def"
do_execsql_test 2.4 {
  SELECT highlight(t2, 0, '(', ')') FROM t2('bac');
} "(b\u00E3c)def"
do_execsql_test 2.5 {
  SELECT highlight(t2, 0, '(', ')') FROM t2('bca');
} "(bc\u00E3)def"
do_execsql_test 2.6 "
  SELECT highlight(t2, 0, '(', ')') FROM t2('\u00E3bc');
" "(\u00E3bc)def"

#-------------------------------------------------------------------------
do_execsql_test 3.0 {
  CREATE VIRTUAL TABLE t3 USING fts5(
      z, tokenize='trigram remove_diacritics 1'
  );
} {}
do_execsql_test 3.1 "
  INSERT INTO t3 VALUES ('\u0303abc\u0303');
"
do_execsql_test 3.2 {
  SELECT highlight(t3, 0, '(', ')') FROM t3('abc');
} "\u0303(abc\u0303)"

#-------------------------------------------------------------------------
do_execsql_test 4.0 {
  CREATE VIRTUAL TABLE t4 USING fts5(z, tokenize=trigram);
} {}

do_execsql_test 4.1 {
  INSERT INTO t4 VALUES('ABCD');
  INSERT INTO t4 VALUES('DEFG');
} {}

db close
sqlite3 db test.db

do_eqp_test 4.1 {
  SELECT rowid FROM t4 WHERE z LIKE '%abc%'
} {VIRTUAL TABLE INDEX 0:L0}

do_execsql_test 4.2 {
  SELECT rowid FROM t4 WHERE z LIKE '%abc%'
} {1}

#-------------------------------------------------------------------------
reset_db
do_execsql_test 5.0 {
  CREATE VIRTUAL TABLE t5 USING fts5(
      c1, tokenize='trigram', detail='none'
  );
  INSERT INTO t5(rowid, c1) VALUES(1, 'abc_____xyx_yxz');
  INSERT INTO t5(rowid, c1) VALUES(2, 'abc_____xyxz');
  INSERT INTO t5(rowid, c1) VALUES(3, 'ac_____xyxz');
} {}
do_execsql_test 5.1 {
  SELECT rowid FROM t5 WHERE c1 LIKE 'abc%xyxz'
} {2}

finish_test
