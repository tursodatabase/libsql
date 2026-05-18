# 2024 Aug 10
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
# Tests focusing on the built-in fts5 tokenizers. 
#

source [file join [file dirname [info script]] fts5_common.tcl]
set testprefix fts5tokenizer3

# If SQLITE_ENABLE_FTS5 is not defined, omit this file.
ifcapable !fts5 {
  finish_test
  return
}


proc get_sod {args} { return "split_on_dot" }
proc get_lowercase    {args} { return "lowercase" }

proc lowercase {flags txt} {
  set n [string length $txt]
  sqlite3_fts5_token [string tolower $txt] 0 $n
  return 0
}

proc split_on_dot {flags txt} {
  set iOff 0
  foreach t [split $txt "."] {
    set n [string length $txt]
    sqlite3_fts5_token $t $iOff [expr $iOff+$n]
    incr iOff [expr {$n+1}]
  }
  return ""
}

foreach {tn script} {
  1 {
    sqlite3_fts5_create_tokenizer db lowercase get_lowercase
    sqlite3_fts5_create_tokenizer -parent lowercase db split_on_dot get_sod
  }
  2 {
    sqlite3_fts5_create_tokenizer -v2 db lowercase get_lowercase
    sqlite3_fts5_create_tokenizer -parent lowercase db split_on_dot get_sod
  }
  3 {
    sqlite3_fts5_create_tokenizer db lowercase get_lowercase
    sqlite3_fts5_create_tokenizer -v2 -parent lowercase db split_on_dot get_sod
  }
  4 {
    sqlite3_fts5_create_tokenizer -v2 db lowercase get_lowercase
    sqlite3_fts5_create_tokenizer -v2 -parent lowercase db split_on_dot get_sod
  }
} {
  reset_db
  eval $script

  do_execsql_test 1.$tn.0 {
    CREATE VIRTUAL TABLE t1 USING fts5(x, tokenize=split_on_dot);
    CREATE VIRTUAL TABLE t1vocab USING fts5vocab(t1, instance);
    INSERT INTO t1 VALUES('ABC.Def.ghi');
  }

  do_execsql_test 1.$tn.1 {
    SELECT term FROM t1vocab ORDER BY 1
  } {abc def ghi}
}


finish_test
