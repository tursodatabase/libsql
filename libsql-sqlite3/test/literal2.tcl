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


start_test literal2 "2024 Jan 23"

execsql_test  1.0 { SELECT 123_456 }
errorsql_test 1.1 { SELECT 123__456 }

execsql_float_test 2.1 { SELECT 1.0e1_2 }


execsql_test  3.0.0 { SELECT 0xFF_FF }
execsql_test  3.0.1 { SELECT 0xFF_EF }
errorsql_test  3.0.2 { SELECT 0xFF__EF }
# errorsql_test   3.0.3 { SELECT 0x_FFEF }
errorsql_test  3.0.4 { SELECT 0xFFEF_ }

execsql_test  3.1.0 { SELECT 0XFF_FF }
execsql_test  3.1.1 { SELECT 0XFF_EF }
errorsql_test  3.1.2 { SELECT 0XFF__EF }
# errorsql_test   3.1.3 { SELECT 0X_FFEF }
errorsql_test  3.1.4 { SELECT 0XFFEF_ }

finish_test


