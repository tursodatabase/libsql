#ifndef CRSQLITE_CONSTS_H
#define CRSQLITE_CONSTS_H

// db version is a signed 64bit int since sqlite doesn't support saving and
// retrieving unsigned 64bit ints. (2^64 / 2) is a big enough number to write 1
// million entries per second for 3,000 centuries.
#define MIN_POSSIBLE_DB_VERSION 0L

#define __CRSQL_CLOCK_LEN 13

#define CRR_SPACE 0
#define USER_SPACE 1

#define CLOCK_TABLES_SELECT                                                  \
  "SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name LIKE " \
  "'%__crsql_clock'"

#define SET_SYNC_BIT "SELECT crsql_internal_sync_bit(1)"
#define CLEAR_SYNC_BIT "SELECT crsql_internal_sync_bit(0)"

#define TBL_SITE_ID "__crsql_site_id"
#define TBL_DB_VERSION "__crsql_db_version"
#define TBL_SCHEMA "crsql_master"
#define UNION_ALL "UNION ALL"

#define MAX_TBL_NAME_LEN 2048
#define SITE_ID_LEN 16

// Version int:
// M - major
// m - minor
// p - patch
// b - build
// MM.mm.pp.bb
// 00 00 00 00
// Given we can't prefix an int with 0s, read from right to left.
// Rightmost is always `bb`
#define CRSQLITE_VERSION 130000

#endif
