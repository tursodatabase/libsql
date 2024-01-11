/*
** Name:        cipher_config.h
** Purpose:     Header for the cipher configuration of SQLite3 Multiple Ciphers
** Author:      Ulrich Telle
** Created:     2020-03-10
** Copyright:   (c) 2006-2020 Ulrich Telle
** License:     MIT
*/

#ifndef CIPHER_CONFIG_H_
#define CIPHER_CONFIG_H_

#include "sqlite3mc.h"

SQLITE_PRIVATE void sqlite3mcConfigTable(sqlite3_context* context, int argc, sqlite3_value** argv);
SQLITE_PRIVATE CodecParameter* sqlite3mcGetCodecParams(sqlite3* db);

/* Forward declaration */
static unsigned char* sqlite3mcGetSaltWriteCipher(Codec* codec);

SQLITE_PRIVATE void sqlite3mcCodecDataSql(sqlite3_context* context, int argc, sqlite3_value** argv);
SQLITE_PRIVATE void sqlite3mcConfigParams(sqlite3_context* context, int argc, sqlite3_value** argv);
SQLITE_PRIVATE int sqlite3mcConfigureFromUri(sqlite3* db, const char *zDbName, int configDefault);

SQLITE_PRIVATE int sqlite3mcFileControlPragma(sqlite3* db, const char* zDbName, int op, void* pArg);
SQLITE_PRIVATE int sqlite3mcCodecQueryParameters(sqlite3* db, const char* zDb, const char* zUri);
SQLITE_PRIVATE int sqlite3mcHandleAttachKey(sqlite3* db, const char* zName, const char* zPath, sqlite3_value* pKey, char** zErrDyn);
SQLITE_PRIVATE int sqlite3mcHandleMainKey(sqlite3* db, const char* zPath);

#endif
