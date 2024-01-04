/*
** Name:        sqlite3mc_vfs.h
** Purpose:     Header file for VFS of SQLite3 Multiple Ciphers support
** Author:      Ulrich Telle
** Created:     2020-03-01
** Copyright:   (c) 2020-2023 Ulrich Telle
** License:     MIT
*/

#ifndef SQLITE3MC_VFS_H_

#ifdef __cplusplus
extern "C" {
#endif

#ifndef SQLITE_PRIVATE
#define SQLITE_PRIVATE
#endif
SQLITE_PRIVATE int sqlite3mcCheckVfs(const char* zVfs);

SQLITE_API int sqlite3mc_vfs_create(const char* zVfsReal, int makeDefault);
SQLITE_API void sqlite3mc_vfs_destroy(const char* zName);
SQLITE_API void sqlite3mc_vfs_shutdown();

#ifdef __cplusplus
}
#endif

#endif /* SQLITE3MC_VFS_H_ */
