#!/bin/sh
# Generate patched sqlite3.c from SQLite3 amalgamation and write it to stdout.
# Usage: ./script/patchsqlite3.sh sqlite3.c >sqlite3patched.c

INPUT="$([ "$#" -eq 1 ] && echo "$1" || echo "sqlite3.c")"
if ! [ -f "$INPUT" ]; then
  echo "Usage: $0 <SQLITE3_AMALGAMATION>" >&2
  echo " e.g.: $0 sqlite3.c" >&2
  exit 1
fi

die() {
    echo "[-]" "$@" >&2
    exit 2
}

# 1) Intercept VFS pragma handling
# 2) Add handling of KEY parameter in ATTACH statements
sed 's/sqlite3_file_control\(.*SQLITE_FCNTL_PRAGMA\)/sqlite3mcFileControlPragma\1/' "$INPUT" \
    | sed '/\#endif \/\* SQLITE3\_H \*\//a \ \n\/\* Function prototypes of SQLite3 Multiple Ciphers \*\/\nSQLITE_PRIVATE int sqlite3mcCheckVfs(const char*);\nSQLITE_PRIVATE int sqlite3mcFileControlPragma(sqlite3*, const char*, int, void*);\nSQLITE_PRIVATE int sqlite3mcHandleAttachKey(sqlite3*, const char*, const char*, sqlite3_value*, char**);\nSQLITE_PRIVATE int sqlite3mcHandleMainKey(sqlite3*, const char*);\ntypedef struct PgHdr PgHdrMC;\nSQLITE_PRIVATE void* sqlite3mcPagerCodec(PgHdrMC* pPg);\ntypedef struct Pager PagerMC;\nSQLITE_PRIVATE int sqlite3mcPagerHasCodec(PagerMC* pPager);\nSQLITE_PRIVATE void sqlite3mcInitMemoryMethods();' \
    | sed '/\#define MAX\_PATHNAME 512/c #if SQLITE3MC\_MAX\_PATHNAME \> 512\n#define MAX_PATHNAME SQLITE3MC\_MAX\_PATHNAME\n#else\n#define MAX_PATHNAME 512\n#endif' \
    | sed '/pData = pPage->pData;/c \  if( (pData = sqlite3mcPagerCodec(pPage))==0 ) return SQLITE_NOMEM_BKPT;' \
    | sed '/pData = p->pData;/c \        if( (pData = sqlite3mcPagerCodec(p))==0 ) return SQLITE_NOMEM;' \
    | sed '/sqlite3_free_filename( zPath );/i \\n  \/\* Handle KEY parameter. \*\/\n  if( rc==SQLITE_OK ){\n    rc = sqlite3mcHandleAttachKey(db, zName, zPath, argv[2], &zErrDyn);\n  }' \
    | sed '/\*ppVfs = sqlite3_vfs_find(zVfs);/i \  \/\* Check VFS. \*\/\n  sqlite3mcCheckVfs(zVfs);\n' \
    | sed '/sqlite3_free_filename(zOpen);/i \\n  \/\* Handle encryption related URI parameters. \*\/\n  if( rc==SQLITE_OK ){\n    rc = sqlite3mcHandleMainKey(db, zOpen);\n  }' \
    | sed '/^  if( sqlite3PCacheIsDirty(pPager->pPCache) ) return 0;/a \  if( sqlite3mcPagerHasCodec(pPager) != 0 ) return 0;' \
    | sed '/^  }else if( USEFETCH(pPager) ){/c \  }else if( USEFETCH(pPager) && sqlite3mcPagerHasCodec(pPager) == 0 ){' \
    | sed '/^  if( rc!=SQLITE_OK ) memset(&mem0, 0, sizeof(mem0));/a \\n  \/\* Initialize wrapper for memory management.\*\/\n  if( rc==SQLITE_OK ) {\n    sqlite3mcInitMemoryMethods();\n  }\n'
