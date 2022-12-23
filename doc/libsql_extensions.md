# libSQL extensions

This document describes extensions to the library provided by libSQL, not available in upstream SQLite at the time of writing.

## RANDOM ROWID

Regular tables use an implicitly defined, unique, 64-bit rowid column as its primary key.
If rowid value is not specified during insertion, it's auto-generated with the following heuristics:
 1. Find the current max rowid value.
 2. If max value is less than i64::max, use the next available value
 3. If max value is i64::max:
     a. pick a random value
     b. if it's not taken, use it
     c. if it's taken, go to (a.), rinse, repeat

Based on this algorithm, the following trick can be used to trick libSQL into generating random rowid values instead of consecutive ones - simply insert a sentinel row with `rowid = i64::max`.

The newly introduced `RANDOM ROWID` option can be used to explicitly state that the table generates random rowid values on insertions, without having to insert a dummy row with special rowid value, or manually trying to generate a random unique rowid, which some user applications may find problematic.

### Usage

`RANDOM ROWID` keywords can be used during table creation, in a manner similar to its syntactic cousin, `WITHOUT ROWID`:
```sql
CREATE TABLE shopping_list(item text, quantity int) RANDOM ROWID;
```

On insertion, pseudorandom rowid values will be generated:
```sql
CREATE TABLE shopping_list(item text, quantity int) RANDOM ROWID;
INSERT INTO shopping_list(item, quantity) VALUES ('bread', 2);
INSERT INTO shopping_list(item, quantity) VALUES ('butter', 1);
.mode column
SELECT rowid, * FROM shopping_list;
rowid                item    quantity
-------------------  ------  --------
1177193729061749947  bread   2       
4433412906245401374  butter  1  
```

### Restrictions

`RANDOM ROWID` is mutually exclusive with `WITHOUT ROWID` option, and cannot be used with tables having an `AUTOINCREMENT` primary key.


## WebAssembly-based user-defined functions (experimental)

In addition to being able to define functions via the C API (http://www.sqlite.org/c3ref/create_function.html), it's possible to enable experimental support for `CREATE FUNCTION` syntax allowing users to dynamically register functions coded in WebAssembly.

Once enabled, `CREATE FUNCTION` and `DROP FUNCTION` are available in SQL. They act as syntactic sugar for managing data stored in a special internal table: `libsql_wasm_func_table(name TEXT, body TEXT)`. This table can also be inspected with regular tools - e.g. to see which functions are registered and what's their source code.

### How to enable

This feature is experimental and opt-in, and can be enabled by the following configure:
```sh
./configure --enable-wasm-runtime
```

Then, in your source code, the internal table for storing WebAssembly source code can be created via `libsql_try_initialize_wasm_func_table(sqlite3 *db)` function.

You can also download a pre-compiled binary from https://github.com/libsql/libsql/releases/tag/libsql-0.1.0, or use a docker image for experiments:
```
docker run -it piotrsarna/libsql:libsql-0.1.0-wasm-udf ./libsql
```

#### shell support
In order to initialize the internal WebAssembly function lookup table in libsql shell (sqlite3 binary), one can use the `.init_wasm_func_table` command. This command is safe to be called multiple times, even if the internal table already exists.

### CREATE FUNCTION

Creating a function requires providing its name and WebAssembly source code (in WebAssembly text format). The ABI for translating between WebAssembly types and libSQL types is to be standardized soon.

Example SQL:
```sql
CREATE FUNCTION IF NOT EXISTS fib LANGUAGE wasm AS '
(module 
 (type (;0;) (func (param i64) (result i64))) 
 (func $fib (type 0) (param i64) (result i64) 
 (local i64) 
 i64.const 0 
 local.set 1 
 block ;; label = @1 
 local.get 0 
 i64.const 2 
 i64.lt_u 
 br_if 0 (;@1;) 
 i64.const 0 
 local.set 1 
 loop ;; label = @2 
 local.get 0 
 i64.const -1 
 i64.add 
 call $fib 
 local.get 1 
 i64.add 
 local.set 1 
 local.get 0 
 i64.const -2 
 i64.add 
 local.tee 0 
 i64.const 1 
 i64.gt_u 
 br_if 0 (;@2;) 
 end 
 end 
 local.get 0 
 local.get 1 
 i64.add) 
 (memory (;0;) 16) 
 (global $__stack_pointer (mut i32) (i32.const 1048576)) 
 (global (;1;) i32 (i32.const 1048576)) 
 (global (;2;) i32 (i32.const 1048576)) 
 (export "memory" (memory 0)) 
 (export "fib" (func $fib)))
';
```
[1] WebAssembly source: https://github.com/psarna/libsql_bindgen/blob/55b69d8d08fc0e6e096b37467c05c5dd10398eb7/src/lib.rs#L68-L75 .

### Drop function

Dropping a dynamically created function can be done via a `DROP FUNCTION` statement.

Example:
```sql
DROP FUNCTION IF EXISTS fib;
```

### How to implement user-defined functions in WebAssembly

This paragraph is based on our [blog post](https://blog.chiselstrike.com/webassembly-functions-for-your-sqlite-compatible-database-7e1ad95a2aa7) which describes the process in more detail.

In order for a WebAssembly function to be runnable from libSQL, it must follow its ABI - which in this case can be reduced to "how to translate libSQL types to WebAssembly and back". Fortunately, both projects have a very small set of supported types, so the whole mapping fits in a short table:
| libSQL type  | Wasm type  |
|---|---|
| INTEGER  | i64  |
| REAL  | f64  |
| TEXT  | i32*  |
| BLOB  | i32*  |
| NULL  | i32*  |

where `i32` represents a pointer to WebAssembly memory. Underneath, indirectly represented types are encoded as follows:
| libSQL type | representation |
|---|---|
| TEXT  | [1 byte with value `3` (`SQLITE_TEXT`)][null-terminated string] |
| BLOB  | [1 byte with value `4` (`SQLITE_BLOB`)][4 bytes of size][binary string] |
| NULL  | [1 byte with value `5` (`SQLITE_NULL`)  |

The compiled module should export at least the function that is supposed to be later used as a user-defined function, and its `memory` instance.

Encoding type translation manually for each function can be cumbersome, so we provide helper libraries for languages compilable to WebAssembly. Right now the only implementation is for Rust: https://crates.io/crates/libsql_bindgen

With `libsql_bindgen`, a native Rust function can be annotated with a macro:
```rust
#[libsql_bindgen::libsql_bindgen]
pub fn decrypt(data: String, key: String) -> String {
  use magic_crypt::MagicCryptTrait;
  let mc = magic_crypt::new_magic_crypt!(key, 256);
  mc.decrypt_base64_to_string(data)
      .unwrap_or("[ACCESS DENIED]".to_owned())
}
```

Compiling the function to WebAssembly will produce code that can be registered as a user-defined function in libSQL.
```
cargo build --release --target wasm32-unknown-unknown
```

For quick experiments, our playground application can be used: https://bindgen.libsql.org

After the function is compiled, it can be registered via SQL by:
```sql
CREATE FUNCTION your_function LANGUAGE wasm AS <source-code>
```
, where `<source-code>` is either a binary .wasm blob or text presented in WebAssembly Text format.

See an example in `CREATE FUNCTION` paragraph above.

## Virtual WAL

Write-ahead log is a journaling mode which enables nice write concurrency characteristics - it not only allows a single writer to run in parallel with readers, but also makes `BEGIN CONCURRENT` transactions with optimistic locking possible. In SQLite, WAL is not a virtual interface, it only has a single file-based implementation, with an additional WAL index kept in shared memory (in form of another mapped file). In libSQL, akin to VFS, it's possible to override WAL routines with custom code. That allows implementing pluggable backends for write-ahead log, which opens many possibilities (again, similar to the VFS mechanism).

### API

In order to register a new set of virtual WAL methods, these methods need to be implemented. This is the current API:
```c
typedef struct libsql_wal_methods {
  int iVersion; /* Current version is 1, versioning is here for backward compatibility *.
  /* Open and close a connection to a write-ahead log. */
  int (*xOpen)(sqlite3_vfs*, sqlite3_file* , const char*, int no_shm_mode, i64 max_size, struct libsql_wal_methods*, Wal**);
  int (*xClose)(Wal*, sqlite3* db, int sync_flags, int nBuf, u8 *zBuf);

  /* Set the limiting size of a WAL file. */
  void (*xLimit)(Wal*, i64 limit);

  /* Used by readers to open (lock) and close (unlock) a snapshot.  A
  ** snapshot is like a read-transaction.  It is the state of the database
  ** at an instant in time.  sqlite3WalOpenSnapshot gets a read lock and
  ** preserves the current state even if the other threads or processes
  ** write to or checkpoint the WAL.  sqlite3WalCloseSnapshot() closes the
  ** transaction and releases the lock.
  */
  int (*xBeginReadTransaction)(Wal *, int *);
  void (*xEndReadTransaction)(Wal *);

  /* Read a page from the write-ahead log, if it is present. */
  int (*xFindFrame)(Wal *, Pgno, u32 *);
  int (*xReadFrame)(Wal *, u32, int, u8 *);

  /* If the WAL is not empty, return the size of the database. */
  Pgno (*xDbsize)(Wal *pWal);

  /* Obtain or release the WRITER lock. */
  int (*xBeginWriteTransaction)(Wal *pWal);
  int (*xEndWriteTransaction)(Wal *pWal);

  /* Undo any frames written (but not committed) to the log */
  int (*xUndo)(Wal *pWal, int (*xUndo)(void *, Pgno), void *pUndoCtx);

  /* Return an integer that records the current (uncommitted) write
  ** position in the WAL */
  void (*xSavepoint)(Wal *pWal, u32 *aWalData);

  /* Move the write position of the WAL back to iFrame.  Called in
  ** response to a ROLLBACK TO command. */
  int (*xSavepointUndo)(Wal *pWal, u32 *aWalData);

  /* Write a frame or frames to the log. */
  int (*xFrames)(Wal *pWal, int, PgHdr *, Pgno, int, int);

  /* Copy pages from the log to the database file */ 
  int (*xCheckpoint)(
    Wal *pWal,                      /* Write-ahead log connection */
    sqlite3 *db,                    /* Check this handle's interrupt flag */
    int eMode,                      /* One of PASSIVE, FULL and RESTART */
    int (*xBusy)(void*),            /* Function to call when busy */
    void *pBusyArg,                 /* Context argument for xBusyHandler */
    int sync_flags,                 /* Flags to sync db file with (or 0) */
    int nBuf,                       /* Size of buffer nBuf */
    u8 *zBuf,                       /* Temporary buffer to use */
    int *pnLog,                     /* OUT: Number of frames in WAL */
    int *pnCkpt                     /* OUT: Number of backfilled frames in WAL */
  );

  /* Return the value to pass to a sqlite3_wal_hook callback, the
  ** number of frames in the WAL at the point of the last commit since
  ** sqlite3WalCallback() was called.  If no commits have occurred since
  ** the last call, then return 0.
  */
  int (*xCallback)(Wal *pWal);

  /* Tell the wal layer that an EXCLUSIVE lock has been obtained (or released)
  ** by the pager layer on the database file.
  */
  int (*xExclusiveMode)(Wal *pWal, int op);

  /* Return true if the argument is non-NULL and the WAL module is using
  ** heap-memory for the wal-index. Otherwise, if the argument is NULL or the
  ** WAL module is using shared-memory, return false. 
  */
  int (*xHeapMemory)(Wal *pWal);

  // Only needed with SQLITE_ENABLE_SNAPSHOT, but part of the ABI
  int (*xSnapshotGet)(Wal *pWal, sqlite3_snapshot **ppSnapshot);
  void (*xSnapshotOpen)(Wal *pWal, sqlite3_snapshot *pSnapshot);
  int (*xSnapshotRecover)(Wal *pWal);
  int (*xSnapshotCheck)(Wal *pWal, sqlite3_snapshot *pSnapshot);
  void (*xSnapshotUnlock)(Wal *pWal);

  // Only needed with SQLITE_ENABLE_ZIPVFS, but part of the ABI
  /* If the WAL file is not empty, return the number of bytes of content
  ** stored in each frame (i.e. the db page-size when the WAL was created).
  */
  int (*xFramesize)(Wal *pWal);


  /* Return the sqlite3_file object for the WAL file */
  sqlite3_file *(*xFile)(Wal *pWal);

  // Only needed with  SQLITE_ENABLE_SETLK_TIMEOUT
  int (*xWriteLock)(Wal *pWal, int bLock);

  void (*xDb)(Wal *pWal, sqlite3 *db);

  /* Return the WAL pathname length based on the owning pager's pathname len.
  ** For WAL implementations not based on a single file, 0 should be returned. */
  int (*xPathnameLen)(int origPathname);

  /* Get the WAL pathname to given buffer. Assumes that the buffer can hold
  ** at least xPathnameLen bytes. For WAL implementations not based on a single file,
  ** this operation can safely be a no-op.
  ** */
  void (*xGetWalPathname)(char *buf, const char *orig, int orig_len);

  /*
  ** This optional callback gets called before the main database file which owns
  ** the WAL file is open. It is a good place for initialization routines, as WAL
  ** is otherwise open lazily.
  */
  int (*xPreMainDbOpen)(libsql_wal_methods *methods, const char *main_db_path);

  /* True if the implementation relies on shared memory routines (e.g. locks) */
  int bUsesShm;

  const char *zName;
  struct libsql_wal_methods *pNext;
} libsql_wal_methods;
```

### Registering WAL methods

After the implementation is ready, the following public functions can be used
to manage it:
```c
  libsql_wal_methods_find
  libsql_wal_methods_register
  libsql_wal_methods_unregister
```
, and they are quite self-descriptive. They also work similarly to their `sqlite3_vfs*` counterparts, which they were modeled after.

### Using WAL methods

Custom WAL methods need to be declared when opening a new database connection.
That can be achieved either programatically by using a new flavor of the `sqlite3_open*` function:
```c
int libsql_open(
  const char *filename,   /* Database filename (UTF-8) */
  sqlite3 **ppDb,         /* OUT: SQLite db handle */
  int flags,              /* Flags */
  const char *zVfs,       /* Name of VFS module to use, NULL for default */
  const char *zWal        /* Name of WAL module to use, NULL for default */
)
```

... or via URI, by using a new `wal` parameter:
```
.open file:test.db?wal=my_impl_of_wal_methods
```

### Example

An example implementation can be browsed in the Rust test suite, at `test/rust_suite/src/virtual_wal.rs`

## xPreparedSql virtual table callback

Virtual tables provide an interface to expose different data sources. Several callbacks are already defined via function pointers on `struct sqlite3_module`, including xConnect and xBestIndex. libSQL introduces a new callback named `xPreparedSql` that allows a virtual table implementation to receive the SQL string submitted by the application for execution.

### How  to enable

In order to enable this callback, applications must define a value for `iVersion >= 700` on `struct sqlite3_module`.

### Usage

The following C99 snippet shows how to declare a virtual table that implements the new callback.

```sql
static int helloPreparedSql(sqlite3_vtab_cursor *cur, const char *sql) {
    printf("Prepared SQL: %s\n", sql);
    return SQLITE_OK;
}

static sqlite3_module helloModule = {
    .iVersion     = 700,
    .xCreate      = helloCreate,
    .xConnect     = helloConnect,
    // ...
    .xPreparedSql = helloPreparedSql,
};
```
