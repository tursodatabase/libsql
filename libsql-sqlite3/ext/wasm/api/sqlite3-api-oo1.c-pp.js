//#if not omit-oo1
/*
  2022-07-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file contains the so-called OO #1 API wrapper for the sqlite3
  WASM build. It requires that sqlite3-api-glue.js has already run
  and it installs its deliverable as sqlite3.oo1.
*/
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
  const toss3 = (...args)=>{throw new sqlite3.SQLite3Error(...args)};

  const capi = sqlite3.capi, wasm = sqlite3.wasm, util = sqlite3.util;
  /* What follows is colloquially known as "OO API #1". It is a
     binding of the sqlite3 API which is designed to be run within
     the same thread (main or worker) as the one in which the
     sqlite3 WASM binding was initialized. This wrapper cannot use
     the sqlite3 binding if, e.g., the wrapper is in the main thread
     and the sqlite3 API is in a worker. */

  /**
     In order to keep clients from manipulating, perhaps
     inadvertently, the underlying pointer values of DB and Stmt
     instances, we'll gate access to them via the `pointer` property
     accessor and store their real values in this map. Keys = DB/Stmt
     objects, values = pointer values. This also unifies how those are
     accessed, for potential use downstream via custom
     wasm.xWrap() function signatures which know how to extract
     it.
  */
  const __ptrMap = new WeakMap();
  /**
     A Set of oo1.DB or oo1.Stmt objects which are proxies for
     (sqlite3*) resp. (sqlite3_stmt*) pointers which themselves are
     owned elsewhere. Objects in this Set do not own their underlying
     handle and that handle must be guaranteed (by the client) to
     outlive the proxy. DB.close()/Stmt.finalize() methods will remove
     the object from this Set _instead_ of closing/finalizing the
     pointer. These proxies are primarily intended as a way to briefly
     wrap an (sqlite3[_stmt]*) object as an oo1.DB/Stmt without taking
     over ownership, to take advantage of simplifies usage compared to
     the C API while not imposing any change of ownership.

     See DB.wrapHandle() and Stmt.wrapHandle().
  */
  const __doesNotOwnHandle = new Set();
  /**
     Map of DB instances to objects, each object being a map of Stmt
     wasm pointers to Stmt objects.
  */
  const __stmtMap = new WeakMap();

  /** If object opts has _its own_ property named p then that
      property's value is returned, else dflt is returned. */
  const getOwnOption = (opts, p, dflt)=>{
    const d = Object.getOwnPropertyDescriptor(opts,p);
    return d ? d.value : dflt;
  };

  // Documented in DB.checkRc()
  const checkSqlite3Rc = function(dbPtr, sqliteResultCode){
    if(sqliteResultCode){
      if(dbPtr instanceof DB) dbPtr = dbPtr.pointer;
      toss3(
        sqliteResultCode,
        "sqlite3 result code",sqliteResultCode+":",
        (dbPtr
         ? capi.sqlite3_errmsg(dbPtr)
         : capi.sqlite3_errstr(sqliteResultCode))
      );
    }
    return arguments[0];
  };

  /**
     sqlite3_trace_v2() callback which gets installed by the DB ctor
     if its open-flags contain "t".
  */
  const __dbTraceToConsole =
        wasm.installFunction('i(ippp)', function(t,c,p,x){
          if(capi.SQLITE_TRACE_STMT===t){
            // x == SQL, p == sqlite3_stmt*
            console.log("SQL TRACE #"+(++this.counter)+' via sqlite3@'+c+':',
                        wasm.cstrToJs(x));
          }
        }.bind({counter: 0}));

  /**
     A map of sqlite3_vfs pointers to SQL code or a callback function
     to run when the DB constructor opens a database with the given
     VFS. In the latter case, the call signature is
     (theDbObject,sqlite3Namespace) and the callback is expected to
     throw on error.
  */
  const __vfsPostOpenCallback = Object.create(null);

//#if enable-see
  /**
     Converts ArrayBuffer or Uint8Array ba into a string of hex
     digits.
  */
  const byteArrayToHex = function(ba){
    if( ba instanceof ArrayBuffer ){
      ba = new Uint8Array(ba);
    }
    const li = [];
    const digits = "0123456789abcdef";
    for( const d of ba ){
      li.push( digits[(d & 0xf0) >> 4], digits[d & 0x0f] );
    }
    return li.join('');
  };

  /**
     Internal helper to apply an SEE key to a just-opened
     database. Requires that db be-a DB object which has just been
     opened, opt be the options object processed by its ctor, and opt
     must have either the key, hexkey, or textkey properties, either
     as a string, an ArrayBuffer, or a Uint8Array.

     This is a no-op in non-SEE builds. It throws on error and returns
     without side effects if none of the key/textkey/hexkey options
     are set. It throws if more than one is set or if any are set to
     values of an invalid type.

     Returns true if it applies the key, else an unspecified falsy
     value.  Note that applying the key does not imply that the key is
     correct, only that it was passed on to the db.
  */
  const dbCtorApplySEEKey = function(db,opt){
    if( !capi.sqlite3_key_v2 ) return;
    let keytype;
    let key;
    const check = (opt.key ? 1 : 0) + (opt.hexkey ? 1 : 0) + (opt.textkey ? 1 : 0);
    if( !check ) return;
    else if( check>1 ){
      toss3(capi.SQLITE_MISUSE,
            "Only ONE of (key, hexkey, textkey) may be provided.");
    }
    if( opt.key ){
      /* It is not legal to bind an argument to PRAGMA key=?, so we
         convert it to a hexkey... */
      keytype = 'key';
      key = opt.key;
      if('string'===typeof key){
        key = new TextEncoder('utf-8').encode(key);
      }
      if((key instanceof ArrayBuffer) || (key instanceof Uint8Array)){
        key = byteArrayToHex(key);
        keytype = 'hexkey';
      }else{
        toss3(capi.SQLITE_MISUSE,
              "Invalid value for the 'key' option. Expecting a string,",
              "ArrayBuffer, or Uint8Array.");
        return;
      }
    }else if( opt.textkey ){
      /* For textkey we need it to be in string form, so convert it to
         a string if it's a byte array... */
      keytype = 'textkey';
      key = opt.textkey;
      if(key instanceof ArrayBuffer){
        key = new Uint8Array(key);
      }
      if(key instanceof Uint8Array){
        key = new TextDecoder('utf-8').decode(key);
      }else if('string'!==typeof key){
        toss3(capi.SQLITE_MISUSE,
              "Invalid value for the 'textkey' option. Expecting a string,",
              "ArrayBuffer, or Uint8Array.");
      }
    }else if( opt.hexkey ){
      keytype = 'hexkey';
      key = opt.hexkey;
      if((key instanceof ArrayBuffer) || (key instanceof Uint8Array)){
        key = byteArrayToHex(key);
      }else if('string'!==typeof key){
        toss3(capi.SQLITE_MISUSE,
              "Invalid value for the 'hexkey' option. Expecting a string,",
              "ArrayBuffer, or Uint8Array.");
      }
      /* else assume it's valid hex codes */
    }else{
      return;
    }
    let stmt;
    try{
      stmt = db.prepare("PRAGMA "+keytype+"="+util.sqlite3__wasm_qfmt_token(key, 1));
      stmt.step();
      return true;
    }finally{
      if(stmt) stmt.finalize();
    }
  };
//#endif enable-see

  /**
     A proxy for DB class constructors. It must be called with the
     being-construct DB object as its "this". See the DB constructor
     for the argument docs. This is split into a separate function
     in order to enable simple creation of special-case DB constructors,
     e.g. JsStorageDb and OpfsDb.

     Expects to be passed a configuration object with the following
     properties:

     - `.filename`: the db filename. It may be a special name like ":memory:"
       or "".

     - `.flags`: as documented in the DB constructor.

     - `.vfs`: as documented in the DB constructor.

     It also accepts those as the first 3 arguments.
  */
  const dbCtorHelper = function ctor(...args){
    if(!ctor._name2vfs){
      /**
         Map special filenames which we handle here (instead of in C)
         to some helpful metadata...

         As of 2022-09-20, the C API supports the names :localStorage:
         and :sessionStorage: for kvvfs. However, C code cannot
         determine (without embedded JS code, e.g. via Emscripten's
         EM_JS()) whether the kvvfs is legal in the current browser
         context (namely the main UI thread). In order to help client
         code fail early on, instead of it being delayed until they
         try to read or write a kvvfs-backed db, we'll check for those
         names here and throw if they're not legal in the current
         context.
      */
      ctor._name2vfs = Object.create(null);
      const isWorkerThread = ('function'===typeof importScripts/*===running in worker thread*/)
            ? (n)=>toss3("The VFS for",n,"is only available in the main window thread.")
            : false;
      ctor._name2vfs[':localStorage:'] = {
        vfs: 'kvvfs', filename: isWorkerThread || (()=>'local')
      };
      ctor._name2vfs[':sessionStorage:'] = {
        vfs: 'kvvfs', filename: isWorkerThread || (()=>'session')
      };
    }
    const opt = ctor.normalizeArgs(...args);
    //sqlite3.config.debug("DB ctor",opt);
    let pDb;
    if( (pDb = opt['sqlite3*']) ){
      /* This property ^^^^^ is very specifically NOT DOCUMENTED and
         NOT part of the public API. This is a back door for functions
         like DB.wrapDbHandle(). */
      //sqlite3.config.debug("creating proxy db from",opt);
      if( !opt['sqlite3*:takeOwnership'] ){
        /* This is object does not own its handle. */
        __doesNotOwnHandle.add(this);
      }
      this.filename = capi.sqlite3_db_filename(pDb,'main');
    }else{
      let fn = opt.filename, vfsName = opt.vfs, flagsStr = opt.flags;
      if( ('string'!==typeof fn && !wasm.isPtr(fn))
          || 'string'!==typeof flagsStr
          || (vfsName && ('string'!==typeof vfsName && !wasm.isPtr(vfsName))) ){
        sqlite3.config.error("Invalid DB ctor args",opt,arguments);
        toss3("Invalid arguments for DB constructor:", arguments, "opts:", opt);
      }
      let fnJs = wasm.isPtr(fn) ? wasm.cstrToJs(fn) : fn;
      const vfsCheck = ctor._name2vfs[fnJs];
      if(vfsCheck){
        vfsName = vfsCheck.vfs;
        fn = fnJs = vfsCheck.filename(fnJs);
      }
      let oflags = 0;
      if( flagsStr.indexOf('c')>=0 ){
        oflags |= capi.SQLITE_OPEN_CREATE | capi.SQLITE_OPEN_READWRITE;
      }
      if( flagsStr.indexOf('w')>=0 ) oflags |= capi.SQLITE_OPEN_READWRITE;
      if( 0===oflags ) oflags |= capi.SQLITE_OPEN_READONLY;
      oflags |= capi.SQLITE_OPEN_EXRESCODE;
      const stack = wasm.pstack.pointer;
      try {
        const pPtr = wasm.pstack.allocPtr() /* output (sqlite3**) arg */;
        let rc = capi.sqlite3_open_v2(fn, pPtr, oflags, vfsName || wasm.ptr.null);
        pDb = wasm.peekPtr(pPtr);
        checkSqlite3Rc(pDb, rc);
        capi.sqlite3_extended_result_codes(pDb, 1);
        if(flagsStr.indexOf('t')>=0){
          capi.sqlite3_trace_v2(pDb, capi.SQLITE_TRACE_STMT,
                                __dbTraceToConsole, pDb);
        }
      }catch( e ){
        if( pDb ) capi.sqlite3_close_v2(pDb);
        throw e;
      }finally{
        wasm.pstack.restore(stack);
      }
      this.filename = fnJs;
    }
    __ptrMap.set(this, pDb);
    __stmtMap.set(this, Object.create(null));
    if( !opt['sqlite3*'] ){
      try{
//#if enable-see
        dbCtorApplySEEKey(this,opt);
//#endif
        // Check for per-VFS post-open SQL/callback...
        const pVfs = capi.sqlite3_js_db_vfs(pDb)
              || toss3("Internal error: cannot get VFS for new db handle.");
        const postInitSql = __vfsPostOpenCallback[pVfs];
        if(postInitSql){
          /**
             Reminder: if this db is encrypted and the client did _not_ pass
             in the key, any init code will fail, causing the ctor to throw.
             We don't actually know whether the db is encrypted, so we cannot
             sensibly apply any heuristics which skip the init code only for
             encrypted databases for which no key has yet been supplied.
          */
          if(postInitSql instanceof Function){
            postInitSql(this, sqlite3);
          }else{
            checkSqlite3Rc(
              pDb, capi.sqlite3_exec(pDb, postInitSql, 0, 0, 0)
            );
          }
        }
      }catch(e){
        this.close();
        throw e;
      }
    }
  };

  /**
     Sets a callback which should be called after a db is opened with
     the given sqlite3_vfs pointer. The 2nd argument must be a
     function, which gets called with
     (theOo1DbObject,sqlite3Namespace) at the end of the DB()
     constructor. The function must throw on error, in which case the
     db is closed and the exception is propagated.  This function is
     intended only for use by DB subclasses or sqlite3_vfs
     implementations.

     Prior to 2024-07-22, it was legal to pass SQL code as the second
     argument, but that can interfere with a client's ability to run
     pragmas which must be run before anything else, namely (pragma
     locking_mode=exclusive) for use with WAL mode.  That capability
     had only ever been used as an internal detail of the two OPFS
     VFSes, and they no longer use it that way.
  */
  dbCtorHelper.setVfsPostOpenCallback = function(pVfs, callback){
    if( !(callback instanceof Function)){
      toss3("dbCtorHelper.setVfsPostOpenCallback() should not be used with "+
            "a non-function argument.",arguments);
    }
    __vfsPostOpenCallback[pVfs] = callback;
  };

  /**
     A helper for DB constructors. It accepts either a single
     config-style object or up to 3 arguments (filename, dbOpenFlags,
     dbVfsName). It returns a new object containing:

     { filename: ..., flags: ..., vfs: ... }

     If passed an object, any additional properties it has are copied
     as-is into the new object.
  */
  dbCtorHelper.normalizeArgs = function(filename=':memory:',flags = 'c',vfs = null){
    const arg = {};
    if(1===arguments.length && arguments[0] && 'object'===typeof arguments[0]){
      Object.assign(arg, arguments[0]);
      if(undefined===arg.flags) arg.flags = 'c';
      if(undefined===arg.vfs) arg.vfs = null;
      if(undefined===arg.filename) arg.filename = ':memory:';
    }else{
      arg.filename = filename;
      arg.flags = flags;
      arg.vfs = vfs;
    }
    return arg;
  };
  /**
     The DB class provides a high-level OO wrapper around an sqlite3
     db handle.

     The given db filename must be resolvable using whatever
     filesystem layer (virtual or otherwise) is set up for the default
     sqlite3 VFS.

     Note that the special sqlite3 db names ":memory:" and ""
     (temporary db) have their normal special meanings here and need
     not resolve to real filenames, but "" uses an on-storage
     temporary database and requires that the VFS support that.

     The second argument specifies the open/create mode for the
     database. It must be string containing a sequence of letters (in
     any order, but case sensitive) specifying the mode:

     - "c": create if it does not exist, else fail if it does not
       exist. Implies the "w" flag.

     - "w": write. Implies "r": a db cannot be write-only.

     - "r": read-only if neither "w" nor "c" are provided, else it
       is ignored.

     - "t": enable tracing of SQL executed on this database handle,
       sending it to `console.log()`. To disable it later, call
       `sqlite3.capi.sqlite3_trace_v2(thisDb.pointer, 0, 0, 0)`.

     If "w" is not provided, the db is implicitly read-only, noting
     that "rc" is meaningless

     Any other letters are currently ignored. The default is
     "c". These modes are ignored for the special ":memory:" and ""
     names and _may_ be ignored altogether for certain VFSes.

     The final argument is analogous to the final argument of
     sqlite3_open_v2(): the name of an sqlite3 VFS. Pass a falsy value,
     or none at all, to use the default. If passed a value, it must
     be the string name of a VFS.

     The constructor optionally (and preferably) takes its arguments
     in the form of a single configuration object with the following
     properties:

     - `filename`: database file name
     - `flags`: open-mode flags
     - `vfs`: the VFS fname

//#if enable-see
     SEE-capable builds optionally support ONE of the following
     additional options:

     - `key`, `hexkey`, or `textkey`: encryption key as a string,
       ArrayBuffer, or Uint8Array. These flags function as documented
       for the SEE pragmas of the same names. Using a byte array for
       `hexkey` is equivalent to the same series of hex codes in
       string form, so `'666f6f'` is equivalent to
       `Uint8Array([0x66,0x6f,0x6f])`. A `textkey` byte array is
       assumed to be UTF-8. A `key` string is transformed into a UTF-8
       byte array, and a `key` byte array is transformed into a
       `hexkey` with the same bytes.

     In non-SEE builds, these options are ignored. In SEE builds,
     `PRAGMA key/textkey/hexkey=X` is executed immediately after
     opening the db. If more than one of the options is provided,
     or any option has an invalid argument type, an exception is
     thrown.

     Note that some DB subclasses may run post-initialization SQL
     code, e.g. to set a busy-handler timeout or tweak the page cache
     size. Such code is run _after_ the SEE key is applied. If no key
     is supplied and the database is encrypted, execution of the
     post-initialization SQL will fail, causing the constructor to
     throw.
//#endif enable-see

     The `filename` and `vfs` arguments may be either JS strings or
     C-strings allocated via WASM. `flags` is required to be a JS
     string (because it's specific to this API, which is specific
     to JS).

     For purposes of passing a DB instance to C-style sqlite3
     functions, the DB object's read-only `pointer` property holds its
     `sqlite3*` pointer value. That property can also be used to check
     whether this DB instance is still open: it will evaluate to
     `undefined` after the DB object's close() method is called.

     In the main window thread, the filenames `":localStorage:"` and
     `":sessionStorage:"` are special: they cause the db to use either
     localStorage or sessionStorage for storing the database using
     the kvvfs. If one of these names are used, they trump
     any vfs name set in the arguments.
  */
  const DB = function(...args){
    dbCtorHelper.apply(this, args);
  };
  DB.dbCtorHelper = dbCtorHelper;

  /**
     Internal-use enum for mapping JS types to DB-bindable types.
     These do not (and need not) line up with the SQLITE_type
     values. All values in this enum must be truthy and (mostly)
     distinct but they need not be numbers.
  */
  const BindTypes = {
    null: 1,
    number: 2,
    string: 3,
    boolean: 4,
    blob: 5
  };
  if(wasm.bigIntEnabled){
    BindTypes.bigint = BindTypes.number;
  }

  /**
     This class wraps sqlite3_stmt. Calling this constructor
     directly will trigger an exception. Use DB.prepare() to create
     new instances.

     For purposes of passing a Stmt instance to C-style sqlite3
     functions, its read-only `pointer` property holds its `sqlite3_stmt*`
     pointer value.

     Other non-function properties include:

     - `db`: the DB object which created the statement.

     - `columnCount`: the number of result columns in the query, or 0
     for queries which cannot return results. This property is a
     read-only proxy for sqlite3_column_count() and its use in loops
     should be avoided because of the call overhead associated with
     that. The `columnCount` is not cached when the Stmt is created
     because a schema change made between this statement's preparation
     and when it is stepped may invalidate it.

     - `parameterCount`: the number of bindable parameters in the
     query.  Like `columnCount`, this property is ready-only and is a
     proxy for a C API call.

     As a general rule, most methods of this class will throw if
     called on an instance which has been finalized. For brevity's
     sake, the method docs do not all repeat this warning.
  */
  const Stmt = function(/*oo1db, stmtPtr, BindTypes [,takeOwnership=true] */){
    if(BindTypes!==arguments[2]){
      toss3(capi.SQLITE_MISUSE, "Do not call the Stmt constructor directly. Use DB.prepare().");
    }
    this.db = arguments[0];
    __ptrMap.set(this, arguments[1]);
    if( arguments.length>3 && !arguments[3] ){
      __doesNotOwnHandle.add(this);
    }
  };

  /** Throws if the given DB has been closed, else it is returned. */
  const affirmDbOpen = function(db){
    if(!db.pointer) toss3("DB has been closed.");
    return db;
  };

  /** Throws if ndx is not an integer or if it is out of range
      for stmt.columnCount, else returns stmt.

      Reminder: this will also fail after the statement is finalized
      but the resulting error will be about an out-of-bounds column
      index rather than a statement-is-finalized error.
  */
  const affirmColIndex = function(stmt,ndx){
    if((ndx !== (ndx|0)) || ndx<0 || ndx>=stmt.columnCount){
      toss3("Column index",ndx,"is out of range.");
    }
    return stmt;
  };

  /**
     Expects to be passed the `arguments` object from DB.exec(). Does
     the argument processing/validation, throws on error, and returns
     a new object on success:

     { sql: the SQL, opt: optionsObj, cbArg: function}

     The opt object is a normalized copy of any passed to this
     function. The sql will be converted to a string if it is provided
     in one of the supported non-string formats.

     cbArg is only set if the opt.callback or opt.resultRows are set,
     in which case it's a function which expects to be passed the
     current Stmt and returns the callback argument of the type
     indicated by the input arguments.
  */
  const parseExecArgs = function(db, args){
    const out = Object.create(null);
    out.opt = Object.create(null);
    switch(args.length){
        case 1:
          if('string'===typeof args[0] || util.isSQLableTypedArray(args[0])){
            out.sql = args[0];
          }else if(Array.isArray(args[0])){
            out.sql = args[0];
          }else if(args[0] && 'object'===typeof args[0]){
            out.opt = args[0];
            out.sql = out.opt.sql;
          }
          break;
        case 2:
          out.sql = args[0];
          out.opt = args[1];
          break;
        default: toss3("Invalid argument count for exec().");
    };
    out.sql = util.flexibleString(out.sql);
    if('string'!==typeof out.sql){
      toss3("Missing SQL argument or unsupported SQL value type.");
    }
    const opt = out.opt;
    switch(opt.returnValue){
        case 'resultRows':
          if(!opt.resultRows) opt.resultRows = [];
          out.returnVal = ()=>opt.resultRows;
          break;
        case 'saveSql':
          if(!opt.saveSql) opt.saveSql = [];
          out.returnVal = ()=>opt.saveSql;
          break;
        case undefined:
        case 'this':
          out.returnVal = ()=>db;
          break;
        default:
          toss3("Invalid returnValue value:",opt.returnValue);
    }
    if(!opt.callback && !opt.returnValue && undefined!==opt.rowMode){
      if(!opt.resultRows) opt.resultRows = [];
      out.returnVal = ()=>opt.resultRows;
    }
    if(opt.callback || opt.resultRows){
      switch((undefined===opt.rowMode) ? 'array' : opt.rowMode) {
        case 'object':
          out.cbArg = (stmt,cache)=>{
            if( !cache.columnNames ) cache.columnNames = stmt.getColumnNames([]);
            /* https://sqlite.org/forum/forumpost/3632183d2470617d:
               conversion of rows to objects (key/val pairs) is
               somewhat expensive for large data sets because of the
               native-to-JS conversion of the column names. If we
               instead cache the names and build objects from that
               list of strings, it can run twice as fast. The
               difference is not noticeable for small data sets but
               becomes human-perceivable when enough rows are
               involved. */
            const row = stmt.get([]);
            const rv = Object.create(null);
            for( const i in cache.columnNames ) rv[cache.columnNames[i]] = row[i];
            return rv;
          };
          break;
        case 'array': out.cbArg = (stmt)=>stmt.get([]); break;
        case 'stmt':
          if(Array.isArray(opt.resultRows)){
            toss3("exec(): invalid rowMode for a resultRows array: must",
                  "be one of 'array', 'object',",
                  "a result column number, or column name reference.");
          }
          out.cbArg = (stmt)=>stmt;
          break;
        default:
          if(util.isInt32(opt.rowMode)){
            out.cbArg = (stmt)=>stmt.get(opt.rowMode);
            break;
          }else if('string'===typeof opt.rowMode
                   && opt.rowMode.length>1
                   && '$'===opt.rowMode[0]){
            /* "$X": fetch column named "X" (case-sensitive!). Prior
               to 2022-12-14 ":X" and "@X" were also permitted, but
               having so many options is unnecessary and likely to
               cause confusion. */
            const $colName = opt.rowMode.substr(1);
            out.cbArg = (stmt)=>{
              const rc = stmt.get(Object.create(null))[$colName];
              return (undefined===rc)
                ? toss3(capi.SQLITE_NOTFOUND,
                        "exec(): unknown result column:",$colName)
                : rc;
            };
            break;
          }
          toss3("Invalid rowMode:",opt.rowMode);
      }
    }
    return out;
  };

  /**
     Internal impl of the DB.selectValue(), selectArray(), and
     selectObject() methods.
  */
  const __selectFirstRow = (db, sql, bind, ...getArgs)=>{
    const stmt = db.prepare(sql);
    try {
      const rc = stmt.bind(bind).step() ? stmt.get(...getArgs) : undefined;
      stmt.reset(/*for INSERT...RETURNING locking case*/);
      return rc;
    }finally{
      stmt.finalize();
    }
  };

  /**
     Internal impl of the DB.selectArrays() and selectObjects()
     methods.
  */
  const __selectAll =
        (db, sql, bind, rowMode)=>db.exec({
          sql, bind, rowMode, returnValue: 'resultRows'
        });

  /**
     Expects to be given a DB instance or an `sqlite3*` pointer (may
     be null) and an sqlite3 API result code. If the result code is
     not falsy, this function throws an SQLite3Error with an error
     message from sqlite3_errmsg(), using db (or, if db is-a DB,
     db.pointer) as the db handle, or sqlite3_errstr() if db is
     falsy. Note that if it's passed a non-error code like SQLITE_ROW
     or SQLITE_DONE, it will still throw but the error string might be
     "Not an error."  The various non-0 non-error codes need to be
     checked for in client code where they are expected.

     The thrown exception's `resultCode` property will be the value of
     the second argument to this function.

     If it does not throw, it returns its first argument.
  */
  DB.checkRc = (db,resultCode)=>checkSqlite3Rc(db,resultCode);

  DB.prototype = {
    /** Returns true if this db handle is open, else false. */
    isOpen: function(){
      return !!this.pointer;
    },
    /** Throws if this given DB has been closed, else returns `this`. */
    affirmOpen: function(){
      return affirmDbOpen(this);
    },
    /**
       Finalizes all open statements and closes this database
       connection (with one exception noted below). This is a no-op if
       the db has already been closed. After calling close(),
       `this.pointer` will resolve to `undefined`, and that can be
       used to check whether the db instance is still opened.

       If this.onclose.before is a function then it is called before
       any close-related cleanup.

       If this.onclose.after is a function then it is called after the
       db is closed but before auxiliary state like this.filename is
       cleared.

       Both onclose handlers are passed this object, with the onclose
       object as their "this," noting that the db will have been
       closed when onclose.after is called. If this db is not opened
       when close() is called, neither of the handlers are called. Any
       exceptions the handlers throw are ignored because "destructors
       must not throw".

       Garbage collection of a db handle, if it happens at all, will
       never trigger close(), so onclose handlers are not a reliable
       way to implement close-time cleanup or maintenance of a db.

       If this instance was created using DB.wrapHandle() and does not
       own this.pointer then it does not close the db handle but it
       does perform all other work, such as calling onclose callbacks
       and disassociating this object from this.pointer.
    */
    close: function(){
      const pDb = this.pointer;
      if(pDb){
        if(this.onclose && (this.onclose.before instanceof Function)){
          try{this.onclose.before(this)}
          catch(e){/*ignore*/}
        }
        Object.keys(__stmtMap.get(this)).forEach((k,s)=>{
          if(s && s.pointer){
            try{s.finalize()}
            catch(e){/*ignore*/}
          }
        });
        __ptrMap.delete(this);
        __stmtMap.delete(this);
        if( !__doesNotOwnHandle.delete(this) ){
          capi.sqlite3_close_v2(pDb);
        }
        if(this.onclose && (this.onclose.after instanceof Function)){
          try{this.onclose.after(this)}
          catch(e){/*ignore*/}
        }
        delete this.filename;
      }
    },
    /**
       Returns the number of changes, as per sqlite3_changes()
       (if the first argument is false) or sqlite3_total_changes()
       (if it's true). If the 2nd argument is true, it uses
       sqlite3_changes64() or sqlite3_total_changes64(), which
       will trigger an exception if this build does not have
       BigInt support enabled.
    */
    changes: function(total=false,sixtyFour=false){
      const p = affirmDbOpen(this).pointer;
      if(total){
        return sixtyFour
          ? capi.sqlite3_total_changes64(p)
          : capi.sqlite3_total_changes(p);
      }else{
        return sixtyFour
          ? capi.sqlite3_changes64(p)
          : capi.sqlite3_changes(p);
      }
    },
    /**
       Similar to the this.filename but returns the
       sqlite3_db_filename() value for the given database name,
       defaulting to "main".  The argument may be either a JS string
       or a pointer to a WASM-allocated C-string.
    */
    dbFilename: function(dbName='main'){
      return capi.sqlite3_db_filename(affirmDbOpen(this).pointer, dbName);
    },
    /**
       Returns the name of the given 0-based db number, as documented
       for sqlite3_db_name().
    */
    dbName: function(dbNumber=0){
      return capi.sqlite3_db_name(affirmDbOpen(this).pointer, dbNumber);
    },
    /**
       Returns the name of the sqlite3_vfs used by the given database
       of this connection (defaulting to 'main'). The argument may be
       either a JS string or a WASM C-string. Returns undefined if the
       given db name is invalid. Throws if this object has been
       close()d.
    */
    dbVfsName: function(dbName=0){
      let rc;
      const pVfs = capi.sqlite3_js_db_vfs(
        affirmDbOpen(this).pointer, dbName
      );
      if(pVfs){
        const v = new capi.sqlite3_vfs(pVfs);
        try{ rc = wasm.cstrToJs(v.$zName) }
        finally { v.dispose() }
      }
      return rc;
    },
    /**
       Compiles the given SQL and returns a prepared Stmt. This is
       the only way to create new Stmt objects. Throws on error.

       The given SQL must be a string, a Uint8Array holding SQL, a
       WASM pointer to memory holding the NUL-terminated SQL string,
       or an array of strings. In the latter case, the array is
       concatenated together, with no separators, to form the SQL
       string (arrays are often a convenient way to formulate long
       statements).  If the SQL contains no statements, an
       SQLite3Error is thrown.

       Design note: the C API permits empty SQL, reporting it as a 0
       result code and a NULL stmt pointer. Supporting that case here
       would cause extra work for all clients: any use of the Stmt API
       on such a statement will necessarily throw, so clients would be
       required to check `stmt.pointer` after calling `prepare()` in
       order to determine whether the Stmt instance is empty or not.
       Long-time practice (with other sqlite3 script bindings)
       suggests that the empty-prepare case is sufficiently rare that
       supporting it here would simply hurt overall usability.
    */
    prepare: function(sql){
      affirmDbOpen(this);
      const stack = wasm.pstack.pointer;
      let ppStmt, pStmt;
      try{
        ppStmt = wasm.pstack.alloc(8)/* output (sqlite3_stmt**) arg */;
        DB.checkRc(this, capi.sqlite3_prepare_v2(this.pointer, sql, -1, ppStmt, null));
        pStmt = wasm.peekPtr(ppStmt);
      }
      finally {
        wasm.pstack.restore(stack);
      }
      if(!pStmt) toss3("Cannot prepare empty SQL.");
      const stmt = new Stmt(this, pStmt, BindTypes);
      __stmtMap.get(this)[pStmt] = stmt;
      return stmt;
    },
    /**
       Executes one or more SQL statements in the form of a single
       string. Its arguments must be either (sql,optionsObject) or
       (optionsObject). In the latter case, optionsObject.sql must
       contain the SQL to execute. By default it returns this object
       but that can be changed via the `returnValue` option as
       described below. Throws on error.

       If no SQL is provided, or a non-string is provided, an
       exception is triggered. Empty SQL, on the other hand, is
       simply a no-op.

       The optional options object may contain any of the following
       properties:

       - `sql` = the SQL to run (unless it's provided as the first
       argument). This must be of type string, Uint8Array, or an array
       of strings. In the latter case they're concatenated together
       as-is, _with no separator_ between elements, before evaluation.
       The array form is often simpler for long hand-written queries.

       - `bind` = a single value valid as an argument for
       Stmt.bind(). This is _only_ applied to the _first_ non-empty
       statement in the SQL which has any bindable parameters. (Empty
       statements are skipped entirely.)

       - `saveSql` = an optional array. If set, the SQL of each
       executed statement is appended to this array before the
       statement is executed (but after it is prepared - we don't have
       the string until after that). Empty SQL statements are elided
       but can have odd effects in the output. e.g. SQL of: `"select
       1; -- empty\n; select 2"` will result in an array containing
       `["select 1;", "--empty \n; select 2"]`. That's simply how
       sqlite3 records the SQL for the 2nd statement.

       ==================================================================
       The following options apply _only_ to the _first_ statement
       which has a non-zero result column count, regardless of whether
       the statement actually produces any result rows.
       ==================================================================

       - `columnNames`: if this is an array, the column names of the
       result set are stored in this array before the callback (if
       any) is triggered (regardless of whether the query produces any
       result rows). If no statement has result columns, this value is
       unchanged. Achtung: an SQL result may have multiple columns
       with identical names.

       - `callback` = a function which gets called for each row of the
       result set, but only if that statement has any result rows. The
       callback's "this" is the options object, noting that this
       function synthesizes one if the caller does not pass one to
       exec(). The second argument passed to the callback is always
       the current Stmt object, as it's needed if the caller wants to
       fetch the column names or some such (noting that they could
       also be fetched via `this.columnNames`, if the client provides
       the `columnNames` option). If the callback returns a literal
       `false` (as opposed to any other falsy value, e.g. an implicit
       `undefined` return), any ongoing statement-`step()` iteration
       stops without an error. The return value of the callback is
       otherwise ignored.

       ACHTUNG: The callback MUST NOT modify the Stmt object. Calling
       any of the Stmt.get() variants, Stmt.getColumnName(), or
       similar, is legal, but calling step() or finalize() is
       not. Member methods which are illegal in this context will
       trigger an exception, but clients must also refrain from using
       any lower-level (C-style) APIs which might modify the
       statement.

       The first argument passed to the callback defaults to an array of
       values from the current result row but may be changed with ...

       - `rowMode` = specifies the type of he callback's first argument.
       It may be any of...

       A) A string describing what type of argument should be passed
       as the first argument to the callback:

         A.1) `'array'` (the default) causes the results of
         `stmt.get([])` to be passed to the `callback` and/or appended
         to `resultRows`.

         A.2) `'object'` causes the results of
         `stmt.get(Object.create(null))` to be passed to the
         `callback` and/or appended to `resultRows`.  Achtung: an SQL
         result may have multiple columns with identical names. In
         that case, the right-most column will be the one set in this
         object!

         A.3) `'stmt'` causes the current Stmt to be passed to the
         callback, but this mode will trigger an exception if
         `resultRows` is an array because appending the transient
         statement to the array would be downright unhelpful.

       B) An integer, indicating a zero-based column in the result
       row. Only that one single value will be passed on.

       C) A string with a minimum length of 2 and leading character of
       '$' will fetch the row as an object, extract that one field,
       and pass that field's value to the callback. Note that these
       keys are case-sensitive so must match the case used in the
       SQL. e.g. `"select a A from t"` with a `rowMode` of `'$A'`
       would work but `'$a'` would not. A reference to a column not in
       the result set will trigger an exception on the first row (as
       the check is not performed until rows are fetched).  Note also
       that `$` is a legal identifier character in JS so need not be
       quoted.

       Any other `rowMode` value triggers an exception.

       - `resultRows`: if this is an array, it functions similarly to
       the `callback` option: each row of the result set (if any),
       with the exception that the `rowMode` 'stmt' is not legal. It
       is legal to use both `resultRows` and `callback`, but
       `resultRows` is likely much simpler to use for small data sets
       and can be used over a WebWorker-style message interface.
       exec() throws if `resultRows` is set and `rowMode` is 'stmt'.

       - `returnValue`: is a string specifying what this function
       should return:

         A) The default value is (usually) `"this"`, meaning that the
            DB object itself should be returned. The exception is if
            the caller passes neither of `callback` nor `returnValue`
            but does pass an explicit `rowMode` then the default
            `returnValue` is `"resultRows"`, described below.

         B) `"resultRows"` means to return the value of the
            `resultRows` option. If `resultRows` is not set, this
            function behaves as if it were set to an empty array.

         C) `"saveSql"` means to return the value of the
            `saveSql` option. If `saveSql` is not set, this
            function behaves as if it were set to an empty array.

       Potential TODOs:

       - `bind`: permit an array of arrays/objects to bind. The first
       sub-array would act on the first statement which has bindable
       parameters (as it does now). The 2nd would act on the next such
       statement, etc.

       - `callback` and `resultRows`: permit an array entries with
       semantics similar to those described for `bind` above.

    */
    exec: function(/*(sql [,obj]) || (obj)*/){
      affirmDbOpen(this);
      const arg = parseExecArgs(this, arguments);
      if(!arg.sql){
        return toss3("exec() requires an SQL string.");
      }
      const opt = arg.opt;
      const callback = opt.callback;
      const resultRows =
            Array.isArray(opt.resultRows) ? opt.resultRows : undefined;
      let stmt;
      let bind = opt.bind;
      let evalFirstResult = !!(
        arg.cbArg || opt.columnNames || resultRows
      ) /* true to step through the first result-returning statement */;
      const stack = wasm.scopedAllocPush();
      const saveSql = Array.isArray(opt.saveSql) ? opt.saveSql : undefined;
      try{
        const isTA = util.isSQLableTypedArray(arg.sql)
        /* Optimization: if the SQL is a TypedArray we can save some string
           conversion costs. */;
        /* Allocate the two output pointers (ppStmt, pzTail) and heap
           space for the SQL (pSql). When prepare_v2() returns, pzTail
           will point to somewhere in pSql. */
        let sqlByteLen = isTA ? arg.sql.byteLength : wasm.jstrlen(arg.sql);
        const ppStmt  = wasm.scopedAlloc(
          /* output (sqlite3_stmt**) arg and pzTail */
          (2 * wasm.ptr.size) + (sqlByteLen + 1/* SQL + NUL */)
        );
        const pzTail = wasm.ptr.add(ppStmt, wasm.ptr.size) /* final arg to sqlite3_prepare_v2() */;
        let pSql = wasm.ptr.add(pzTail, wasm.ptr.size);
        const pSqlEnd = wasm.ptr.add(pSql, sqlByteLen);
        if(isTA) wasm.heap8().set(arg.sql, pSql);
        else wasm.jstrcpy(arg.sql, wasm.heap8(), pSql, sqlByteLen, false);
        wasm.poke(wasm.ptr.add(pSql, sqlByteLen), 0/*NUL terminator*/);
        while(pSql && wasm.peek(pSql, 'i8')
              /* Maintenance reminder:^^^ _must_ be 'i8' or else we
                 will very likely cause an endless loop. What that's
                 doing is checking for a terminating NUL byte. If we
                 use i32 or similar then we read 4 bytes, read stuff
                 around the NUL terminator, and get stuck in and
                 endless loop at the end of the SQL, endlessly
                 re-preparing an empty statement. */ ){
          wasm.pokePtr([ppStmt, pzTail], 0);
          DB.checkRc(this, capi.sqlite3_prepare_v3(
            this.pointer, pSql, sqlByteLen, 0, ppStmt, pzTail
          ));
          const pStmt = wasm.peekPtr(ppStmt);
          pSql = wasm.peekPtr(pzTail);
          sqlByteLen = Number(wasm.ptr.add(pSqlEnd,-pSql));
          if(!pStmt) continue;
          //sqlite3.config.debug("exec() pSql =",capi.sqlite3_sql(pStmt));
          if(saveSql) saveSql.push(capi.sqlite3_sql(pStmt).trim());
          stmt = new Stmt(this, pStmt, BindTypes);
          if(bind && stmt.parameterCount){
            stmt.bind(bind);
            bind = null;
          }
          if(evalFirstResult && stmt.columnCount){
            /* Only forward SELECT-style results for the FIRST query
               in the SQL which potentially has them. */
            let gotColNames = Array.isArray(
              opt.columnNames
              /* As reported in
                 https://sqlite.org/forum/forumpost/7774b773937cbe0a
                 we need to delay fetching of the column names until
                 after the first step() (if we step() at all) because
                 a schema change between the prepare() and step(), via
                 another connection, may invalidate the column count
                 and names. */) ? 0 : 1;
            evalFirstResult = false;
            if(arg.cbArg || resultRows){
              const cbArgCache = Object.create(null)
              /* 2nd arg for arg.cbArg, used by (at least) row-to-object
                 converter */;
              for( ; stmt.step(); __execLock.delete(stmt) ){
                if(0===gotColNames++){
                  stmt.getColumnNames(cbArgCache.columnNames = (opt.columnNames || []));
                }
                __execLock.add(stmt);
                const row = arg.cbArg(stmt,cbArgCache);
                if(resultRows) resultRows.push(row);
                if(callback && false === callback.call(opt, row, stmt)){
                  break;
                }
              }
              __execLock.delete(stmt);
            }
            if(0===gotColNames){
              /* opt.columnNames was provided but we visited no result rows */
              stmt.getColumnNames(opt.columnNames);
            }
          }else{
            stmt.step();
          }
          stmt.reset(
            /* In order to trigger an exception in the
               INSERT...RETURNING locking scenario:
               https://sqlite.org/forum/forumpost/36f7a2e7494897df
            */).finalize();
          stmt = null;
        }/*prepare() loop*/
      }/*catch(e){
        sqlite3.config.warn("DB.exec() is propagating exception",opt,e);
        throw e;
      }*/finally{
        if(stmt){
          __execLock.delete(stmt);
          stmt.finalize();
        }
        wasm.scopedAllocPop(stack);
      }
      return arg.returnVal();
    }/*exec()*/,

    /**
       Creates a new UDF (User-Defined Function) which is accessible
       via SQL code. This function may be called in any of the
       following forms:

       - (name, function)
       - (name, function, optionsObject)
       - (name, optionsObject)
       - (optionsObject)

       In the final two cases, the function must be defined as the
       `callback` property of the options object (optionally called
       `xFunc` to align with the C API documentation). In the final
       case, the function's name must be the 'name' property.

       The first two call forms can only be used for creating scalar
       functions. Creating an aggregate or window function requires
       the options-object form (see below for details).

       UDFs can be removed as documented for
       sqlite3_create_function_v2() and
       sqlite3_create_window_function(), but doing so will "leak" the
       JS-created WASM binding of those functions (meaning that their
       entries in the WASM indirect function table still
       exist). Eliminating that potential leak is a pending TODO.

       On success, returns this object. Throws on error.

       When called from SQL arguments to the UDF, and its result,
       will be converted between JS and SQL with as much fidelity as
       is feasible, triggering an exception if a type conversion
       cannot be determined. The docs for sqlite3_create_function_v2()
       describe the conversions in more detail.

       The values set in the options object differ for scalar and
       aggregate functions:

       - Scalar: set the `xFunc` function-type property to the UDF
         function.

       - Aggregate: set the `xStep` and `xFinal` function-type
         properties to the "step" and "final" callbacks for the
         aggregate. Do not set the `xFunc` property.

       - Window: set the `xStep`, `xFinal`, `xValue`, and `xInverse`
         function-type properties. Do not set the `xFunc` property.

       The options object may optionally have an `xDestroy`
       function-type property, as per sqlite3_create_function_v2().
       Its argument will be the WASM-pointer-type value of the `pApp`
       property, and this function will throw if `pApp` is defined but
       is not null, undefined, or a numeric (WASM pointer)
       value. i.e. `pApp`, if set, must be value suitable for use as a
       WASM pointer argument, noting that `null` or `undefined` will
       translate to 0 for that purpose.

       The options object may contain flags to modify how
       the function is defined:

       - `arity`: the number of arguments which SQL calls to this
       function expect or require. The default value is `xFunc.length`
       or `xStep.length` (i.e. the number of declared parameters it
       has) **MINUS 1** (see below for why). As a special case, if the
       `length` is 0, its arity is also 0 instead of -1. A negative
       arity value means that the function is variadic and may accept
       any number of arguments, up to sqlite3's compile-time
       limits. sqlite3 will enforce the argument count if is zero or
       greater. The callback always receives a pointer to an
       `sqlite3_context` object as its first argument. Any arguments
       after that are from SQL code. The leading context argument does
       _not_ count towards the function's arity. See the docs for
       sqlite3.capi.sqlite3_create_function_v2() for why that argument
       is needed in the interface.

       The following options-object properties correspond to flags
       documented at:

       https://sqlite.org/c3ref/create_function.html

       - `deterministic` = sqlite3.capi.SQLITE_DETERMINISTIC
       - `directOnly` = sqlite3.capi.SQLITE_DIRECTONLY
       - `innocuous` = sqlite3.capi.SQLITE_INNOCUOUS

       Sidebar: the ability to add new WASM-accessible functions to
       the runtime requires that the WASM build is compiled with the
       equivalent functionality as that provided by Emscripten's
       `-sALLOW_TABLE_GROWTH` flag.
    */
    createFunction: function f(name, xFunc, opt){
      const isFunc = (f)=>(f instanceof Function);
      switch(arguments.length){
          case 1: /* (optionsObject) */
            opt = name;
            name = opt.name;
            xFunc = opt.xFunc || 0;
            break;
          case 2: /* (name, callback|optionsObject) */
            if(!isFunc(xFunc)){
              opt = xFunc;
              xFunc = opt.xFunc || 0;
            }
            break;
          case 3: /* name, xFunc, opt */
            break;
          default: break;
      }
      if(!opt) opt = {};
      if('string' !== typeof name){
        toss3("Invalid arguments: missing function name.");
      }
      let xStep = opt.xStep || 0;
      let xFinal = opt.xFinal || 0;
      const xValue = opt.xValue || 0;
      const xInverse = opt.xInverse || 0;
      let isWindow = undefined;
      if(isFunc(xFunc)){
        isWindow = false;
        if(isFunc(xStep) || isFunc(xFinal)){
          toss3("Ambiguous arguments: scalar or aggregate?");
        }
        xStep = xFinal = null;
      }else if(isFunc(xStep)){
        if(!isFunc(xFinal)){
          toss3("Missing xFinal() callback for aggregate or window UDF.");
        }
        xFunc = null;
      }else if(isFunc(xFinal)){
        toss3("Missing xStep() callback for aggregate or window UDF.");
      }else{
        toss3("Missing function-type properties.");
      }
      if(false === isWindow){
        if(isFunc(xValue) || isFunc(xInverse)){
          toss3("xValue and xInverse are not permitted for non-window UDFs.");
        }
      }else if(isFunc(xValue)){
        if(!isFunc(xInverse)){
          toss3("xInverse must be provided if xValue is.");
        }
        isWindow = true;
      }else if(isFunc(xInverse)){
        toss3("xValue must be provided if xInverse is.");
      }
      const pApp = opt.pApp;
      if( undefined!==pApp
          && null!==pApp
          && !wasm.isPtr(pApp) ){
        toss3("Invalid value for pApp property. Must be a legal WASM pointer value.");
      }
      const xDestroy = opt.xDestroy || 0;
      if(xDestroy && !isFunc(xDestroy)){
        toss3("xDestroy property must be a function.");
      }
      let fFlags = 0 /*flags for sqlite3_create_function_v2()*/;
      if(getOwnOption(opt, 'deterministic')) fFlags |= capi.SQLITE_DETERMINISTIC;
      if(getOwnOption(opt, 'directOnly')) fFlags |= capi.SQLITE_DIRECTONLY;
      if(getOwnOption(opt, 'innocuous')) fFlags |= capi.SQLITE_INNOCUOUS;
      name = name.toLowerCase();
      const xArity = xFunc || xStep;
      const arity = getOwnOption(opt, 'arity');
      const arityArg = ('number'===typeof arity
                        ? arity
                        : (xArity.length ? xArity.length-1/*for pCtx arg*/ : 0));
      let rc;
      if( isWindow ){
        rc = capi.sqlite3_create_window_function(
          this.pointer, name, arityArg,
          capi.SQLITE_UTF8 | fFlags, pApp || 0,
          xStep, xFinal, xValue, xInverse, xDestroy);
      }else{
        rc = capi.sqlite3_create_function_v2(
          this.pointer, name, arityArg,
          capi.SQLITE_UTF8 | fFlags, pApp || 0,
          xFunc, xStep, xFinal, xDestroy);
      }
      DB.checkRc(this, rc);
      return this;
    }/*createFunction()*/,
    /**
       Prepares the given SQL, step()s it one time, and returns
       the value of the first result column. If it has no results,
       undefined is returned.

       If passed a second argument, it is treated like an argument
       to Stmt.bind(), so may be any type supported by that
       function. Passing the undefined value is the same as passing
       no value, which is useful when...

       If passed a 3rd argument, it is expected to be one of the
       SQLITE_{typename} constants. Passing the undefined value is
       the same as not passing a value.

       Throws on error (e.g. malformed SQL).
    */
    selectValue: function(sql,bind,asType){
      return __selectFirstRow(this, sql, bind, 0, asType);
    },

    /**
       Runs the given query and returns an array of the values from
       the first result column of each row of the result set. The 2nd
       argument is an optional value for use in a single-argument call
       to Stmt.bind(). The 3rd argument may be any value suitable for
       use as the 2nd argument to Stmt.get(). If a 3rd argument is
       desired but no bind data are needed, pass `undefined` for the 2nd
       argument.

       If there are no result rows, an empty array is returned.
    */
    selectValues: function(sql,bind,asType){
      const stmt = this.prepare(sql), rc = [];
      try {
        stmt.bind(bind);
        while(stmt.step()) rc.push(stmt.get(0,asType));
        stmt.reset(/*for INSERT...RETURNING locking case*/);
      }finally{
        stmt.finalize();
      }
      return rc;
    },

    /**
       Prepares the given SQL, step()s it one time, and returns an
       array containing the values of the first result row. If it has
       no results, `undefined` is returned.

       If passed a second argument other than `undefined`, it is
       treated like an argument to Stmt.bind(), so may be any type
       supported by that function.

       Throws on error (e.g. malformed SQL).
    */
    selectArray: function(sql,bind){
      return __selectFirstRow(this, sql, bind, []);
    },

    /**
       Prepares the given SQL, step()s it one time, and returns an
       object containing the key/value pairs of the first result
       row. If it has no results, `undefined` is returned.

       Note that the order of returned object's keys is not guaranteed
       to be the same as the order of the fields in the query string.

       If passed a second argument other than `undefined`, it is
       treated like an argument to Stmt.bind(), so may be any type
       supported by that function.

       Throws on error (e.g. malformed SQL).
    */
    selectObject: function(sql,bind){
      return __selectFirstRow(this, sql, bind, {});
    },

    /**
       Runs the given SQL and returns an array of all results, with
       each row represented as an array, as per the 'array' `rowMode`
       option to `exec()`. An empty result set resolves
       to an empty array. The second argument, if any, is treated as
       the 'bind' option to a call to exec().
    */
    selectArrays: function(sql,bind){
      return __selectAll(this, sql, bind, 'array');
    },

    /**
       Works identically to selectArrays() except that each value
       in the returned array is an object, as per the 'object' `rowMode`
       option to `exec()`.
    */
    selectObjects: function(sql,bind){
      return __selectAll(this, sql, bind, 'object');
    },

    /**
       Returns the number of currently-opened Stmt handles for this db
       handle, or 0 if this DB instance is closed. Note that only
       handles prepared via this.prepare() are counted, and not
       handles prepared using capi.sqlite3_prepare_v3() (or
       equivalent).
    */
    openStatementCount: function(){
      return this.pointer ? Object.keys(__stmtMap.get(this)).length : 0;
    },

    /**
       Starts a transaction, calls the given callback, and then either
       rolls back or commits the transaction, depending on whether the
       callback throws. The callback is passed this db object as its
       only argument. On success, returns the result of the
       callback. Throws on error.

       Note that transactions may not be nested, so this will throw if
       it is called recursively. For nested transactions, use the
       savepoint() method or manually manage SAVEPOINTs using exec().

       If called with 2 arguments, the first must be a keyword which
       is legal immediately after a BEGIN statement, e.g. one of
       "DEFERRED", "IMMEDIATE", or "EXCLUSIVE". Though the exact list
       of supported keywords is not hard-coded here, in order to be
       future-compatible, if the argument does not look like a single
       keyword then an exception is triggered with a description of
       the problem.
     */
    transaction: function(/* [beginQualifier,] */callback){
      let opener = 'BEGIN';
      if(arguments.length>1){
        if(/[^a-zA-Z]/.test(arguments[0])){
          toss3(capi.SQLITE_MISUSE, "Invalid argument for BEGIN qualifier.");
        }
        opener += ' '+arguments[0];
        callback = arguments[1];
      }
      affirmDbOpen(this).exec(opener);
      try {
        const rc = callback(this);
        this.exec("COMMIT");
        return rc;
      }catch(e){
        this.exec("ROLLBACK");
        throw e;
      }
    },

    /**
       This works similarly to transaction() but uses sqlite3's SAVEPOINT
       feature. This function starts a savepoint (with an unspecified name)
       and calls the given callback function, passing it this db object.
       If the callback returns, the savepoint is released (committed). If
       the callback throws, the savepoint is rolled back. If it does not
       throw, it returns the result of the callback.
    */
    savepoint: function(callback){
      affirmDbOpen(this).exec("SAVEPOINT oo1");
      try {
        const rc = callback(this);
        this.exec("RELEASE oo1");
        return rc;
      }catch(e){
        this.exec("ROLLBACK to SAVEPOINT oo1; RELEASE SAVEPOINT oo1");
        throw e;
      }
    },

    /**
       A convenience form of DB.checkRc(this,resultCode). If it does
       not throw, it returns this object.
    */
    checkRc: function(resultCode){
      return checkSqlite3Rc(this, resultCode);
    },
  }/*DB.prototype*/;

  /**
     Returns a new oo1.DB instance which wraps the given (sqlite3*)
     WASM pointer, optionally with or without taking over ownership of
     that pointer.

     The first argument must be either a non-NULL (sqlite3*) WASM
     pointer.

     The second argument, defaulting to false, specifies ownership of
     the first argument. If it is truthy, the returned object will
     pass that pointer to sqlite3_close() when its close() method is
     called, otherwise it will not.

     Throws if pDb is not a non-0 WASM pointer.

     The caller MUST GUARANTEE that the passed-in handle will outlive
     the returned object, i.e. that it will not be closed. If it is closed,
     this object will hold a stale pointer and results are undefined.

     Aside from its lifetime, the proxy is to be treated as any other
     DB instance, including the requirement of calling close() on
     it. close() will free up internal resources owned by the proxy
     and disassociate the proxy from that handle but will not
     actually close the proxied db handle unless this function is
     passed a thruthy second argument.

     To stress:

     - DO NOT call sqlite3_close() (or similar) on the being-proxied
       pointer while a proxy is active.

     - ALWAYS eventually call close() on the returned object. If the
       proxy does not own the underlying handle then its MUST be
       closed BEFORE the being-proxied handle is closed.

     Design notes:

     - wrapHandle() "could" accept a DB object instance as its first
       argument and proxy thatDb.pointer but there is currently no use
       case where doing so would be useful, so it does not allow
       that. That restriction may be lifted in a future version.
  */
  DB.wrapHandle = function(pDb, takeOwnership=false){
    if( !pDb || !wasm.isPtr(pDb) ){
      throw new sqlite3.SQLite3Error(capi.SQLITE_MISUSE,
                                     "Argument must be a WASM sqlite3 pointer");
    }
    return new DB({
      /* This ctor call style is very specifically internal-use-only.
         It is not documented and may change at any time. */
      "sqlite3*": pDb,
      "sqlite3*:takeOwnership": !!takeOwnership
    });
  };

  /** Throws if the given Stmt has been finalized, else stmt is
      returned. */
  const affirmStmtOpen = function(stmt){
    if(!stmt.pointer) toss3("Stmt has been closed.");
    return stmt;
  };

  /** Returns an opaque truthy value from the BindTypes
      enum if v's type is a valid bindable type, else
      returns a falsy value. As a special case, a value of
      undefined is treated as a bind type of null. */
  const isSupportedBindType = function(v){
    let t = BindTypes[(null===v||undefined===v) ? 'null' : typeof v];
    switch(t){
        case BindTypes.boolean:
        case BindTypes.null:
        case BindTypes.number:
        case BindTypes.string:
          return t;
        case BindTypes.bigint:
          return wasm.bigIntEnabled ? t : undefined;
        default:
          return util.isBindableTypedArray(v) ? BindTypes.blob : undefined;
    }
  };

  /**
     If isSupportedBindType(v) returns a truthy value, this
     function returns that value, else it throws.
  */
  const affirmSupportedBindType = function(v){
    //sqlite3.config.log('affirmSupportedBindType',v);
    return isSupportedBindType(v) || toss3("Unsupported bind() argument type:",typeof v);
  };

  /**
     If key is a number and within range of stmt's bound parameter
     count, key is returned.

     If key is not a number then it must be a JS string (not a WASM
     string) and it is checked against named parameters. If a match is
     found, its index is returned.

     Else it throws.
  */
  const affirmParamIndex = function(stmt,key){
    const n = ('number'===typeof key)
          ? key : capi.sqlite3_bind_parameter_index(stmt.pointer, key);
    if( 0===n || !util.isInt32(n) ) toss3("Invalid bind() parameter name: "+key);
    else if( n<1 || n>stmt.parameterCount ) toss3("Bind index",key,"is out of range.");
    return n;
  };

  /**
     Each Stmt object which is "locked" by DB.exec() gets an entry
     here to note that "lock".

     The reason this is in place is because exec({callback:...})'s
     callback gets access to the Stmt objects created internally by
     exec() but it must not use certain Stmt APIs.
  */
  const __execLock = new Set();
  /**
     This is a Stmt.get() counterpart of __execLock. Each time
     Stmt.step() returns true, the statement is added to this set,
     indicating that Stmt.get() is legal. Stmt APIs which invalidate
     that status remove the Stmt object from this set, which will
     cause Stmt.get() to throw with a descriptive error message
     instead of a more generic "API misuse" if we were to allow that
     call to reach the C API.
  */
  const __stmtMayGet = new Set();

  /**
     Stmt APIs which are prohibited on locked objects must call
     affirmNotLockedByExec() before doing any work.

     If __execLock.has(stmt) is truthy, this throws an exception
     complaining that the 2nd argument (an operation name,
     e.g. "bind()") is not legal while the statement is "locked".
     Locking happens before an exec()-like callback is passed a
     statement, to ensure that the callback does not mutate or
     finalize the statement. If it does not throw, it returns stmt.
  */
  const affirmNotLockedByExec = function(stmt,currentOpName){
    if(__execLock.has(stmt)){
      toss3("Operation is illegal when statement is locked:",currentOpName);
    }
    return stmt;
  };

  /**
     Binds a single bound parameter value on the given stmt at the
     given index (numeric or named) using the given bindType (see
     the BindTypes enum) and value. Throws on error. Returns stmt on
     success.
  */
  const bindOne = function f(stmt,ndx,bindType,val){
    affirmNotLockedByExec(affirmStmtOpen(stmt), 'bind()');
    if(!f._){
      f._tooBigInt = (v)=>toss3(
        "BigInt value is too big to store without precision loss:", v
      );
      f._ = {
        string: function(stmt, ndx, val, asBlob){
          const [pStr, n] = wasm.allocCString(val, true);
          const f = asBlob ? capi.sqlite3_bind_blob : capi.sqlite3_bind_text;
          return f(stmt.pointer, ndx, pStr, n, capi.SQLITE_WASM_DEALLOC);
        }
      };
    }/* static init */
    affirmSupportedBindType(val);
    ndx = affirmParamIndex(stmt,ndx);
    let rc = 0;
    switch((null===val || undefined===val) ? BindTypes.null : bindType){
        case BindTypes.null:
          rc = capi.sqlite3_bind_null(stmt.pointer, ndx);
          break;
        case BindTypes.string:
          rc = f._.string(stmt, ndx, val, false);
          break;
        case BindTypes.number: {
          let m;
          if(util.isInt32(val)) m = capi.sqlite3_bind_int;
          else if('bigint'===typeof val){
            if(!util.bigIntFits64(val)){
              f._tooBigInt(val);
            }else if(wasm.bigIntEnabled){
              m = capi.sqlite3_bind_int64;
            }else if(util.bigIntFitsDouble(val)){
              val = Number(val);
              m = capi.sqlite3_bind_double;
            }else{
              f._tooBigInt(val);
            }
          }else{ // !int32, !bigint
            val = Number(val);
            if(wasm.bigIntEnabled && Number.isInteger(val)){
              m = capi.sqlite3_bind_int64;
            }else{
              m = capi.sqlite3_bind_double;
            }
          }
          rc = m(stmt.pointer, ndx, val);
          break;
        }
        case BindTypes.boolean:
          rc = capi.sqlite3_bind_int(stmt.pointer, ndx, val ? 1 : 0);
          break;
        case BindTypes.blob: {
          if('string'===typeof val){
            rc = f._.string(stmt, ndx, val, true);
            break;
          }else if(val instanceof ArrayBuffer){
            val = new Uint8Array(val);
          }else if(!util.isBindableTypedArray(val)){
            toss3("Binding a value as a blob requires",
                  "that it be a string, Uint8Array, Int8Array, or ArrayBuffer.");
          }
          const pBlob = wasm.alloc(val.byteLength || 1);
          wasm.heap8().set(val.byteLength ? val : [0], Number(pBlob))
          rc = capi.sqlite3_bind_blob(stmt.pointer, ndx, pBlob, val.byteLength,
                                      capi.SQLITE_WASM_DEALLOC);
          break;
        }
        default:
          sqlite3.config.warn("Unsupported bind() argument type:",val);
          toss3("Unsupported bind() argument type: "+(typeof val));
    }
    if(rc) DB.checkRc(stmt.db.pointer, rc);
    return stmt;
  };

  Stmt.prototype = {
    /**
       "Finalizes" this statement. This is a no-op if the statement
       has already been finalized. Returns the result of
       sqlite3_finalize() (0 on success, non-0 on error), or the
       undefined value if the statement has already been
       finalized. Regardless of success or failure, most methods in
       this class will throw if called after this is.

       This method always throws if called when it is illegal to do
       so. Namely, when triggered via a per-row callback handler of a
       DB.exec() call.

       If Stmt does not own its underlying (sqlite3_stmt*) (see
       Stmt.wrapHandle()) then this function will not pass it to
       sqlite3_finalize().
    */
    finalize: function(){
      const ptr = this.pointer;
      if(ptr){
        affirmNotLockedByExec(this,'finalize()');
        const rc = (__doesNotOwnHandle.delete(this)
                    ? 0
                    : capi.sqlite3_finalize(ptr));
        delete __stmtMap.get(this.db)[ptr];
        __ptrMap.delete(this);
        __execLock.delete(this);
        __stmtMayGet.delete(this);
        delete this.parameterCount;
        delete this.db;
        return rc;
      }
    },
    /**
       Clears all bound values. Returns this object.  Throws if this
       statement has been finalized or if modification of the
       statement is currently illegal (e.g. in the per-row callback of
       a DB.exec() call).
    */
    clearBindings: function(){
      affirmNotLockedByExec(affirmStmtOpen(this), 'clearBindings()')
      capi.sqlite3_clear_bindings(this.pointer);
        __stmtMayGet.delete(this);
      return this;
    },
    /**
       Resets this statement so that it may be step()ed again from the
       beginning. Returns this object. Throws if this statement has
       been finalized, if it may not legally be reset because it is
       currently being used from a DB.exec() callback, or if the
       underlying call to sqlite3_reset() returns non-0.

       If passed a truthy argument then this.clearBindings() is
       also called, otherwise any existing bindings, along with
       any memory allocated for them, are retained.

       In versions 3.42.0 and earlier, this function did not throw if
       sqlite3_reset() returns non-0, but it was discovered that
       throwing (or significant extra client-side code) is necessary
       in order to avoid certain silent failure scenarios, as
       discussed at:

       https://sqlite.org/forum/forumpost/36f7a2e7494897df
    */
    reset: function(alsoClearBinds){
      affirmNotLockedByExec(this,'reset()');
      if(alsoClearBinds) this.clearBindings();
      const rc = capi.sqlite3_reset(affirmStmtOpen(this).pointer);
      __stmtMayGet.delete(this);
      checkSqlite3Rc(this.db, rc);
      return this;
    },
    /**
       Binds one or more values to its bindable parameters. It
       accepts 1 or 2 arguments:

       If passed a single argument, it must be either an array, an
       object, or a value of a bindable type (see below).

       If passed 2 arguments, the first one is the 1-based bind
       index or bindable parameter name and the second one must be
       a value of a bindable type.

       Bindable value types:

       - null is bound as NULL.

       - undefined as a standalone value is a no-op intended to
         simplify certain client-side use cases: passing undefined as
         a value to this function will not actually bind anything and
         this function will skip confirmation that binding is even
         legal. (Those semantics simplify certain client-side uses.)
         Conversely, a value of undefined as an array or object
         property when binding an array/object (see below) is treated
         the same as null.

       - Numbers are bound as either doubles or integers: doubles if
         they are larger than 32 bits, else double or int32, depending
         on whether they have a fractional part. Booleans are bound as
         integer 0 or 1. It is not expected the distinction of binding
         doubles which have no fractional parts and integers is
         significant for the majority of clients due to sqlite3's data
         typing model. If BigInt support is enabled then this routine
         will bind BigInt values as 64-bit integers if they'll fit in
         64 bits. If that support disabled, it will store the BigInt
         as an int32 or a double if it can do so without loss of
         precision. If the BigInt is _too BigInt_ then it will throw.

       - Strings are bound as strings (use bindAsBlob() to force
         blob binding).

       - Uint8Array, Int8Array, and ArrayBuffer instances are bound as
         blobs.

       If passed an array, each element of the array is bound at
       the parameter index equal to the array index plus 1
       (because arrays are 0-based but binding is 1-based).

       If passed an object, each object key is treated as a
       bindable parameter name. The object keys _must_ match any
       bindable parameter names, including any `$`, `@`, or `:`
       prefix. Because `$` is a legal identifier chararacter in
       JavaScript, that is the suggested prefix for bindable
       parameters: `stmt.bind({$a: 1, $b: 2})`.

       It returns this object on success and throws on
       error. Errors include:

       - Any bind index is out of range, a named bind parameter
       does not match, or this statement has no bindable
       parameters.

       - Any value to bind is of an unsupported type.

       - Passed no arguments or more than two.

       - The statement has been finalized.
    */
    bind: function(/*[ndx,] arg*/){
      affirmStmtOpen(this);
      let ndx, arg;
      switch(arguments.length){
          case 1: ndx = 1; arg = arguments[0]; break;
          case 2: ndx = arguments[0]; arg = arguments[1]; break;
          default: toss3("Invalid bind() arguments.");
      }
      if(undefined===arg){
        /* It might seem intuitive to bind undefined as NULL
           but this approach simplifies certain client-side
           uses when passing on arguments between 2+ levels of
           functions. */
        return this;
      }else if(!this.parameterCount){
        toss3("This statement has no bindable parameters.");
      }
      __stmtMayGet.delete(this);
      if(null===arg){
        /* bind NULL */
        return bindOne(this, ndx, BindTypes.null, arg);
      }
      else if(Array.isArray(arg)){
        /* bind each entry by index */
        if(1!==arguments.length){
          toss3("When binding an array, an index argument is not permitted.");
        }
        arg.forEach((v,i)=>bindOne(this, i+1, affirmSupportedBindType(v), v));
        return this;
      }else if(arg instanceof ArrayBuffer){
        arg = new Uint8Array(arg);
      }
      if('object'===typeof arg/*null was checked above*/
              && !util.isBindableTypedArray(arg)){
        /* Treat each property of arg as a named bound parameter. */
        if(1!==arguments.length){
          toss3("When binding an object, an index argument is not permitted.");
        }
        Object.keys(arg)
          .forEach(k=>bindOne(this, k,
                              affirmSupportedBindType(arg[k]),
                              arg[k]));
        return this;
      }else{
        return bindOne(this, ndx, affirmSupportedBindType(arg), arg);
      }
      toss3("Should not reach this point.");
    },
    /**
       Special case of bind() which binds the given value using the
       BLOB binding mechanism instead of the default selected one for
       the value. The ndx may be a numbered or named bind index. The
       value must be of type string, null/undefined (both get treated
       as null), or a TypedArray of a type supported by the bind()
       API. This API cannot bind numbers as blobs.

       If passed a single argument, a bind index of 1 is assumed and
       the first argument is the value.
    */
    bindAsBlob: function(ndx,arg){
      affirmStmtOpen(this);
      if(1===arguments.length){
        arg = ndx;
        ndx = 1;
      }
      const t = affirmSupportedBindType(arg);
      if(BindTypes.string !== t && BindTypes.blob !== t
         && BindTypes.null !== t){
        toss3("Invalid value type for bindAsBlob()");
      }
      return bindOne(this, ndx, BindTypes.blob, arg);
    },
    /**
       Steps the statement one time. If the result indicates that a
       row of data is available, a truthy value is returned.  If no
       row of data is available, a falsy value is returned.  Throws on
       error.
    */
    step: function(){
      affirmNotLockedByExec(this, 'step()');
      const rc = capi.sqlite3_step(affirmStmtOpen(this).pointer);
      switch(rc){
        case capi.SQLITE_DONE:
          __stmtMayGet.delete(this);
          return false;
        case capi.SQLITE_ROW:
          __stmtMayGet.add(this);
          return true;
        default:
          __stmtMayGet.delete(this);
          sqlite3.config.warn("sqlite3_step() rc=",rc,
                              capi.sqlite3_js_rc_str(rc),
                              "SQL =", capi.sqlite3_sql(this.pointer));
          DB.checkRc(this.db.pointer, rc);
      }
    },
    /**
       Functions exactly like step() except that...

       1) On success, it calls this.reset() and returns this object.

       2) On error, it throws and does not call reset().

       This is intended to simplify constructs like:

       ```
       for(...) {
         stmt.bind(...).stepReset();
       }
       ```

       Note that the reset() call makes it illegal to call this.get()
       after the step.
    */
    stepReset: function(){
      this.step();
      return this.reset();
    },
    /**
       Functions like step() except that it calls finalize() on this
       statement immediately after stepping, even if the step() call
       throws.

       On success, it returns true if the step indicated that a row of
       data was available, else it returns a falsy value.

       This is intended to simplify use cases such as:

       ```
       aDb.prepare("insert into foo(a) values(?)").bind(123).stepFinalize();
       ```
    */
    stepFinalize: function(){
      try{
        const rc = this.step();
        this.reset(/*for INSERT...RETURNING locking case*/);
        return rc;
      }finally{
        try{this.finalize()}
        catch(e){/*ignored*/}
      }
    },

    /**
       Fetches the value from the given 0-based column index of
       the current data row, throwing if index is out of range.

       Requires that step() has just returned a truthy value, else
       an exception is thrown.

       By default it will determine the data type of the result
       automatically. If passed a second argument, it must be one
       of the enumeration values for sqlite3 types, which are
       defined as members of the sqlite3 module: SQLITE_INTEGER,
       SQLITE_FLOAT, SQLITE_TEXT, SQLITE_BLOB. Any other value,
       except for undefined, will trigger an exception. Passing
       undefined is the same as not passing a value. It is legal
       to, e.g., fetch an integer value as a string, in which case
       sqlite3 will convert the value to a string.

       If ndx is an array, this function behaves a differently: it
       assigns the indexes of the array, from 0 to the number of
       result columns, to the values of the corresponding column,
       and returns that array.

       If ndx is a plain object, this function behaves even
       differentlier: it assigns the properties of the object to
       the values of their corresponding result columns and returns
       that object.

       Blobs are returned as Uint8Array instances.

       Potential TODO: add type ID SQLITE_JSON, which fetches the
       result as a string and passes it (if it's not null) to
       JSON.parse(), returning the result of that. Until then,
       getJSON() can be used for that.
    */
    get: function(ndx,asType){
      if(!__stmtMayGet.has(affirmStmtOpen(this))){
        toss3("Stmt.step() has not (recently) returned true.");
      }
      if(Array.isArray(ndx)){
        let i = 0;
        const n = this.columnCount;
        while(i<n){
          ndx[i] = this.get(i++);
        }
        return ndx;
      }else if(ndx && 'object'===typeof ndx){
        let i = 0;
        const n = this.columnCount;
        while(i<n){
          ndx[capi.sqlite3_column_name(this.pointer,i)] = this.get(i++);
        }
        return ndx;
      }
      affirmColIndex(this, ndx);
      switch(undefined===asType
             ? capi.sqlite3_column_type(this.pointer, ndx)
             : asType){
          case capi.SQLITE_NULL: return null;
          case capi.SQLITE_INTEGER:{
            if(wasm.bigIntEnabled){
              const rc = capi.sqlite3_column_int64(this.pointer, ndx);
              if(rc>=Number.MIN_SAFE_INTEGER && rc<=Number.MAX_SAFE_INTEGER){
                /* Coerce "normal" number ranges to normal number values,
                   and only return BigInt-type values for numbers out of this
                   range. */
                return Number(rc).valueOf();
              }
              return rc;
            }else{
              const rc = capi.sqlite3_column_double(this.pointer, ndx);
              if(rc>Number.MAX_SAFE_INTEGER || rc<Number.MIN_SAFE_INTEGER){
                /* Throwing here is arguable but, since we're explicitly
                   extracting an SQLITE_INTEGER-type value, it seems fair to throw
                   if the extracted number is out of range for that type.
                   This policy may be laxened to simply pass on the number and
                   hope for the best, as the C API would do. */
                toss3("Integer is out of range for JS integer range: "+rc);
              }
              //sqlite3.config.log("get integer rc=",rc,isInt32(rc));
              return util.isInt32(rc) ? (rc | 0) : rc;
            }
          }
          case capi.SQLITE_FLOAT:
            return capi.sqlite3_column_double(this.pointer, ndx);
          case capi.SQLITE_TEXT:
            return capi.sqlite3_column_text(this.pointer, ndx);
          case capi.SQLITE_BLOB: {
            const n = capi.sqlite3_column_bytes(this.pointer, ndx),
                  ptr = capi.sqlite3_column_blob(this.pointer, ndx),
                  rc = new Uint8Array(n);
            if(n){
              rc.set(wasm.heap8u().slice(Number(ptr), Number(ptr)+n), 0);
              if(this.db._blobXfer instanceof Array){
                /* This is an optimization soley for the Worker1 API. It
                   will transfer these to the main thread directly
                   instead of copying them. */
                this.db._blobXfer.push(rc.buffer);
              }
            }
            return rc;
          }
          default: toss3("Don't know how to translate",
                         "type of result column #"+ndx+".");
      }
      toss3("Not reached.");
    },
    /** Equivalent to get(ndx) but coerces the result to an
        integer. */
    getInt: function(ndx){return this.get(ndx,capi.SQLITE_INTEGER)},
    /** Equivalent to get(ndx) but coerces the result to a
        float. */
    getFloat: function(ndx){return this.get(ndx,capi.SQLITE_FLOAT)},
    /** Equivalent to get(ndx) but coerces the result to a
        string. */
    getString: function(ndx){return this.get(ndx,capi.SQLITE_TEXT)},
    /** Equivalent to get(ndx) but coerces the result to a
        Uint8Array. */
    getBlob: function(ndx){return this.get(ndx,capi.SQLITE_BLOB)},
    /**
       A convenience wrapper around get() which fetches the value
       as a string and then, if it is not null, passes it to
       JSON.parse(), returning that result. Throws if parsing
       fails. If the result is null, null is returned. An empty
       string, on the other hand, will trigger an exception.
    */
    getJSON: function(ndx){
      const s = this.get(ndx, capi.SQLITE_STRING);
      return null===s ? s : JSON.parse(s);
    },
    // Design note: the only reason most of these getters have a 'get'
    // prefix is for consistency with getVALUE_TYPE().  The latter
    // arguably really need that prefix for API readability and the
    // rest arguably don't, but consistency is a powerful thing.
    /**
       Returns the result column name of the given index, or
       throws if index is out of bounds or this statement has been
       finalized. This can be used without having run step()
       first.
    */
    getColumnName: function(ndx){
      return capi.sqlite3_column_name(
        affirmColIndex(affirmStmtOpen(this),ndx).pointer, ndx
      );
    },
    /**
       If this statement potentially has result columns, this function
       returns an array of all such names. If passed an array, it is
       used as the target and all names are appended to it. Returns
       the target array. Throws if this statement cannot have result
       columns. This object's columnCount property holds the number of
       columns.
    */
    getColumnNames: function(tgt=[]){
      affirmColIndex(affirmStmtOpen(this),0);
      const n = this.columnCount;
      for(let i = 0; i < n; ++i){
        tgt.push(capi.sqlite3_column_name(this.pointer, i));
      }
      return tgt;
    },
    /**
       If this statement has named bindable parameters and the
       given name matches one, its 1-based bind index is
       returned. If no match is found, 0 is returned. If it has no
       bindable parameters, the undefined value is returned.
    */
    getParamIndex: function(name){
      return (affirmStmtOpen(this).parameterCount
              ? capi.sqlite3_bind_parameter_index(this.pointer, name)
              : undefined);
    },
    /**
       If this statement has named bindable parameters and the given
       index refers to one, its name is returned, else null is
       returned. If this statement has no bound parameters, undefined
       is returned.

       Added in 3.47.
    */
    getParamName: function(ndx){
      return (affirmStmtOpen(this).parameterCount
              ? capi.sqlite3_bind_parameter_name(this.pointer, ndx)
              : undefined);
    },

    /**
       Behaves like sqlite3_stmt_busy() but throws if this statement
       is closed and returns a value of type boolean instead of integer.

       Added in 3.47.
    */
    isBusy: function(){
      return 0!==capi.sqlite3_stmt_busy(affirmStmtOpen(this));
    },

    /**
       Behaves like sqlite3_stmt_readonly() but throws if this statement
       is closed and returns a value of type boolean instead of integer.

       Added in 3.47.
    */
    isReadOnly: function(){
      return 0!==capi.sqlite3_stmt_readonly(affirmStmtOpen(this));
    }
  }/*Stmt.prototype*/;

  {/* Add the `pointer` property to DB and Stmt. */
    const prop = {
      enumerable: true,
      get: function(){return __ptrMap.get(this)},
      set: ()=>toss3("The pointer property is read-only.")
    }
    Object.defineProperty(Stmt.prototype, 'pointer', prop);
    Object.defineProperty(DB.prototype, 'pointer', prop);
  }
  /**
     Stmt.columnCount is an interceptor for sqlite3_column_count().

     This requires an unfortunate performance hit compared to caching
     columnCount when the Stmt is created/prepared (as was done in
     SQLite <=3.42.0), but is necessary in order to handle certain
     corner cases, as described in
     https://sqlite.org/forum/forumpost/7774b773937cbe0a.
  */
  Object.defineProperty(Stmt.prototype, 'columnCount', {
    enumerable: false,
    get: function(){return capi.sqlite3_column_count(this.pointer)},
    set: ()=>toss3("The columnCount property is read-only.")
  });

  Object.defineProperty(Stmt.prototype, 'parameterCount', {
    enumerable: false,
    get: function(){return capi.sqlite3_bind_parameter_count(this.pointer)},
    set: ()=>toss3("The parameterCount property is read-only.")
  });

  /**
     The Stmt counterpart of oo1.DB.wrapHandle(), this creates a Stmt
     instance which wraps a WASM (sqlite3_stmt*) in the oo1 API,
     optionally with or without taking over ownership of that pointer.

     The first argument must be an oo1.DB instance[^1].

     The second argument must be a valid WASM (sqlite3_stmt*), as
     produced by sqlite3_prepare_v2() and sqlite3_prepare_v3().

     The third argument, defaulting to false, specifies whether the
     returned Stmt object takes over ownership of the underlying
     (sqlite3_stmt*). If true, the returned object's finalize() method
     will finalize that handle, else it will not. If it is false,
     ownership of pStmt is unchanged and pStmt MUST outlive the
     returned object or results are undefined.

     This function throws if the arguments are invalid. On success it
     returns a new Stmt object which wraps the given statement
     pointer.

     Like all Stmt objects, the finalize() method must eventually be
     called on the returned object to free up internal resources,
     regardless of whether this function's third argument is true or
     not.

     [^1]: The first argument cannot be a (sqlite3*) because the
     resulting Stmt object requires a parent DB object. It is not yet
     determined whether it would be of general benefit to refactor the
     DB/Stmt pair internals to communicate in terms of the underlying
     (sqlite3*) rather than a DB object. If so, we could laxen the
     first argument's requirement and allow an (sqlite3*). Because
     DB.wrapHandle() enables multiple DB objects to proxy the same
     (sqlite3*), we cannot unambiguously translate the first arugment
     from (sqlite3*) to DB instances for us with this function's first
     argument.
  */
  Stmt.wrapHandle = function(oo1db, pStmt, takeOwnership=false){
    let ctor = Stmt;
    if( !(oo1db instanceof DB) || !oo1db.pointer ){
      throw new sqlite3.SQLite3Error(sqlite3.SQLITE_MISUSE,
                                     "First argument must be an opened "+
                                     "sqlite3.oo1.DB instance");
    }
    if( !pStmt || !wasm.isPtr(pStmt) ){
      throw new sqlite3.SQLite3Error(sqlite3.SQLITE_MISUSE,
                                     "Second argument must be a WASM "+
                                     "sqlite3_stmt pointer");
    }
    return new Stmt(oo1db, pStmt, BindTypes, !!takeOwnership);
  }

  /** The OO API's public namespace. */
  sqlite3.oo1 = {
    DB,
    Stmt
  }/*oo1 object*/;

  if(util.isUIThread()){
    /**
       Functionally equivalent to DB(storageName,'c','kvvfs') except
       that it throws if the given storage name is not one of 'local'
       or 'session'.

       As of version 3.46, the argument may optionally be an options
       object in the form:

       {
         filename: 'session'|'local',
         ... etc. (all options supported by the DB ctor)
       }

       noting that the 'vfs' option supported by main DB
       constructor is ignored here: the vfs is always 'kvvfs'.
    */
    sqlite3.oo1.JsStorageDb = function(storageName='session'){
      const opt = dbCtorHelper.normalizeArgs(...arguments);
      storageName = opt.filename;
      if('session'!==storageName && 'local'!==storageName){
        toss3("JsStorageDb db name must be one of 'session' or 'local'.");
      }
      opt.vfs = 'kvvfs';
      dbCtorHelper.call(this, opt);
    };
    const jdb = sqlite3.oo1.JsStorageDb;
    jdb.prototype = Object.create(DB.prototype);
    /** Equivalent to sqlite3_js_kvvfs_clear(). */
    jdb.clearStorage = capi.sqlite3_js_kvvfs_clear;
    /**
       Clears this database instance's storage or throws if this
       instance has been closed. Returns the number of
       database blocks which were cleaned up.
    */
    jdb.prototype.clearStorage = function(){
      return jdb.clearStorage(affirmDbOpen(this).filename);
    };
    /** Equivalent to sqlite3_js_kvvfs_size(). */
    jdb.storageSize = capi.sqlite3_js_kvvfs_size;
    /**
       Returns the _approximate_ number of bytes this database takes
       up in its storage or throws if this instance has been closed.
    */
    jdb.prototype.storageSize = function(){
      return jdb.storageSize(affirmDbOpen(this).filename);
    };
  }/*main-window-only bits*/

});
//#else
/* Built with the omit-oo1 flag. */
//#endif if not omit-oo1
