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
  and it installs its deliverable as self.sqlite3.oo1.
*/
(function(self){
  const toss = (...args)=>{throw new Error(args.join(' '))};

  const sqlite3 = self.sqlite3 || toss("Missing main sqlite3 object.");
  const capi = sqlite3.capi, util = capi.util;
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
     capi.wasm.xWrap() function signatures which know how to extract
     it.
  */
  const __ptrMap = new WeakMap();
  /**
     Map of DB instances to objects, each object being a map of UDF
     names to wasm function _pointers_ added to that DB handle via
     createFunction().
  */
  const __udfMap = new WeakMap();
  /**
     Map of DB instances to objects, each object being a map of Stmt
     wasm pointers to Stmt objects.
  */
  const __stmtMap = new WeakMap();

  /** If object opts has _its own_ property named p then that
      property's value is returned, else dflt is returned. */
  const getOwnOption = (opts, p, dflt)=>
        opts.hasOwnProperty(p) ? opts[p] : dflt;

  /**
     An Error subclass specifically for reporting DB-level errors and
     enabling clients to unambiguously identify such exceptions.
  */
  class SQLite3Error extends Error {
    constructor(...args){
      super(...args);
      this.name = 'SQLite3Error';
    }
  };
  const toss3 = (...args)=>{throw new SQLite3Error(args)};
  sqlite3.SQLite3Error = SQLite3Error;

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

     The db is currently opened with a fixed set of flags:
     (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
     SQLITE_OPEN_EXRESCODE).  This API will change in the future
     permit the caller to provide those flags via an additional
     argument.

     For purposes of passing a DB instance to C-style sqlite3
     functions, its read-only `pointer` property holds its `sqlite3*`
     pointer value. That property can also be used to check whether
     this DB instance is still open.
  */
  const DB = function ctor(fn=':memory:'){
    if('string'!==typeof fn){
      toss3("Invalid filename for DB constructor.");
    }
    const stack = capi.wasm.scopedAllocPush();
    let ptr;
    try {
      const ppDb = capi.wasm.scopedAllocPtr() /* output (sqlite3**) arg */;
      const rc = capi.sqlite3_open_v2(fn, ppDb, capi.SQLITE_OPEN_READWRITE
                                      | capi.SQLITE_OPEN_CREATE
                                      | capi.SQLITE_OPEN_EXRESCODE, null);
      ptr = capi.wasm.getMemValue(ppDb, '*');
      ctor.checkRc(ptr, rc);
    }catch(e){
      if(ptr) capi.sqlite3_close_v2(ptr);
      throw e;
    }
    finally{capi.wasm.scopedAllocPop(stack);}
    this.filename = fn;
    __ptrMap.set(this, ptr);
    __stmtMap.set(this, Object.create(null));
    __udfMap.set(this, Object.create(null));
  };

  /**
     Internal-use enum for mapping JS types to DB-bindable types.
     These do not (and need not) line up with the SQLITE_type
     values. All values in this enum must be truthy and distinct
     but they need not be numbers.
  */
  const BindTypes = {
    null: 1,
    number: 2,
    string: 3,
    boolean: 4,
    blob: 5
  };
  BindTypes['undefined'] == BindTypes.null;
  if(capi.wasm.bigIntEnabled){
    BindTypes.bigint = BindTypes.number;
  }

  /**
     This class wraps sqlite3_stmt. Calling this constructor
     directly will trigger an exception. Use DB.prepare() to create
     new instances.

     For purposes of passing a Stmt instance to C-style sqlite3
     functions, its read-only `pointer` property holds its `sqlite3_stmt*`
     pointer value.
  */
  const Stmt = function(){
    if(BindTypes!==arguments[2]){
      toss3("Do not call the Stmt constructor directly. Use DB.prepare().");
    }
    this.db = arguments[0];
    __ptrMap.set(this, arguments[1]);
    this.columnCount = capi.sqlite3_column_count(this.pointer);
    this.parameterCount = capi.sqlite3_bind_parameter_count(this.pointer);
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
      index.
  */
  const affirmColIndex = function(stmt,ndx){
    if((ndx !== (ndx|0)) || ndx<0 || ndx>=stmt.columnCount){
      toss3("Column index",ndx,"is out of range.");
    }
    return stmt;
  };

  /**
     Expects to be passed (arguments) from DB.exec() and
     DB.execMulti(). Does the argument processing/validation, throws
     on error, and returns a new object on success:

     { sql: the SQL, opt: optionsObj, cbArg: function}

     cbArg is only set if the opt.callback is set, in which case
     it's a function which expects to be passed the current Stmt
     and returns the callback argument of the type indicated by
     the input arguments.
  */
  const parseExecArgs = function(args){
    const out = Object.create(null);
    out.opt = Object.create(null);
    switch(args.length){
        case 1:
          if('string'===typeof args[0] || util.isSQLableTypedArray(args[0])){
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
    if(util.isSQLableTypedArray(out.sql)){
      out.sql = util.typedArrayToString(out.sql);
    }else if(Array.isArray(out.sql)){
      out.sql = out.sql.join('');
    }else if('string'!==typeof out.sql){
      toss3("Missing SQL argument.");
    }
    if(out.opt.callback || out.opt.resultRows){
      switch((undefined===out.opt.rowMode)
             ? 'stmt' : out.opt.rowMode) {
          case 'object': out.cbArg = (stmt)=>stmt.get({}); break;
          case 'array': out.cbArg = (stmt)=>stmt.get([]); break;
          case 'stmt':
            if(Array.isArray(out.opt.resultRows)){
              toss3("Invalid rowMode for resultRows array: must",
                    "be one of 'array', 'object',",
                    "or a result column number.");
            }
            out.cbArg = (stmt)=>stmt;
            break;
          default:
            if(util.isInt32(out.opt.rowMode)){
              out.cbArg = (stmt)=>stmt.get(out.opt.rowMode);
              break;
            }
            toss3("Invalid rowMode:",out.opt.rowMode);
      }
    }
    return out;
  };

  /**
     Expects to be given a DB instance or an `sqlite3*` pointer, and an
     sqlite3 API result code. If the result code is not falsy, this
     function throws an SQLite3Error with an error message from
     sqlite3_errmsg(), using dbPtr as the db handle. Note that if it's
     passed a non-error code like SQLITE_ROW or SQLITE_DONE, it will
     still throw but the error string might be "Not an error."  The
     various non-0 non-error codes need to be checked for in client
     code where they are expected.
  */
  DB.checkRc = function(dbPtr, sqliteResultCode){
    if(sqliteResultCode){
      if(dbPtr instanceof DB) dbPtr = dbPtr.pointer;
      throw new SQLite3Error([
        "sqlite result code",sqliteResultCode+":",
        capi.sqlite3_errmsg(dbPtr) || "Unknown db error."
      ].join(' '));
    }
  };

  DB.prototype = {
    /**
       Finalizes all open statements and closes this database
       connection. This is a no-op if the db has already been
       closed. After calling close(), `this.pointer` will resolve to
       `undefined`, so that can be used to check whether the db
       instance is still opened.
    */
    close: function(){
      if(this.pointer){
        const pDb = this.pointer;
        let s;
        const that = this;
        Object.keys(__stmtMap.get(this)).forEach((k,s)=>{
          if(s && s.pointer) s.finalize();
        });
        Object.values(__udfMap.get(this)).forEach(
          capi.wasm.uninstallFunction.bind(capi.wasm)
        );
        __ptrMap.delete(this);
        __stmtMap.delete(this);
        __udfMap.delete(this);
        capi.sqlite3_close_v2(pDb);
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
       Similar to this.filename but will return NULL for
       special names like ":memory:". Not of much use until
       we have filesystem support. Throws if the DB has
       been closed. If passed an argument it then it will return
       the filename of the ATTACHEd db with that name, else it assumes
       a name of `main`.
    */
    fileName: function(dbName){
      return capi.sqlite3_db_filename(affirmDbOpen(this).pointer, dbName||"main");
    },
    /**
       Returns true if this db instance has a name which resolves to a
       file. If the name is "" or ":memory:", it resolves to false.
       Note that it is not aware of the peculiarities of URI-style
       names and a URI-style name for a ":memory:" db will fool it.
    */
    hasFilename: function(){
      const fn = this.filename;
      if(!fn || ':memory'===fn) return false;
      return true;
    },
    /**
       Returns the name of the given 0-based db number, as documented
       for sqlite3_db_name().
    */
    dbName: function(dbNumber=0){
      return capi.sqlite3_db_name(affirmDbOpen(this).pointer, dbNumber);
    },
    /**
       Compiles the given SQL and returns a prepared Stmt. This is
       the only way to create new Stmt objects. Throws on error.

       The given SQL must be a string, a Uint8Array holding SQL, or a
       WASM pointer to memory holding the NUL-terminated SQL string.
       If the SQL contains no statements, an SQLite3Error is thrown.

       Design note: the C API permits empty SQL, reporting it as a 0
       result code and a NULL stmt pointer. Supporting that case here
       would cause extra work for all clients: any use of the Stmt API
       on such a statement will necessarily throw, so clients would be
       required to check `stmt.pointer` after calling `prepare()` in
       order to determine whether the Stmt instance is empty or not.
       Long-time practice (with other sqlite3 script bindings)
       suggests that the empty-prepare case is sufficiently rare (and
       useless) that supporting it here would simply hurt overall
       usability.
    */
    prepare: function(sql){
      affirmDbOpen(this);
      const stack = capi.wasm.scopedAllocPush();
      let ppStmt, pStmt;
      try{
        ppStmt = capi.wasm.scopedAllocPtr()/* output (sqlite3_stmt**) arg */;
        DB.checkRc(this, capi.sqlite3_prepare_v2(this.pointer, sql, -1, ppStmt, null));
        pStmt = capi.wasm.getMemValue(ppStmt, '*');
      }
      finally {capi.wasm.scopedAllocPop(stack)}
      if(!pStmt) toss3("Cannot prepare empty SQL.");
      const stmt = new Stmt(this, pStmt, BindTypes);
      __stmtMap.get(this)[pStmt] = stmt;
      return stmt;
    },
    /**
       This function works like execMulti(), and takes most of the
       same arguments, but is more efficient (performs much less
       work) when the input SQL is only a single statement. If
       passed a multi-statement SQL, it only processes the first
       one.

       This function supports the following additional options not
       supported by execMulti():

       - .multi: if true, this function acts as a proxy for
       execMulti() and behaves identically to that function.

       - .columnNames: if this is an array and the query has
       result columns, the array is passed to
       Stmt.getColumnNames() to append the column names to it
       (regardless of whether the query produces any result
       rows). If the query has no result columns, this value is
       unchanged.

       The following options to execMulti() are _not_ supported by
       this method (they are simply ignored):

       - .saveSql
    */
    exec: function(/*(sql [,optionsObj]) or (optionsObj)*/){
      affirmDbOpen(this);
      const arg = parseExecArgs(arguments);
      if(!arg.sql) return this;
      else if(arg.opt.multi){
        return this.execMulti(arg, undefined, BindTypes);
      }
      const opt = arg.opt;
      let stmt, rowTarget;
      try {
        if(Array.isArray(opt.resultRows)){
          rowTarget = opt.resultRows;
        }
        stmt = this.prepare(arg.sql);
        if(stmt.columnCount && Array.isArray(opt.columnNames)){
          stmt.getColumnNames(opt.columnNames);
        }
        if(opt.bind) stmt.bind(opt.bind);
        if(opt.callback || rowTarget){
          while(stmt.step()){
            const row = arg.cbArg(stmt);
            if(rowTarget) rowTarget.push(row);
            if(opt.callback){
              stmt._isLocked = true;
              opt.callback(row, stmt);
              stmt._isLocked = false;
            }
          }
        }else{
          stmt.step();
        }
      }finally{
        if(stmt){
          delete stmt._isLocked;
          stmt.finalize();
        }
      }
      return this;
    }/*exec()*/,
    /**
       Executes one or more SQL statements in the form of a single
       string. Its arguments must be either (sql,optionsObject) or
       (optionsObject). In the latter case, optionsObject.sql
       must contain the SQL to execute. Returns this
       object. Throws on error.

       If no SQL is provided, or a non-string is provided, an
       exception is triggered. Empty SQL, on the other hand, is
       simply a no-op.

       The optional options object may contain any of the following
       properties:

       - .sql = the SQL to run (unless it's provided as the first
       argument). This must be of type string, Uint8Array, or an
       array of strings (in which case they're concatenated
       together as-is, with no separator between elements,
       before evaluation).

       - .bind = a single value valid as an argument for
       Stmt.bind(). This is ONLY applied to the FIRST non-empty
       statement in the SQL which has any bindable
       parameters. (Empty statements are skipped entirely.)

       - .callback = a function which gets called for each row of
       the FIRST statement in the SQL which has result
       _columns_, but only if that statement has any result
       _rows_. The second argument passed to the callback is
       always the current Stmt object (so that the caller may
       collect column names, or similar). The first argument
       passed to the callback defaults to the current Stmt
       object but may be changed with ...

       - .rowMode = either a string describing what type of argument
       should be passed as the first argument to the callback or an
       integer representing a result column index. A `rowMode` of
       'object' causes the results of `stmt.get({})` to be passed to
       the `callback` and/or appended to `resultRows`. A value of
       'array' causes the results of `stmt.get([])` to be passed to
       passed on.  A value of 'stmt' is equivalent to the default,
       passing the current Stmt to the callback (noting that it's
       always passed as the 2nd argument), but this mode will trigger
       an exception if `resultRows` is an array. If `rowMode` is an
       integer, only the single value from that result column will be
       passed on. Any other value for the option triggers an
       exception.

       - .resultRows: if this is an array, it functions similarly to
       the `callback` option: each row of the result set (if any) of
       the FIRST first statement which has result _columns_ is
       appended to the array in the format specified for the `rowMode`
       option, with the exception that the only legal values for
       `rowMode` in this case are 'array' or 'object', neither of
       which is the default. It is legal to use both `resultRows` and
       `callback`, but `resultRows` is likely much simpler to use for
       small data sets and can be used over a WebWorker-style message
       interface.  execMulti() throws if `resultRows` is set and
       `rowMode` is 'stmt' (which is the default!).

       - saveSql = an optional array. If set, the SQL of each
       executed statement is appended to this array before the
       statement is executed (but after it is prepared - we
       don't have the string until after that). Empty SQL
       statements are elided.

       See also the exec() method, which is a close cousin of this
       one.

       ACHTUNG #1: The callback MUST NOT modify the Stmt
       object. Calling any of the Stmt.get() variants,
       Stmt.getColumnName(), or similar, is legal, but calling
       step() or finalize() is not. Routines which are illegal
       in this context will trigger an exception.

       ACHTUNG #2: The semantics of the `bind` and `callback`
       options may well change or those options may be removed
       altogether for this function (but retained for exec()).
       Generally speaking, neither bind parameters nor a callback
       are generically useful when executing multi-statement SQL.
    */
    execMulti: function(/*(sql [,obj]) || (obj)*/){
      affirmDbOpen(this);
      const wasm = capi.wasm;
      const arg = (BindTypes===arguments[2]
                   /* ^^^ Being passed on from exec() */
                   ? arguments[0] : parseExecArgs(arguments));
      if(!arg.sql) return this;
      const opt = arg.opt;
      const callback = opt.callback;
      const resultRows = (Array.isArray(opt.resultRows)
                          ? opt.resultRows : undefined);
      if(resultRows && 'stmt'===opt.rowMode){
        toss3("rowMode 'stmt' is not valid in combination",
              "with a resultRows array.");
      }
      let rowMode = (((callback||resultRows) && (undefined!==opt.rowMode))
                     ? opt.rowMode : undefined);
      let stmt;
      let bind = opt.bind;
      const stack = wasm.scopedAllocPush();
      try{
        const isTA = util.isSQLableTypedArray(arg.sql)
        /* Optimization: if the SQL is a TypedArray we can save some string
           conversion costs. */;
        /* Allocate the two output pointers (ppStmt, pzTail) and heap
           space for the SQL (pSql). When prepare_v2() returns, pzTail
           will point to somewhere in pSql. */
        let sqlByteLen = isTA ? arg.sql.byteLength : wasm.jstrlen(arg.sql);
        const ppStmt  = wasm.scopedAlloc(/* output (sqlite3_stmt**) arg and pzTail */
          (2 * wasm.ptrSizeof)
          + (sqlByteLen + 1/* SQL + NUL */));
        const pzTail = ppStmt + wasm.ptrSizeof /* final arg to sqlite3_prepare_v2() */;
        let pSql = pzTail + wasm.ptrSizeof;
        const pSqlEnd = pSql + sqlByteLen;
        if(isTA) wasm.heap8().set(arg.sql, pSql);
        else wasm.jstrcpy(arg.sql, wasm.heap8(), pSql, sqlByteLen, false);
        wasm.setMemValue(pSql + sqlByteLen, 0/*NUL terminator*/);
        while(wasm.getMemValue(pSql, 'i8')
              /* Maintenance reminder:   ^^^^ _must_ be i8 or else we
                 will very likely cause an endless loop. What that's
                 doing is checking for a terminating NUL byte. If we
                 use i32 or similar then we read 4 bytes, read stuff
                 around the NUL terminator, and get stuck in and
                 endless loop at the end of the SQL, endlessly
                 re-preparing an empty statement. */ ){
          wasm.setMemValue(ppStmt, 0, wasm.ptrIR);
          wasm.setMemValue(pzTail, 0, wasm.ptrIR);
          DB.checkRc(this, capi.sqlite3_prepare_v2(
            this.pointer, pSql, sqlByteLen, ppStmt, pzTail
          ));
          const pStmt = wasm.getMemValue(ppStmt, wasm.ptrIR);
          pSql = wasm.getMemValue(pzTail, wasm.ptrIR);
          sqlByteLen = pSqlEnd - pSql;
          if(!pStmt) continue;
          if(Array.isArray(opt.saveSql)){
            opt.saveSql.push(capi.sqlite3_sql(pStmt).trim());
          }
          stmt = new Stmt(this, pStmt, BindTypes);
          if(bind && stmt.parameterCount){
            stmt.bind(bind);
            bind = null;
          }
          if(stmt.columnCount && undefined!==rowMode){
            /* Only forward SELECT results for the FIRST query
               in the SQL which potentially has them. */
            while(stmt.step()){
              stmt._isLocked = true;
              const row = arg.cbArg(stmt);
              if(callback) callback(row, stmt);
              if(resultRows) resultRows.push(row);
              stmt._isLocked = false;
            }
            rowMode = undefined;
          }else{
            // Do we need to while(stmt.step()){} here?
            stmt.step();
          }
          stmt.finalize();
          stmt = null;
        }
      }catch(e){
        console.warn("DB.execMulti() is propagating exception",opt,e);
        throw e;
      }finally{
        if(stmt){
          delete stmt._isLocked;
          stmt.finalize();
        }
        wasm.scopedAllocPop(stack);
      }
      return this;
    }/*execMulti()*/,
    /**
       Creates a new scalar UDF (User-Defined Function) which is
       accessible via SQL code. This function may be called in any
       of the following forms:

       - (name, function)
       - (name, function, optionsObject)
       - (name, optionsObject)
       - (optionsObject)

       In the final two cases, the function must be defined as the
       'callback' property of the options object. In the final
       case, the function's name must be the 'name' property.

       This can only be used to create scalar functions, not
       aggregate or window functions. UDFs cannot be removed from
       a DB handle after they're added.

       On success, returns this object. Throws on error.

       When called from SQL, arguments to the UDF, and its result,
       will be converted between JS and SQL with as much fidelity
       as is feasible, triggering an exception if a type
       conversion cannot be determined. Some freedom is afforded
       to numeric conversions due to friction between the JS and C
       worlds: integers which are larger than 32 bits will be
       treated as doubles, as JS does not support 64-bit integers
       and it is (as of this writing) illegal to use WASM
       functions which take or return 64-bit integers from JS.

       The optional options object may contain flags to modify how
       the function is defined:

       - .arity: the number of arguments which SQL calls to this
       function expect or require. The default value is the
       callback's length property (i.e. the number of declared
       parameters it has). A value of -1 means that the function
       is variadic and may accept any number of arguments, up to
       sqlite3's compile-time limits. sqlite3 will enforce the
       argument count if is zero or greater.

       The following properties correspond to flags documented at:

       https://sqlite.org/c3ref/create_function.html

       - .deterministic = SQLITE_DETERMINISTIC
       - .directOnly = SQLITE_DIRECTONLY
       - .innocuous = SQLITE_INNOCUOUS

       Maintenance reminder: the ability to add new
       WASM-accessible functions to the runtime requires that the
       WASM build is compiled with emcc's `-sALLOW_TABLE_GROWTH`
       flag.
    */
    createFunction: function f(name, callback,opt){
      switch(arguments.length){
          case 1: /* (optionsObject) */
            opt = name;
            name = opt.name;
            callback = opt.callback;
            break;
          case 2: /* (name, callback|optionsObject) */
            if(!(callback instanceof Function)){
              opt = callback;
              callback = opt.callback;
            }
            break;
          default: break;
      }
      if(!opt) opt = {};
      if(!(callback instanceof Function)){
        toss3("Invalid arguments: expecting a callback function.");
      }else if('string' !== typeof name){
        toss3("Invalid arguments: missing function name.");
      }
      if(!f._extractArgs){
        /* Static init */
        f._extractArgs = function(argc, pArgv){
          let i, pVal, valType, arg;
          const tgt = [];
          for(i = 0; i < argc; ++i){
            pVal = capi.wasm.getMemValue(pArgv + (capi.wasm.ptrSizeof * i),
                                        capi.wasm.ptrIR);
            /**
               Curiously: despite ostensibly requiring 8-byte
               alignment, the pArgv array is parcelled into chunks of
               4 bytes (1 pointer each). The values those point to
               have 8-byte alignment but the individual argv entries
               do not.
            */            
            valType = capi.sqlite3_value_type(pVal);
            switch(valType){
                case capi.SQLITE_INTEGER:
                case capi.SQLITE_FLOAT:
                  arg = capi.sqlite3_value_double(pVal);
                  break;
                case capi.SQLITE_TEXT:
                  arg = capi.sqlite3_value_text(pVal);
                  break;
                case capi.SQLITE_BLOB:{
                  const n = capi.sqlite3_value_bytes(pVal);
                  const pBlob = capi.sqlite3_value_blob(pVal);
                  arg = new Uint8Array(n);
                  let i;
                  const heap = n ? capi.wasm.heap8() : false;
                  for(i = 0; i < n; ++i) arg[i] = heap[pBlob+i];
                  break;
                }
                case capi.SQLITE_NULL:
                  arg = null; break;
                default:
                  toss3("Unhandled sqlite3_value_type()",valType,
                        "is possibly indicative of incorrect",
                        "pointer size assumption.");
            }
            tgt.push(arg);
          }
          return tgt;
        }/*_extractArgs()*/;
        f._setResult = function(pCx, val){
          switch(typeof val) {
              case 'boolean':
                capi.sqlite3_result_int(pCx, val ? 1 : 0);
                break;
              case 'number': {
                (util.isInt32(val)
                 ? capi.sqlite3_result_int
                 : capi.sqlite3_result_double)(pCx, val);
                break;
              }
              case 'string':
                capi.sqlite3_result_text(pCx, val, -1, capi.SQLITE_TRANSIENT);
                break;
              case 'object':
                if(null===val) {
                  capi.sqlite3_result_null(pCx);
                  break;
                }else if(util.isBindableTypedArray(val)){
                  const pBlob = capi.wasm.mallocFromTypedArray(val);
                  capi.sqlite3_result_blob(pCx, pBlob, val.byteLength,
                                          capi.SQLITE_TRANSIENT);
                  capi.wasm.dealloc(pBlob);
                  break;
                }
                // else fall through
              default:
                toss3("Don't not how to handle this UDF result value:",val);
          };
        }/*_setResult()*/;
      }/*static init*/
      const wrapper = function(pCx, argc, pArgv){
        try{
          f._setResult(pCx, callback.apply(null, f._extractArgs(argc, pArgv)));
        }catch(e){
          if(e instanceof capi.WasmAllocError){
            capi.sqlite3_result_error_nomem(pCx);
          }else{
            capi.sqlite3_result_error(pCx, e.message, -1);
          }
        }
      };
      const pUdf = capi.wasm.installFunction(wrapper, "v(iii)");
      let fFlags = 0 /*flags for sqlite3_create_function_v2()*/;
      if(getOwnOption(opt, 'deterministic')) fFlags |= capi.SQLITE_DETERMINISTIC;
      if(getOwnOption(opt, 'directOnly')) fFlags |= capi.SQLITE_DIRECTONLY;
      if(getOwnOption(opt, 'innocuous')) fFlags |= capi.SQLITE_INNOCUOUS;
      name = name.toLowerCase();
      try {
        DB.checkRc(this, capi.sqlite3_create_function_v2(
          this.pointer, name,
          (opt.hasOwnProperty('arity') ? +opt.arity : callback.length),
          capi.SQLITE_UTF8 | fFlags, null/*pApp*/, pUdf,
          null/*xStep*/, null/*xFinal*/, null/*xDestroy*/));
      }catch(e){
        capi.wasm.uninstallFunction(pUdf);
        throw e;
      }
      const udfMap = __udfMap.get(this);
      if(udfMap[name]){
        try{capi.wasm.uninstallFunction(udfMap[name])}
        catch(e){/*ignore*/}
      }
      udfMap[name] = pUdf;
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

       Throws on error (e.g. malformedSQL).
    */
    selectValue: function(sql,bind,asType){
      let stmt, rc;
      try {
        stmt = this.prepare(sql).bind(bind);
        if(stmt.step()) rc = stmt.get(0,asType);
      }finally{
        if(stmt) stmt.finalize();
      }
      return rc;
    },

    /**
       Returns the number of currently-opened Stmt handles for this db
       handle, or 0 if this DB instance is closed.
    */
    openStatementCount: function(){
      return this.pointer ? Object.keys(__stmtMap.get(this)).length : 0;
    },

    /**
       This function currently does nothing and always throws.  It
       WILL BE REMOVED pending other refactoring, to eliminate a hard
       dependency on Emscripten. This feature will be moved into a
       higher-level API or a runtime-configurable feature.

       That said, what its replacement should eventually do is...

       Exports a copy of this db's file as a Uint8Array and
       returns it. It is technically not legal to call this while
       any prepared statement are currently active because,
       depending on the platform, it might not be legal to read
       the db while a statement is locking it. Throws if this db
       is not open or has any opened statements.

       The resulting buffer can be passed to this class's
       constructor to restore the DB.

       Maintenance reminder: the corresponding sql.js impl of this
       feature closes the current db, finalizing any active
       statements and (seemingly unnecessarily) destroys any UDFs,
       copies the file, and then re-opens it (without restoring
       the UDFs). Those gymnastics are not necessary on the tested
       platform but might be necessary on others. Because of that
       eventuality, this interface currently enforces that no
       statements are active when this is run. It will throw if
       any are.
    */
    exportBinaryImage: function(){
      toss3("exportBinaryImage() is slated for removal for portability reasons.");
      /***********************
         The following is currently kept only for reference when
         porting to some other layer, noting that we may well not be
         able to implement this, at this level, when using the OPFS
         VFS because of its exclusive locking policy.

         affirmDbOpen(this);
         if(this.openStatementCount()>0){
           toss3("Cannot export with prepared statements active!",
                 "finalize() all statements and try again.");
         }
         return MODCFG.FS.readFile(this.filename, {encoding:"binary"});
      ***********************/
    }
  }/*DB.prototype*/;


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
          if(capi.wasm.bigIntEnabled) return t;
          /* else fall through */
        default:
          //console.log("isSupportedBindType",t,v);
          return util.isBindableTypedArray(v) ? BindTypes.blob : undefined;
    }
  };

  /**
     If isSupportedBindType(v) returns a truthy value, this
     function returns that value, else it throws.
  */
  const affirmSupportedBindType = function(v){
    //console.log('affirmSupportedBindType',v);
    return isSupportedBindType(v) || toss3("Unsupported bind() argument type:",typeof v);
  };

  /**
     If key is a number and within range of stmt's bound parameter
     count, key is returned.

     If key is not a number then it is checked against named
     parameters. If a match is found, its index is returned.

     Else it throws.
  */
  const affirmParamIndex = function(stmt,key){
    const n = ('number'===typeof key)
          ? key : capi.sqlite3_bind_parameter_index(stmt.pointer, key);
    if(0===n || !util.isInt32(n)){
      toss3("Invalid bind() parameter name: "+key);
    }
    else if(n<1 || n>stmt.parameterCount) toss3("Bind index",key,"is out of range.");
    return n;
  };

  /**
     If stmt._isLocked is truthy, this throws an exception
     complaining that the 2nd argument (an operation name,
     e.g. "bind()") is not legal while the statement is "locked".
     Locking happens before an exec()-like callback is passed a
     statement, to ensure that the callback does not mutate or
     finalize the statement. If it does not throw, it returns stmt.
  */
  const affirmUnlocked = function(stmt,currentOpName){
    if(stmt._isLocked){
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
    affirmUnlocked(stmt, 'bind()');
    if(!f._){
      if(capi.wasm.bigIntEnabled){
        f._maxInt = BigInt("0x7fffffffffffffff");
        f._minInt = ~f._maxInt;
      }
      /* Reminder: when not in BigInt mode, it's impossible for
         JS to represent a number out of the range we can bind,
         so we have no range checking. */
      f._ = {
        string: function(stmt, ndx, val, asBlob){
          if(1){
            /* _Hypothetically_ more efficient than the impl in the 'else' block. */
            const stack = capi.wasm.scopedAllocPush();
            try{
              const n = capi.wasm.jstrlen(val);
              const pStr = capi.wasm.scopedAlloc(n);
              capi.wasm.jstrcpy(val, capi.wasm.heap8u(), pStr, n, false);
              const f = asBlob ? capi.sqlite3_bind_blob : capi.sqlite3_bind_text;
              return f(stmt.pointer, ndx, pStr, n, capi.SQLITE_TRANSIENT);
            }finally{
              capi.wasm.scopedAllocPop(stack);
            }
          }else{
            const bytes = capi.wasm.jstrToUintArray(val,false);
            const pStr = capi.wasm.alloc(bytes.length || 1);
            capi.wasm.heap8u().set(bytes.length ? bytes : [0], pStr);
            try{
              const f = asBlob ? capi.sqlite3_bind_blob : capi.sqlite3_bind_text;
              return f(stmt.pointer, ndx, pStr, bytes.length, capi.SQLITE_TRANSIENT);
            }finally{
              capi.wasm.dealloc(pStr);
            }
          }
        }
      };
    }
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
          else if(capi.wasm.bigIntEnabled && ('bigint'===typeof val)){
            if(val<f._minInt || val>f._maxInt){
              toss3("BigInt value is out of range for int64: "+val);
            }
            m = capi.sqlite3_bind_int64;
          }else if(Number.isInteger(val)){
            m = capi.sqlite3_bind_int64;
          }else{
            m = capi.sqlite3_bind_double;
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
          }else if(!util.isBindableTypedArray(val)){
            toss3("Binding a value as a blob requires",
                  "that it be a string, Uint8Array, or Int8Array.");
          }else if(1){
            /* _Hypothetically_ more efficient than the impl in the 'else' block. */
            const stack = capi.wasm.scopedAllocPush();
            try{
              const pBlob = capi.wasm.scopedAlloc(val.byteLength || 1);
              capi.wasm.heap8().set(val.byteLength ? val : [0], pBlob)
              rc = capi.sqlite3_bind_blob(stmt.pointer, ndx, pBlob, val.byteLength,
                                         capi.SQLITE_TRANSIENT);
            }finally{
              capi.wasm.scopedAllocPop(stack);
            }
          }else{
            const pBlob = capi.wasm.mallocFromTypedArray(val);
            try{
              rc = capi.sqlite3_bind_blob(stmt.pointer, ndx, pBlob, val.byteLength,
                                         capi.SQLITE_TRANSIENT);
            }finally{
              capi.wasm.dealloc(pBlob);
            }
          }
          break;
        }
        default:
          console.warn("Unsupported bind() argument type:",val);
          toss3("Unsupported bind() argument type: "+(typeof val));
    }
    if(rc) checkDbRc(stmt.db.pointer, rc);
    return stmt;
  };

  Stmt.prototype = {
    /**
       "Finalizes" this statement. This is a no-op if the
       statement has already been finalizes. Returns
       undefined. Most methods in this class will throw if called
       after this is.
    */
    finalize: function(){
      if(this.pointer){
        affirmUnlocked(this,'finalize()');
        delete __stmtMap.get(this.db)[this.pointer];
        capi.sqlite3_finalize(this.pointer);
        __ptrMap.delete(this);
        delete this.columnCount;
        delete this.parameterCount;
        delete this.db;
        delete this._isLocked;
      }
    },
    /** Clears all bound values. Returns this object.
        Throws if this statement has been finalized. */
    clearBindings: function(){
      affirmUnlocked(affirmStmtOpen(this), 'clearBindings()')
      capi.sqlite3_clear_bindings(this.pointer);
      this._mayGet = false;
      return this;
    },
    /**
       Resets this statement so that it may be step()ed again
       from the beginning. Returns this object. Throws if this
       statement has been finalized.

       If passed a truthy argument then this.clearBindings() is
       also called, otherwise any existing bindings, along with
       any memory allocated for them, are retained.
    */
    reset: function(alsoClearBinds){
      affirmUnlocked(this,'reset()');
      if(alsoClearBinds) this.clearBindings();
      capi.sqlite3_reset(affirmStmtOpen(this).pointer);
      this._mayGet = false;
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
       simplify certain client-side use cases: passing undefined
       as a value to this function will not actually bind
       anything and this function will skip confirmation that
       binding is even legal. (Those semantics simplify certain
       client-side uses.) Conversely, a value of undefined as an
       array or object property when binding an array/object
       (see below) is treated the same as null.

       - Numbers are bound as either doubles or integers: doubles
       if they are larger than 32 bits, else double or int32,
       depending on whether they have a fractional part. (It is,
       as of this writing, illegal to call (from JS) a WASM
       function which either takes or returns an int64.)
       Booleans are bound as integer 0 or 1. It is not expected
       the distinction of binding doubles which have no
       fractional parts is integers is significant for the
       majority of clients due to sqlite3's data typing
       model. If capi.wasm.bigIntEnabled is true then this
       routine will bind BigInt values as 64-bit integers.

       - Strings are bound as strings (use bindAsBlob() to force
       blob binding).

       - Uint8Array and Int8Array instances are bound as blobs.
       (TODO: binding the other TypedArray types.)

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
      this._mayGet = false;
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
      }
      else if('object'===typeof arg/*null was checked above*/
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
       API.

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
      bindOne(this, ndx, BindTypes.blob, arg);
      this._mayGet = false;
      return this;
    },
    /**
       Steps the statement one time. If the result indicates that
       a row of data is available, true is returned.  If no row of
       data is available, false is returned.  Throws on error.
    */
    step: function(){
      affirmUnlocked(this, 'step()');
      const rc = capi.sqlite3_step(affirmStmtOpen(this).pointer);
      switch(rc){
          case capi.SQLITE_DONE: return this._mayGet = false;
          case capi.SQLITE_ROW: return this._mayGet = true;
          default:
            this._mayGet = false;
            console.warn("sqlite3_step() rc=",rc,"SQL =",
                         capi.sqlite3_sql(this.pointer));
            checkDbRc(this.db.pointer, rc);
      };
    },
    /**
       Fetches the value from the given 0-based column index of
       the current data row, throwing if index is out of range. 

       Requires that step() has just returned a truthy value, else
       an exception is thrown.

       By default it will determine the data type of the result
       automatically. If passed a second arugment, it must be one
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
       the values of their corresponding result columns.

       Blobs are returned as Uint8Array instances.

       Potential TODO: add type ID SQLITE_JSON, which fetches the
       result as a string and passes it (if it's not null) to
       JSON.parse(), returning the result of that. Until then,
       getJSON() can be used for that.
    */
    get: function(ndx,asType){
      if(!affirmStmtOpen(this)._mayGet){
        toss3("Stmt.step() has not (recently) returned true.");
      }
      if(Array.isArray(ndx)){
        let i = 0;
        while(i<this.columnCount){
          ndx[i] = this.get(i++);
        }
        return ndx;
      }else if(ndx && 'object'===typeof ndx){
        let i = 0;
        while(i<this.columnCount){
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
            if(capi.wasm.bigIntEnabled){
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
              //console.log("get integer rc=",rc,isInt32(rc));
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
            //heap = n ? capi.wasm.heap8() : false;
            if(n) rc.set(capi.wasm.heap8u().slice(ptr, ptr+n), 0);
            //for(let i = 0; i < n; ++i) rc[i] = heap[ptr + i];
            if(n && this.db._blobXfer instanceof Array){
              /* This is an optimization soley for the
                 Worker-based API. These values will be
                 transfered to the main thread directly
                 instead of being copied. */
              this.db._blobXfer.push(rc.buffer);
            }
            return rc;
          }
          default: toss3("Don't know how to translate",
                         "type of result column #"+ndx+".");
      }
      abort("Not reached.");
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
    // arguablly really need that prefix for API readability and the
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
       If this statement potentially has result columns, this
       function returns an array of all such names. If passed an
       array, it is used as the target and all names are appended
       to it. Returns the target array. Throws if this statement
       cannot have result columns. This object's columnCount member
       holds the number of columns.
    */
    getColumnNames: function(tgt){
      affirmColIndex(affirmStmtOpen(this),0);
      if(!tgt) tgt = [];
      for(let i = 0; i < this.columnCount; ++i){
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
  
  /** The OO API's public namespace. */
  sqlite3.oo1 = {
    version: {
      lib: capi.sqlite3_libversion(),
      ooApi: "0.1"
    },
    DB,
    Stmt
  }/*SQLite3 object*/;
})(self);
