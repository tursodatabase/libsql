/**
  2022-07-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file implements the initializer for SQLite's "Worker API #1", a
  very basic DB access API intended to be scripted from a main window
  thread via Worker-style messages. Because of limitations in that
  type of communication, this API is minimalistic and only capable of
  serving relatively basic DB requests (e.g. it cannot process nested
  query loops concurrently).

  This file requires that the core C-style sqlite3 API and OO API #1
  have been loaded.
*/

/**
  sqlite3.initWorker1API() implements a Worker-based wrapper around
  SQLite3 OO API #1, colloquially known as "Worker API #1".

  In order to permit this API to be loaded in worker threads without
  automatically registering onmessage handlers, initializing the
  worker API requires calling initWorker1API(). If this function is
  called from a non-worker thread then it throws an exception.  It
  must only be called once per Worker.

  When initialized, it installs message listeners to receive Worker
  messages and then it posts a message in the form:

  ```
  {type:'sqlite3-api', result:'worker1-ready'}
  ```

  to let the client know that it has been initialized. Clients may
  optionally depend on this function not returning until
  initialization is complete, as the initialization is synchronous.
  In some contexts, however, listening for the above message is
  a better fit.

  Note that the worker-based interface can be slightly quirky because
  of its async nature. In particular, any number of messages may be posted
  to the worker before it starts handling any of them. If, e.g., an
  "open" operation fails, any subsequent messages will fail. The
  Promise-based wrapper for this API (`sqlite3-worker1-promiser.js`)
  is more comfortable to use in that regard.

  The documentation for the input and output worker messages for
  this API follows...

  ====================================================================
  Common message format...

  Each message posted to the worker has an operation-independent
  envelope and operation-dependent arguments:

  ```
  {
    type: string, // one of: 'open', 'close', 'exec', 'config-get'

    messageId: OPTIONAL arbitrary value. The worker will copy it as-is
    into response messages to assist in client-side dispatching.

    dbId: a db identifier string (returned by 'open') which tells the
    operation which database instance to work on. If not provided, the
    first-opened db is used. This is an "opaque" value, with no
    inherently useful syntax or information. Its value is subject to
    change with any given build of this API and cannot be used as a
    basis for anything useful beyond its one intended purpose.

    args: ...operation-dependent arguments...

    // the framework may add other properties for testing or debugging
    // purposes.

  }
  ```

  Response messages, posted back to the main thread, look like:

  ```
  {
    type: string. Same as above except for error responses, which have the type
    'error',

    messageId: same value, if any, provided by the inbound message

    dbId: the id of the db which was operated on, if any, as returned
    by the corresponding 'open' operation.

    result: ...operation-dependent result...

  }
  ```

  ====================================================================
  Error responses

  Errors are reported messages in an operation-independent format:

  ```
  {
    type: "error",

    messageId: ...as above...,

    dbId: ...as above...

    result: {

      operation: type of the triggering operation: 'open', 'close', ...

      message: ...error message text...

      errorClass: string. The ErrorClass.name property from the thrown exception.

      input: the message object which triggered the error.

      stack: _if available_, a stack trace array.

    }

  }
  ```


  ====================================================================
  "config-get"

  This operation fetches the serializable parts of the sqlite3 API
  configuration.

  Message format:

  ```
  {
    type: "config-get",
    messageId: ...as above...,
    args: currently ignored and may be elided.
  }
  ```

  Response:

  ```
  {
    type: "config-get",
    messageId: ...as above...,
    result: {

      version: sqlite3.version object

      bigIntEnabled: bool. True if BigInt support is enabled.

      vfsList: result of sqlite3.capi.sqlite3_js_vfs_list()
   }
  }
  ```


  ====================================================================
  "open" a database

  Message format:

  ```
  {
    type: "open",
    messageId: ...as above...,
    args:{

      filename [=":memory:" or "" (unspecified)]: the db filename.
      See the sqlite3.oo1.DB constructor for peculiarities and
      transformations,

      vfs: sqlite3_vfs name. Ignored if filename is ":memory:" or "".
           This may change how the given filename is resolved.
    }
  }
  ```

  Response:

  ```
  {
    type: "open",
    messageId: ...as above...,
    result: {
      filename: db filename, possibly differing from the input.

      dbId: an opaque ID value which must be passed in the message
      envelope to other calls in this API to tell them which db to
      use. If it is not provided to future calls, they will default to
      operating on the least-recently-opened db. This property is, for
      API consistency's sake, also part of the containing message
      envelope.  Only the `open` operation includes it in the `result`
      property.

      persistent: true if the given filename resides in the
      known-persistent storage, else false.

      vfs: name of the VFS the "main" db is using.
   }
  }
  ```

  ====================================================================
  "close" a database

  Message format:

  ```
  {
    type: "close",
    messageId: ...as above...
    dbId: ...as above...
    args: OPTIONAL {unlink: boolean}
  }
  ```

  If the `dbId` does not refer to an opened ID, this is a no-op. If
  the `args` object contains a truthy `unlink` value then the database
  will be unlinked (deleted) after closing it. The inability to close a
  db (because it's not opened) or delete its file does not trigger an
  error.

  Response:

  ```
  {
    type: "close",
    messageId: ...as above...,
    result: {

      filename: filename of closed db, or undefined if no db was closed

    }
  }
  ```

  ====================================================================
  "exec" SQL

  All SQL execution is processed through the exec operation. It offers
  most of the features of the oo1.DB.exec() method, with a few limitations
  imposed by the state having to cross thread boundaries.

  Message format:

  ```
  {
    type: "exec",
    messageId: ...as above...
    dbId: ...as above...
    args: string (SQL) or {... see below ...}
  }
  ```

  Response:

  ```
  {
    type: "exec",
    messageId: ...as above...,
    dbId: ...as above...
    result: {
      input arguments, possibly modified. See below.
    }
  }
  ```

  The arguments are in the same form accepted by oo1.DB.exec(), with
  the exceptions noted below.

  If the `countChanges` arguments property (added in version 3.43) is
  truthy then the `result` property contained by the returned object
  will have a `changeCount` property which holds the number of changes
  made by the provided SQL. Because the SQL may contain an arbitrary
  number of statements, the `changeCount` is calculated by calling
  `sqlite3_total_changes()` before and after the SQL is evaluated. If
  the value of `countChanges` is 64 then the `changeCount` property
  will be returned as a 64-bit integer in the form of a BigInt (noting
  that that will trigger an exception if used in a BigInt-incapable
  build).  In the latter case, the number of changes is calculated by
  calling `sqlite3_total_changes64()` before and after the SQL is
  evaluated.

  A function-type args.callback property cannot cross
  the window/Worker boundary, so is not useful here. If
  args.callback is a string then it is assumed to be a
  message type key, in which case a callback function will be
  applied which posts each row result via:

  postMessage({type: thatKeyType,
               rowNumber: 1-based-#,
               row: theRow,
               columnNames: anArray
               })

  And, at the end of the result set (whether or not any result rows
  were produced), it will post an identical message with
  (row=undefined, rowNumber=null) to alert the caller than the result
  set is completed. Note that a row value of `null` is a legal row
  result for certain arg.rowMode values.

    (Design note: we don't use (row=undefined, rowNumber=undefined) to
    indicate end-of-results because fetching those would be
    indistinguishable from fetching from an empty object unless the
    client used hasOwnProperty() (or similar) to distinguish "missing
    property" from "property with the undefined value".  Similarly,
    `null` is a legal value for `row` in some case , whereas the db
    layer won't emit a result value of `undefined`.)

  The callback proxy must not recurse into this interface. An exec()
  call will tie up the Worker thread, causing any recursion attempt
  to wait until the first exec() is completed.

  The response is the input options object (or a synthesized one if
  passed only a string), noting that options.resultRows and
  options.columnNames may be populated by the call to db.exec().

*/
globalThis.sqlite3ApiBootstrap.initializers.push(function(sqlite3){
sqlite3.initWorker1API = function(){
  'use strict';
  const toss = (...args)=>{throw new Error(args.join(' '))};
  if(!(globalThis.WorkerGlobalScope instanceof Function)){
    toss("initWorker1API() must be run from a Worker thread.");
  }
  const self = this.self;
  const sqlite3 = this.sqlite3 || toss("Missing this.sqlite3 object.");
  const DB = sqlite3.oo1.DB;

  /**
     Returns the app-wide unique ID for the given db, creating one if
     needed.
  */
  const getDbId = function(db){
    let id = wState.idMap.get(db);
    if(id) return id;
    id = 'db#'+(++wState.idSeq)+'@'+db.pointer;
    /** ^^^ can't simply use db.pointer b/c closing/opening may re-use
        the same address, which could map pending messages to a wrong
        instance. */
    wState.idMap.set(db, id);
    return id;
  };

  /**
     Internal helper for managing Worker-level state.
  */
  const wState = {
    /**
       Each opened DB is added to this.dbList, and the first entry in
       that list is the default db. As each db is closed, its entry is
       removed from the list.
    */
    dbList: [],
    /** Sequence number of dbId generation. */
    idSeq: 0,
    /** Map of DB instances to dbId. */
    idMap: new WeakMap,
    /** Temp holder for "transferable" postMessage() state. */
    xfer: [],
    open: function(opt){
      const db = new DB(opt);
      this.dbs[getDbId(db)] = db;
      if(this.dbList.indexOf(db)<0) this.dbList.push(db);
      return db;
    },
    close: function(db,alsoUnlink){
      if(db){
        delete this.dbs[getDbId(db)];
        const filename = db.filename;
        const pVfs = sqlite3.wasm.sqlite3_wasm_db_vfs(db.pointer, 0);
        db.close();
        const ddNdx = this.dbList.indexOf(db);
        if(ddNdx>=0) this.dbList.splice(ddNdx, 1);
        if(alsoUnlink && filename && pVfs){
          sqlite3.wasm.sqlite3_wasm_vfs_unlink(pVfs, filename);
        }
      }
    },
    /**
       Posts the given worker message value. If xferList is provided,
       it must be an array, in which case a copy of it passed as
       postMessage()'s second argument and xferList.length is set to
       0.
    */
    post: function(msg,xferList){
      if(xferList && xferList.length){
        globalThis.postMessage( msg, Array.from(xferList) );
        xferList.length = 0;
      }else{
        globalThis.postMessage(msg);
      }
    },
    /** Map of DB IDs to DBs. */
    dbs: Object.create(null),
    /** Fetch the DB for the given id. Throw if require=true and the
        id is not valid, else return the db or undefined. */
    getDb: function(id,require=true){
      return this.dbs[id]
        || (require ? toss("Unknown (or closed) DB ID:",id) : undefined);
    }
  };

  /** Throws if the given db is falsy or not opened, else returns its
      argument. */
  const affirmDbOpen = function(db = wState.dbList[0]){
    return (db && db.pointer) ? db : toss("DB is not opened.");
  };

  /** Extract dbId from the given message payload. */
  const getMsgDb = function(msgData,affirmExists=true){
    const db = wState.getDb(msgData.dbId,false) || wState.dbList[0];
    return affirmExists ? affirmDbOpen(db) : db;
  };

  const getDefaultDbId = function(){
    return wState.dbList[0] && getDbId(wState.dbList[0]);
  };

  const guessVfs = function(filename){
    const m = /^file:.+(vfs=(\w+))/.exec(filename);
    return sqlite3.capi.sqlite3_vfs_find(m ? m[2] : 0);
  };

  const isSpecialDbFilename = (n)=>{
    return ""===n || ':'===n[0];
  };

  /**
     A level of "organizational abstraction" for the Worker1
     API. Each method in this object must map directly to a Worker1
     message type key. The onmessage() dispatcher attempts to
     dispatch all inbound messages to a method of this object,
     passing it the event.data part of the inbound event object. All
     methods must return a plain Object containing any result
     state, which the dispatcher may amend. All methods must throw
     on error.
  */
  const wMsgHandler = {
    open: function(ev){
      const oargs = Object.create(null), args = (ev.args || Object.create(null));
      if(args.simulateError){ // undocumented internal testing option
        toss("Throwing because of simulateError flag.");
      }
      const rc = Object.create(null);
      let byteArray, pVfs;
      oargs.vfs = args.vfs;
      if(isSpecialDbFilename(args.filename)){
        oargs.filename = args.filename || "";
      }else{
        oargs.filename = args.filename;
        byteArray = args.byteArray;
        if(byteArray) pVfs = guessVfs(args.filename);
      }
      if(pVfs){
        /* 2022-11-02: this feature is as-yet untested except that
           sqlite3_wasm_vfs_create_file() has been tested from the
           browser dev console. */
        let pMem;
        try{
          pMem = sqlite3.wasm.allocFromTypedArray(byteArray);
          const rc = sqlite3.wasm.sqlite3_wasm_vfs_create_file(
            pVfs, oargs.filename, pMem, byteArray.byteLength
          );
          if(rc) sqlite3.SQLite3Error.toss(rc);
        }catch(e){
          throw new sqlite3.SQLite3Error(
            e.name+' creating '+args.filename+": "+e.message, {
              cause: e
            }
          );
        }finally{
          if(pMem) sqlite3.wasm.dealloc(pMem);
        }
      }
      const db = wState.open(oargs);
      rc.filename = db.filename;
      rc.persistent = !!sqlite3.capi.sqlite3_js_db_uses_vfs(db.pointer, "opfs");
      rc.dbId = getDbId(db);
      rc.vfs = db.dbVfsName();
      return rc;
    },

    close: function(ev){
      const db = getMsgDb(ev,false);
      const response = {
        filename: db && db.filename
      };
      if(db){
        const doUnlink = ((ev.args && 'object'===typeof ev.args)
                         ? !!ev.args.unlink : false);
        wState.close(db, doUnlink);
      }
      return response;
    },

    exec: function(ev){
      const rc = (
        'string'===typeof ev.args
      ) ? {sql: ev.args} : (ev.args || Object.create(null));
      if('stmt'===rc.rowMode){
        toss("Invalid rowMode for 'exec': stmt mode",
             "does not work in the Worker API.");
      }else if(!rc.sql){
        toss("'exec' requires input SQL.");
      }
      const db = getMsgDb(ev);
      if(rc.callback || Array.isArray(rc.resultRows)){
        // Part of a copy-avoidance optimization for blobs
        db._blobXfer = wState.xfer;
      }
      const theCallback = rc.callback;
      let rowNumber = 0;
      const hadColNames = !!rc.columnNames;
      if('string' === typeof theCallback){
        if(!hadColNames) rc.columnNames = [];
        /* Treat this as a worker message type and post each
           row as a message of that type. */
        rc.callback = function(row,stmt){
          wState.post({
            type: theCallback,
            columnNames: rc.columnNames,
            rowNumber: ++rowNumber,
            row: row
          }, wState.xfer);
        }
      }
      try {
        const changeCount = !!rc.countChanges
              ? db.changes(true,(64===rc.countChanges))
              : undefined;
        db.exec(rc);
        if(undefined !== changeCount){
          rc.changeCount = db.changes(true,64===rc.countChanges) - changeCount;
        }
        if(rc.callback instanceof Function){
          rc.callback = theCallback;
          /* Post a sentinel message to tell the client that the end
             of the result set has been reached (possibly with zero
             rows). */
          wState.post({
            type: theCallback,
            columnNames: rc.columnNames,
            rowNumber: null /*null to distinguish from "property not set"*/,
            row: undefined /*undefined because null is a legal row value
                             for some rowType values, but undefined is not*/
          });
        }
      }finally{
        delete db._blobXfer;
        if(rc.callback) rc.callback = theCallback;
      }
      return rc;
    }/*exec()*/,

    'config-get': function(){
      const rc = Object.create(null), src = sqlite3.config;
      [
        'bigIntEnabled'
      ].forEach(function(k){
        if(Object.getOwnPropertyDescriptor(src, k)) rc[k] = src[k];
      });
      rc.version = sqlite3.version;
      rc.vfsList = sqlite3.capi.sqlite3_js_vfs_list();
      rc.opfsEnabled = !!sqlite3.opfs;
      return rc;
    },

    /**
       Exports the database to a byte array, as per
       sqlite3_serialize(). Response is an object:

       {
         byteArray:  Uint8Array (db file contents),
         filename: the current db filename,
         mimetype: 'application/x-sqlite3'
       }
    */
    export: function(ev){
      const db = getMsgDb(ev);
      const response = {
        byteArray: sqlite3.capi.sqlite3_js_db_export(db.pointer),
        filename: db.filename,
        mimetype: 'application/x-sqlite3'
      };
      wState.xfer.push(response.byteArray.buffer);
      return response;
    }/*export()*/,

    toss: function(ev){
      toss("Testing worker exception");
    },

    'opfs-tree': async function(ev){
      if(!sqlite3.opfs) toss("OPFS support is unavailable.");
      const response = await sqlite3.opfs.treeList();
      return response;
    }
  }/*wMsgHandler*/;

  globalThis.onmessage = async function(ev){
    ev = ev.data;
    let result, dbId = ev.dbId, evType = ev.type;
    const arrivalTime = performance.now();
    try {
      if(wMsgHandler.hasOwnProperty(evType) &&
         wMsgHandler[evType] instanceof Function){
        result = await wMsgHandler[evType](ev);
      }else{
        toss("Unknown db worker message type:",ev.type);
      }
    }catch(err){
      evType = 'error';
      result = {
        operation: ev.type,
        message: err.message,
        errorClass: err.name,
        input: ev
      };
      if(err.stack){
        result.stack = ('string'===typeof err.stack)
          ? err.stack.split(/\n\s*/) : err.stack;
      }
      if(0) sqlite3.config.warn("Worker is propagating an exception to main thread.",
                                "Reporting it _here_ for the stack trace:",err,result);
    }
    if(!dbId){
      dbId = result.dbId/*from 'open' cmd*/
        || getDefaultDbId();
    }
    // Timing info is primarily for use in testing this API. It's not part of
    // the public API. arrivalTime = when the worker got the message.
    wState.post({
      type: evType,
      dbId: dbId,
      messageId: ev.messageId,
      workerReceivedTime: arrivalTime,
      workerRespondTime: performance.now(),
      departureTime: ev.departureTime,
      // TODO: move the timing bits into...
      //timing:{
      //  departure: ev.departureTime,
      //  workerReceived: arrivalTime,
      //  workerResponse: performance.now();
      //},
      result: result
    }, wState.xfer);
  };
  globalThis.postMessage({type:'sqlite3-api',result:'worker1-ready'});
}.bind({self, sqlite3});
});
