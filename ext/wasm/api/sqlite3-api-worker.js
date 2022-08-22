/*
  2022-07-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file implements a Worker-based wrapper around SQLite3 OO API
  #1.

  In order to permit this API to be loaded in worker threads without
  automatically registering onmessage handlers, initializing the
  worker API requires calling initWorkerAPI(). If this function
  is called from a non-worker thread then it throws an exception.

  When initialized, it installs message listeners to receive messages
  from the main thread and then it posts a message in the form:

  ```
  {type:'sqlite3-api',data:'worker-ready'}
  ```

  This file requires that the core C-style sqlite3 API and OO API #1
  have been loaded and that self.sqlite3 contains both,
  as documented for those APIs.
*/
self.sqlite3.initWorkerAPI = function(){
  'use strict';
  /**
     UNDER CONSTRUCTION

     We need an API which can proxy the DB API via a Worker message
     interface. The primary quirky factor in such an API is that we
     cannot pass callback functions between the window thread and a
     worker thread, so we have to receive all db results via
     asynchronous message-passing. That requires an asychronous API
     with a distinctly different shape that the main OO API.

     Certain important considerations here include:

     - Support only one db connection or multiple? The former is far
     easier, but there's always going to be a user out there who wants
     to juggle six database handles at once. Do we add that complexity
     or tell such users to write their own code using the provided
     lower-level APIs?

     - Fetching multiple results: do we pass them on as a series of
     messages, with start/end messages on either end, or do we collect
     all results and bundle them back in a single message?  The former
     is, generically speaking, more memory-efficient but the latter
     far easier to implement in this environment. The latter is
     untennable for large data sets. Despite a web page hypothetically
     being a relatively limited environment, there will always be
     those users who feel that they should/need to be able to work
     with multi-hundred-meg (or larger) blobs, and passing around
     arrays of those may quickly exhaust the JS engine's memory.

     TODOs include, but are not limited to:

     - The ability to manage multiple DB handles. This can
     potentially be done via a simple mapping of DB.filename or
     DB.pointer (`sqlite3*` handle) to DB objects. The open()
     interface would need to provide an ID (probably DB.pointer) back
     to the user which can optionally be passed as an argument to
     the other APIs (they'd default to the first-opened DB, for
     ease of use). Client-side usability of this feature would
     benefit from making another wrapper class (or a singleton)
     available to the main thread, with that object proxying all(?)
     communication with the worker.

     - Revisit how virtual files are managed. We currently delete DBs
     from the virtual filesystem when we close them, for the sake of
     saving memory (the VFS lives in RAM). Supporting multiple DBs may
     require that we give up that habit. Similarly, fully supporting
     ATTACH, where a user can upload multiple DBs and ATTACH them,
     also requires the that we manage the VFS entries better.
  */
  const toss = (...args)=>{throw new Error(args.join(' '))};
  if('function' !== typeof importScripts){
    toss("Cannot initalize the sqlite3 worker API in the main thread.");
  }
  const self = this.self;
  const sqlite3 = this.sqlite3 || toss("Missing this.sqlite3 object.");
  const SQLite3 = sqlite3.oo1 || toss("Missing this.sqlite3.oo1 OO API.");
  const DB = SQLite3.DB;

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
     Helper for managing Worker-level state.
  */
  const wState = {
    defaultDb: undefined,
    idSeq: 0,
    idMap: new WeakMap,
    open: function(arg){
      // TODO: if arg is a filename, look for a db in this.dbs with the
      // same filename and close/reopen it (or just pass it back as is?).
      if(!arg && this.defaultDb) return this.defaultDb;
      //???if(this.defaultDb) this.defaultDb.close();
      let db;
      db = (Array.isArray(arg) ? new DB(...arg) : new DB(arg));
      this.dbs[getDbId(db)] = db;
      if(!this.defaultDb) this.defaultDb = db;
      return db;
    },
    close: function(db,alsoUnlink){
      if(db){
        delete this.dbs[getDbId(db)];
        db.close(alsoUnlink);
        if(db===this.defaultDb) this.defaultDb = undefined;
      }
    },
    post: function(type,data,xferList){
      if(xferList){
        self.postMessage({type, data},xferList);
        xferList.length = 0;
      }else{
        self.postMessage({type, data});
      }
    },
    /** Map of DB IDs to DBs. */
    dbs: Object.create(null),
    getDb: function(id,require=true){
      return this.dbs[id]
        || (require ? toss("Unknown (or closed) DB ID:",id) : undefined);
    }
  };

  /** Throws if the given db is falsy or not opened. */
  const affirmDbOpen = function(db = wState.defaultDb){
    return (db && db.pointer) ? db : toss("DB is not opened.");
  };

  /** Extract dbId from the given message payload. */
  const getMsgDb = function(msgData,affirmExists=true){
    const db = wState.getDb(msgData.dbId,false) || wState.defaultDb;
    return affirmExists ? affirmDbOpen(db) : db;
  };

  const getDefaultDbId = function(){
    return wState.defaultDb && getDbId(wState.defaultDb);
  };

  /**
     A level of "organizational abstraction" for the Worker
     API. Each method in this object must map directly to a Worker
     message type key. The onmessage() dispatcher attempts to
     dispatch all inbound messages to a method of this object,
     passing it the event.data part of the inbound event object. All
     methods must return a plain Object containing any response
     state, which the dispatcher may amend. All methods must throw
     on error.
  */
  const wMsgHandler = {
    xfer: [/*Temp holder for "transferable" postMessage() state.*/],
    /**
       Proxy for DB.exec() which expects a single argument of type
       string (SQL to execute) or an options object in the form
       expected by exec(). The notable differences from exec()
       include:

       - The default value for options.rowMode is 'array' because
       the normal default cannot cross the window/Worker boundary.

       - A function-type options.callback property cannot cross
       the window/Worker boundary, so is not useful here. If
       options.callback is a string then it is assumed to be a
       message type key, in which case a callback function will be
       applied which posts each row result via:

       postMessage({type: thatKeyType, data: theRow})

       And, at the end of the result set (whether or not any
       result rows were produced), it will post an identical
       message with data:null to alert the caller than the result
       set is completed.

       The callback proxy must not recurse into this interface, or
       results are undefined. (It hypothetically cannot recurse
       because an exec() call will be tying up the Worker thread,
       causing any recursion attempt to wait until the first
       exec() is completed.)

       The response is the input options object (or a synthesized
       one if passed only a string), noting that
       options.resultRows and options.columnNames may be populated
       by the call to exec().

       This opens/creates the Worker's db if needed.
    */
    exec: function(ev){
      const opt = (
        'string'===typeof ev.data
      ) ? {sql: ev.data} : (ev.data || Object.create(null));
      if(undefined===opt.rowMode){
        /* Since the default rowMode of 'stmt' is not useful
           for the Worker interface, we'll default to
           something else. */
        opt.rowMode = 'array';
      }else if('stmt'===opt.rowMode){
        toss("Invalid rowMode for exec(): stmt mode",
             "does not work in the Worker API.");
      }
      const db = getMsgDb(ev);
      if(opt.callback || Array.isArray(opt.resultRows)){
        // Part of a copy-avoidance optimization for blobs
        db._blobXfer = this.xfer;
      }
      const callbackMsgType = opt.callback;
      if('string' === typeof callbackMsgType){
        /* Treat this as a worker message type and post each
           row as a message of that type. */
        const that = this;
        opt.callback =
          (row)=>wState.post(callbackMsgType,row,this.xfer);
      }
      try {
        db.exec(opt);
        if(opt.callback instanceof Function){
          opt.callback = callbackMsgType;
          wState.post(callbackMsgType, null);
        }
      }/*catch(e){
         console.warn("Worker is propagating:",e);throw e;
         }*/finally{
           delete db._blobXfer;
           if(opt.callback){
             opt.callback = callbackMsgType;
           }
         }
      return opt;
    }/*exec()*/,
    /**
       TO(re)DO, once we can abstract away access to the
       JS environment's virtual filesystem. Currently this
       always throws.

       Response is (should be) an object:

       {
         buffer: Uint8Array (db file contents),
         filename: the current db filename,
         mimetype: 'application/x-sqlite3'
       }

       TODO is to determine how/whether this feature can support
       exports of ":memory:" and "" (temp file) DBs. The latter is
       ostensibly easy because the file is (potentially) on disk, but
       the former does not have a structure which maps directly to a
       db file image.
    */
    export: function(ev){
      toss("export() requires reimplementing for portability reasons.");
      /**const db = getMsgDb(ev);
      const response = {
        buffer: db.exportBinaryImage(),
        filename: db.filename,
        mimetype: 'application/x-sqlite3'
      };
      this.xfer.push(response.buffer.buffer);
      return response;**/
    }/*export()*/,
    /**
       Proxy for the DB constructor. Expects to be passed a single
       object or a falsy value to use defaults. The object may
       have a filename property to name the db file (see the DB
       constructor for peculiarities and transformations) and/or a
       buffer property (a Uint8Array holding a complete database
       file's contents). The response is an object:

       {
         filename: db filename (possibly differing from the input),

         id: an opaque ID value intended for future distinction
             between multiple db handles. Messages including a specific
             ID will use the DB for that ID.

       }

       If the Worker's db is currently opened, this call closes it
       before proceeding.
    */
    open: function(ev){
      wState.close(/*true???*/);
      const args = [], data = (ev.data || {});
      if(data.simulateError){
        toss("Throwing because of open.simulateError flag.");
      }
      if(data.filename) args.push(data.filename);
      if(data.buffer){
        args.push(data.buffer);
        this.xfer.push(data.buffer.buffer);
      }
      const db = wState.open(args);
      return {
        filename: db.filename,
        dbId: getDbId(db)
      };
    },
    /**
       Proxy for DB.close(). If ev.data may either be a boolean or
       an object with an `unlink` property. If that value is
       truthy then the db file (if the db is currently open) will
       be unlinked from the virtual filesystem, else it will be
       kept intact. The response object is:

       {
         filename: db filename _if_ the db is opened when this
                   is called, else the undefined value
       }
    */
    close: function(ev){
      const db = getMsgDb(ev,false);
      const response = {
        filename: db && db.filename
      };
      if(db){
        wState.close(db, !!((ev.data && 'object'===typeof ev.data)
                            ? ev.data.unlink : ev.data));
      }
      return response;
    },
    toss: function(ev){
      toss("Testing worker exception");
    }
  }/*wMsgHandler*/;

  /**
     UNDER CONSTRUCTION!

     A subset of the DB API is accessible via Worker messages in the
     form:

     { type: apiCommand,
       dbId: optional DB ID value (else uses a default db handle)
       data: apiArguments
     }

     As a rule, these commands respond with a postMessage() of their
     own in the same form, but will, if needed, transform the `data`
     member to an object and may add state to it. The responses
     always have an object-format `data` part. If the inbound `data`
     is an object which has a `messageId` property, that property is
     always mirrored in the result object, for use in client-side
     dispatching of these asynchronous results. Exceptions thrown
     during processing result in an `error`-type event with a
     payload in the form:

     {
       message: error string,
       errorClass: class name of the error type,
       dbId: DB handle ID,
       input: ev.data,
       [messageId: if set in the inbound message]
     }

     The individual APIs are documented in the wMsgHandler object.
  */
  self.onmessage = function(ev){
    ev = ev.data;
    let response, dbId = ev.dbId, evType = ev.type;
    const arrivalTime = performance.now();
    try {
      if(wMsgHandler.hasOwnProperty(evType) &&
         wMsgHandler[evType] instanceof Function){
        response = wMsgHandler[evType](ev);
      }else{
        toss("Unknown db worker message type:",ev.type);
      }
    }catch(err){
      evType = 'error';
      response = {
        message: err.message,
        errorClass: err.name,
        input: ev
      };
      if(err.stack){
        response.stack = ('string'===typeof err.stack)
          ? err.stack.split('\n') : err.stack;
      }
      if(0) console.warn("Worker is propagating an exception to main thread.",
                         "Reporting it _here_ for the stack trace:",err,response);
    }
    if(!response.messageId && ev.data
       && 'object'===typeof ev.data && ev.data.messageId){
      response.messageId = ev.data.messageId;
    }
    if(!dbId){
      dbId = response.dbId/*from 'open' cmd*/
        || getDefaultDbId();
    }
    if(!response.dbId) response.dbId = dbId;
    // Timing info is primarily for use in testing this API. It's not part of
    // the public API. arrivalTime = when the worker got the message.
    response.workerReceivedTime = arrivalTime;
    response.workerRespondTime = performance.now();
    response.departureTime = ev.departureTime;
    wState.post(evType, response, wMsgHandler.xfer);
  };
  setTimeout(()=>self.postMessage({type:'sqlite3-api',data:'worker-ready'}), 0);
}.bind({self, sqlite3: self.sqlite3});
