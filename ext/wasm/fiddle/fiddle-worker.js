/*
  2022-05-20

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This is the JS Worker file for the sqlite3 fiddle app. It loads the
  sqlite3 wasm module and offers access to the db via the Worker
  message-passing interface.

  Forewarning: this API is still very much Under Construction and
  subject to any number of changes as experience reveals what those
  need to be.

  Because we can have only a single message handler, as opposed to an
  arbitrary number of discrete event listeners like with DOM elements,
  we have to define a lower-level message API. Messages abstractly
  look like:

  { type: string, data: type-specific value }

  Where 'type' is used for dispatching and 'data' is a
  'type'-dependent value.

  The 'type' values expected by each side of the main/worker
  connection vary. The types are described below but subject to
  change at any time as this experiment evolves.

  Workers-to-Main types

  - stdout, stderr: indicate stdout/stderr output from the wasm
    layer. The data property is the string of the output, noting
    that the emscripten binding emits these one line at a time. Thus,
    if a C-side puts() emits multiple lines in a single call, the JS
    side will see that as multiple calls. Example:

    {type:'stdout', data: 'Hi, world.'}

  - module: Status text. This is intended to alert the main thread
    about module loading status so that, e.g., the main thread can
    update a progress widget and DTRT when the module is finished
    loading and available for work. Status messages come in the form
    
    {type:'module', data:{
        type:'status',
        data: {text:string|null, step:1-based-integer}
    }

    with an incrementing step value for each subsequent message. When
    the module loading is complete, a message with a text value of
    null is posted.

  - working: data='start'|'end'. Indicates that work is about to be
    sent to the module or has just completed. This can be used, e.g.,
    to disable UI elements which should not be activated while work
    is pending. Example:

    {type:'working', data:'start'}

  Main-to-Worker types:

  - shellExec: data=text to execute as if it had been entered in the
    sqlite3 CLI shell app (as opposed to sqlite3_exec()). This event
    causes the worker to emit a 'working' event (data='start') before
    it starts and a 'working' event (data='end') when it finished. If
    called while work is currently being executed it emits stderr
    message instead of doing actual work, as the underlying db cannot
    handle concurrent tasks. Example:

    {type:'shellExec', data: 'select * from sqlite_master'}

  - More TBD as the higher-level db layer develops.
*/

/*
  Apparent browser(s) bug: console messages emitted may be duplicated
  in the console, even though they're provably only run once. See:

  https://stackoverflow.com/questions/49659464

  Noting that it happens in Firefox as well as Chrome. Harmless but
  annoying.
*/
"use strict";
(function(){
  /**
     Posts a message in the form {type,data}. If passed more than 2
     args, the 3rd must be an array of "transferable" values to pass
     as the 2nd argument to postMessage(). */
  const wMsg =
        (type,data,transferables)=>{
          postMessage({type, data}, transferables || []);
        };
  const stdout = (...args)=>wMsg('stdout', args);
  const stderr = (...args)=>wMsg('stderr', args);
  const toss = (...args)=>{
    throw new Error(args.join(' '));
  };
  const fixmeOPFS = "(FIXME: won't work with vanilla OPFS.)";
  let sqlite3 /* gets assigned when the wasm module is loaded */;

  self.onerror = function(/*message, source, lineno, colno, error*/) {
    const err = arguments[4];
    if(err && 'ExitStatus'==err.name){
      /* This is relevant for the sqlite3 shell binding but not the
         lower-level binding. */
      fiddleModule.isDead = true;
      stderr("FATAL ERROR:", err.message);
      stderr("Restarting the app requires reloading the page.");
      wMsg('error', err);
    }
    console.error(err);
    fiddleModule.setStatus('Exception thrown, see JavaScript console: '+err);
  };

  const Sqlite3Shell = {
    /** Returns the name of the currently-opened db. */
    dbFilename: function f(){
      if(!f._) f._ = sqlite3.capi.wasm.xWrap('fiddle_db_filename', "string", ['string']);
      return f._(0);
    },
    dbHandle: function f(){
      if(!f._) f._ = sqlite3.capi.wasm.xWrap("fiddle_db_handle", "sqlite3*");
      return f._();
    },
    dbIsOpfs: function f(){
      return sqlite3.opfs && sqlite3.capi.sqlite3_web_db_uses_vfs(
        this.dbHandle(), "opfs"
      );
    },
    runMain: function f(){
      if(f.argv) return 0===f.argv.rc;
      const dbName = "/fiddle.sqlite3";
      f.argv = [
        'sqlite3-fiddle.wasm',
        '-bail', '-safe',
        dbName
        /* Reminder: because of how we run fiddle, we have to ensure
           that any argv strings passed to its main() are valid until
           the wasm environment shuts down. */
      ];
      const capi = sqlite3.capi;
      /* We need to call sqlite3_shutdown() in order to avoid numerous
         legitimate warnings from the shell about it being initialized
         after sqlite3_initialize() has been called. This means,
         however, that any initialization done by the JS code may need
         to be re-done (e.g.  re-registration of dynamically-loaded
         VFSes). We need a more generic approach to running such
         init-level code. */
      capi.sqlite3_shutdown();
      f.argv.pArgv = capi.wasm.allocMainArgv(f.argv);
      f.argv.rc = capi.wasm.exports.fiddle_main(
        f.argv.length, f.argv.pArgv
      );
      if(f.argv.rc){
        stderr("Fatal error initializing sqlite3 shell.");
        fiddleModule.isDead = true;
        return false;
      }
      stdout("SQLite version", capi.sqlite3_libversion(),
             capi.sqlite3_sourceid().substr(0,19));
      stdout('Welcome to the "fiddle" shell.');
      if(S.opfs){
        stdout("\nOPFS is available. To open a persistent db, use:\n\n",
               "  .open file:name?vfs=opfs\n\nbut note that some",
               "features (e.g. upload) do not yet work with OPFS.");
        S.opfs.registerVfs();
      }
      stdout('\nEnter ".help" for usage hints.');
      this.exec([ // initialization commands...
        '.nullvalue NULL',
        '.headers on'
      ].join('\n'));
      return true;
    },
    /**
       Runs the given text through the shell as if it had been typed
       in by a user. Fires a working/start event before it starts and
       working/end event when it finishes.
    */
    exec: function f(sql){
      if(!f._){
        if(!this.runMain()) return;
        f._ = sqlite3.capi.wasm.xWrap('fiddle_exec', null, ['string']);
      }
      if(fiddleModule.isDead){
        stderr("shell module has exit()ed. Cannot run SQL.");
        return;
      }
      wMsg('working','start');
      try {
        if(f._running){
          stderr('Cannot run multiple commands concurrently.');
        }else if(sql){
          if(Array.isArray(sql)) sql = sql.join('');
          f._running = true;
          f._(sql);
        }
      }finally{
        delete f._running;
        wMsg('working','end');
      }
    },
    resetDb: function f(){
      if(Sqlite3Shell.dbIsOpfs()){
        /* The problem is that fiddle_reset_db() uses the POSIX APIs
           for file removal, which cannot see OPFS-hosted files. */
        stderr("TODO: cannot currently reset an OPFS-hosted db.");
        return;
      }
      if(!f._) f._ = sqlite3.capi.wasm.xWrap('fiddle_reset_db', null);
      stdout("Resetting database.",fixmeOPFS);
      f._();
      stdout("Reset",this.dbFilename());
    },
    /* Interrupt can't work: this Worker is tied up working, so won't get the
       interrupt event which would be needed to perform the interrupt. */
    interrupt: function f(){
      if(!f._) f._ = sqlite3.capi.wasm.xWrap('fiddle_interrupt', null);
      stdout("Requesting interrupt.");
      f._();
    }
  };

  /**
     Exports the shell's current db file in such a way that it can
     export DBs hosted in the Emscripten-supplied FS or in native OPFS
     (and, hypothetically, kvvfs). Throws on error. On success returns
     a Blob containing the whole db contents.

     Bug/to investigate: xFileSize() is returning garbage for the
     default VFS but works in OPFS. Thus for exporting that impl we'll
     use the fiddleModule.FS API for the time being. The equivalent
     native impl, fiddle_export_db(), works okay with both VFSes, so
     the bug is apparently in (or via) this code.
  */
  const brokenExportDbFileToBlob = function(){
    const capi = sqlite3.capi, wasm = capi.wasm;
    const pDb = Sqlite3Shell.dbHandle();
    if(!pDb) toss("No db is opened.");
    const scope = wasm.scopedAllocPush();
    try{
      const ppFile = wasm.scopedAlloc(12/*sizeof(i32 + i64)*/);
      const pFileSize = ppFile + 4;
      wasm.setMemValue(ppFile, 0, '*');
      let rc = capi.sqlite3_file_control(
        pDb, "main", capi.SQLITE_FCNTL_FILE_POINTER, ppFile
      );
      if(rc) toss("Cannot get sqlite3_file handle.");
      const jFile = new capi.sqlite3_file(wasm.getPtrValue(ppFile));
      const jIom = new capi.sqlite3_io_methods(jFile.$pMethods);
      const xFileSize = wasm.functionEntry(jIom.$xFileSize);
      const xRead = wasm.functionEntry(jIom.$xRead);
      wasm.setMemValue(pFileSize, 0, 'i64');
      //stderr("nFileSize =",wasm.getMemValue(pFileSize,'i64'),"pFileSize =",pFileSize);
      rc = xFileSize( jFile.pointer, pFileSize );
      if(rc) toss("Cannot get db file size.");
      //stderr("nFileSize =",wasm.getMemValue(pFileSize,'i64'),"pFileSize =",pFileSize);
      const nFileSize = Number( wasm.getMemValue(pFileSize,'i64') );
      if(nFileSize <= 0n || nFileSize>=Number.MAX_SAFE_INTEGER){
        toss("Unexpected DB size:",nFileSize);
      }
      //stderr("nFileSize =",nFileSize,"pFileSize =",pFileSize);
      const nIobuf = 1024 * 4;
      const iobuf = wasm.scopedAlloc(nIobuf);
      let nPos = 0;
      const blobList = [];
      const heap = wasm.heap8u();
      for( ; nPos < nFileSize; nPos += nIobuf ){
        rc = xRead(jFile.pointer, iobuf, nIobuf, BigInt(nPos));
        if(rc){
          if(capi.SQLITE_IOERR_SHORT_READ === rc){
            //stderr('rc =',rc,'nPos =',nPos,'nIobuf =',nIobuf,'nFileSize =',nFileSize);
            rc = ((nPos + nIobuf) < nFileSize) ? rc : 0/*assume EOF*/;
          }
          if(rc) toss("xRead() SQLITE_xxx error #"+rc,capi.sqlite3_wasm_rc_str(rc));
        }
        blobList.push(heap.slice(iobuf, iobuf+nIobuf));
      }
      return new Blob(blobList);
    }catch(e){
      console.error('exportDbFileToBlob()',e);
      stderr("exportDbFileToBlob():",e.message);
    }finally{
      wasm.scopedAllocPop(scope);
    }
  }/*brokenExportDbFileToBlob()*/;

  const exportDbFileToBlob = function f(){
    if(!f._){
      f._ = sqlite3.capi.wasm.xWrap('fiddle_export_db', 'int', '*');
    }
    const capi = sqlite3.capi;
    const wasm = capi.wasm;
    const blobList = [];
    const heap = wasm.heap8u();
    const callback = wasm.installFunction('ipi', function(buf, n){
      blobList.push(heap.slice(buf, buf+n));
      return 0;
    });
    try {
      const rc = wasm.exports.fiddle_export_db( callback );
      if(rc) toss("DB export failed with code", capi.sqlite3_wasm_rc_str(rc));
      return new Blob(blobList);
    }catch(e){
      console.error("exportDbFileToBlob():",e.message);
      throw(e);
    }finally{
      wasm.uninstallFunction(callback);
    }
  }/*exportDbFileToBlob()*/;
  
  self.onmessage = function f(ev){
    ev = ev.data;
    if(!f.cache){
      f.cache = {
        prevFilename: null
      };
    }
    //console.debug("worker: onmessage.data",ev);
    switch(ev.type){
        case 'shellExec': Sqlite3Shell.exec(ev.data); return;
        case 'db-reset': Sqlite3Shell.resetDb(); return;
        case 'interrupt': Sqlite3Shell.interrupt(); return;
          /** Triggers the export of the current db. Fires an
              event in the form:

              {type:'db-export',
                data:{
                  filename: name of db,
                  buffer: contents of the db file (Uint8Array),
                  error: on error, a message string and no buffer property.
                }
              }
          */
        case 'db-export': {
          const fn = Sqlite3Shell.dbFilename();
          stdout("Exporting",fn+".");
          const fn2 = fn ? fn.split(/[/\\]/).pop() : null;
          try{
            if(!fn2) throw new Error("DB appears to be closed.");
            exportDbFileToBlob().arrayBuffer().then((buffer)=>{
              wMsg('db-export',{filename: fn2, buffer}, [buffer]);
            });
          }catch(e){
            /* Post a failure message so that UI elements disabled
               during the export can be re-enabled. */
            wMsg('db-export',{
              filename: fn,
              error: e.message
            });
          }
          return;
        }
        case 'open': {
          /* Expects: {
               buffer: ArrayBuffer | Uint8Array,
               filename: the filename for the db. Any dir part is
                         stripped.
              }
          */
          const opt = ev.data;
          let buffer = opt.buffer;
          stderr('open():',fixmeOPFS);
          if(buffer instanceof ArrayBuffer){
            buffer = new Uint8Array(buffer);
          }else if(!(buffer instanceof Uint8Array)){
            stderr("'open' expects {buffer:Uint8Array} containing an uploaded db.");
            return;
          }
          const fn = (
            opt.filename
              ? opt.filename.split(/[/\\]/).pop().replace('"','_')
              : ("db-"+((Math.random() * 10000000) | 0)+
                 "-"+((Math.random() * 10000000) | 0)+".sqlite3")
          );
          try {
            /* We cannot delete the existing db file until the new one
               is installed, which means that we risk overflowing our
               quota (if any) by having both the previous and current
               db briefly installed in the virtual filesystem. */
            const fnAbs = '/'+fn;
            const oldName = Sqlite3Shell.dbFilename();
            if(oldName && oldName===fnAbs){
              /* We cannot create the replacement file while the current file
                 is opened, nor does the shell have a .close command, so we
                 must temporarily switch to another db... */
              Sqlite3Shell.exec('.open :memory:');
              fiddleModule.FS.unlink(fnAbs);
            }
            fiddleModule.FS.createDataFile("/", fn, buffer, true, true);
            Sqlite3Shell.exec('.open "'+fnAbs+'"');
            if(oldName && oldName!==fnAbs){
              try{fiddleModule.fsUnlink(oldName)}
              catch(e){/*ignored*/}
            }
            stdout("Replaced DB with",fn+".");
          }catch(e){
            stderr("Error installing db",fn+":",e.message);
          }
          return;
        }
    };
    console.warn("Unknown fiddle-worker message type:",ev);
  };
  
  /**
     emscripten module for use with build mode -sMODULARIZE.
  */
  const fiddleModule = {
    print: stdout,
    printErr: stderr,
    /**
       Intercepts status updates from the emscripting module init
       and fires worker events with a type of 'status' and a
       payload of:

       {
       text: string | null, // null at end of load process
       step: integer // starts at 1, increments 1 per call
       }

       We have no way of knowing in advance how many steps will
       be processed/posted, so creating a "percentage done" view is
       not really practical. One can be approximated by giving it a
       current value of message.step and max value of message.step+1,
       though.

       When work is finished, a message with a text value of null is
       submitted.

       After a message with text==null is posted, the module may later
       post messages about fatal problems, e.g. an exit() being
       triggered, so it is recommended that UI elements for posting
       status messages not be outright removed from the DOM when
       text==null, and that they instead be hidden until/unless
       text!=null.
    */
    setStatus: function f(text){
      if(!f.last) f.last = { step: 0, text: '' };
      else if(text === f.last.text) return;
      f.last.text = text;
      wMsg('module',{
        type:'status',
        data:{step: ++f.last.step, text: text||null}
      });
    }
  };

  importScripts('fiddle-module.js'+self.location.search);
  /**
     initFiddleModule() is installed via fiddle-module.js due to
     building with:

     emcc ... -sMODULARIZE=1 -sEXPORT_NAME=initFiddleModule
  */
  sqlite3InitModule(fiddleModule).then((_sqlite3)=>{
    sqlite3 = _sqlite3;
    fiddleModule.fsUnlink = (fn)=>{
      stderr("unlink:",fixmeOPFS);
      return sqlite3.capi.wasm.sqlite3_wasm_vfs_unlink(fn);
    };
    wMsg('fiddle-ready');
  })/*then()*/;
})();
