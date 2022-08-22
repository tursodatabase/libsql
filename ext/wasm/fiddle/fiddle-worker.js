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
       Posts a message in the form {type,data} unless passed more than 2
       args, in which case it posts {type, data:[arg1...argN]}.
    */
    const wMsg = function(type,data){
        postMessage({
            type,
            data: arguments.length<3
                ? data
                : Array.prototype.slice.call(arguments,1)
        });
    };

    const stdout = function(){wMsg('stdout', Array.prototype.slice.call(arguments));};
    const stderr = function(){wMsg('stderr', Array.prototype.slice.call(arguments));};

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
            if(!f._) f._ = fiddleModule.cwrap('fiddle_db_filename', "string", ['string']);
            return f._();
        },
        /**
           Runs the given text through the shell as if it had been typed
           in by a user. Fires a working/start event before it starts and
           working/end event when it finishes.
        */
        exec: function f(sql){
            if(!f._) f._ = fiddleModule.cwrap('fiddle_exec', null, ['string']);
            if(fiddleModule.isDead){
                stderr("shell module has exit()ed. Cannot run SQL.");
                return;
            }
            wMsg('working','start');
            try {
                if(f._running){
                    stderr('Cannot run multiple commands concurrently.');
                }else{
                    f._running = true;
                    f._(sql);
                }
            } finally {
                delete f._running;
                wMsg('working','end');
            }
        },
        resetDb: function f(){
            if(!f._) f._ = fiddleModule.cwrap('fiddle_reset_db', null);
            stdout("Resetting database.");
            f._();
            stdout("Reset",this.dbFilename());
        },
        /* Interrupt can't work: this Worker is tied up working, so won't get the
           interrupt event which would be needed to perform the interrupt. */
        interrupt: function f(){
            if(!f._) f._ = fiddleModule.cwrap('fiddle_interrupt', null);
            stdout("Requesting interrupt.");
            f._();
        }
    };

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
                    wMsg('db-export',{
                        filename: fn2,
                        buffer: fiddleModule.FS.readFile(fn, {encoding:"binary"})
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
                   filename: for logging/informational purposes only
                   } */
                const opt = ev.data;
                let buffer = opt.buffer;
                if(buffer instanceof Uint8Array){
                }else if(buffer instanceof ArrayBuffer){
                    buffer = new Uint8Array(buffer);
                }else{
                    stderr("'open' expects {buffer:Uint8Array} containing an uploaded db.");
                    return;
                }
                const fn = (
                    opt.filename
                        ? opt.filename.split(/[/\\]/).pop().replace('"','_')
                        : ("db-"+((Math.random() * 10000000) | 0)+
                           "-"+((Math.random() * 10000000) | 0)+".sqlite3")
                );
                /* We cannot delete the existing db file until the new one
                   is installed, which means that we risk overflowing our
                   quota (if any) by having both the previous and current
                   db briefly installed in the virtual filesystem. */
                fiddleModule.FS.createDataFile("/", fn, buffer, true, true);
                const oldName = Sqlite3Shell.dbFilename();
                Sqlite3Shell.exec('.open "/'+fn+'"');
                if(oldName && oldName !== fn){
                    try{fiddleModule.FS.unlink(oldName);}
                    catch(e){/*ignored*/}
                }
                stdout("Replaced DB with",fn+".");
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

    importScripts('fiddle-module.js');
    /**
       initFiddleModule() is installed via fiddle-module.js due to
       building with:

       emcc ... -sMODULARIZE=1 -sEXPORT_NAME=initFiddleModule
    */
    initFiddleModule(fiddleModule).then(function(thisModule){
        wMsg('fiddle-ready');
    });
})();
