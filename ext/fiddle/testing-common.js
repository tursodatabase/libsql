/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file contains bootstrapping code used by various test scripts
  which live in this file's directory.
*/
(function(){
    /* querySelectorAll() proxy */
    const EAll = function(/*[element=document,] cssSelector*/){
        return (arguments.length>1 ? arguments[0] : document)
            .querySelectorAll(arguments[arguments.length-1]);
    };
    /* querySelector() proxy */
    const E = function(/*[element=document,] cssSelector*/){
        return (arguments.length>1 ? arguments[0] : document)
            .querySelector(arguments[arguments.length-1]);
    };

    /* emscripten-related bits... */
    const statusElement = E('#module-status');
    const progressElement = E('#module-progress');
    const spinnerElement = E('#module-spinner');
    self.Module = {
        /* ^^^ cannot declare that const because sqlite3.js
           (auto-generated) includes a decl for it and runs in this
           scope. */
        preRun: [],
        postRun: [],
        //onRuntimeInitialized: function(){},
        print: function(){
            console.log(Array.prototype.slice.call(arguments));
        },
        printErr: function(){
            console.error(Array.prototype.slice.call(arguments));
        },
        setStatus: function f(text){
            if(!f.last) f.last = { time: Date.now(), text: '' };
            if(text === f.last.text) return;
            const m = text.match(/([^(]+)\((\d+(\.\d+)?)\/(\d+)\)/);
            const now = Date.now();
            if(m && now - f.last.time < 30) return; // if this is a progress update, skip it if too soon
            f.last.time = now;
            f.last.text = text;
            if(m) {
                text = m[1];
                progressElement.value = parseInt(m[2])*100;
                progressElement.max = parseInt(m[4])*100;
                progressElement.hidden = false;
                spinnerElement.hidden = false;
            } else {
                progressElement.remove();
                if(!text) spinnerElement.remove();
            }
            if(text) statusElement.innerText = text;
            else statusElement.remove();
        },
        totalDependencies: 0,
        monitorRunDependencies: function(left) {
            this.totalDependencies = Math.max(this.totalDependencies, left);
            this.setStatus(left
                           ? ('Preparing... (' + (this.totalDependencies-left)
                              + '/' + this.totalDependencies + ')')
                           : 'All downloads complete.');
        },
        /* Loads sqlite3-api.js and calls the given callback (if
           provided), passing it an object which contains the sqlite3
           and SQLite3 modules. Whether this is synchronous or async
           depends on whether it's run in the main thread or a
           worker.*/
        loadSqliteAPI: function(callback){
            const theScript = 'sqlite3-api.js';
            if(self.importScripts){/*worker*/
                importScripts(theScript);
                if(callback) callback(self.sqlite3);
            }else{/*main thread*/
                new Promise((resolve, reject) => {
                    const script = document.createElement('script');
                    document.body.appendChild(script);
                    script.onload = resolve;
                    script.onerror = reject;
                    script.async = true;
                    script.src = theScript;
                }).then(() => {
                    if(callback) callback({sqlite3:self.sqlite3,
                                           SQLite3:self.SQLite3});
                });
            }
        }
    };
    
    /**
       Helpers for writing sqlite3-specific tests.
    */
    self.SqliteTester = {
        /** Running total of the number of tests run via
            this API. */
        counter: 0,
        /**
           If expr is a function, it is called and its result
           is returned, coerced to a bool, else expr, coerced to
           a bool, is returned.
        */
        toBool: function(expr){
            return (expr instanceof Function) ? !!expr() : !!expr;
        },
        /** abort() if expr is false. If expr is a function, it
            is called and its result is evaluated.
        */
        assert: function(expr, msg){
            ++this.counter;
            if(!this.toBool(expr)) abort(msg || "Assertion failed.");
            return this;
        },
        /** Identical to assert() but throws instead of calling
            abort(). */
        affirm: function(expr, msg){
            ++this.counter;
            if(!this.toBool(expr)) throw new Error(msg || "Affirmation failed.");
            return this;
        },
        /** Calls f() and squelches any exception it throws. If it
            does not throw, this function throws. */
        mustThrow: function(f, msg){
            ++this.counter;
            let err;
            try{ f(); } catch(e){err=e;}
            if(!err) throw new Error(msg || "Expected exception.");
            return this;
        },
        /** Throws if expr is truthy or expr is a function and expr()
            returns truthy. */
        throwIf: function(expr, msg){
            ++this.counter;
            if(this.toBool(expr)) throw new Error(msg || "throwIf() failed");
            return this;
        },
        /** Throws if expr is falsy or expr is a function and expr()
            returns falsy. */
        throwUnless: function(expr, msg){
            ++this.counter;
            if(!this.toBool(expr)) throw new Error(msg || "throwUnless() failed");
            return this;
        }
    };

})(self/*window or worker*/);
