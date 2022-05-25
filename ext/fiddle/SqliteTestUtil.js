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

    /**
       Helpers for writing sqlite3-specific tests.
    */
    self/*window or worker*/.SqliteTestUtil = {
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
        assert: function f(expr, msg){
            if(!f._){
                f._ = ('undefined'===typeof abort
                       ? (msg)=>{throw new Error(msg)}
                       : abort);
            }
            ++this.counter;
            if(!this.toBool(expr)){
                f._(msg || "Assertion failed.");
            }
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

    
    /**
       This is a module object for use with the emscripten-installed
       initSqlite3Module() factory function.
    */
    self.sqlite3TestModule = {
        postRun: [
            /* function(theModule){...} */
        ],
        //onRuntimeInitialized: function(){},
        /* Proxy for C-side stdout output. */
        print: function(){
            console.log.apply(console, Array.prototype.slice.call(arguments));
        },
        /* Proxy for C-side stderr output. */
        printErr: function(){
            console.error.apply(console, Array.prototype.slice.call(arguments));
        },
        /**
           Called by the module init bits to report loading
           progress. It gets passed an empty argument when loading is
           done (after onRuntimeInitialized() and any this.postRun
           callbacks have been run).
        */
        setStatus: function f(text){
            if(!f.last){
                f.last = { text: '', step: 0 };
                f.ui = {
                    status: E('#module-status'),
                    progress: E('#module-progress'),
                    spinner: E('#module-spinner')
                };
            }
            if(text === f.last.text) return;
            f.last.text = text;
            if(f.ui.progress){
                f.ui.progress.value = f.last.step;
                f.ui.progress.max = f.last.step + 1;
            }
            ++f.last.step;
            if(text) {
                f.ui.status.classList.remove('hidden');
                f.ui.status.innerText = text;
            }else{
                if(f.ui.progress){
                    f.ui.progress.remove();
                    f.ui.spinner.remove();
                    delete f.ui.progress;
                    delete f.ui.spinner;
                }
                f.ui.status.classList.add('hidden');
            }
        }
    };
})(self/*window or worker*/);
