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
'use strict';
(function(self){
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
  self.SqliteTestUtil = {
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
    /**
       Works like mustThrow() but expects filter to be a regex,
       function, or string to match/filter the resulting exception
       against. If f() does not throw, this test fails and an Error is
       thrown. If filter is a regex, the test passes if
       filter.test(error.message) passes. If it's a function, the test
       passes if filter(error) returns truthy. If it's a string, the
       test passes if the filter matches the exception message
       precisely. In all other cases the test fails, throwing an
       Error.

       If it throws, msg is used as the error report unless it's falsy,
       in which case a default is used.
    */
    mustThrowMatching: function(f, filter, msg){
      ++this.counter;
      let err;
      try{ f(); } catch(e){err=e;}
      if(!err) throw new Error(msg || "Expected exception.");
      let pass = false;
      if(filter instanceof RegExp) pass = filter.test(err.message);
      else if(filter instanceof Function) pass = filter(err);
      else if('string' === typeof filter) pass = (err.message === filter);
      if(!pass){
        throw new Error(msg || ("Filter rejected this exception: "+err.message));
      }
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
    },

    /**
       Parses window.location.search-style string into an object
       containing key/value pairs of URL arguments (already
       urldecoded). The object is created using Object.create(null),
       so contains only parsed-out properties and has no prototype
       (and thus no inherited properties).

       If the str argument is not passed (arguments.length==0) then
       window.location.search.substring(1) is used by default. If
       neither str is passed in nor window exists then false is returned.

       On success it returns an Object containing the key/value pairs
       parsed from the string. Keys which have no value are treated
       has having the boolean true value.

       Pedantic licensing note: this code has appeared in other source
       trees, but was originally written by the same person who pasted
       it into those trees.
    */
    processUrlArgs: function(str) {
      if( 0 === arguments.length ) {
        if( ('undefined' === typeof window) ||
            !window.location ||
            !window.location.search )  return false;
        else str = (''+window.location.search).substring(1);
      }
      if( ! str ) return false;
      str = (''+str).split(/#/,2)[0]; // remove #... to avoid it being added as part of the last value.
      const args = Object.create(null);
      const sp = str.split(/&+/);
      const rx = /^([^=]+)(=(.+))?/;
      var i, m;
      for( i in sp ) {
        m = rx.exec( sp[i] );
        if( ! m ) continue;
        args[decodeURIComponent(m[1])] = (m[3] ? decodeURIComponent(m[3]) : true);
      }
      return args;
    }
  };

  /**
     This is a module object for use with the emscripten-installed
     sqlite3InitModule() factory function.
  */
  self.sqlite3TestModule = {
    /**
       Array of functions to call after Emscripten has initialized the
       wasm module. Each gets passed the Emscripten module object
       (which is _this_ object).
    */
    postRun: [
      /* function(theModule){...} */
    ],
    //onRuntimeInitialized: function(){},
    /* Proxy for C-side stdout output. */
    print: (...args)=>{console.log(...args)},
    /* Proxy for C-side stderr output. */
    printErr: (...args)=>{console.error(...args)},
    /**
       Called by the Emscripten module init bits to report loading
       progress. It gets passed an empty argument when loading is done
       (after onRuntimeInitialized() and any this.postRun callbacks
       have been run).
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
    },
    /**
       Config options used by the Emscripten-dependent initialization
       which happens via this.initSqlite3(). This object gets
       (indirectly) passed to sqlite3ApiBootstrap() to configure the
       sqlite3 API.
    */
    sqlite3ApiConfig: {
      wasmfsOpfsDir: "/opfs"
    },
    /**
       Intended to be called by apps which need to call the
       Emscripten-installed sqlite3InitModule() routine. This function
       temporarily installs this.sqlite3ApiConfig into the self
       object, calls it sqlite3InitModule(), and removes
       self.sqlite3ApiConfig after initialization is done. Returns the
       promise from sqlite3InitModule(), and the next then() handler
       will get the sqlite3 API object as its argument.
    */
    initSqlite3: function(){
      self.sqlite3ApiConfig = this.sqlite3ApiConfig;
      return self.sqlite3InitModule(this).finally(()=>delete self.sqlite3ApiConfig);
    }
  };
})(self/*window or worker*/);
