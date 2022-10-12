/*
  2022-10-12

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  Main functional and regression tests for the sqlite3 WASM API.
*/
'use strict';
(function(){
  /**
     Set up our output channel differently depending
     on whether we are running in a worker thread or
     the main (UI) thread.
  */
  let logHtml;
  const isUIThread = ()=>(self.window===self && self.document);
  const mapToString = (v)=>{
    switch(typeof v){
        case 'number': case 'string': case 'boolean':
        case 'undefined':
          return ''+v;
        default: break;
    }
    if(null===v) return 'null';
    return JSON.stringify(v,undefined,2);
  };
  const normalizeArgs = (args)=>args.map(mapToString);
  if( isUIThread() ){
    console.log("Running UI thread.");
    logHtml = function(cssClass,...args){
      const ln = document.createElement('div');
      if(cssClass) ln.classList.add(cssClass);
      ln.append(document.createTextNode(normalizeArgs(args).join(' ')));
      document.body.append(ln);
    };
  }else{ /* Worker thread */
    console.log("Running Worker thread.");
    logHtml = function(cssClass,...args){
      postMessage({
        type:'log',
        payload:{cssClass, args: normalizeArgs(args)}
      });
    };
  }
  const log = (...args)=>{
    //console.log(...args);
    logHtml('',...args);
  }
  const warn = (...args)=>{
    console.warn(...args);
    logHtml('warning',...args);
  }
  const error = (...args)=>{
    console.error(...args);
    logHtml('error',...args);
  };

  const toss = (...args)=>{
    error(...args);
    throw new Error(args.join(' '));
  };

  /**
     Helpers for writing sqlite3-specific tests.
  */
  const TestUtil = {
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
    TestGroup: class {
      constructor(name){
        this.name = name;
        this.tests = [];
      }
      push(name,callback){
      }
    }/*TestGroup*/,
    testGroups: [],
    currentTestGroup: undefined,
    addGroup: function(name){
      if(this.testGroups[name]){
        toss("Test group already exists:",name);
      }
      this.testGroups.push( this.currentTestGroup = new this.TestGroup(name) );
      return this;
    },
    addTest: function(name, callback){
      this.currentTestGroup.push(name, callback);
    },
    runTests: function(){
      toss("TODO: runTests()");
    }
  }/*TestUtil*/;

  
  log("Loading and initializing sqlite3 WASM module...");
  if(!isUIThread()){
    importScripts("sqlite3.js");
  }
  self.sqlite3InitModule({
    print: log,
    printErr: error
  }).then(function(sqlite3){
    //console.log('sqlite3 =',sqlite3);
    log("Done initializing. Running tests...");
    try {
      TestUtil.runTests();
    }catch(e){
      error("Tests failed:",e.message);
    }
  });
})();
