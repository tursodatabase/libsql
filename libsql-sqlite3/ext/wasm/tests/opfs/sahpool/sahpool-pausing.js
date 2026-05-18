/*
  2025-01-31

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  These tests are specific to the opfs-sahpool VFS and are limited to
  demonstrating its pause/unpause capabilities.

  Most of this file is infrastructure for displaying results to the
  user. Search for runTests() to find where the work actually starts.
*/
'use strict';
(function(){
  let logClass;

  const mapToString = (v)=>{
    switch(typeof v){
      case 'number': case 'string': case 'boolean':
      case 'undefined': case 'bigint':
        return ''+v;
      default: break;
    }
    if(null===v) return 'null';
    if(v instanceof Error){
      v = {
        message: v.message,
        stack: v.stack,
        errorClass: v.name
      };
    }
    return JSON.stringify(v,undefined,2);
  };
  const normalizeArgs = (args)=>args.map(mapToString);
  const logTarget = document.querySelector('#test-output');
  logClass = function(cssClass,...args){
    const ln = document.createElement('div');
    if(cssClass){
      for(const c of (Array.isArray(cssClass) ? cssClass : [cssClass])){
        ln.classList.add(c);
      }
    }
    ln.append(document.createTextNode(normalizeArgs(args).join(' ')));
    logTarget.append(ln);
  };
  const cbReverse = document.querySelector('#cb-log-reverse');
  //cbReverse.setAttribute('checked','checked');
  const cbReverseKey = 'tester1:cb-log-reverse';
  const cbReverseIt = ()=>{
    logTarget.classList[cbReverse.checked ? 'add' : 'remove']('reverse');
    //localStorage.setItem(cbReverseKey, cbReverse.checked ? 1 : 0);
  };
  cbReverse.addEventListener('change', cbReverseIt, true);
  /*if(localStorage.getItem(cbReverseKey)){
    cbReverse.checked = !!(+localStorage.getItem(cbReverseKey));
    }*/
  cbReverseIt();

  const log = (...args)=>{
    //console.log(...args);
    logClass('',...args);
  }
  const warn = (...args)=>{
    console.warn(...args);
    logClass('warning',...args);
  }
  const error = (...args)=>{
    console.error(...args);
    logClass('error',...args);
  };

  const toss = (...args)=>{
    error(...args);
    throw new Error(args.join(' '));
  };

  const endOfWork = (passed=true)=>{
    const eH = document.querySelector('#color-target');
    const eT = document.querySelector('title');
    if(passed){
      log("End of work chain. If you made it this far, you win.");
      eH.innerText = 'PASS: '+eH.innerText;
      eH.classList.add('tests-pass');
      eT.innerText = 'PASS: '+eT.innerText;
    }else{
      eH.innerText = 'FAIL: '+eH.innerText;
      eH.classList.add('tests-fail');
      eT.innerText = 'FAIL: '+eT.innerText;
    }
  };

  const nextHandlerQueue = [];

  const nextHandler = function(workerId,...msg){
    log(workerId,...msg);
    (nextHandlerQueue.shift())();
  };

  const postThen = function(W, msgType, callback){
    nextHandlerQueue.push(callback);
    W.postMessage({type:msgType});
  };

  /**
     Run a series of operations on an sahpool db spanning two workers.
     This would arguably be more legible with Promises, but creating a
     Promise-based communication channel for this purpose is left as
     an exercise for the reader. An example of such a proxy can be
     found in the SQLite source tree:

     https://sqlite.org/src/file/ext/wasm/api/sqlite3-worker1-promiser.c-pp.js
  */
  const runPyramidOfDoom = function(W1, W2){
    postThen(W1, 'vfs-acquire', function(){
      postThen(W1, 'db-init', function(){
        postThen(W1, 'db-query', function(){
          postThen(W1, 'vfs-pause', function(){
            postThen(W2, 'vfs-acquire', function(){
              postThen(W2, 'db-query', function(){
                postThen(W2, 'vfs-remove', endOfWork);
              });
            });
          });
        });
      });
    });
  };

  const runTests = function(){
    log("Running opfs-sahpool pausing tests...");
    const wjs = 'sahpool-worker.js?sqlite3.dir=../../../jswasm';
    const W1 = new Worker(wjs+'&workerId=w1'),
          W2 = new Worker(wjs+'&workerId=w2');
    W1.workerId = 'w1';
    W2.workerId = 'w2';
    let initCount = 0;
    const onmessage = function({data}){
      //log("onmessage:",data);
      switch(data.type){
        case 'vfs-acquired':
          nextHandler(data.workerId, "VFS acquired");
          break;
        case 'vfs-paused':
          nextHandler(data.workerId, "VFS paused");
          break;
        case 'vfs-unpaused':
          nextHandler(data.workerId, 'VFS unpaused');
          break;
        case 'vfs-removed':
          nextHandler(data.workerId, 'VFS removed');
          break;
        case 'db-inited':
          nextHandler(data.workerId, 'db initialized');
          break;
        case 'query-result':
          nextHandler(data.workerId, 'query result', data.payload);
          break;
        case 'log':
          log(data.workerId, ':', ...data.payload);
          break;
        case 'error':
          error(data.workerId, ':', ...data.payload);
          endOfWork(false);
          break;
        case 'initialized':
          log(data.workerId, ': Worker initialized',...data.payload);
          if( 2===++initCount ){
            runPyramidOfDoom(W1, W2);
          }
          break;
      }
    };
    W1.onmessage = W2.onmessage = onmessage;
  };

  runTests();
})();
