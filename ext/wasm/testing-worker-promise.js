/*
  2022-08-23

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  
  UNDER CONSTRUCTION: a Promise-based proxy for for the sqlite3 Worker
  #1 API.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const DbState = {
    id: undefined
  };
  const eOutput = document.querySelector('#test-output');
  const log = console.log.bind(console);
  const logHtml = async function(cssClass,...args){
    log.apply(this, args);
    const ln = document.createElement('div');
    if(cssClass) ln.classList.add(cssClass);
    ln.append(document.createTextNode(args.join(' ')));
    eOutput.append(ln);
  };
  const warn = console.warn.bind(console);
  const error = console.error.bind(console);

  let startTime;
  const logEventResult = async function(evd){
    logHtml(evd.errorClass ? 'error' : '',
            "response to",evd.messageId,"Worker time =",
            (evd.workerRespondTime - evd.workerReceivedTime),"ms.",
            "Round-trip event time =",
            (performance.now() - evd.departureTime),"ms.",
            (evd.errorClass ? evd.message : "")
           );
  };

  const testCount = async ()=>{
    logHtml("","Total test count:",T.counter+". Total time =",(performance.now() - startTime),"ms");
  };

  // Inspiration: https://stackoverflow.com/a/52439530
  const worker = new Worker("sqlite3-worker1.js");
  worker.onerror = function(event){
    error("worker.onerror",event);
  };
  const WorkerPromiseHandler = Object.create(null);
  WorkerPromiseHandler.nextId = function f(){
    return 'msg#'+(f._ = (f._ || 0) + 1);
  };

  /** Posts a worker message as {type:eventType, data:eventData}. */
  const requestWork = async function(eventType, eventData){
    //log("requestWork", eventType, eventData);
    T.assert(eventData && 'object'===typeof eventData);
    /* ^^^ that is for the testing and messageId-related code, not
       a hard requirement of all of the Worker-exposed APIs. */
    const wph = WorkerPromiseHandler;
    const msgId = wph.nextId();
    const proxy = wph[msgId] = Object.create(null);
    proxy.promise = new Promise(function(resolve, reject){
      proxy.resolve = resolve;
      proxy.reject = reject;
      const msg = {
        type: eventType,
        args: eventData,
        dbId: DbState.id,
        messageId: msgId,
        departureTime: performance.now()
      };
      log("Posting",eventType,"message to worker dbId="+(DbState.id||'default')+':',msg);
      worker.postMessage(msg);
    });
    log("Set up promise",proxy);
    return proxy.promise;
  };


  const runOneTest = async function(eventType, eventData, callback){
    T.assert(eventData && 'object'===typeof eventData);
    /* ^^^ that is for the testing and messageId-related code, not
       a hard requirement of all of the Worker-exposed APIs. */
    let p = requestWork(eventType, eventData);
    if(callback) p.then(callback).finally(testCount);
    return p;
  };

  const runTests = async function(){
    logHtml('',
            "Sending 'open' message and waiting for its response before continuing.");
    startTime = performance.now();
    runOneTest('open', {
      filename:'testing2.sqlite3',
      simulateError: 0 /* if true, fail the 'open' */
    }, function(ev){
      log("then open result",ev);
      T.assert('testing2.sqlite3'===ev.result.filename)
        .assert(ev.dbId)
        .assert(ev.messageId)
        .assert(DbState.id === ev.dbId);
    }).catch((err)=>error("error response:",err));
  };

  worker.onmessage = function(ev){
    ev = ev.data;
    (('error'===ev.type) ? error : log)('worker.onmessage',ev);
    const msgHandler = WorkerPromiseHandler[ev.messageId];
    if(!msgHandler){
      if('worker1-ready'===ev.result) {
        /*sqlite3-api/worker1-ready is fired when the Worker1 API initializes*/
        self.sqlite3TestModule.setStatus(null)/*hide the HTML-side is-loading spinner*/;
        runTests();
        return;
      }
      error("Unhandled worker message:",ev);
      return;
    }
    logEventResult(ev);
    delete WorkerPromiseHandler[ev.messageId];
    if('error'===ev.type){
      msgHandler.reject(ev);
    }
    else{
      if(!DbState.id && ev.dbId) DbState.id = ev.dbId;
      msgHandler.resolve(ev); // async, so testCount() results on next line are out of order
      //testCount();
    }
  };
  
  log("Init complete, but async init bits may still be running.");
})();
