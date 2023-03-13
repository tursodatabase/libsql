/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-worker1.js.

  Note that the wrapper interface demonstrated in
  demo-worker1-promiser.js is much easier to use from client code, as it
  lacks the message-passing acrobatics demonstrated in this file.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const SW = new Worker("jswasm/sqlite3-worker1.js");
  const DbState = {
    id: undefined
  };
  const eOutput = document.querySelector('#test-output');
  const log = console.log.bind(console);
  const logHtml = function(cssClass,...args){
    log.apply(this, args);
    const ln = document.createElement('div');
    if(cssClass) ln.classList.add(cssClass);
    ln.append(document.createTextNode(args.join(' ')));
    eOutput.append(ln);
  };
  const warn = console.warn.bind(console);
  const error = console.error.bind(console);
  const toss = (...args)=>{throw new Error(args.join(' '))};

  SW.onerror = function(event){
    error("onerror",event);
  };

  let startTime;

  /**
     A queue for callbacks which are to be run in response to async
     DB commands. See the notes in runTests() for why we need
     this. The event-handling plumbing of this file requires that
     any DB command which includes a `messageId` property also have
     a queued callback entry, as the existence of that property in
     response payloads is how it knows whether or not to shift an
     entry off of the queue.
  */
  const MsgHandlerQueue = {
    queue: [],
    id: 0,
    push: function(type,callback){
      this.queue.push(callback);
      return type + '-' + (++this.id);
    },
    shift: function(){
      return this.queue.shift();
    }
  };

  const testCount = ()=>{
    logHtml("","Total test count:",T.counter+". Total time =",(performance.now() - startTime),"ms");
  };

  const logEventResult = function(ev){
    const evd = ev.result;
    logHtml(evd.errorClass ? 'error' : '',
            "runOneTest",ev.messageId,"Worker time =",
            (ev.workerRespondTime - ev.workerReceivedTime),"ms.",
            "Round-trip event time =",
            (performance.now() - ev.departureTime),"ms.",
            (evd.errorClass ? evd.message : "")//, JSON.stringify(evd)
           );
  };

  const runOneTest = function(eventType, eventArgs, callback){
    T.assert(eventArgs && 'object'===typeof eventArgs);
    /* ^^^ that is for the testing and messageId-related code, not
       a hard requirement of all of the Worker-exposed APIs. */
    const messageId = MsgHandlerQueue.push(eventType,function(ev){
      logEventResult(ev);
      if(callback instanceof Function){
        callback(ev);
        testCount();
      }
    });
    const msg = {
      type: eventType,
      args: eventArgs,
      dbId: DbState.id,
      messageId: messageId,
      departureTime: performance.now()
    };
    log("Posting",eventType,"message to worker dbId="+(DbState.id||'default')+':',msg);
    SW.postMessage(msg);
  };

  /** Methods which map directly to onmessage() event.type keys.
      They get passed the inbound event.data. */
  const dbMsgHandler = {
    open: function(ev){
      DbState.id = ev.dbId;
      log("open result",ev);
    },
    exec: function(ev){
      log("exec result",ev);
    },
    export: function(ev){
      log("export result",ev);
    },
    error: function(ev){
      error("ERROR from the worker:",ev);
      logEventResult(ev);
    },
    resultRowTest1: function f(ev){
      if(undefined === f.counter) f.counter = 0;
      if(null === ev.rowNumber){
        /* End of result set. */
        T.assert(undefined === ev.row)
          .assert(Array.isArray(ev.columnNames))
          .assert(ev.columnNames.length);
      }else{
        T.assert(ev.rowNumber > 0);
        ++f.counter;
      }
      //log("exec() result row:",ev);
      T.assert(null === ev.rowNumber || 'number' === typeof ev.row.b);
    }
  };

  /**
     "The problem" now is that the test results are async. We
     know, however, that the messages posted to the worker will
     be processed in the order they are passed to it, so we can
     create a queue of callbacks to handle them. The problem
     with that approach is that it's not error-handling
     friendly, in that an error can cause us to bypass a result
     handler queue entry. We have to perform some extra
     acrobatics to account for that.

     Problem #2 is that we cannot simply start posting events: we
     first have to post an 'open' event, wait for it to respond, and
     collect its db ID before continuing. If we don't wait, we may
     well fire off 10+ messages before the open actually responds.
  */
  const runTests2 = function(){
    const mustNotReach = ()=>{
      throw new Error("This is not supposed to be reached.");
    };
    runOneTest('exec',{
      sql: ["create table t(a,b);",
            "insert into t(a,b) values(1,2),(3,4),(5,6)"
           ],
      resultRows: [], columnNames: []
    }, function(ev){
      ev = ev.result;
      T.assert(0===ev.resultRows.length)
        .assert(0===ev.columnNames.length);
    });
    runOneTest('exec',{
      sql: 'select a a, b b from t order by a',
      resultRows: [], columnNames: [], saveSql:[]
    }, function(ev){
      ev = ev.result;
      T.assert(3===ev.resultRows.length)
        .assert(1===ev.resultRows[0][0])
        .assert(6===ev.resultRows[2][1])
        .assert(2===ev.columnNames.length)
        .assert('b'===ev.columnNames[1]);
    });
    //if(1){ error("Returning prematurely for testing."); return; }
    runOneTest('exec',{
      sql: 'select a a, b b from t order by a',
      resultRows: [], columnNames: [],
      rowMode: 'object'
    }, function(ev){
      ev = ev.result;
      T.assert(3===ev.resultRows.length)
        .assert(1===ev.resultRows[0].a)
        .assert(6===ev.resultRows[2].b)
    });
    runOneTest('exec',{sql:'intentional_error'}, mustNotReach);
    // Ensure that the message-handler queue survives ^^^ that error...
    runOneTest('exec',{
      sql:'select 1',
      resultRows: [],
      //rowMode: 'array', // array is the default in the Worker interface
    }, function(ev){
      ev = ev.result;
      T.assert(1 === ev.resultRows.length)
        .assert(1 === ev.resultRows[0][0]);
    });
    runOneTest('exec',{
      sql: 'select a a, b b from t order by a',
      callback: 'resultRowTest1',
      rowMode: 'object'
    }, function(ev){
      T.assert(3===dbMsgHandler.resultRowTest1.counter);
      dbMsgHandler.resultRowTest1.counter = 0;
    });
    runOneTest('exec',{
      sql:[
        "pragma foreign_keys=0;",
        // ^^^ arbitrary query with no result columns
        "select a, b from t order by a desc;",
        "select a from t;"
        // multi-statement exec only honors results from the first
        // statement with result columns (regardless of whether)
        // it has any rows).
      ],
      rowMode: 1,
      resultRows: []
    },function(ev){
      const rows = ev.result.resultRows;
      T.assert(3===rows.length).
        assert(6===rows[0]);
    });
    runOneTest('exec',{sql: 'delete from t where a>3'});
    runOneTest('exec',{
      sql: 'select count(a) from t',
      resultRows: []
    },function(ev){
      ev = ev.result;
      T.assert(1===ev.resultRows.length)
        .assert(2===ev.resultRows[0][0]);
    });
    runOneTest('export',{}, function(ev){
      ev = ev.result;
      log("export result:",ev);
      T.assert('string' === typeof ev.filename)
        .assert(ev.byteArray instanceof Uint8Array)
        .assert(ev.byteArray.length > 1024)
        .assert('application/x-sqlite3' === ev.mimetype);
    });
    /***** close() tests must come last. *****/
    runOneTest('close',{unlink:true},function(ev){
      ev = ev.result;
      T.assert('string' === typeof ev.filename);
    });
    runOneTest('close',{unlink:true},function(ev){
      ev = ev.result;
      T.assert(undefined === ev.filename);
      logHtml('warning',"This is the final test.");
    });
    logHtml('warning',"Finished posting tests. Waiting on async results.");
  };

  const runTests = function(){
    /**
       Design decision time: all remaining tests depend on the 'open'
       command having succeeded. In order to support multiple DBs, the
       upcoming commands ostensibly have to know the ID of the DB they
       want to talk to. We have two choices:

       1) We run 'open' and wait for its response, which contains the
       db id.

       2) We have the Worker automatically use the current "default
       db" (the one which was most recently opened) if no db id is
       provided in the message. When we do this, the main thread may
       well fire off _all_ of the test messages before the 'open'
       actually responds, but because the messages are handled on a
       FIFO basis, those after the initial 'open' will pick up the
       "default" db. However, if the open fails, then all pending
       messages (until next next 'open', at least) except for 'close'
       will fail and we have no way of cancelling them once they've
       been posted to the worker.

       Which approach we use below depends on the boolean value of
       waitForOpen.
    */
    const waitForOpen = 1,
          simulateOpenError = 0 /* if true, the remaining tests will
                                   all barf if waitForOpen is
                                   false. */;
    logHtml('',
            "Sending 'open' message and",(waitForOpen ? "" : "NOT ")+
            "waiting for its response before continuing.");
    startTime = performance.now();
    runOneTest('open', {
      filename:'testing2.sqlite3',
      simulateError: simulateOpenError
    }, function(ev){
      log("open result",ev);
      T.assert('testing2.sqlite3'===ev.result.filename)
        .assert(ev.dbId)
        .assert(ev.messageId)
        .assert('string' === typeof ev.result.vfs);
      DbState.id = ev.dbId;
      if(waitForOpen) setTimeout(runTests2, 0);
    });
    if(!waitForOpen) runTests2();
  };

  SW.onmessage = function(ev){
    if(!ev.data || 'object'!==typeof ev.data){
      warn("Unknown sqlite3-worker message type:",ev);
      return;
    }
    ev = ev.data/*expecting a nested object*/;
    //log("main window onmessage:",ev);
    if(ev.result && ev.messageId){
      /* We're expecting a queued-up callback handler. */
      const f = MsgHandlerQueue.shift();
      if('error'===ev.type){
        dbMsgHandler.error(ev);
        return;
      }
      T.assert(f instanceof Function);
      f(ev);
      return;
    }
    switch(ev.type){
        case 'sqlite3-api':
          switch(ev.result){
              case 'worker1-ready':
                log("Message:",ev);
                self.sqlite3TestModule.setStatus(null);
                runTests();
                return;
              default:
                warn("Unknown sqlite3-api message type:",ev);
                return;
          }
        default:
          if(dbMsgHandler.hasOwnProperty(ev.type)){
            try{dbMsgHandler[ev.type](ev);}
            catch(err){
              error("Exception while handling db result message",
                    ev,":",err);
            }
            return;
          }
          warn("Unknown sqlite3-api message type:",ev);
    }
  };
  log("Init complete, but async init bits may still be running.");
  log("Installing Worker into global scope SW for dev purposes.");
  self.SW = SW;
})();
