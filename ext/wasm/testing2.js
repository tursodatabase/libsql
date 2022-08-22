/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-worker.js.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const SW = new Worker("api/sqlite3-worker.js");
  const DbState = {
    id: undefined
  };
  const eOutput = document.querySelector('#test-output');
  const log = console.log.bind(console)
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
  /** Posts a worker message as {type:type, data:data}. */
  const wMsg = function(type,data){
    log("Posting message to worker dbId="+(DbState.id||'default')+':',data);
    SW.postMessage({
      type,
      dbId: DbState.id,
      data,
      departureTime: performance.now()
    });
    return SW;
  };

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

  const logEventResult = function(evd){
    logHtml(evd.errorClass ? 'error' : '',
            "runOneTest",evd.messageId,"Worker time =",
            (evd.workerRespondTime - evd.workerReceivedTime),"ms.",
            "Round-trip event time =",
            (performance.now() - evd.departureTime),"ms.",
            (evd.errorClass ? evd.message : "")
           );
  };

  const runOneTest = function(eventType, eventData, callback){
    T.assert(eventData && 'object'===typeof eventData);
    /* ^^^ that is for the testing and messageId-related code, not
       a hard requirement of all of the Worker-exposed APIs. */
    eventData.messageId = MsgHandlerQueue.push(eventType,function(ev){
      logEventResult(ev.data);
      if(callback instanceof Function){
        callback(ev);
        testCount();
      }
    });
    wMsg(eventType, eventData);
  };

  /** Methods which map directly to onmessage() event.type keys.
      They get passed the inbound event.data. */
  const dbMsgHandler = {
    open: function(ev){
      DbState.id = ev.dbId;
      log("open result",ev.data);
    },
    exec: function(ev){
      log("exec result",ev.data);
    },
    export: function(ev){
      log("export result",ev.data);
    },
    error: function(ev){
      error("ERROR from the worker:",ev.data);
      logEventResult(ev.data);
    },
    resultRowTest1: function f(ev){
      if(undefined === f.counter) f.counter = 0;
      if(ev.data) ++f.counter;
      //log("exec() result row:",ev.data);
      T.assert(null===ev.data || 'number' === typeof ev.data.b);
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
      sql: ["create table t(a,b)",
            "insert into t(a,b) values(1,2),(3,4),(5,6)"
           ].join(';'),
      multi: true,
      resultRows: [], columnNames: []
    }, function(ev){
      ev = ev.data;
      T.assert(0===ev.resultRows.length)
        .assert(0===ev.columnNames.length);
    });
    runOneTest('exec',{
      sql: 'select a a, b b from t order by a',
      resultRows: [], columnNames: [],
    }, function(ev){
      ev = ev.data;
      T.assert(3===ev.resultRows.length)
        .assert(1===ev.resultRows[0][0])
        .assert(6===ev.resultRows[2][1])
        .assert(2===ev.columnNames.length)
        .assert('b'===ev.columnNames[1]);
    });
    runOneTest('exec',{
      sql: 'select a a, b b from t order by a',
      resultRows: [], columnNames: [],
      rowMode: 'object'
    }, function(ev){
      ev = ev.data;
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
      ev = ev.data;
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
      multi: true,
      sql:[
        'pragma foreign_keys=0;',
        // ^^^ arbitrary query with no result columns
        'select a, b from t order by a desc; select a from t;'
        // multi-exec only honors results from the first
        // statement with result columns (regardless of whether)
        // it has any rows).
      ],
      rowMode: 1,
      resultRows: []
    },function(ev){
      const rows = ev.data.resultRows;
      T.assert(3===rows.length).
        assert(6===rows[0]);
    });
    runOneTest('exec',{sql: 'delete from t where a>3'});
    runOneTest('exec',{
      sql: 'select count(a) from t',
      resultRows: []
    },function(ev){
      ev = ev.data;
      T.assert(1===ev.resultRows.length)
        .assert(2===ev.resultRows[0][0]);
    });
    if(0){
      // export requires reimpl. for portability reasons.
      runOneTest('export',{}, function(ev){
        ev = ev.data;
        T.assert('string' === typeof ev.filename)
          .assert(ev.buffer instanceof Uint8Array)
          .assert(ev.buffer.length > 1024)
          .assert('application/x-sqlite3' === ev.mimetype);
      });
    }
    /***** close() tests must come last. *****/
    runOneTest('close',{unlink:true},function(ev){
      ev = ev.data;
      T.assert('string' === typeof ev.filename);
    });
    runOneTest('close',{unlink:true},function(ev){
      ev = ev.data;
      T.assert(undefined === ev.filename);
    });
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

       We currently do (2) because (A) it's certainly the most
       client-friendly thing to do and (B) it seems likely that most
       apps using this API will only have a single db to work with so
       won't need to juggle multiple DB ids. If we revert to (1) then
       the following call to runTests2() needs to be moved into the
       callback function of the runOneTest() check for the 'open'
       command. Note, also, that using approach (2) does not keep the
       user from instead using approach (1), noting that doing so
       requires explicit handling of the 'open' message to account for
       it.
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
      //log("open result",ev);
      T.assert('testing2.sqlite3'===ev.data.filename)
        .assert(ev.data.dbId)
        .assert(ev.data.messageId);
      DbState.id = ev.data.dbId;
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
    if(ev.data && ev.data.messageId){
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
          switch(ev.data){
              case 'worker-ready':
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
})();
