/*
  2022-08-23

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************
  
  Demonstration of the sqlite3 Worker API #1 Promiser: a Promise-based
  proxy for for the sqlite3 Worker #1 API.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const eOutput = document.querySelector('#test-output');
  const warn = console.warn.bind(console);
  const error = console.error.bind(console);
  const log = console.log.bind(console);
  const logHtml = async function(cssClass,...args){
    log.apply(this, args);
    const ln = document.createElement('div');
    if(cssClass) ln.classList.add(cssClass);
    ln.append(document.createTextNode(args.join(' ')));
    eOutput.append(ln);
  };

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

  //why is this triggered even when we catch() a Promise?
  //window.addEventListener('unhandledrejection', function(event) {
  //  warn('unhandledrejection',event);
  //});

  const promiserConfig = {
    worker: ()=>{
      const w = new Worker("sqlite3-worker1.js");
      w.onerror = (event)=>error("worker.onerror",event);
      return w;
    },
    //debug: (...args)=>console.debug('worker debug',...args),
    onunhandled: function(ev){
      error("Unhandled worker message:",ev.data);
    },
    onready: function(){
      self.sqlite3TestModule.setStatus(null)/*hide the HTML-side is-loading spinner*/;
      runTests();
    },
    onerror: function(ev){
      error("worker1 error:",ev);
    }
  };
  const workerPromise = self.sqlite3Worker1Promiser(promiserConfig);
  delete self.sqlite3Worker1Promiser;

  const wtest = async function(msgType, msgArgs, callback){
    let p = workerPromise({type: msgType, args:msgArgs});
    if(callback) p.then(callback).finally(testCount);
    return p;
  };

  const runTests = async function(){
    logHtml('',
            "Sending 'open' message and waiting for its response before continuing.");
    startTime = performance.now();
    wtest('open', {
      filename:'testing2.sqlite3',
      simulateError: 0 /* if true, fail the 'open' */
    }, function(ev){
      log("then open result",ev);
      T.assert('testing2.sqlite3'===ev.result.filename)
        .assert(ev.dbId)
        .assert(ev.messageId)
        .assert(promiserConfig.dbId === ev.dbId);
    }).then(runTests2)
      .catch((err)=>error("error response:",err));
  };

  const runTests2 = async function(){
    const mustNotReach = ()=>toss("This is not supposed to be reached.");

    await wtest('exec',{
      sql: ["create table t(a,b)",
            "insert into t(a,b) values(1,2),(3,4),(5,6)"
           ].join(';'),
      multi: true,
      resultRows: [], columnNames: []
    }, function(ev){
      ev = ev.result;
      T.assert(0===ev.resultRows.length)
        .assert(0===ev.columnNames.length);
    });

    await wtest('exec',{
      sql: 'select a a, b b from t order by a',
      resultRows: [], columnNames: [],
    }, function(ev){
      ev = ev.result;
      T.assert(3===ev.resultRows.length)
        .assert(1===ev.resultRows[0][0])
        .assert(6===ev.resultRows[2][1])
        .assert(2===ev.columnNames.length)
        .assert('b'===ev.columnNames[1]);
    });

    await wtest('exec',{
      sql: 'select a a, b b from t order by a',
      resultRows: [], columnNames: [],
      rowMode: 'object'
    }, function(ev){
      ev = ev.result;
      T.assert(3===ev.resultRows.length)
        .assert(1===ev.resultRows[0].a)
        .assert(6===ev.resultRows[2].b)
    });

    await wtest(
      'exec',
      {sql:'intentional_error'},
      mustNotReach
    ).catch((e)=>{
      warn("Intentional error:",e);
      // Why does the browser report console.error "Uncaught (in
      // promise)" when we catch(), and does so _twice_ if we don't
      // catch()? According to all docs, that error must be supressed
      // if we explicitly catch().
    });

    await wtest('exec',{
      sql:'select 1 union all select 3',
      resultRows: [],
      //rowMode: 'array', // array is the default in the Worker interface
    }, function(ev){
      ev = ev.result;
      T.assert(2 === ev.resultRows.length)
        .assert(1 === ev.resultRows[0][0])
        .assert(3 === ev.resultRows[1][0]);
    });

    const resultRowTest1 = function f(row){
      if(undefined === f.counter) f.counter = 0;
      if(row) ++f.counter;
      //log("exec() result row:",row);
      T.assert(null===row || 'number' === typeof row.b);
    };
    await wtest('exec',{
      sql: 'select a a, b b from t order by a',
      callback: resultRowTest1,
      rowMode: 'object'
    }, function(ev){
      T.assert(3===resultRowTest1.counter);
      resultRowTest1.counter = 0;
    });

    await wtest('exec',{
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
      const rows = ev.result.resultRows;
      T.assert(3===rows.length).
        assert(6===rows[0]);
    });

    await wtest('exec',{sql: 'delete from t where a>3'});

    await wtest('exec',{
      sql: 'select count(a) from t',
      resultRows: []
    },function(ev){
      ev = ev.result;
      T.assert(1===ev.resultRows.length)
        .assert(2===ev.resultRows[0][0]);
    });

    /***** close() tests must come last. *****/
    await wtest('close',{unlink:true},function(ev){
      T.assert(!promiserConfig.dbId);
      T.assert('string' === typeof ev.result.filename);
    });

    await wtest('close').then((ev)=>{
      T.assert(undefined === ev.result.filename);
      log("That's all, folks!");
    });
  }/*runTests2()*/;

  
  log("Init complete, but async init bits may still be running.");
})();
