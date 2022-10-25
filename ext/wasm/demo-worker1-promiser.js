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
  const testCount = async ()=>{
    logHtml("","Total test count:",T.counter+". Total time =",(performance.now() - startTime),"ms");
  };

  //why is this triggered even when we catch() a Promise?
  //window.addEventListener('unhandledrejection', function(event) {
  //  warn('unhandledrejection',event);
  //});

  const promiserConfig = {
    worker: ()=>{
      const w = new Worker("jswasm/sqlite3-worker1.js");
      w.onerror = (event)=>error("worker.onerror",event);
      return w;
    },
    debug: 1 ? undefined : (...args)=>console.debug('worker debug',...args),
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
    if(2===arguments.length && 'function'===typeof msgArgs){
      callback = msgArgs;
      msgArgs = undefined;
    }
    const p = workerPromise({type: msgType, args:msgArgs});
    return callback ? p.then(callback).finally(testCount) : p;
  };

  const runTests = async function(){
    const dbFilename = '/testing2.sqlite3';
    startTime = performance.now();

    let sqConfig;
    await wtest('config-get', (ev)=>{
      const r = ev.result;
      log('sqlite3.config subset:', r);
      T.assert('boolean' === typeof r.bigIntEnabled)
        .assert('string'===typeof r.wasmfsOpfsDir)
        .assert('boolean' === typeof r.wasmfsOpfsEnabled);
      sqConfig = r;
    });
    logHtml('',
            "Sending 'open' message and waiting for its response before continuing...");
    
    await wtest('open', {
      filename: dbFilename,
      simulateError: 0 /* if true, fail the 'open' */,
    }, function(ev){
      const r = ev.result;
      log("then open result",r);
      T.assert(ev.dbId === r.dbId)
        .assert(ev.messageId);
      promiserConfig.dbId = ev.dbId;
    }).then(runTests2);
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
    });

    await wtest('exec',{
      sql:'select 1 union all select 3',
      resultRows: [],
    }, function(ev){
      ev = ev.result;
      T.assert(2 === ev.resultRows.length)
        .assert(1 === ev.resultRows[0][0])
        .assert(3 === ev.resultRows[1][0]);
    });

    const resultRowTest1 = function f(ev){
      if(undefined === f.counter) f.counter = 0;
      if(null === ev.rowNumber){
        /* End of result set. */
        T.assert(undefined === ev.row)
          .assert(2===ev.columnNames.length)
          .assert('a'===ev.columnNames[0])
          .assert('B'===ev.columnNames[1]);
      }else{
        T.assert(ev.rowNumber > 0);
        ++f.counter;
      }
      log("exec() result row:",ev);
      T.assert(null === ev.rowNumber || 'number' === typeof ev.row.B);
    };
    await wtest('exec',{
      sql: 'select a a, b B from t order by a limit 3',
      callback: resultRowTest1,
      rowMode: 'object'
    }, function(ev){
      T.assert(3===resultRowTest1.counter);
      resultRowTest1.counter = 0;
    });

    const resultRowTest2 = function f(ev){
      if(null === ev.rowNumber){
        /* End of result set. */
        T.assert(undefined === ev.row)
          .assert(1===ev.columnNames.length)
          .assert('a'===ev.columnNames[0])
      }else{
        T.assert(ev.rowNumber > 0);
        f.counter = ev.rowNumber;
      }
      log("exec() result row:",ev);
      T.assert(null === ev.rowNumber || 'number' === typeof ev.row);
    };
    await wtest('exec',{
      sql: 'select a a from t limit 3',
      callback: resultRowTest2,
      rowMode: 0
    }, function(ev){
      T.assert(3===resultRowTest2.counter);
    });

    const resultRowTest3 = function f(ev){
      if(null === ev.rowNumber){
        T.assert(3===ev.columnNames.length)
          .assert('foo'===ev.columnNames[0])
          .assert('bar'===ev.columnNames[1])
          .assert('baz'===ev.columnNames[2]);
      }else{
        f.counter = ev.rowNumber;
        T.assert('number' === typeof ev.row);
      }
    };
    await wtest('exec',{
      sql: "select 'foo' foo, a bar, 'baz' baz  from t limit 2",
      callback: resultRowTest3,
      columnNames: [],
      rowMode: ':bar'
    }, function(ev){
      log("exec() result row:",ev);
      T.assert(2===resultRowTest3.counter);
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
    await wtest('close',{},function(ev){
      T.assert('string' === typeof ev.result.filename);
    });

    await wtest('close', (ev)=>{
      T.assert(undefined === ev.result.filename);
    }).finally(()=>logHtml('',"That's all, folks!"));
  }/*runTests2()*/;

  log("Init complete, but async init bits may still be running.");
})();
