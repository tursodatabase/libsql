/*
** 2023-08-29
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains a test application for SQLTester.mjs. It loads
** test scripts and runs them through the SQLTester class.
*/
import {default as ns} from './SQLTester.mjs';
import {default as allTests} from './test-list.mjs';

globalThis.sqlite3 = ns.sqlite3;
const log = function f(...args){
  console.log('SQLTester.run:',...args);
  return f;
};

const out = function f(...args){ return f.outer.out(...args) };
out.outer = new ns.Outer();
out.outer.getOutputPrefix = ()=>'SQLTester.run: ';
const outln = (...args)=>{ return out.outer.outln(...args) };

const affirm = function(expr, msg){
  if( !expr ){
    throw new Error(arguments[1]
                    ? ("Assertion failed: "+arguments[1])
                    : "Assertion failed");
  }
}

let ts = new ns.TestScript('SQLTester-sanity-check.test',`
/*
** This is a comment. There are many like it but this one is mine.
**
** SCRIPT_MODULE_NAME:      sanity-check-0
** xMIXED_MODULE_NAME:       mixed-module
** xMODULE_NAME:             module-name
** xREQUIRED_PROPERTIES:      small fast reliable
** xREQUIRED_PROPERTIES:      RECURSIVE_TRIGGERS
** xREQUIRED_PROPERTIES:      TEMPSTORE_FILE TEMPSTORE_MEM
** xREQUIRED_PROPERTIES:      AUTOVACUUM INCRVACUUM
**
*/
/* --verbosity 3 */
/* ---must-fail */
/* # must fail */
/* --verbosity 0 */
--print Hello, world.
--close all
--oom
--db 0
--new my.db
--null zilch
--testcase 1.0
SELECT 1, null;
--result 1 zilch
--glob *zil*
--notglob *ZIL*
SELECT 1, 2;
intentional error;
--run
/* ---intentional-failure */
--testcase json-1
SELECT json_array(1,2,3)
--json [1,2,3]
--testcase tableresult-1
  select 1, 'a' UNION
  select 2, 'b' UNION
  select 3, 'c' ORDER by 1
--tableresult
  # [a-z]
  2 b
  3 c
--end
--testcase json-block-1
  select json_array(1,2,3);
  select json_object('a',1,'b',2);
--json-block
  [1,2,3]
  {"a":1,"b":2}
--end
--testcase col-names-on
--column-names 1
  select 1 as 'a', 2 as 'b';
--result a 1 b 2
--testcase col-names-off
--column-names 0
  select 1 as 'a', 2 as 'b';
--result 1 2
--close
--testcase the-end
--print Until next time
`);

const sqt = new ns.SQLTester()
      .setLogger(console.log.bind(console))
      .verbosity(1)
      .addTestScript(ts);
sqt.outer().outputPrefix('');

const runTests = function(){
  try{
    if( 0 ){
      affirm( !sqt.getCurrentDb(), 'sqt.getCurrentDb()' );
      sqt.openDb('/foo.db', true);
      affirm( !!sqt.getCurrentDb(),'sqt.getCurrentDb()' );
      affirm( 'zilch' !== sqt.nullValue() );
      ts.run(sqt);
      affirm( 'zilch' === sqt.nullValue() );
      sqt.addTestScript(ts);
    }else{
      for(const t of allTests){
        sqt.addTestScript( new ns.TestScript(t) );
      }
      allTests.length = 0;
    }
    sqt.runTests();
  }finally{
    //log( "Metrics:", sqt.metrics );
    sqt.reset();
  }
};

if( globalThis.WorkerGlobalScope ){
  const wPost = (type,payload)=>globalThis.postMessage({type, payload});
  globalThis.onmessage = function({data}){
    switch(data.type){
      case 'run-tests':{
        try{ runTests(); }
        finally{ wPost('tests-end', sqt.metrics); }
        break;
      }
      default:
        log("unhandled onmessage: ",data);
        break;
    }
  };
  sqt.setLogger((msg)=>{
    wPost('stdout', {message: msg});
  });
  wPost('is-ready');
  //globalThis.onmessage({data:{type:'run-tests'}});
}else{
  runTests();
}
