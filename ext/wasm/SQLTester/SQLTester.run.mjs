import {default as ns} from './SQLTester.mjs';

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

console.log("Loaded",ns);

log("ns =",ns);
out("Hi there. ").outln("SQLTester is ostensibly ready.");

let ts = new ns.TestScript('/foo.test', ns.Util.utf8Encode(
`# comment line
--print Starting up...
--null NIL
--new :memory:
--testcase 0.0.1
select '0.0.1';
#--result 0.0.1
--print done
`));

const sqt = new ns.SQLTester();
try{
  log( 'sqt.getCurrentDb()', sqt.getCurrentDb() );
  sqt.openDb('/foo.db', true);
  log( 'sqt.getCurrentDb()', sqt.getCurrentDb() );
  sqt.verbosity(0);
  affirm( 'NIL' !== sqt.nullValue() );
  ts.run(sqt);
  affirm( 'NIL' === sqt.nullValue() );
}finally{
  sqt.reset();
}
log( 'sqt.getCurrentDb()', sqt.getCurrentDb() );

