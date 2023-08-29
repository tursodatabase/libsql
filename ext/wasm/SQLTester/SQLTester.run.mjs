import {default as ns} from './SQLTester.mjs';

const log = function f(...args){
  console.log('SQLTester.run:',...args);
  return f;
};

console.log("Loaded",ns);
const out = function f(...args){ return f.outer.out(...args) };
out.outer = new ns.Outer();
out.outer.getOutputPrefix = ()=>'SQLTester.run: ';
const outln = (...args)=>{ return out.outer.outln(...args) };

log("ns =",ns);
out("Hi there. ").outln("SQLTester is ostensibly ready.");

let ts = new ns.TestScript('/foo.test', ns.Util.utf8Encode(`
# comment line
select 1;
--testcase 0.0
#--result 1
`));

const sqt = new ns.SQLTester();
sqt.verbosity(3);
ts.run(sqt);
