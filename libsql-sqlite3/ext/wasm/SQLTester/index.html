<!doctype html>
<html lang="en-us">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <link rel="shortcut icon" href="data:image/x-icon;," type="image/x-icon">
    <!--link rel="stylesheet" href="../common/emscripten.css"/-->
    <link rel="stylesheet" href="../common/testing.css"/>
    <title>SQLTester</title>
  </head>
  <style>
    fieldset {
        display: flex;
        flex-direction: row;
        padding-right: 1em;
    }
    fieldset > :not(.legend) {
        display: flex;
        flex-direction: row;
        padding-right: 1em;
    }
  </style>
  <body>
    <h1>SQLTester for JS/WASM</h1>
    <p>This app reads in a build-time-defined set of SQLTester test
      scripts and runs them through the test suite.
    </p>
    <fieldset>
      <legend>Options</legend>
      <span class='input-wrapper'>
        <input type='checkbox' id='cb-log-reverse' checked>
        <label for='cb-log-reverse'>Reverse log order?</label>
      </span>
      <input type='button' id='btn-run-tests' value='Run tests'/>
    </fieldset>
    <div id='test-output'>Test output will go here.</div>
    <!--script src='SQLTester.run.mjs' type='module'></script-->
    <script>
      (async function(){
        const W = new Worker('SQLTester.run.mjs',{
          type: 'module'
        });
        const wPost = (type,payload)=>W.postMessage({type,payload});
        const mapToString = (v)=>{
          switch(typeof v){
            case 'string': return v;
            case 'number': case 'boolean':
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
        const logClass = function(cssClass,...args){
          const ln = document.createElement('div');
          if(cssClass){
            for(const c of (Array.isArray(cssClass) ? cssClass : [cssClass])){
              ln.classList.add(c);
            }
          }
          ln.append(document.createTextNode(normalizeArgs(args).join(' ')));
          logTarget.append(ln);
        };
        {
          const cbReverse = document.querySelector('#cb-log-reverse');
          const cbReverseKey = 'SQLTester:cb-log-reverse';
          const cbReverseIt = ()=>{
            logTarget.classList[cbReverse.checked ? 'add' : 'remove']('reverse');
          };
          cbReverse.addEventListener('change', cbReverseIt, true);
          cbReverseIt();
        }

        const btnRun = document.querySelector('#btn-run-tests');
        const runTests = ()=>{
          btnRun.setAttribute('disabled','disabled');
          wPost('run-tests');
          logTarget.innerText = 'Running tests...';
        }
        btnRun.addEventListener('click', runTests);
        const log2 = function f(...args){
          logClass('', ...args);
          return f;
        };
        const log = function f(...args){
          logClass('','index.html:',...args);
          return f;
        };

        const timerId = setTimeout( ()=>{
          logClass('error',"The SQLTester module is taking an unusually ",
                   "long time to load. More information may be available",
                   "in the dev console.");
        }, 3000 /* assuming localhost */ );

        W.onmessage = function({data}){
          switch(data.type){
            case 'stdout': log2(data.payload.message); break;
            case 'tests-end':
              btnRun.removeAttribute('disabled');
              delete data.payload.nTest;
              log("test results:",data.payload);
              break;
            case 'is-ready':
              clearTimeout(timerId);
              runTests(); break;
            default:
              log("unhandled onmessage",data);
              break;
          }
        };
      })();
    </script>
  </body>
</html>
