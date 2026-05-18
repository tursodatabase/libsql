<!doctype html>
//#@policy error
<html lang="en-us">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <link rel="shortcut icon" href="data:image/x-icon;," type="image/x-icon">
    <link rel="stylesheet" href="common/emscripten.css"/>
    <link rel="stylesheet" href="common/testing.css"/>
    <title>sqlite3 tester #1: Worker thread (@bitness@-bit WASM)</title>
    <style></style>
  </head>
  <body>
    <h1 id='color-target'>sqlite3 tester #1: Worker thread (@bitness@-bit WASM)</h1>
    <div>Variants:
      <a href='tester1.html' target='tester1.html'>conventional UI thread</a>,
      <a href='tester1-worker.html' target='tester1-worker.html'>conventional worker</a>,
      <a href='tester1-esm.html' target='tester1-esm.html'>ESM in UI thread</a>,
      <a href='tester1-worker.html?esm' target='tester1-worker.html?esm'>ESM worker</a>
    </div>
    <div class='input-wrapper'>
      <input type='checkbox' id='cb-log-reverse'>
      <label for='cb-log-reverse'>Reverse log order?</label>
    </div>
    <div id='test-output'></div>
    <script>(function(){
      const logTarget = document.querySelector('#test-output');
      const logHtml = function(cssClass,...args){
        const ln = document.createElement('div');
        if(cssClass){
          for(const c of (Array.isArray(cssClass) ? cssClass : [cssClass])){
            ln.classList.add(c);
          }
        }
        ln.append(document.createTextNode(args.join(' ')));
        logTarget.append(ln);
      };
      const cbReverse = document.querySelector('#cb-log-reverse');
      const cbReverseKey = 'tester1:cb-log-reverse';
      const cbReverseIt = ()=>{
        logTarget.classList[cbReverse.checked ? 'add' : 'remove']('reverse');
        localStorage.setItem(cbReverseKey, cbReverse.checked ? 1 : 0);
      };
      cbReverse.addEventListener('change',cbReverseIt,true);
      if(localStorage.getItem(cbReverseKey)){
        cbReverse.checked = !!(+localStorage.getItem(cbReverseKey));
      }
      cbReverseIt();
      const urlParams = new URL(self.location.href).searchParams;
      const workerArgs = [];
      const baseName = "64"==="@bitness@" ? 'tester1-64bit' : 'tester1';
      if(urlParams.has('esm')){
        logHtml('warning',"Attempting to run an ES6 Worker Module, "+
                "which is not supported by all browsers!.");
        workerArgs.push(baseName+'.mjs', {type:"module"});
        document.querySelectorAll('title,#color-target').forEach((e)=>{
          e.innerText =
            "sqlite3 tester #1: ES6 Worker Module (@bitness@-bit WASM)";
        });
      }else{
        workerArgs.push(baseName+'.js?sqlite3.dir=jswasm');
      }
      const w = new Worker(...workerArgs);
      w.onmessage = function({data}){
        switch(data.type){
            case 'log':
              logHtml(data.payload.cssClass, ...data.payload.args);
              break;
            case 'error':
              logHtml('error', ...data.payload.args);
              break;
            case 'test-result':{
                document.querySelector('#color-target').classList.add(
                    data.payload.pass ? 'tests-pass' : 'tests-fail'
                );
                const e = document.querySelector('title');
                e.innerText = (
                    data.payload.pass ? 'PASS' : 'FAIL'
                ) + ': ' + e.innerText;
                break;
            }
            default:
              logHtml('error',"Unhandled message:",data.type);
        };
      };
    })();</script>
  </body>
</html>
