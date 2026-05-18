<!doctype html>
<html lang="en-us">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <link rel="shortcut icon" href="data:image/x-icon;," type="image/x-icon">
    <link rel="stylesheet" href="../../../common/emscripten.css"/>
    <link rel="stylesheet" href="../../../common/testing.css"/>
    <title>sqlite3 tester: OpfsSAHPool Digest</title>
    <style></style>
  </head>
  <body><h1 id='color-target'></h1>

    <p>
      This is a test app for the digest calculation of the OPFS
      SAHPool VFS. It requires running it with a new database created using
      v3.49.0 or older, then running it again with a newer version, then
      again with 3.49.0 or older.
    </p>
    <div class='input-wrapper'>
      <input type='checkbox' id='cb-log-reverse'>
      <label for='cb-log-reverse'>Reverse log order?</label>
    </div>
    <div id='test-output'></div>
    <script>
      /*
        2025-02-03

        The author disclaims copyright to this source code.  In place of a
        legal notice, here is a blessing:

        *   May you do good and not evil.
        *   May you find forgiveness for yourself and forgive others.
        *   May you share freely, never taking more than you give.

        ***********************************************************************

        This is a bugfix test for the OPFS SAHPool VFS. It requires
        setting up a database created using v3.49.0 or older, then
        running it again with a newer version. In that case, the newer
        version should be able to read the older version's db files
        just fine. Revering back to the old version should also still
        work - it should be able to read databases modified by the
        newer version. However, a database _created_ by a version with
        this fix will _not_ be legible by a version which predates
        this fix, in which case the older version will see that VFS
        file slot as corrupt and will clear it for re-use.

        This is unfortunately rather cumbersome to test properly,
        and essentially impossible to automate.
      */
      (function(){
        'use strict';
        document.querySelector('h1').innerHTML =
        document.querySelector('title').innerHTML;
        const mapToString = (v)=>{
          switch(typeof v){
            case 'number': case 'string': case 'boolean':
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
        const cbReverse = document.querySelector('#cb-log-reverse');
        //cbReverse.setAttribute('checked','checked');
        const cbReverseKey = 'tester1:cb-log-reverse';
        const cbReverseIt = ()=>{
          logTarget.classList[cbReverse.checked ? 'add' : 'remove']('reverse');
          //localStorage.setItem(cbReverseKey, cbReverse.checked ? 1 : 0);
        };
        cbReverse.addEventListener('change', cbReverseIt, true);
        /*if(localStorage.getItem(cbReverseKey)){
          cbReverse.checked = !!(+localStorage.getItem(cbReverseKey));
          }*/
        cbReverseIt();

        const log = (...args)=>{
          //console.log(...args);
          logClass('',...args);
        }
        const warn = (...args)=>{
          console.warn(...args);
          logClass('warning',...args);
        }
        const error = (...args)=>{
          console.error(...args);
          logClass('error',...args);
        };

        const toss = (...args)=>{
          error(...args);
          throw new Error(args.join(' '));
        };

        const endOfWork = (passed=true)=>{
          const eH = document.querySelector('#color-target');
          const eT = document.querySelector('title');
          if(passed){
            log("End of work chain. If you made it this far, you win.");
            eH.innerText = 'PASS: '+eH.innerText;
            eH.classList.add('tests-pass');
            eT.innerText = 'PASS: '+eT.innerText;
          }else{
            eH.innerText = 'FAIL: '+eH.innerText;
            eH.classList.add('tests-fail');
            eT.innerText = 'FAIL: '+eT.innerText;
          }
        };

        log("Running opfs-sahpool digest tests...");
        const W1 = new Worker('digest-worker.js?sqlite3.dir=../../../jswasm');
        W1.onmessage = function({data}){
          //log("onmessage:",data);
          switch(data.type){
            case 'log':
              log('worker says:', ...data.payload);
              break;
            case 'error':
              error('worker says:', ...data.payload);
              endOfWork(false);
              break;
            case 'initialized':
              log(data.workerId, ': Worker initialized',...data.payload);
              break;
          }
        };
      })();
    </script>
  </body>
</html>
