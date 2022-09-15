/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-api.js. This file must be run in
  main JS thread. It will load sqlite3.js in a worker thread.
*/
'use strict';
(function(){
  const toss = function(...args){throw new Error(args.join(' '))};
  const eOutput = document.querySelector('#test-output');
  const logHtml = function(cssClass,...args){
    if(Array.isArray(args[0])) args = args[0];
    const ln = document.createElement('div');
    if(cssClass) ln.classList.add(cssClass);
    ln.append(document.createTextNode(args.join(' ')));
    eOutput.append(ln);
  };
  const log = function(...args){
    logHtml('',...args);
  };
  const error = function(...args){
    logHtml('error',...args);
  };
  const warn = function(...args){
    logHtml('warning',...args);
  };

  const W = new Worker("scratchpad-opfs-worker2.js");
  W.onmessage = function(ev){
    ev = ev.data;
    const d = ev.data;
    switch(ev.type){
        case 'stdout': log(d); break;
        case 'stderr': error(d); break;
        default: warn("Unhandled message type:",ev); break;
    }
  };
})();
