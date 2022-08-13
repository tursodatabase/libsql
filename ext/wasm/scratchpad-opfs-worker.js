/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-api.js. This file must be run in
  main JS thread and sqlite3.js must have been loaded before it.
*/
'use strict';
(function(){
  const toss = function(...args){throw new Error(args.join(' '))};
  const log = console.log.bind(console),
        warn = console.warn.bind(console),
        error = console.error.bind(console);

  const W = new Worker("api/scratchpad-opfs-worker.js");
  self.onmessage = function(ev){
    ev = ev.data;
    const d = ev.data;
    switch(ev.type){
        case 'stdout': log(d); break;
        case 'stderr': error(d); break;
        default: warn("Unhandled message type:",ev); break;
    }
  };
})();
