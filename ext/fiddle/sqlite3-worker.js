/*
  2022-05-23

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  UNDER CONSTRUCTION

  This is a JS Worker file for the main sqlite3 api. It loads
  sqlite3.js and offers access to the db via the Worker
  message-passing interface.
*/

"use strict";
(function(){
    /** Posts a worker message as {type:type, data:data}. */
    const wMsg = (type,data)=>self.postMessage({type, data});
    self.onmessage = function(ev){
        /*ev = ev.data;
        switch(ev.type){
            default: break;
        };*/
        console.warn("Unknown sqlite3-worker message type:",ev);
    };
    importScripts('sqlite3.js');
    initSqlite3Module().then(function(){
        wMsg('sqlite3-api','ready');
    });
})();
