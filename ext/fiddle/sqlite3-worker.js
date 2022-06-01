/*
  2022-05-23

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This is a JS Worker file for the main sqlite3 api. It loads
  sqlite3.js, initializes the module, and postMessage()'s a message
  after the module is initialized:

  {type: 'sqlite3-api', data: 'ready'}

  This seemingly superfluous level of indirection is necessary when
  loading sqlite3.js via a Worker. Loading sqlite3.js from the main
  window thread elides the Worker-specific API. Instantiating a worker
  with new Worker("sqlite.js") will not (cannot) call
  initSqlite3Module() to initialize the module due to a
  timing/order-of-operations conflict (and that symbol is not exported
  in a way that a Worker loading it that way can see it).  Thus JS
  code wanting to load the sqlite3 Worker-specific API needs to pass
  _this_ file (or equivalent) to the Worker constructor and then
  listen for an event in the form shown above in order to know when
  the module has completed initialization. sqlite3.js will fire a
  similar event, with data:'loaded' as the final step in its loading
  process. Whether or not we _really_ need both 'loaded' and 'ready'
  events is unclear, but they are currently separate events primarily
  for the sake of clarity in the timing of when it's okay to use the
  loaded module. At the time the 'loaded' event is fired, it's
  possible (but unknown and unknowable) that the emscripten-generated
  module-setup infrastructure still has work to do. Thus it is
  hypothesized that client code is better off waiting for the 'ready'
  even before using the API.
*/
"use strict";
importScripts('sqlite3.js');
initSqlite3Module().then(function(){
    setTimeout(()=>self.postMessage({type:'sqlite3-api',data:'ready'}), 0);
});
