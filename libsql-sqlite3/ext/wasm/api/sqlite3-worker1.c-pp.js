//#ifnot omit-oo1
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

  {type: 'sqlite3-api', result: 'worker1-ready'}

  This seemingly superfluous level of indirection is necessary when
  loading sqlite3.js via a Worker. Instantiating a worker with new
  Worker("sqlite.js") will not (cannot) call sqlite3InitModule() to
  initialize the module due to a timing/order-of-operations conflict
  (and that symbol is not exported in a way that a Worker loading it
  that way can see it).  Thus JS code wanting to load the sqlite3
  Worker-specific API needs to pass _this_ file (or equivalent) to the
  Worker constructor and then listen for an event in the form shown
  above in order to know when the module has completed initialization.

  This file accepts a URL arguments to adjust how it loads sqlite3.js:

  - `sqlite3.dir`, if set, treats the given directory name as the
    directory from which `sqlite3.js` will be loaded.
*/
//#if target=es6-bundler-friendly
import {default as sqlite3InitModule} from './sqlite3-bundler-friendly.mjs';
//#else
"use strict";
{
  const urlParams = globalThis.location
        ? new URL(globalThis.location.href).searchParams
        : new URLSearchParams();
  let theJs = 'sqlite3.js';
  if(urlParams.has('sqlite3.dir')){
    theJs = urlParams.get('sqlite3.dir') + '/' + theJs;
  }
  //console.warn("worker1 theJs =",theJs);
  importScripts(theJs);
}
//#endif
sqlite3InitModule().then(sqlite3 => sqlite3.initWorker1API());
//#else
/* Built with the omit-oo1 flag. */
//#endif ifnot omit-oo1
