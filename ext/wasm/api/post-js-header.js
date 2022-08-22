/**
   post-js-header.js is to be prepended to other code to create
   post-js.js for use with Emscripten's --post-js flag. This code
   requires that it be running in that context. The Emscripten
   environment must have been set up already but it will not have
   loaded its WASM when the code in this file is run. The function it
   installs will be run after the WASM module is loaded, at which
   point the sqlite3 WASM API bits will be set up.
*/
if(!Module.postRun) Module.postRun = [];
Module.postRun.push(function(Module/*the Emscripten-style module object*/){
  'use strict';
  /* This function will contain:

     - post-js-header.js (this file)
     - sqlite3-api-prologue.js  => Bootstrapping bits to attach the rest to
     - sqlite3-api-whwasmutil.js  => Replacements for much of Emscripten's glue
     - sqlite3-api-jaccwabyt.js => Jaccwabyt (C/JS struct binding)
     - sqlite3-api-glue.js      => glues previous parts together
     - sqlite3-api-oo.js        => SQLite3 OO API #1.
     - sqlite3-api-worker.js    => Worker-based API
     - sqlite3-api-cleanup.js   => final API cleanup
     - post-js-footer.js        => closes this postRun() function

     Whew!
  */
