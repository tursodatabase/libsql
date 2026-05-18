/**
   post-js-header.js is to be prepended to other code to create
   post-js.js for use with Emscripten's --post-js flag. This code
   requires that it be running in that context. The Emscripten
   environment must have been set up already but it will not have
   loaded its WASM when the code in this file is run. The function it
   installs will be run after the WASM module is loaded, at which
   point the sqlite3 JS API bits will get set up.
*/
if(!Module.postRun) Module.postRun = [];
Module.postRun.push(function(Module/*the Emscripten-style module object*/){
  'use strict';
  /* This function will contain at least the following:

     - post-js-header.js (this file)
     - sqlite3-api-prologue.js  => Bootstrapping bits to attach the rest to
     - common/whwasmutil.js     => Replacements for much of Emscripten's glue
     - jaccwaby/jaccwabyt.js    => Jaccwabyt (C/JS struct binding)
     - sqlite3-api-glue.js      => glues previous parts together
     - sqlite3-api-oo.js        => SQLite3 OO API #1
     - sqlite3-api-worker1.js   => Worker-based API
     - sqlite3-vfs-helper.c-pp.js  => Utilities for VFS impls
     - sqlite3-vtab-helper.c-pp.js => Utilities for virtual table impls
     - sqlite3-vfs-opfs.c-pp.js  => OPFS VFS
     - sqlite3-vfs-opfs-sahpool.c-pp.js => OPFS SAHPool VFS
     - sqlite3-api-cleanup.js   => final API cleanup
     - post-js-footer.js        => closes this postRun() function
  */
