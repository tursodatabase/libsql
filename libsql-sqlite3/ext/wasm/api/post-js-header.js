/**
   post-js-header.js is to be prepended to other code to create
   post-js.js for use with Emscripten's --post-js flag, so it gets
   injected in the earliest stages of sqlite3InitModule().

   This function wraps the whole SQLite3 library but does not
   bootstrap it.

   Running this function will bootstrap the library and return
   a Promise to the sqlite3 namespace object.
*/
Module.runSQLite3PostLoadInit = function(
  sqlite3InitScriptInfo /* populated by extern-post-js.c-pp.js */,
  EmscriptenModule/*the Emscripten-style module object*/,
  sqlite3IsUnderTest
){
  /** ^^^ Don't use Module.postRun, as that runs a different time
      depending on whether this file is built with emcc 3.1.x or
      4.0.x. This function name is intentionally obnoxiously verbose to
      ensure that we don't collide with current and future Emscripten
      symbol names. */
  'use strict';
  delete EmscriptenModule.runSQLite3PostLoadInit;
  //console.warn("This is the start of Module.runSQLite3PostLoadInit()");
  /* This function will contain at least the following:

     - post-js-header.js          => this file
       - sqlite3-api-prologue.js  => Bootstrapping bits for the following files
       - common/whwasmutil.js     => Generic JS/WASM glue
       - jaccwabyt/jaccwabyt.js   => C struct-binding glue
       - sqlite3-api-glue.js      => glues previous parts together
       - sqlite3-api-oo1.js       => SQLite3 OO API #1
       - sqlite3-api-worker1.js   => Worker-based API
       - sqlite3-vfs-helper.c-pp.js  => Utilities for VFS impls
       - sqlite3-vtab-helper.c-pp.js => Utilities for virtual table impls
       - sqlite3-vfs-opfs.c-pp.js => OPFS VFS
       - sqlite3-vfs-opfs-sahpool.c-pp.js => OPFS SAHPool VFS
       - sqlite3-api-cleanup.js   => final bootstrapping phase
     - post-js-footer.js          => this file's epilogue

     And all of that gets sandwiched between extern-pre-js.js and
     extern-post-js.js.
  */
