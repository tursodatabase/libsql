/*
  2022-07-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file is the tail end of the sqlite3-api.js constellation,
  intended to be appended after all other sqlite3-api-*.js files so
  that it can finalize any setup and clean up any global symbols
  temporarily used for setting up the API's various subsystems.

  In Emscripten builds it's run in the context of what amounts to a
  Module.postRun handler, though it's no longer actually a postRun
  handler because Emscripten 4.0 changed postRun semantics in an
  incompatible way.

  In terms of amalgamation code placement, this file is appended
  immediately after the final sqlite3-api-*.js piece. Those files
  cooperate to prepare sqlite3ApiBootstrap() and this file calls it.
  It is run within a context which gives it access to Emscripten's
  Module object, after sqlite3.wasm is loaded but before
  sqlite3ApiBootstrap() has been called.

  Because this code resides (after building) inside the function
  installed by post-js-header.js, it has access to the
*/
'use strict';
if( 'undefined' === typeof EmscriptenModule/*from post-js-header.js*/ ){
  console.warn("This is not running in the context of Module.runSQLite3PostLoadInit()");
  throw new Error("sqlite3-api-cleanup.js expects to be running in the "+
                  "context of its Emscripten module loader.");
}
try{
  /* Config options for sqlite3ApiBootstrap(). */
  const bootstrapConfig = Object.assign(
    Object.create(null),
    globalThis.sqlite3ApiBootstrap.defaultConfig, // default options
    globalThis.sqlite3ApiConfig || {}, // optional client-provided options
    /** The WASM-environment-dependent configuration for sqlite3ApiBootstrap() */
    {
      memory: ('undefined'!==typeof wasmMemory)
        ? wasmMemory
        : EmscriptenModule['wasmMemory'],
      exports: ('undefined'!==typeof wasmExports)
        ? wasmExports /* emscripten >=3.1.44 */
        : (Object.prototype.hasOwnProperty.call(EmscriptenModule,'wasmExports')
           ? EmscriptenModule['wasmExports']
           : EmscriptenModule['asm']/* emscripten <=3.1.43 */)
    }
  );

  /** Figure out if this is a 32- or 64-bit WASM build. */
  bootstrapConfig.wasmPtrIR =
    'number'===(typeof bootstrapConfig.exports.sqlite3_libversion())
    ?  'i32' :'i64';
  const sIMS = sqlite3InitScriptInfo;
  sIMS.debugModule("Bootstrapping lib config", sIMS);

  /**
     For purposes of the Emscripten build, call sqlite3ApiBootstrap().
     Ideally clients should be able to inject their own config here,
     but that's not practical in this particular build constellation
     because of the order everything happens in.  Clients may either
     define globalThis.sqlite3ApiConfig or modify
     globalThis.sqlite3ApiBootstrap.defaultConfig to tweak the default
     configuration used by a no-args call to sqlite3ApiBootstrap(),
     but must have first loaded their WASM module in order to be able
     to provide the necessary configuration state.
  */
  const p = globalThis.sqlite3ApiBootstrap(bootstrapConfig);
  delete globalThis.sqlite3ApiBootstrap;
  return p /* the eventual result of globalThis.sqlite3InitModule() */;
}catch(e){
  console.error("sqlite3ApiBootstrap() error:",e);
  throw e;
}
throw new Error("Maintenance required: this line should never be reached");
