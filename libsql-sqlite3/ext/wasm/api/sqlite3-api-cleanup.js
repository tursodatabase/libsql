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
*/
'use strict';
if('undefined' !== typeof Module){ // presumably an Emscripten build
  /**
     Install a suitable default configuration for sqlite3ApiBootstrap().
  */
  const SABC = Object.assign(
    Object.create(null), {
      exports: ('undefined'===typeof wasmExports)
        ? Module['asm']/* emscripten <=3.1.43 */
        : wasmExports  /* emscripten >=3.1.44 */,
      memory: Module.wasmMemory /* gets set if built with -sIMPORTED_MEMORY */
    },
    globalThis.sqlite3ApiConfig || {}
  );

  /**
     For current (2022-08-22) purposes, automatically call
     sqlite3ApiBootstrap().  That decision will be revisited at some
     point, as we really want client code to be able to call this to
     configure certain parts. Clients may modify
     globalThis.sqlite3ApiBootstrap.defaultConfig to tweak the default
     configuration used by a no-args call to sqlite3ApiBootstrap(),
     but must have first loaded their WASM module in order to be
     able to provide the necessary configuration state.
  */
  //console.warn("globalThis.sqlite3ApiConfig = ",globalThis.sqlite3ApiConfig);
  globalThis.sqlite3ApiConfig = SABC;
  let sqlite3;
  try{
    sqlite3 = globalThis.sqlite3ApiBootstrap();
  }catch(e){
    console.error("sqlite3ApiBootstrap() error:",e);
    throw e;
  }finally{
    delete globalThis.sqlite3ApiBootstrap;
    delete globalThis.sqlite3ApiConfig;
  }

  Module.sqlite3 = sqlite3 /* Needed for customized sqlite3InitModule() to be able to
                              pass the sqlite3 object off to the client. */;
}else{
  console.warn("This is not running in an Emscripten module context, so",
               "globalThis.sqlite3ApiBootstrap() is _not_ being called due to lack",
               "of config info for the WASM environment.",
               "It must be called manually.");
}
