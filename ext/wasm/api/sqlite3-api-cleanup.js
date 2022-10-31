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
      Module: Module /* ==> Currently needs to be exposed here for
                        test code. NOT part of the public API. */,
      exports: Module['asm'],
      memory: Module.wasmMemory /* gets set if built with -sIMPORT_MEMORY */
    },
    self.sqlite3ApiConfig || Object.create(null)
  );

  /**
     For current (2022-08-22) purposes, automatically call
     sqlite3ApiBootstrap().  That decision will be revisited at some
     point, as we really want client code to be able to call this to
     configure certain parts. Clients may modify
     self.sqlite3ApiBootstrap.defaultConfig to tweak the default
     configuration used by a no-args call to sqlite3ApiBootstrap(),
     but must have first loaded their WASM module in order to be
     able to provide the necessary configuration state.
  */
  //console.warn("self.sqlite3ApiConfig = ",self.sqlite3ApiConfig);
  self.sqlite3ApiConfig = SABC;
  let sqlite3;
  try{
    sqlite3 = self.sqlite3ApiBootstrap();
  }catch(e){
    console.error("sqlite3ApiBootstrap() error:",e);
    throw e;
  }finally{
    delete self.sqlite3ApiBootstrap;
    delete self.sqlite3ApiConfig;
  }

  if(self.location && +self.location.port > 1024){
    console.warn("Installing sqlite3 bits as global S for local dev/test purposes.");
    self.S = sqlite3;
  }

  /* Clean up temporary references to our APIs... */
  delete sqlite3.util /* arguable, but these are (currently) internal-use APIs */;
  Module.sqlite3 = sqlite3 /* Needed for customized sqlite3InitModule() to be able to
                              pass the sqlite3 object off to the client. */;
}else{
  console.warn("This is not running in an Emscripten module context, so",
               "self.sqlite3ApiBootstrap() is _not_ being called due to lack",
               "of config info for the WASM environment.",
               "It must be called manually.");
}
