/**
   BEGIN FILE: api/pre-js.js

   This file is intended to be prepended to the sqlite3.js build using
   Emscripten's --pre-js=THIS_FILE flag (or equivalent). It is run
   from inside of sqlite3InitModule(), after Emscripten's Module is
   defined, but early enough that we can ammend, or even outright
   replace, Module from here.

   Because this runs in-between Emscripten's own bootstrapping and
   Emscripten's main work, we must be careful with file-local symbol
   names. e.g. don't overwrite anything Emscripten defines and do not
   use 'const' for local symbols which Emscripten might try to use for
   itself. i.e. try to keep file-local symbol names obnoxiously
   collision-resistant.
*/
(function(Module){
  const sIMS =
        globalThis.sqlite3InitModuleState/*from extern-post-js.c-pp.js*/
        || Object.assign(Object.create(null),{
          debugModule: ()=>{
            console.warn("globalThis.sqlite3InitModuleState is missing");
          }
        });
  delete globalThis.sqlite3InitModuleState;
  sIMS.debugModule('pre-js.js sqlite3InitModuleState =',sIMS);

  /**
     This custom locateFile() tries to figure out where to load `path`
     from. The intent is to provide a way for foo/bar/X.js loaded from a
     Worker constructor or importScripts() to be able to resolve
     foo/bar/X.wasm (in the latter case, with some help):

     1) If URL param named the same as `path` is set, it is returned.

     2) If sqlite3InitModuleState.sqlite3Dir is set, then (thatName + path)
     is returned (it's assumed to end with '/').

     3) If this code is running in the main UI thread AND it was loaded
     from a SCRIPT tag, the directory part of that URL is used
     as the prefix. (This form of resolution unfortunately does not
     function for scripts loaded via importScripts().)

     4) If none of the above apply, (prefix+path) is returned.

     None of the above apply in ES6 builds, which uses a much simpler
     approach.
  */
  Module['locateFile'] = function(path, prefix) {
//#if target:es6-module
    return new URL(path, import.meta.url).href;
//#else
    'use strict';
    let theFile;
    const up = this.urlParams;
    if(up.has(path)){
      theFile = up.get(path);
    }else if(this.sqlite3Dir){
      theFile = this.sqlite3Dir + path;
    }else if(this.scriptDir){
      theFile = this.scriptDir + path;
    }else{
      theFile = prefix + path;
    }
    this.debugModule(
      "locateFile(",arguments[0], ',', arguments[1],")",
      'sqlite3InitModuleState.scriptDir =',this.scriptDir,
      'up.entries() =',Array.from(up.entries()),
      "result =", theFile
    );
    return theFile;
//#endif target:es6-module
  }.bind(sIMS);

//#if Module.instantiateWasm
//#if not wasmfs
  /**
     Override Module.instantiateWasm().

     A custom Module.instantiateWasm() does not work in WASMFS builds:

     https://github.com/emscripten-core/emscripten/issues/17951

     In such builds we must disable this.
  */
  Module['instantiateWasm'] = function callee(imports,onSuccess){
    const sims = this;
    const uri = Module.locateFile(
      sims.wasmFilename, (
        ('undefined'===typeof scriptDirectory/*var defined by Emscripten glue*/)
          ? "" : scriptDirectory)
    );
    sims.debugModule("instantiateWasm() uri =", uri, "sIMS =",this);
    const wfetch = ()=>fetch(uri, {credentials: 'same-origin'});
    const finalThen = (arg)=>{
      arg.imports = imports;
      sims.instantiateWasm = arg /* used by sqlite3-api-prologue.c-pp.js */;
      onSuccess(arg.instance, arg.module);
    };
    const loadWasm = WebAssembly.instantiateStreaming
          ? async ()=>
          WebAssembly
          .instantiateStreaming(wfetch(), imports)
          .then(finalThen)
          : async ()=>// Safari < v15
          wfetch()
          .then(response => response.arrayBuffer())
          .then(bytes => WebAssembly.instantiate(bytes, imports))
          .then(finalThen)
    return loadWasm();
  }.bind(sIMS);
//#endif not wasmfs
//#endif Module.instantiateWasm
})(Module);
/* END FILE: api/pre-js.js. */
