/**
   BEGIN FILE: api/pre-js.js

   This file is intended to be prepended to the sqlite3.js build using
   Emscripten's --pre-js=THIS_FILE flag (or equivalent).
*/

// See notes in extern-post-js.js
const sqlite3InitModuleState = self.sqlite3InitModuleState || Object.create(null);
delete self.sqlite3InitModuleState;
sqlite3InitModuleState.debugModule('self.location =',self.location);

/**
   This custom locateFile() tries to figure out where to load `path`
   from. The intent is to provide a way for foo/bar/X.js loaded from a
   Worker constructor or importScripts() to be able to resolve
   foo/bar/X.wasm (in the latter case, with some help):

   1) If URL param named the same as `path` is set, it is returned.

   2) If sqlite3InitModuleState.sqlite3Dir is set, then (thatName + path)
      is returned (note that it's assumed to end with '/').

   3) If this code is running in the main UI thread AND it was loaded
      from a SCRIPT tag, the directory part of that URL is used
      as the prefix. (This form of resolution unfortunately does not
      function for scripts loaded via importScripts().)

   4) If none of the above apply, (prefix+path) is returned.
*/
Module['locateFile'] = function(path, prefix) {
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
  sqlite3InitModuleState.debugModule(
    "locateFile(",arguments[0], ',', arguments[1],")",
    'sqlite3InitModuleState.scriptDir =',this.scriptDir,
    'up.entries() =',Array.from(up.entries()),
    "result =", theFile
  );
  return theFile;
}.bind(sqlite3InitModuleState);

/**
   Bug warning: a custom Module.instantiateWasm() does not work
   in WASMFS builds:

   https://github.com/emscripten-core/emscripten/issues/17951

   In such builds we must disable this.
*/
const xNameOfInstantiateWasm = true
      ? 'instantiateWasm'
      : 'emscripten-bug-17951';
Module[xNameOfInstantiateWasm] = function callee(imports,onSuccess){
  imports.env.foo = function(){};
  const uri = Module.locateFile(
    callee.uri, (
      ('undefined'===typeof scriptDirectory/*var defined by Emscripten glue*/)
        ? '' : scriptDirectory)
  );
  sqlite3InitModuleState.debugModule(
    "instantiateWasm() uri =", uri
  );
  const wfetch = ()=>fetch(uri, {credentials: 'same-origin'});
  const loadWasm = WebAssembly.instantiateStreaming
        ? async ()=>{
          return WebAssembly.instantiateStreaming(wfetch(), imports)
            .then((arg)=>onSuccess(arg.instance, arg.module));
        }
        : async ()=>{ // Safari < v15
          return wfetch()
            .then(response => response.arrayBuffer())
            .then(bytes => WebAssembly.instantiate(bytes, imports))
            .then((arg)=>onSuccess(arg.instance, arg.module));
        };
  loadWasm();
  return {};
};
/*
  It is literally impossible to reliably get the name of _this_ script
  at runtime, so impossible to derive X.wasm from script name
  X.js. Thus we need, at build-time, to redefine
  Module[xNameOfInstantiateWasm].uri by appending it to a build-specific
  copy of this file with the name of the wasm file. This is apparently
  why Emscripten hard-codes the name of the wasm file into their glue
  scripts.
*/
Module[xNameOfInstantiateWasm].uri = 'sqlite3.wasm';
/* END FILE: api/pre-js.js, noting that the build process may add a
   line after this one to change the above .uri to a build-specific
   one. */
