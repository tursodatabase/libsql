/**
   BEGIN FILE: api/pre-js.js

   This file is intended to be prepended to the sqlite3.js build using
   Emscripten's --pre-js=THIS_FILE flag (or equivalent).
*/
Module['locateFile'] = function(path, prefix) {
  return prefix + path;
};

/**
   Bug warning: this xInstantiateWasm bit must remain disabled
   until this bug is resolved or wasmfs won't work:

   https://github.com/emscripten-core/emscripten/issues/17951
*/
const xInstantiateWasm = 1
      ? 'emscripten-bug-17951'
      : 'instantiateWasm';
Module[xInstantiateWasm] = function callee(imports,onSuccess){
  imports.env.foo = function(){};
  console.warn("instantiateWasm() uri =",callee.uri, self.location.href);
  const wfetch = ()=>fetch(callee.uri, {credentials: 'same-origin'});
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
  Module[xInstantiateWasm].uri by appending it to a build-specific
  copy of this file with the name of the wasm file. This is apparently
  why Emscripten hard-codes the name of the wasm file into their glue
  scripts.
*/
Module[xInstantiateWasm].uri = 'sqlite3.wasm';
/* END FILE: api/pre-js.js */
