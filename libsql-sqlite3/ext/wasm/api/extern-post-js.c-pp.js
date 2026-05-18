
/* ^^^^ ACHTUNG: blank line at the start is necessary because
   Emscripten will not add a newline in some cases and we need
   a blank line for a sed-based kludge for the ES6 build.

   extern-post-js.js must be appended to the resulting sqlite3.js
   file. It gets its name from being used as the value for the
   --extern-post-js=... Emscripten flag. This code, unlike most of the
   associated JS code, runs outside of the Emscripten-generated module
   init scope, in the current global scope.

   At the time this is run, the global-scope sqlite3InitModule
   function will have just been defined.
*/
//#if target:es6-module
const toExportForESM =
//#endif
(function(){
  //console.warn("this is extern-post-js");
  /**
     In order to hide the sqlite3InitModule()'s resulting
     Emscripten module from downstream clients (and simplify our
     documentation by being able to elide those details), we hide that
     function and expose a hand-written sqlite3InitModule() to return
     the sqlite3 object (most of the time).
  */
  const originalInit = sqlite3InitModule;
  if(!originalInit){
    throw new Error("Expecting globalThis.sqlite3InitModule to be defined by the Emscripten build.");
  }
  /**
     We need to add some state which our custom Module.locateFile()
     can see, but an Emscripten limitation currently prevents us from
     attaching it to the sqlite3InitModule function object:

     https://github.com/emscripten-core/emscripten/issues/18071

     The only(?) current workaround is to temporarily stash this state
     into the global scope and delete it when sqlite3InitModule()
     is called.
  */
  const sIMS = globalThis.sqlite3InitModuleState = Object.assign(Object.create(null),{
    moduleScript: globalThis?.document?.currentScript,
    isWorker: ('undefined' !== typeof WorkerGlobalScope),
    location: globalThis.location,
    urlParams:  globalThis?.location?.href
      ? new URL(globalThis.location.href).searchParams
      : new URLSearchParams(),
    /*
      It is literally impossible to reliably get the name of _this_ script
      at runtime, so impossible to reliably derive X.wasm from script name
      X.js. (This is apparently why Emscripten hard-codes the name of the
      wasm file into their output.)  Thus we need, at build-time, to set
      the name of the WASM file which our custom instantiateWasm() should to
      load. The build process populates this.

      Module.instantiateWasm() is found in pre-js.c-pp.js.
    */
    wasmFilename: '@sqlite3.wasm@' /* replaced by the build process */
  });
  sIMS.debugModule =
    sIMS.urlParams.has('sqlite3.debugModule')
    ? (...args)=>console.warn('sqlite3.debugModule:',...args)
    : ()=>{};

  if(sIMS.urlParams.has('sqlite3.dir')){
    sIMS.sqlite3Dir = sIMS.urlParams.get('sqlite3.dir') +'/';
  }else if(sIMS.moduleScript){
    const li = sIMS.moduleScript.src.split('/');
    li.pop();
    sIMS.sqlite3Dir = li.join('/') + '/';
  }

  const sIM = globalThis.sqlite3InitModule = function ff(...args){
    //console.warn("Using replaced sqlite3InitModule()",globalThis.location);
    return originalInit(...args).then((EmscriptenModule)=>{
      sIMS.debugModule("sqlite3InitModule() sIMS =",sIMS);
      sIMS.debugModule("sqlite3InitModule() EmscriptenModule =",EmscriptenModule);
      const s = EmscriptenModule.runSQLite3PostLoadInit(
        sIMS,
        EmscriptenModule /* see post-js-header/footer.js */,
        !!ff.__isUnderTest
      );
      sIMS.debugModule("sqlite3InitModule() sqlite3 =",s);
      //const rv = s.asyncPostInit();
      //delete s.asyncPostInit;
//#if wasmfs
      if('undefined'!==typeof WorkerGlobalScope &&
         EmscriptenModule['ENVIRONMENT_IS_PTHREAD']){
        /** Workaround for wasmfs-generated worker, which calls this
            routine from each individual thread and requires that its
            argument be returned. The if() condition above is fragile,
            based solely on inspection of the offending code, not
            public Emscripten details. */
        //console.warn("sqlite3InitModule() returning E-module.",EmscriptenModule);
        return EmscriptenModule;
      }
//#endif
      return s;
    }).catch((e)=>{
      console.error("Exception loading sqlite3 module:",e);
      throw e;
    });
  };
  sIM.ready = originalInit.ready;

  if(sIMS.moduleScript){
    let src = sIMS.moduleScript.src.split('/');
    src.pop();
    sIMS.scriptDir = src.join('/') + '/';
  }
  sIMS.debugModule('extern-post-js.c-pp.js sqlite3InitModuleState =',sIMS);
//#if not target:es6-module
// Emscripten does not inject these module-loader bits in ES6 module
// builds and including them here breaks JS bundlers, so elide them
// from ESM builds.
  /* Replace the various module exports performed by the Emscripten
     glue... */
  if (typeof exports === 'object' && typeof module === 'object'){
    module.exports = sIM;
    module.exports.default = sIM;
  }else if( 'function'===typeof define && define.amd ){
    define([], ()=>sIM);
  }else if (typeof exports === 'object'){
    exports["sqlite3InitModule"] = sIM;
  }
  /* AMD modules get injected in a way we cannot override,
     so we can't handle those here. */
//#endif // !target:es6-module
  return sIM;
})();
//#if target:es6-module
sqlite3InitModule = toExportForESM;
export default sqlite3InitModule;
//#endif
