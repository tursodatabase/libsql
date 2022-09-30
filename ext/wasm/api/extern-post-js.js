/* extern-post-js.js must be appended to the resulting sqlite3.js
   file. */
(function(){
  /**
     In order to hide the sqlite3InitModule()'s resulting Emscripten
     module from downstream clients (and simplify our documentation by
     being able to elide those details), we rewrite
     sqlite3InitModule() to return the sqlite3 object.

     Unfortunately, we cannot modify the module-loader/exporter-based
     impls which Emscripten installs at some point in the file above
     this.
  */
  const originalInit = self.sqlite3InitModule;
  if(!originalInit){
    throw new Error("Expecting self.sqlite3InitModule to be defined by the Emscripten build.");
  }
  self.sqlite3InitModule = (...args)=>{
    //console.warn("Using replaced sqlite3InitModule()",self.location);
    return originalInit(...args).then((EmscriptenModule)=>{
      if(self.window!==self &&
         (EmscriptenModule['ENVIRONMENT_IS_PTHREAD']
          || EmscriptenModule['_pthread_self']
          || 'function'===typeof threadAlert
          || self.location.pathname.endsWith('.worker.js')
         )){
        /** Workaround for wasmfs-generated worker, which calls this
            routine from each individual thread and requires that its
            argument be returned. All of the criteria above are fragile,
            based solely on inspection of the offending code, not public
            Emscripten details. */
        return EmscriptenModule;
      }
      const f = EmscriptenModule.sqlite3.asyncPostInit;
      delete EmscriptenModule.sqlite3.asyncPostInit;
      return f();
    }).catch((e)=>{
      console.error("Exception loading sqlite3 module:",e);
      throw e;
    });
  };
  self.sqlite3InitModule.ready = originalInit.ready;
  //console.warn("Replaced sqlite3InitModule()");
})();
