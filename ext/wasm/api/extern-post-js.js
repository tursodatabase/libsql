/* extern-post-js.js must be appended to the resulting sqlite3.js
   file. It gets its name from being used as the value for the
   --extern-post-js=... Emscripten flag. Note that this code, unlike
   most of the associated JS code, runs outside of the
   Emscripten-generated module init scope, in the current
   global scope. */
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
  /**
     We need to add some state which our custom Module.locateFile()
     can see, but an Emscripten limitation currently prevents us from
     attaching it to the sqlite3InitModule function object:

     https://github.com/emscripten-core/emscripten/issues/18071

     The only(?) current workaround is to temporarily stash this state
     into the global scope and delete it when sqlite3InitModule()
     is called.
  */
  const initModuleState = self.sqlite3InitModuleState = Object.assign(Object.create(null),{
    moduleScript: self?.document?.currentScript,
    isWorker: ('undefined' !== typeof WorkerGlobalScope),
    location: self.location,
    urlParams: new URL(self.location.href).searchParams
  });
  initModuleState.debugModule =
    (new URL(self.location.href).searchParams).has('sqlite3.debugModule')
    ? (...args)=>console.warn('sqlite3.debugModule:',...args)
    : ()=>{};

  if(initModuleState.urlParams.has('sqlite3.dir')){
    initModuleState.sqlite3Dir = initModuleState.urlParams.get('sqlite3.dir') +'/';
  }else if(initModuleState.moduleScript){
    const li = initModuleState.moduleScript.src.split('/');
    li.pop();
    initModuleState.sqlite3Dir = li.join('/') + '/';
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
      EmscriptenModule.sqlite3.scriptInfo = initModuleState;
      //console.warn("sqlite3.scriptInfo =",EmscriptenModule.sqlite3.scriptInfo);
      const f = EmscriptenModule.sqlite3.asyncPostInit;
      delete EmscriptenModule.sqlite3.asyncPostInit;
      return f();
    }).catch((e)=>{
      console.error("Exception loading sqlite3 module:",e);
      throw e;
    });
  };
  self.sqlite3InitModule.ready = originalInit.ready;

  if(self.sqlite3InitModuleState.moduleScript){
    const sim = self.sqlite3InitModuleState;
    let src = sim.moduleScript.src.split('/');
    src.pop();
    sim.scriptDir = src.join('/') + '/';
  }
  initModuleState.debugModule('sqlite3InitModuleState =',initModuleState);
  if(0){
    console.warn("Replaced sqlite3InitModule()");
    console.warn("self.location.href =",self.location.href);
    if('undefined' !== typeof document){
      console.warn("document.currentScript.src =",
                   document?.currentScript?.src);
    }
  }
  /* Replace the various module exports performed by the Emscripten
     glue... */
  if (typeof exports === 'object' && typeof module === 'object')
    module.exports = sqlite3InitModule;
  else if (typeof exports === 'object')
    exports["sqlite3InitModule"] = sqlite3InitModule;
  /* AMD modules get injected in a way we cannot override,
     so we can't handle those here. */
})();
