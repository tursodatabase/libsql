/*
  2022-08-24

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file implements a Promise-based proxy for the sqlite3 Worker
  API #1. It is intended to be included either from the main thread or
  a Worker, but only if (A) the environment supports nested Workers
  and (B) it's _not_ a Worker which loads the sqlite3 WASM/JS
  module. This file's features will load that module and provide a
  slightly simpler client-side interface than the slightly-lower-level
  Worker API does.

  This script necessarily exposes one global symbol, but clients may
  freely `delete` that symbol after calling it.
*/
'use strict';
/**
   Configures an sqlite3 Worker API #1 Worker such that it can be
   manipulated via a Promise-based interface and returns a factory
   function which returns Promises for communicating with the worker.
   This proxy has an _almost_ identical interface to the normal
   worker API, with any exceptions documented below.

   It requires a configuration object with the following properties:

   - `worker` (required): a Worker instance which loads
   `sqlite3-worker1.js` or a functional equivalent. Note that the
   promiser factory replaces the worker.onmessage property. This
   config option may alternately be a function, in which case this
   function re-assigns this property with the result of calling that
   function, enabling delayed instantiation of a Worker.

   - `onready` (optional, but...): this callback is called with no
   arguments when the worker fires its initial
   'sqlite3-api'/'worker1-ready' message, which it does when
   sqlite3.initWorker1API() completes its initialization. This is
   the simplest way to tell the worker to kick off work at the
   earliest opportunity.

   - `onunhandled` (optional): a callback which gets passed the
   message event object for any worker.onmessage() events which
   are not handled by this proxy. Ideally that "should" never
   happen, as this proxy aims to handle all known message types.

   - `generateMessageId` (optional): a function which, when passed an
   about-to-be-posted message object, generates a _unique_ message ID
   for the message, which this API then assigns as the messageId
   property of the message. It _must_ generate unique IDs on each call
   so that dispatching can work. If not defined, a default generator
   is used (which should be sufficient for most or all cases).

   - `debug` (optional): a console.debug()-style function for logging
   information about messages.

   This function returns a stateful factory function with the
   following interfaces:

   - Promise function(messageType, messageArgs)
   - Promise function({message object})

   The first form expects the "type" and "args" values for a Worker
   message. The second expects an object in the form {type:...,
   args:...}  plus any other properties the client cares to set. This
   function will always set the `messageId` property on the object,
   even if it's already set, and will set the `dbId` property to the
   current database ID if it is _not_ set in the message object.

   The function throws on error.

   The function installs a temporary message listener, posts a
   message to the configured Worker, and handles the message's
   response via the temporary message listener. The then() callback
   of the returned Promise is passed the `message.data` property from
   the resulting message, i.e. the payload from the worker, stripped
   of the lower-level event state which the onmessage() handler
   receives.

   Example usage:

   ```
   const config = {...};
   const sq3Promiser = sqlite3Worker1Promiser(config);
   sq3Promiser('open', {filename:"/foo.db"}).then(function(msg){
     console.log("open response",msg); // => {type:'open', result: {filename:'/foo.db'}, ...}
   });
   sq3Promiser({type:'close'}).then((msg)=>{
     console.log("close response",msg); // => {type:'close', result: {filename:'/foo.db'}, ...}
   });
   ```

   Differences from Worker API #1:

   - exec's {callback: STRING} option does not work via this
   interface (it triggers an exception), but {callback: function}
   does and works exactly like the STRING form does in the Worker:
   the callback is called one time for each row of the result set,
   passed the same worker message format as the worker API emits:

     {type:typeString,
      row:VALUE,
      rowNumber:1-based-#,
      columnNames: array}

   Where `typeString` is an internally-synthesized message type string
   used temporarily for worker message dispatching. It can be ignored
   by all client code except that which tests this API. The `row`
   property contains the row result in the form implied by the
   `rowMode` option (defaulting to `'array'`). The `rowNumber` is a
   1-based integer value incremented by 1 on each call into the
   callback.

   At the end of the result set, the same event is fired with
   (row=undefined, rowNumber=null) to indicate that
   the end of the result set has been reached. Note that the rows
   arrive via worker-posted messages, with all the implications
   of that.

   Notable shortcomings:

   - This API was not designed with ES6 modules in mind. Neither Firefox
     nor Safari support, as of March 2023, the {type:"module"} flag to the
     Worker constructor, so that particular usage is not something we're going
     to target for the time being:

     https://developer.mozilla.org/en-US/docs/Web/API/Worker/Worker
*/
globalThis.sqlite3Worker1Promiser = function callee(config = callee.defaultConfig){
  // Inspired by: https://stackoverflow.com/a/52439530
  if(1===arguments.length && 'function'===typeof arguments[0]){
    const f = config;
    config = Object.assign(Object.create(null), callee.defaultConfig);
    config.onready = f;
  }else{
    config = Object.assign(Object.create(null), callee.defaultConfig, config);
  }
  const handlerMap = Object.create(null);
  const noop = function(){};
  const err = config.onerror
        || noop /* config.onerror is intentionally undocumented
                   pending finding a less ambiguous name */;
  const debug = config.debug || noop;
  const idTypeMap = config.generateMessageId ? undefined : Object.create(null);
  const genMsgId = config.generateMessageId || function(msg){
    return msg.type+'#'+(idTypeMap[msg.type] = (idTypeMap[msg.type]||0) + 1);
  };
  const toss = (...args)=>{throw new Error(args.join(' '))};
  if(!config.worker) config.worker = callee.defaultConfig.worker;
  if('function'===typeof config.worker) config.worker = config.worker();
  let dbId;
  config.worker.onmessage = function(ev){
    ev = ev.data;
    debug('worker1.onmessage',ev);
    let msgHandler = handlerMap[ev.messageId];
    if(!msgHandler){
      if(ev && 'sqlite3-api'===ev.type && 'worker1-ready'===ev.result) {
        /*fired one time when the Worker1 API initializes*/
        if(config.onready) config.onready();
        return;
      }
      msgHandler = handlerMap[ev.type] /* check for exec per-row callback */;
      if(msgHandler && msgHandler.onrow){
        msgHandler.onrow(ev);
        return;
      }
      if(config.onunhandled) config.onunhandled(arguments[0]);
      else err("sqlite3Worker1Promiser() unhandled worker message:",ev);
      return;
    }
    delete handlerMap[ev.messageId];
    switch(ev.type){
        case 'error':
          msgHandler.reject(ev);
          return;
        case 'open':
          if(!dbId) dbId = ev.dbId;
          break;
        case 'close':
          if(ev.dbId===dbId) dbId = undefined;
          break;
        default:
          break;
    }
    try {msgHandler.resolve(ev)}
    catch(e){msgHandler.reject(e)}
  }/*worker.onmessage()*/;
  return function(/*(msgType, msgArgs) || (msgEnvelope)*/){
    let msg;
    if(1===arguments.length){
      msg = arguments[0];
    }else if(2===arguments.length){
      msg = Object.create(null);
      msg.type = arguments[0];
      msg.args = arguments[1];
    }else{
      toss("Invalid arugments for sqlite3Worker1Promiser()-created factory.");
    }
    if(!msg.dbId) msg.dbId = dbId;
    msg.messageId = genMsgId(msg);
    msg.departureTime = performance.now();
    const proxy = Object.create(null);
    proxy.message = msg;
    let rowCallbackId /* message handler ID for exec on-row callback proxy */;
    if('exec'===msg.type && msg.args){
      if('function'===typeof msg.args.callback){
        rowCallbackId = msg.messageId+':row';
        proxy.onrow = msg.args.callback;
        msg.args.callback = rowCallbackId;
        handlerMap[rowCallbackId] = proxy;
      }else if('string' === typeof msg.args.callback){
        toss("exec callback may not be a string when using the Promise interface.");
        /**
           Design note: the reason for this limitation is that this
           API takes over worker.onmessage() and the client has no way
           of adding their own message-type handlers to it. Per-row
           callbacks are implemented as short-lived message.type
           mappings for worker.onmessage().

           We "could" work around this by providing a new
           config.fallbackMessageHandler (or some such) which contains
           a map of event type names to callbacks. Seems like overkill
           for now, seeing as the client can pass callback functions
           to this interface (whereas the string-form "callback" is
           needed for the over-the-Worker interface).
        */
      }
    }
    //debug("requestWork", msg);
    let p = new Promise(function(resolve, reject){
      proxy.resolve = resolve;
      proxy.reject = reject;
      handlerMap[msg.messageId] = proxy;
      debug("Posting",msg.type,"message to Worker dbId="+(dbId||'default')+':',msg);
      config.worker.postMessage(msg);
    });
    if(rowCallbackId) p = p.finally(()=>delete handlerMap[rowCallbackId]);
    return p;
  };
}/*sqlite3Worker1Promiser()*/;
globalThis.sqlite3Worker1Promiser.defaultConfig = {
  worker: function(){
//#if target=es6-bundler-friendly
    return new Worker(new URL("sqlite3-worker1-bundler-friendly.mjs", import.meta.url),{
      type: 'module'
    });
//#else
    let theJs = "sqlite3-worker1.js";
    if(this.currentScript){
      const src = this.currentScript.src.split('/');
      src.pop();
      theJs = src.join('/')+'/' + theJs;
      //sqlite3.config.warn("promiser currentScript, theJs =",this.currentScript,theJs);
    }else if(globalThis.location){
      //sqlite3.config.warn("promiser globalThis.location =",globalThis.location);
      const urlParams = new URL(globalThis.location.href).searchParams;
      if(urlParams.has('sqlite3.dir')){
        theJs = urlParams.get('sqlite3.dir') + '/' + theJs;
      }
    }
    return new Worker(theJs + globalThis.location.search);
//#endif
  }
//#ifnot target=es6-bundler-friendly
  .bind({
    currentScript: globalThis?.document?.currentScript
  })
//#endif
  ,
  onerror: (...args)=>console.error('worker1 promiser error',...args)
};
