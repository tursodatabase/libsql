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

  This script necessarily exposes on global symbol, but clients may
  freely `delete` that symbol after calling it.
*/
'use strict';
/**
   Configures an sqlite3 Worker API #1 Worker such that it can be
   manipulated via a Promise-based interface and returns a factory
   function which returns Promises for communicating with the worker.
   This proxy has an _almost_ identical interface to the normal
   worker API, with any exceptions noted below.

   It requires a configuration object with the following properties:

   - `worker` (required): a Worker instance which loads
   `sqlite3-worker1.js` or a functional equivalent. Note that this
   function replaces the worker.onmessage property. This property
   may alternately be a function, in which case this function
   re-assigns this property with the result of calling that
   function, enabling delayed instantiation of a Worker.

   - `onready` (optional, but...): this callback is called with no
   arguments when the worker fires its initial
   'sqlite3-api'/'worker1-ready' message, which it does when
   sqlite3.initWorker1API() completes its initialization. This is
   the simplest way to tell the worker to kick of work at the
   earliest opportunity.

   - `onerror` (optional): a callback to pass error-type events from
   the worker. The object passed to it will be the error message
   payload from the worker. This is _not_ the same as the
   worker.onerror property!

   - `onunhandled` (optional): a callback which gets passed the
   message event object for any worker.onmessage() events which
   are not handled by this proxy. Ideally that "should" never
   happen, as this proxy aims to handle all known message types.

   - `generateMessageId` (optional): a function which, when passed
   an about-to-be-posted message object, generates a _unique_
   message ID for the message, which this API then assigns as the
   messageId property of the message. It _must_ generate unique
   IDs so that dispatching can work. If not defined, a default
   generator is used.

   - `dbId` (optional): is the database ID to be used by the
   worker. This must initially be unset or a falsy value. The
   first `open` message sent to the worker will cause this config
   entry to be assigned to the ID of the opened database. That ID
   "should" be set as the `dbId` property of the message sent in
   future requests, so that the worker uses that database.
   However, if the worker is not given an explicit dbId, it will
   use the first-opened database by default. If client code needs
   to work with multiple database IDs, the client-level code will
   need to juggle those themselves. A `close` message will clear
   this property if it matches the ID of the closed db. Potential
   TODO: add a config callback specifically for reporting `open`
   and `close` message results, so that clients may track those
   values.

   - `debug` (optional): a console.debug()-style function for logging
   information about messages.


   This function returns a stateful factory function with the following
   interfaces:

   - Promise function(messageType, messageArgs)
   - Promise function({message object})

   The first form expects the "type" and "args" values for a Worker
   message. The second expects an object in the form {type:...,
   args:...}  plus any other properties the client cares to set. This
   function will always set the messageId property on the object,
   even if it's already set, and will set the dbId property to
   config.dbId if it is _not_ set in the message object.

   The function throws on error.

   The function installs a temporarily message listener, posts a
   message to the configured Worker, and handles the message's
   response via the temporary message listener. The then() callback
   of the returned Promise is passed the `message.data` property from
   the resulting message, i.e. the payload from the worker, stripped
   of the lower-level event state which the onmessage() handler
   receives.

   Example usage:

   ```
   const config = {...};
   const eventPromiser = sqlite3Worker1Promiser(config);
   eventPromiser('open', {filename:"/foo.db"}).then(function(msg){
     console.log("open response",msg); // => {type:'open', result: {filename:'/foo.db'}, ...}
     // Recall that config.dbId will be set for the first 'open'
     // call and cleared for a matching 'close' call.
   });
   eventPromiser({type:'close'}).then((msg)=>{
     console.log("open response",msg); // => {type:'open', result: {filename:'/foo.db'}, ...}
     // Recall that config.dbId will be used by default for the message's dbId if
     // none is explicitly provided, and a 'close' op will clear config.dbId if it
     // closes that exact db.
   });
   ```

   Differences from Worker API #1:

   - exec's {callback: STRING} option does not work via this
   interface (it triggers an exception), but {callback: function}
   does and works exactly like the STRING form does in the Worker:
   the callback is called one time for each row of the result set
   and once more, at the end, passed only `null`, to indicate that
   the end of the result set has been reached. Note that the rows
   arrive via worker-posted messages, with all the implications
   of that.


   TODO?: a config option which causes it to queue up events to fire
   one at a time and flush the event queue on the first error. The
   main use for this is test runs which must fail at the first error.
*/
self.sqlite3Worker1Promiser = function callee(config = callee.defaultConfig){
  // Inspired by: https://stackoverflow.com/a/52439530
  let idNumber = 0;
  const handlerMap = Object.create(null);
  const noop = function(){};
  const err = config.onerror || noop;
  const debug = config.debug || noop;
  const genMsgId = config.generateMessageId || function(msg){
    return msg.type+'#'+(++idNumber);
  };
  const toss = (...args)=>{throw new Error(args.join(' '))};
  if('function'===typeof config.worker) config.worker = config.worker();
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
        msgHandler.onrow(ev.row);
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
          if(!config.dbId) config.dbId = ev.dbId;
          break;
        case 'close':
          if(config.dbId === ev.dbId) config.dbId = undefined;
          break;
        default:
          break;
    }
    msgHandler.resolve(ev);
  }/*worker.onmessage()*/;
  return function(/*(msgType, msgArgs) || (msg)*/){
    let msg;
    if(1===arguments.length){
      msg = arguments[0];
    }else if(2===arguments.length){
      msg = {
        type: arguments[0],
        args: arguments[1]
      };
    }else{
      toss("Invalid arugments for sqlite3Worker1Promiser()-created factory.");
    }
    if(!msg.dbId) msg.dbId = config.dbId;
    msg.messageId = genMsgId(msg);
    msg.departureTime = performance.now();
    const proxy = Object.create(null);
    proxy.message = msg;
    let cbId /* message handler ID for exec on-row callback proxy */;
    if('exec'===msg.type && msg.args){
      if('function'===typeof msg.args.callback){
        cbId = genMsgId(msg)+':row';
        proxy.onrow = msg.args.callback;
        msg.args.callback = cbId;
        handlerMap[cbId] = proxy;
      }else if('string' === typeof msg.args.callback){
        toss("exec callback may not be a string when using the Promise interface.");
      }
    }
    //debug("requestWork", msg);
    const p = new Promise(function(resolve, reject){
      proxy.resolve = resolve;
      proxy.reject = reject;
      handlerMap[msg.messageId] = proxy;
      debug("Posting",msg.type,"message to Worker dbId="+(config.dbId||'default')+':',msg);
      config.worker.postMessage(msg);
    });
    if(cbId) p.finally(()=>delete handlerMap[cbId]);
    return p;
  };
}/*sqlite3Worker1Promiser()*/;
self.sqlite3Worker1Promiser.defaultConfig = {
  worker: ()=>new Worker('sqlite3-worker1.js'),
  onerror: console.error.bind(console),
  dbId: undefined
};
