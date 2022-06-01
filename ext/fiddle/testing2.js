/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic test script for sqlite3-worker.js.
*/
(function(){
    /** Posts a worker message as {type:type, data:data}. */
    const SW = new Worker("sqlite3-worker.js");
    const wMsg = (type,data)=>SW.postMessage({type, data});
    const log = console.log.bind(console);
    const warn = console.warn.bind(console);
    SW.onmessage = function(ev){
        if(!ev.data || 'object'!==typeof ev.data){
            warn("Unknown sqlite3-worker message type:",ev);
            return;
        }
        ev = ev.data/*expecting a nested object*/;
        switch(ev.type){
            case 'sqlite3-api':
                switch(ev.data){
                    case 'loaded':
                        log("Message:",ev); return;
                    case 'ready':
                        log("Message:",ev);
                        self.sqlite3TestModule.setStatus(null);
                        return;
                    default: break;
                }
                break;
        }
        warn("Unknown sqlite3-api message type:",ev);
    };
    log("Init complete, but async bits may still be running.");
})();
