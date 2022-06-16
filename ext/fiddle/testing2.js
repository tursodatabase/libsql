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
    const T = self.SqliteTestUtil;
    const SW = new Worker("sqlite3-worker.js");
    /** Posts a worker message as {type:type, data:data}. */
    const wMsg = function(type,data){
        SW.postMessage({type, data});
        return SW;
    };
    const log = console.log.bind(console);
    const warn = console.warn.bind(console);
    const error = console.error.bind(console);

    SW.onerror = function(event){
        error("onerror",event);
    };

    /**
       A queue for callbacks which are to be run in response to async
       DB commands. See the notes in runTests() for why we need
       this. The event-handling plumbing of this file requires that
       any DB command which includes a `messageId` property also have
       a queued callback entry, as the existence of that property in
       response payloads is how it knows whether or not to shift an
       entry off of the queue.
    */
    const MsgHandlerQueue = {
        queue: [],
        id: 0,
        push: function(type,callback){
            this.queue.push(callback);
            return type + '-' + (++this.id);
        },
        shift: function(){
            return this.queue.shift();
        }
    };

    const testCount = ()=>log("Total test count:",T.counter);

    const runOneTest = function(eventType, eventData, callback){
        T.assert(eventData && 'object'===typeof eventData);
        /* ^^^ that is for the testing and messageId-related code, not
           a hard requirement of all of the Worker-exposed APIs. */
        eventData.messageId = MsgHandlerQueue.push(eventType,function(ev){
            log("runOneTest",eventType,"result",ev.data);
            if(callback instanceof Function){
                callback(ev);
                testCount();
            }
        });
        wMsg(eventType, eventData);
    };

    /** Methods which map directly to onmessage() event.type keys.
        They get passed the inbound event.data. */
    const dbMsgHandler = {
        open: function(ev){
            log("open result",ev.data);
        },
        exec: function(ev){
            log("exec result",ev.data);
        },
        export: function(ev){
            log("exec result",ev.data);
        },
        error: function(ev){
            error("ERROR from the worker:",ev.data);
        },
        resultRowTest1: function f(ev){
            if(undefined === f.counter) f.counter = 0;
            if(ev.data) ++f.counter;
            //log("exec() result row:",ev.data);
            T.assert(null===ev.data || 'number' === typeof ev.data.b);
        }
    };

    const runTests = function(){
        const mustNotReach = ()=>{
            throw new Error("This is not supposed to be reached.");
        };
        /**
           "The problem" now is that the test results are async. We
           know, however, that the messages posted to the worker will
           be processed in the order they are passed to it, so we can
           create a queue of callbacks to handle them. The problem
           with that approach is that it's not error-handling
           friendly, in that an error can cause us to bypass a result
           handler queue entry. We have to perform some extra
           acrobatics to account for that.
        */
        runOneTest('open', {filename:'testing2.sqlite3'}, function(ev){
            //log("open result",ev);
            T.assert('testing2.sqlite3'===ev.data.filename)
                .assert(ev.data.messageId);
        });
        runOneTest('exec',{
            sql: ["create table t(a,b)",
                  "insert into t(a,b) values(1,2),(3,4),(5,6)"
                 ].join(';'),
            multi: true,
            resultRows: [], columnNames: []
        }, function(ev){
            ev = ev.data;
            T.assert(0===ev.resultRows.length)
                .assert(0===ev.columnNames.length);
        });
        runOneTest('exec',{
            sql: 'select a a, b b from t order by a',
            resultRows: [], columnNames: [],
        }, function(ev){
            ev = ev.data;
            T.assert(3===ev.resultRows.length)
                .assert(1===ev.resultRows[0][0])
                .assert(6===ev.resultRows[2][1])
                .assert(2===ev.columnNames.length)
                .assert('b'===ev.columnNames[1]);
        });
        runOneTest('exec',{
            sql: 'select a a, b b from t order by a',
            resultRows: [], columnNames: [],
            rowMode: 'object'
        }, function(ev){
            ev = ev.data;
            T.assert(3===ev.resultRows.length)
                .assert(1===ev.resultRows[0].a)
                .assert(6===ev.resultRows[2].b)
        });
        runOneTest('exec',{sql:'intentional_error'}, mustNotReach);
        // Ensure that the message-handler queue survives ^^^ that error...
        runOneTest('exec',{
            sql:'select 1',
            resultRows: [],
            //rowMode: 'array', // array is the default in the Worker interface
        }, function(ev){
            ev = ev.data;
            T.assert(1 === ev.resultRows.length)
                .assert(1 === ev.resultRows[0][0]);
        });
        runOneTest('exec',{
            sql: 'select a a, b b from t order by a',
            callback: 'resultRowTest1',
            rowMode: 'object'
        }, function(ev){
            T.assert(3===dbMsgHandler.resultRowTest1.counter);
            dbMsgHandler.resultRowTest1.counter = 0;
        });
        runOneTest('exec',{sql: 'delete from t where a>3'});
        runOneTest('exec',{
            sql: 'select count(a) from t',
            resultRows: []
        },function(ev){
            ev = ev.data;
            T.assert(1===ev.resultRows.length)
                .assert(2===ev.resultRows[0][0]);
        });
        runOneTest('export',{}, function(ev){
            ev = ev.data;
            T.assert('string' === typeof ev.filename)
                .assert(ev.buffer instanceof Uint8Array)
                .assert(ev.buffer.length > 1024)
                .assert('application/x-sqlite3' === ev.mimetype);
        });

        /***** close() tests must come last. *****/
        runOneTest('close',{unlink:true},function(ev){
            ev = ev.data;
            T.assert('string' === typeof ev.filename);
        });
        runOneTest('close',{unlink:true},function(ev){
            ev = ev.data;
            T.assert(undefined === ev.filename);
        });
    };

    SW.onmessage = function(ev){
        if(!ev.data || 'object'!==typeof ev.data){
            warn("Unknown sqlite3-worker message type:",ev);
            return;
        }
        ev = ev.data/*expecting a nested object*/;
        //log("main window onmessage:",ev);
        if(ev.data && ev.data.messageId){
            /* We're expecting a queued-up callback handler. */
            const f = MsgHandlerQueue.shift();
            if('error'===ev.type){
                dbMsgHandler.error(ev);
                return;
            }
            T.assert(f instanceof Function);
            f(ev);
            return;
        }
        switch(ev.type){
            case 'sqlite3-api':
                switch(ev.data){
                    case 'loaded':
                        log("Message:",ev); return;
                    case 'ready':
                        log("Message:",ev);
                        self.sqlite3TestModule.setStatus(null);
                        setTimeout(runTests, 0);
                        return;
                    default:
                        warn("Unknown sqlite3-api message type:",ev);
                        return;
                }
            default:
                if(dbMsgHandler.hasOwnProperty(ev.type)){
                    try{dbMsgHandler[ev.type](ev);}
                    catch(err){
                        error("Exception while handling db result message",
                              ev,":",err);
                    }
                    return;
                }
                warn("Unknown sqlite3-api message type:",ev);
        }
    };

    log("Init complete, but async init bits may still be running.");
})();
