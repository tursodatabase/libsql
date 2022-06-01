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
        warn("onerror",event);
    };

    const MsgHandlerQueue = {
        queue: [],
        id: 0,
        push: function(type,f){
            this.queue.push(f);
            return type + '-' + (++this.id);
        },
        shift: function(){
            return this.queue.shift();
        }
    };

    const runOneTest = function(eventType, eventData, callback){
        T.assert(eventData && 'object'===typeof eventData);
        eventData.messageId = MsgHandlerQueue.push(eventType,function(ev){
            log("runOneTest",eventType,"result",ev.data);
            callback(ev);
        });
        wMsg(eventType, eventData);
    };

    const testCount = ()=>log("Total test count:",T.counter);

    const runTests = function(){
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
            testCount();
        });
        runOneTest('exec',{
            sql: ["create table t(a,b)",
                  "insert into t(a,b) values(1,2),(3,4),(5,6)"
                 ].join(';'),
            multi: true,
            resultRows: [],
            columnNames: []
        }, function(ev){
            ev = ev.data;
            T.assert(0===ev.resultRows.length)
                .assert(0===ev.columnNames.length);
            testCount();
        });
        runOneTest('exec',{
            sql: 'select a a, b b from t order by a',
            resultRows: [], columnNames: []
        }, function(ev){
            ev = ev.data;
            T.assert(3===ev.resultRows.length)
                .assert(1===ev.resultRows[0][0])
                .assert(6===ev.resultRows[2][1])
                .assert(2===ev.columnNames.length)
                .assert('b'===ev.columnNames[1]);
            testCount();
        });
        runOneTest('exec',{sql:'select 1 from intentional_error'}, function(){
            throw new Error("This is not supposed to be reached.");
        });
        // Ensure that the message-handler queue survives ^^^ that error...
        runOneTest('exec',{
            sql:'select 1',
            resultRows: [],
            rowMode: 'array',
        }, function(ev){
            ev = ev.data;
            T.assert(1 === ev.resultRows.length)
                .assert(1 === ev.resultRows[0][0]);
            testCount();
        });
    };

    const dbMsgHandler = {
        open: function(ev){
            log("open result",ev.data);
        },
        exec: function(ev){
            log("exec result",ev.data);
        },
        error: function(ev){
            error("ERROR from the worker:",ev.data);
        }
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

    log("Init complete, but async bits may still be running.");
})();
