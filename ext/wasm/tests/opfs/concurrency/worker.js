importScripts(
  (new URL(self.location.href).searchParams).get('sqlite3.dir') + '/sqlite3.js'
);
self.sqlite3InitModule().then(async function(sqlite3){
  const wName = Math.round(Math.random()*10000);
  const wPost = (type,...payload)=>{
    postMessage({type, worker: wName, payload});
  };
  const stdout = (...args)=>wPost('stdout',...args);
  const stderr = (...args)=>wPost('stderr',...args);
  const postErr = (...args)=>wPost('error',...args);
  if(!sqlite3.opfs){
    stderr("OPFS support not detected. Aborting.");
    return;
  }

  const wait = async (ms)=>{
    return new Promise((resolve)=>setTimeout(resolve,ms));
  };

  const dbName = 'concurrency-tester.db';
  if((new URL(self.location.href).searchParams).has('unlink-db')){
    await sqlite3.opfs.unlink(dbName);
    stdout("Unlinked",dbName);
  }
  wPost('loaded');

  const run = async function(){
    const db = new sqlite3.opfs.OpfsDb(dbName,'c');
    //sqlite3.capi.sqlite3_busy_timeout(db.pointer, 2000);
    db.transaction((db)=>{
      db.exec([
        "create table if not exists t1(w TEXT UNIQUE ON CONFLICT REPLACE,v);",
        "create table if not exists t2(w TEXT UNIQUE ON CONFLICT REPLACE,v);"
      ]);
    });

    const maxIterations = 10;
    const interval = Object.assign(Object.create(null),{
      delay: 500,
      handle: undefined,
      count: 0
    });
    stdout("Starting interval-based db updates with delay of",interval.delay,"ms.");
    const doWork = async ()=>{
      const tm = new Date().getTime();
      ++interval.count;
      const prefix = "v(#"+interval.count+")";
      stdout("Setting",prefix,"=",tm);
      try{
        db.exec({
          sql:"INSERT OR REPLACE INTO t1(w,v) VALUES(?,?)",
          bind: [wName, new Date().getTime()]
        });
        //stdout("Set",prefix);
      }catch(e){
        interval.error = e;
      }
    };
    const finish = ()=>{
      db.close();
      if(interval.error){
        wPost('failed',"Ending work after interval #"+interval.count,
              "due to error:",interval.error);
      }else{
        wPost('finished',"Ending work after",interval.count,"intervals.");
      }
    };
    if(1){/*use setInterval()*/
      interval.handle = setInterval(async ()=>{
        await doWork();
        if(interval.error || maxIterations === interval.count){
          clearInterval(interval.handle);
          finish();
        }
      }, interval.delay);
    }else{
      /*This approach provides no concurrency whatsoever: each worker
        is run to completion before any others can work.*/
      let i;
      for(i = 0; i < maxIterations; ++i){
        await doWork();
        if(interval.error) break;
        await wait(interval.ms);
      }
      finish();
    }
  }/*run()*/;

  self.onmessage = function({data}){
    switch(data.type){
        case 'run': run().catch((e)=>postErr(e.message));
          break;
        default:
          stderr("Unhandled message type '"+data.type+"'.");
          break;
    }
  };
});
