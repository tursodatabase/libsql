importScripts(
  (new URL(self.location.href).searchParams).get('sqlite3.dir') + '/sqlite3.js'
);
self.sqlite3InitModule().then(async function(sqlite3){
  const urlArgs = new URL(self.location.href).searchParams;
  const options = {
    workerName: urlArgs.get('workerId') || Math.round(Math.random()*10000),
    unlockAsap: urlArgs.get('opfs-unlock-asap') || 0 /*EXPERIMENTAL*/
  };
  const wPost = (type,...payload)=>{
    postMessage({type, worker: options.workerName, payload});
  };
  const stdout = (...args)=>wPost('stdout',...args);
  const stderr = (...args)=>wPost('stderr',...args);
  if(!sqlite3.opfs){
    stderr("OPFS support not detected. Aborting.");
    return;
  }

  const wait = async (ms)=>{
    return new Promise((resolve)=>setTimeout(resolve,ms));
  };

  const dbName = 'concurrency-tester.db';
  if(urlArgs.has('unlink-db')){
    await sqlite3.opfs.unlink(dbName);
    stdout("Unlinked",dbName);
  }
  wPost('loaded');
  let db;
  const interval = Object.assign(Object.create(null),{
    delay: urlArgs.has('interval') ? (+urlArgs.get('interval') || 750) : 750,
    handle: undefined,
    count: 0
  });
  const finish = ()=>{
    if(db){
      if(!db.pointer) return;
      db.close();
    }
    if(interval.error){
      wPost('failed',"Ending work after interval #"+interval.count,
            "due to error:",interval.error);
    }else{
      wPost('finished',"Ending work after",interval.count,"intervals.");
    }
  };
  const run = async function(){
    db = new sqlite3.oo1.OpfsDb({
      filename: 'file:'+dbName+'?opfs-unlock-asap='+options.unlockAsap,
      flags: 'c'
    });
    sqlite3.capi.sqlite3_busy_timeout(db.pointer, 5000);
    db.transaction((db)=>{
      db.exec([
        "create table if not exists t1(w TEXT UNIQUE ON CONFLICT REPLACE,v);",
        "create table if not exists t2(w TEXT UNIQUE ON CONFLICT REPLACE,v);"
      ]);
    });

    const maxIterations =
          urlArgs.has('iterations') ? (+urlArgs.get('iterations') || 10) : 10;
    stdout("Starting interval-based db updates with delay of",interval.delay,"ms.");
    const doWork = async ()=>{
      const tm = new Date().getTime();
      ++interval.count;
      const prefix = "v(#"+interval.count+")";
      stdout("Setting",prefix,"=",tm);
      try{
        db.exec({
          sql:"INSERT OR REPLACE INTO t1(w,v) VALUES(?,?)",
          bind: [options.workerName, new Date().getTime()]
        });
        //stdout("Set",prefix);
      }catch(e){
        interval.error = e;
      }
    };
    if(1){/*use setInterval()*/
      setTimeout(async function timer(){
        await doWork();
        if(interval.error || maxIterations === interval.count){
          finish();
        }else{
          setTimeout(timer, interval.delay);
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
        case 'run': run().catch((e)=>{
          if(!interval.error) interval.error = e;
          finish();
        });
          break;
        default:
          stderr("Unhandled message type '"+data.type+"'.");
          break;
    }
  };
});
