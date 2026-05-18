/*
  2025-01-31

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file is part of sahpool-pausing.js's demonstration of the
  pause/unpause feature of the opfs-sahpool VFS.
*/
const searchParams = new URL(self.location.href).searchParams;
const workerId = searchParams.get('workerId');
const wPost = (type,...args)=>postMessage({type, workerId, payload:args});
const log = (...args)=>wPost('log',...args);
let capi, wasm, S, poolUtil;

const sahPoolConfig = {
  name: 'opfs-sahpool-pausable',
  clearOnInit: false,
  initialCapacity: 3
};

importScripts(searchParams.get('sqlite3.dir') + '/sqlite3.js');

const sqlExec = function(sql){
  const db = new poolUtil.OpfsSAHPoolDb('/my.db');
  try{
    return db.exec(sql);
  }finally{
    db.close();
  }
};

const clog = console.log.bind(console);
globalThis.onmessage = function({data}){
  clog(workerId+": onmessage:",data);
  switch(data.type){
    case 'vfs-acquire':
      if( poolUtil ){
        poolUtil.unpauseVfs().then(()=>wPost('vfs-unpaused'));
      }else{
        S.installOpfsSAHPoolVfs(sahPoolConfig).then(pu=>{
          poolUtil = pu;
          wPost('vfs-acquired');
        });
      }
      break;
    case 'db-init':
      try{
        sqlExec([
          "DROP TABLE IF EXISTS mytable;",
          "CREATE TABLE mytable(a);",
          "INSERT INTO mytable(a) VALUES(11),(22),(33)"
        ]);
        wPost('db-inited');
      }catch(e){
        wPost('error',e.message);
      }
      break;
    case 'db-query': {
      const rc = sqlExec({
        sql: 'select * from mytable order by a',
        rowMode: 'array',
        returnValue: 'resultRows'
      });
      wPost('query-result',rc);
      break;
    }
    case 'vfs-remove':
      poolUtil.removeVfs().then(()=>wPost('vfs-removed'));
      break;
    case 'vfs-pause':
      poolUtil.pauseVfs();
      wPost('vfs-paused');
      break;
  }
};

const hasOpfs = ()=>{
  return globalThis.FileSystemHandle
    && globalThis.FileSystemDirectoryHandle
    && globalThis.FileSystemFileHandle
    && globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle
    && navigator?.storage?.getDirectory;
};
if( !hasOpfs() ){
  wPost('error',"OPFS not detected");
}else{
  globalThis.sqlite3InitModule().then(async function(sqlite3){
    S = sqlite3;
    capi = S.capi;
    wasm = S.wasm;
    log("sqlite3 version:",capi.sqlite3_libversion(),
        capi.sqlite3_sourceid());
    //return sqlite3.installOpfsSAHPoolVfs(sahPoolConfig).then(pu=>poolUtil=pu);
  }).then(()=>{
    wPost('initialized');
  });
}
