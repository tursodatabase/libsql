/*
  2025-01-31

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file is part of testing the OPFS SAHPool VFS's computeDigest()
  fix.  See ./digest.html for the details.
*/
const clog = console.log.bind(console);
const wPost = (type,...args)=>postMessage({type, payload:args});
const log = (...args)=>{
  clog("Worker:",...args);
  wPost('log',...args);
}

const hasOpfs = ()=>{
  return globalThis.FileSystemHandle
    && globalThis.FileSystemDirectoryHandle
    && globalThis.FileSystemFileHandle
    && globalThis.FileSystemFileHandle.prototype.createSyncAccessHandle
    && navigator?.storage?.getDirectory;
};
if( !hasOpfs() ){
  wPost('error',"OPFS not detected");
  throw new Error("OPFS not detected");
}

clog("Importing sqlite3...");
const searchParams = new URL(self.location.href).searchParams;
importScripts(searchParams.get('sqlite3.dir') + '/sqlite3.js');

const runTests = function(sqlite3, poolUtil){
  const fname = '/my.db';
  let db = new poolUtil.OpfsSAHPoolDb(fname);
  let n = (new Date()).valueOf();
  try {
    db.exec([
      "create table if not exists t(a);"
    ]);
    db.exec({
      sql: "insert into t(a) values(?)",
      bind: n++
    });
    log(fname,"record count: ",db.selectValue("select count(*) from t"));
  }finally{
    db.close();
  }

  db = new poolUtil.OpfsSAHPoolDb(fname);
  try {
    db.exec({
      sql: "insert into t(a) values(?)",
      bind: n++
    });
    log(fname,"record count: ",db.selectValue("select count(*) from t"));
  }finally{
    db.close();
  }

  const fname2 = '/my2.db';
  db = new poolUtil.OpfsSAHPoolDb(fname2);
  try {
    db.exec([
      "create table if not exists t(a);"
    ]);
    db.exec({
      sql: "insert into t(a) values(?)",
      bind: n++
    });
    log(fname2,"record count: ",db.selectValue("select count(*) from t"));
  }finally{
    db.close();
  }
};

globalThis.sqlite3InitModule().then(async function(sqlite3){
  log("sqlite3 version:",sqlite3.version);
  const sahPoolConfig = {
    name: 'opfs-sahpool-digest',
    clearOnInit: false,
    initialCapacity: 6
  };
  return sqlite3.installOpfsSAHPoolVfs(sahPoolConfig).then(poolUtil=>{
    log('vfs acquired');
    runTests(sqlite3, poolUtil);
  });
});
