/*
  2022-09-17

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A testing ground for the OPFS VFS.
*/
'use strict';
const tryOpfsVfs = async function(sqlite3){
  const toss = function(...args){throw new Error(args.join(' '))};
  const logPrefix = "OPFS tester:";
  const log = (...args)=>console.log(logPrefix,...args);
  const warn =  (...args)=>console.warn(logPrefix,...args);
  const error =  (...args)=>console.error(logPrefix,...args);
  const opfs = sqlite3.opfs;
  log("tryOpfsVfs()");
  if(!sqlite3.opfs){
    const e = new Error("OPFS is not available.");
    error(e);
    throw e;
  }
  const capi = sqlite3.capi;
  const pVfs = capi.sqlite3_vfs_find("opfs") || toss("Missing 'opfs' VFS.");
  const oVfs = new capi.sqlite3_vfs(pVfs);
  log("OPFS VFS:",pVfs, oVfs);

  const wait = async (ms)=>{
    return new Promise((resolve)=>setTimeout(resolve, ms));
  };

  const urlArgs = new URL(self.location.href).searchParams;
  const dbFile = "my-persistent.db";
  if(urlArgs.has('delete')) sqlite3.opfs.unlink(dbFile);

  const db = new sqlite3.oo1.OpfsDb(dbFile,'ct');
  log("db file:",db.filename);
  try{
    if(opfs.entryExists(dbFile)){
      let n = db.selectValue("select count(*) from sqlite_schema");
      log("Persistent data found. sqlite_schema entry count =",n);
    }
    db.transaction((db)=>{
      db.exec({
        sql:[
          "create table if not exists t(a);",
          "insert into t(a) values(?),(?),(?);",
        ],
        bind: [performance.now() | 0,
               (performance.now() |0) / 2,
               (performance.now() |0) / 4]
      });
    });
    log("count(*) from t =",db.selectValue("select count(*) from t"));

    // Some sanity checks of the opfs utility functions...
    const testDir = '/sqlite3-opfs-'+opfs.randomFilename(12);
    const aDir = testDir+'/test/dir';
    await opfs.mkdir(aDir) || toss("mkdir failed");
    await opfs.mkdir(aDir) || toss("mkdir must pass if the dir exists");
    await opfs.unlink(testDir+'/test') && toss("delete 1 should have failed (dir not empty)");
    //await opfs.entryExists(testDir)
    await opfs.unlink(testDir+'/test/dir') || toss("delete 2 failed");
    await opfs.unlink(testDir+'/test/dir') && toss("delete 2b should have failed (dir already deleted)");
    await opfs.unlink(testDir, true) || toss("delete 3 failed");
    await opfs.entryExists(testDir) && toss("entryExists(",testDir,") should have failed");
  }finally{
    db.close();
  }

  log("Done!");
}/*tryOpfsVfs()*/;

importScripts('jswasm/sqlite3.js');
self.sqlite3InitModule()
  .then((sqlite3)=>tryOpfsVfs(sqlite3))
  .catch((e)=>{
    console.error("Error initializing module:",e);
  });
