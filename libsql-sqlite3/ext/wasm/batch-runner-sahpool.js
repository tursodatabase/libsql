/*
  2023-11-30

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic batch SQL runner for the SAHPool VFS. This file must be run in
  a worker thread. This is not a full-featured app, just a way to get some
  measurements for batch execution of SQL for the OPFS SAH Pool VFS.
*/
'use strict';

const wMsg = function(msgType,...args){
  postMessage({
    type: msgType,
    data: args
  });
};
const toss = function(...args){throw new Error(args.join(' '))};
const warn = (...args)=>{ wMsg('warn',...args); };
const error = (...args)=>{ wMsg('error',...args); };
const log = (...args)=>{ wMsg('stdout',...args); }
let sqlite3;
const urlParams = new URL(globalThis.location.href).searchParams;
const cacheSize = (()=>{
  if(urlParams.has('cachesize')) return +urlParams.get('cachesize');
  return 200;
})();


/** Throws if the given sqlite3 result code is not 0. */
const checkSqliteRc = (dbh,rc)=>{
  if(rc) toss("Prepare failed:",sqlite3.capi.sqlite3_errmsg(dbh));
};

const sqlToDrop = [
  "SELECT type,name FROM sqlite_schema ",
  "WHERE name NOT LIKE 'sqlite\\_%' escape '\\' ",
  "AND name NOT LIKE '\\_%' escape '\\'"
].join('');

const clearDbSqlite = function(db){
  // This would be SO much easier with the oo1 API, but we specifically want to
  // inject metrics we can't get via that API, and we cannot reliably (OPFS)
  // open the same DB twice to clear it using that API, so...
  const rc = sqlite3.wasm.exports.sqlite3_wasm_db_reset(db.handle);
  log("reset db rc =",rc,db.id, db.filename);
};

const App = {
  db: undefined,
  cache:Object.create(null),
  log: log,
  warn: warn,
  error: error,
  metrics: {
    fileCount: 0,
    runTimeMs: 0,
    prepareTimeMs: 0,
    stepTimeMs: 0,
    stmtCount: 0,
    strcpyMs: 0,
    sqlBytes: 0
  },
  fileList: undefined,
  execSql: async function(name,sql){
    const db = this.db;
    const banner = "========================================";
    this.log(banner,
             "Running",name,'('+sql.length,'bytes)');
    const capi = this.sqlite3.capi, wasm = this.sqlite3.wasm;
    let pStmt = 0, pSqlBegin;
    const metrics = db.metrics = Object.create(null);
    metrics.prepTotal = metrics.stepTotal = 0;
    metrics.stmtCount = 0;
    metrics.malloc = 0;
    metrics.strcpy = 0;
    if(this.gotErr){
      this.error("Cannot run SQL: error cleanup is pending.");
      return;
    }
    // Run this async so that the UI can be updated for the above header...
    const endRun = ()=>{
      metrics.evalSqlEnd = performance.now();
      metrics.evalTimeTotal = (metrics.evalSqlEnd - metrics.evalSqlStart);
      this.log("metrics:",JSON.stringify(metrics, undefined, ' '));
      this.log("prepare() count:",metrics.stmtCount);
      this.log("Time in prepare_v2():",metrics.prepTotal,"ms",
               "("+(metrics.prepTotal / metrics.stmtCount),"ms per prepare())");
      this.log("Time in step():",metrics.stepTotal,"ms",
               "("+(metrics.stepTotal / metrics.stmtCount),"ms per step())");
      this.log("Total runtime:",metrics.evalTimeTotal,"ms");
      this.log("Overhead (time - prep - step):",
               (metrics.evalTimeTotal - metrics.prepTotal - metrics.stepTotal)+"ms");
      this.log(banner,"End of",name);
      this.metrics.prepareTimeMs += metrics.prepTotal;
      this.metrics.stepTimeMs += metrics.stepTotal;
      this.metrics.stmtCount += metrics.stmtCount;
      this.metrics.strcpyMs += metrics.strcpy;
      this.metrics.sqlBytes += sql.length;
    };

    const runner = function(resolve, reject){
      ++this.metrics.fileCount;
      metrics.evalSqlStart = performance.now();
      const stack = wasm.scopedAllocPush();
      try {
        let t, rc;
        let sqlByteLen = sql.byteLength;
        const [ppStmt, pzTail] = wasm.scopedAllocPtr(2);
        t = performance.now();
        pSqlBegin = wasm.scopedAlloc( sqlByteLen + 1/*SQL + NUL*/) || toss("alloc(",sqlByteLen,") failed");
        metrics.malloc = performance.now() - t;
        metrics.byteLength = sqlByteLen;
        let pSql = pSqlBegin;
        const pSqlEnd = pSqlBegin + sqlByteLen;
        t = performance.now();
        wasm.heap8().set(sql, pSql);
        wasm.poke(pSql + sqlByteLen, 0);
        //log("SQL:",wasm.cstrToJs(pSql));
        metrics.strcpy = performance.now() - t;
        let breaker = 0;
        while(pSql && wasm.peek8(pSql)){
          wasm.pokePtr(ppStmt, 0);
          wasm.pokePtr(pzTail, 0);
          t = performance.now();
          rc = capi.sqlite3_prepare_v2(
            db.handle, pSql, sqlByteLen, ppStmt, pzTail
          );
          metrics.prepTotal += performance.now() - t;
          checkSqliteRc(db.handle, rc);
          pStmt = wasm.peekPtr(ppStmt);
          pSql = wasm.peekPtr(pzTail);
          sqlByteLen = pSqlEnd - pSql;
          if(!pStmt) continue/*empty statement*/;
          ++metrics.stmtCount;
          t = performance.now();
          rc = capi.sqlite3_step(pStmt);
          capi.sqlite3_finalize(pStmt);
          pStmt = 0;
          metrics.stepTotal += performance.now() - t;
          switch(rc){
            case capi.SQLITE_ROW:
            case capi.SQLITE_DONE: break;
            default: checkSqliteRc(db.handle, rc); toss("Not reached.");
          }
        }
        resolve(this);
      }catch(e){
        if(pStmt) capi.sqlite3_finalize(pStmt);
        this.gotErr = e;
        reject(e);
      }finally{
        capi.sqlite3_exec(db.handle,"rollback;",0,0,0);
        wasm.scopedAllocPop(stack);
      }
    }.bind(this);
    const p = new Promise(runner);
    return p.catch(
      (e)=>this.error("Error via execSql("+name+",...):",e.message)
    ).finally(()=>{
      endRun();
    });
  },

  /**
     Loads batch-runner.list and populates the selection list from
     it. Returns a promise which resolves to nothing in particular
     when it completes. Only intended to be run once at the start
     of the app.
  */
  loadSqlList: async function(){
    const infile = 'batch-runner.list';
    this.log("Loading list of SQL files:", infile);
    let txt;
    try{
      const r = await fetch(infile);
      if(404 === r.status){
        toss("Missing file '"+infile+"'.");
      }
      if(!r.ok) toss("Loading",infile,"failed:",r.statusText);
      txt = await r.text();
    }catch(e){
      this.error(e.message);
      throw e;
    }
    App.fileList = txt.split(/\n+/).filter(x=>!!x);
    this.log("Loaded",infile);
  },

  /** Fetch ./fn and return its contents as a Uint8Array. */
  fetchFile: async function(fn, cacheIt=false){
    if(cacheIt && this.cache[fn]) return this.cache[fn];
    this.log("Fetching",fn,"...");
    let sql;
    try {
      const r = await fetch(fn);
      if(!r.ok) toss("Fetch failed:",r.statusText);
      sql = new Uint8Array(await r.arrayBuffer());
    }catch(e){
      this.error(e.message);
      throw e;
    }
    this.log("Fetched",sql.length,"bytes from",fn);
    if(cacheIt) this.cache[fn] = sql;
    return sql;
  }/*fetchFile()*/,

  /**
     Converts this.metrics() to a form which is suitable for easy conversion to
     CSV. It returns an array of arrays. The first sub-array is the column names.
     The 2nd and subsequent are the values, one per test file (only the most recent
     metrics are kept for any given file).
  */
  metricsToArrays: function(){
    const rc = [];
    Object.keys(this.dbs).sort().forEach((k)=>{
      const d = this.dbs[k];
      const m = d.metrics;
      delete m.evalSqlStart;
      delete m.evalSqlEnd;
      const mk = Object.keys(m).sort();
      if(!rc.length){
        rc.push(['db', ...mk]);
      }
      const row = [k.split('/').pop()/*remove dir prefix from filename*/];
      rc.push(row);
      row.push(...mk.map((kk)=>m[kk]));
    });
    return rc;
  },

  metricsToBlob: function(colSeparator='\t'){
    const ar = [], ma = this.metricsToArrays();
    if(!ma.length){
      this.error("Metrics are empty. Run something.");
      return;
    }
    ma.forEach(function(row){
      ar.push(row.join(colSeparator),'\n');
    });
    return new Blob(ar);
  },

  /**
     Fetch file fn and eval it as an SQL blob. This is an async
     operation and returns a Promise which resolves to this
     object on success.
  */
  evalFile: async function(fn){
    const sql = await this.fetchFile(fn);
    return this.execSql(fn,sql);
  }/*evalFile()*/,

  /**
     Fetches the handle of the db associated with
     this.e.selImpl.value, opening it if needed.
  */
  initDb: function(){
    const capi = this.sqlite3.capi, wasm = this.sqlite3.wasm;
    const stack = wasm.scopedAllocPush();
    let pDb = 0;
    const d = Object.create(null);
    d.filename = "/batch.db";
    try{
      const oFlags = capi.SQLITE_OPEN_CREATE | capi.SQLITE_OPEN_READWRITE;
      const ppDb = wasm.scopedAllocPtr();
      const rc = capi.sqlite3_open_v2(d.filename, ppDb, oFlags, this.PoolUtil.vfsName);
      pDb = wasm.peekPtr(ppDb)
      if(rc) toss("sqlite3_open_v2() failed with code",rc);
      capi.sqlite3_exec(pDb, "PRAGMA cache_size="+cacheSize, 0, 0, 0);
      this.log("cache_size =",cacheSize);
    }catch(e){
      if(pDb) capi.sqlite3_close_v2(pDb);
      throw e;
    }finally{
      wasm.scopedAllocPop(stack);
    }
    d.handle = pDb;
    this.log("Opened db:",d.filename,'@',d.handle);
    return d;
  },

  closeDb: function(){
    if(this.db.handle){
      this.sqlite3.capi.sqlite3_close_v2(this.db.handle);
      this.db.handle = undefined;
    }
  },

  run: async function(sqlite3){
    delete this.run;
    this.sqlite3 = sqlite3;
    const capi = sqlite3.capi, wasm = sqlite3.wasm;
    this.log("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
    this.log("WASM heap size =",wasm.heap8().length);
    let timeStart;
    sqlite3.installOpfsSAHPoolVfs({
      clearOnInit: true, initialCapacity: 4,
      name: 'batch-sahpool',
      verbosity: 2
    }).then(PoolUtil=>{
      App.PoolUtil = PoolUtil;
      App.db = App.initDb();
    })
      .then(async ()=>this.loadSqlList())
      .then(async ()=>{
        timeStart = performance.now();
        for(let i = 0; i < App.fileList.length; ++i){
          const fn = App.fileList[i];
          await App.evalFile(fn);
          if(App.gotErr) throw App.gotErr;
        }
      })
      .then(()=>{
        App.metrics.runTimeMs = performance.now() - timeStart;
        App.log("total metrics:",JSON.stringify(App.metrics, undefined, ' '));
        App.log("Reload the page to run this again.");
        App.closeDb();
        App.PoolUtil.removeVfs();
      })
      .catch(e=>this.error("ERROR:",e));
  }/*run()*/
}/*App*/;

let sqlite3Js = 'sqlite3.js';
if(urlParams.has('sqlite3.dir')){
  sqlite3Js = urlParams.get('sqlite3.dir') + '/' + sqlite3Js;
}
importScripts(sqlite3Js);
globalThis.sqlite3InitModule().then(async function(sqlite3_){
  log("Done initializing. Running batch runner...");
  sqlite3 = sqlite3_;
  App.run(sqlite3_);
});
