/*
  2022-08-29

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic batch SQL runner for sqlite3-api.js. This file must be run in
  main JS thread and sqlite3.js must have been loaded before it.
*/
'use strict';
(function(){
  const toss = function(...args){throw new Error(args.join(' '))};
  const warn = console.warn.bind(console);
  let sqlite3;
  const urlParams = new URL(self.location.href).searchParams;
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
  
  const clearDbWebSQL = function(db){
    db.handle.transaction(function(tx){
      const onErr = (e)=>console.error(e);
      const callback = function(tx, result){
        const rows = result.rows;
        let i, n;
        i = n = rows.length;
        while(i--){
          const row = rows.item(i);
          const name = JSON.stringify(row.name);
          const type = row.type;
          switch(type){
              case 'index': case 'table':
              case 'trigger': case 'view': {
                const sql2 = 'DROP '+type+' '+name;
                tx.executeSql(sql2, [], ()=>{}, onErr);
                break;
              }
              default:
                warn("Unhandled db entry type:",type,'name =',name);
                break;
          }
        }
      };
      tx.executeSql(sqlToDrop, [], callback, onErr);
      db.handle.changeVersion(db.handle.version, "", ()=>{}, onErr, ()=>{});
    });
  };

  const clearDbSqlite = function(db){
    // This would be SO much easier with the oo1 API, but we specifically want to
    // inject metrics we can't get via that API, and we cannot reliably (OPFS)
    // open the same DB twice to clear it using that API, so...
    const rc = sqlite3.wasm.exports.sqlite3_wasm_db_reset(db.handle);
    App.logHtml("reset db rc =",rc,db.id, db.filename);
  };

  
  const E = (s)=>document.querySelector(s);
  const App = {
    e: {
      output: E('#test-output'),
      selSql: E('#sql-select'),
      btnRun: E('#sql-run'),
      btnRunNext: E('#sql-run-next'),
      btnRunRemaining: E('#sql-run-remaining'),
      btnExportMetrics: E('#export-metrics'),
      btnClear: E('#output-clear'),
      btnReset: E('#db-reset'),
      cbReverseLog: E('#cb-reverse-log-order'),
      selImpl: E('#select-impl'),
      fsToolbar: E('#toolbar')
    },
    db: Object.create(null),
    dbs: Object.create(null),
    cache:{},
    log: console.log.bind(console),
    warn: console.warn.bind(console),
    cls: function(){this.e.output.innerHTML = ''},
    logHtml2: function(cssClass,...args){
      const ln = document.createElement('div');
      if(cssClass) ln.classList.add(cssClass);
      ln.append(document.createTextNode(args.join(' ')));
      this.e.output.append(ln);
      //this.e.output.lastElementChild.scrollIntoViewIfNeeded();
    },
    logHtml: function(...args){
      console.log(...args);
      if(1) this.logHtml2('', ...args);
    },
    logErr: function(...args){
      console.error(...args);
      if(1) this.logHtml2('error', ...args);
    },

    execSql: async function(name,sql){
      const db = this.getSelectedDb();
      const banner = "========================================";
      this.logHtml(banner,
                   "Running",name,'('+sql.length,'bytes) using',db.id);
      const capi = this.sqlite3.capi, wasm = this.sqlite3.wasm;
      let pStmt = 0, pSqlBegin;
      const stack = wasm.scopedAllocPush();
      const metrics = db.metrics = Object.create(null);
      metrics.prepTotal = metrics.stepTotal = 0;
      metrics.stmtCount = 0;
      metrics.malloc = 0;
      metrics.strcpy = 0;
      this.blockControls(true);
      if(this.gotErr){
        this.logErr("Cannot run SQL: error cleanup is pending.");
        return;
      }
      // Run this async so that the UI can be updated for the above header...
      const endRun = ()=>{
        metrics.evalSqlEnd = performance.now();
        metrics.evalTimeTotal = (metrics.evalSqlEnd - metrics.evalSqlStart);
        this.logHtml(db.id,"metrics:",JSON.stringify(metrics, undefined, ' '));
        this.logHtml("prepare() count:",metrics.stmtCount);
        this.logHtml("Time in prepare_v2():",metrics.prepTotal,"ms",
                     "("+(metrics.prepTotal / metrics.stmtCount),"ms per prepare())");
        this.logHtml("Time in step():",metrics.stepTotal,"ms",
                     "("+(metrics.stepTotal / metrics.stmtCount),"ms per step())");
        this.logHtml("Total runtime:",metrics.evalTimeTotal,"ms");
        this.logHtml("Overhead (time - prep - step):",
                     (metrics.evalTimeTotal - metrics.prepTotal - metrics.stepTotal)+"ms");
        this.logHtml(banner,"End of",name);
      };

      let runner;
      if('websql'===db.id){
        const who = this;
        runner = function(resolve, reject){
          /* WebSQL cannot execute multiple statements, nor can it execute SQL without
             an explicit transaction. Thus we have to do some fragile surgery on the
             input SQL. Since we're only expecting carefully curated inputs, the hope is
             that this will suffice. PS: it also can't run most SQL functions, e.g. even
             instr() results in "not authorized". */
          if('string'!==typeof sql){ // assume TypedArray
            sql = new TextDecoder().decode(sql);
          }
          sql = sql.replace(/-- [^\n]+\n/g,''); // comment lines interfere with our split()
          const sqls = sql.split(/;+\n/);
          const rxBegin = /^BEGIN/i, rxCommit = /^COMMIT/i;
          try {
            const nextSql = ()=>{
              let x = sqls.shift();
              while(sqls.length && !x) x = sqls.shift();
              return x && x.trim();
            };
            const who = this;
            const transaction = function(tx){
              try {
                let s;
                /* Try to approximate the spirit of the input scripts
                   by running batches bound by BEGIN/COMMIT statements. */
                for(s = nextSql(); !!s; s = nextSql()){
                  if(rxBegin.test(s)) continue;
                  else if(rxCommit.test(s)) break;
                  //console.log("websql sql again",sqls.length, s);
                  ++metrics.stmtCount;
                  const t = performance.now();
                  tx.executeSql(s,[], ()=>{}, (t,e)=>{
                    console.error("WebSQL error",e,"SQL =",s);
                    who.logErr(e.message);
                    //throw e;
                    return false;
                  });
                  metrics.stepTotal += performance.now() - t;
                }
              }catch(e){
                who.logErr("transaction():",e.message);
                throw e;
              }
            };
            const n = sqls.length;
            const nextBatch = function(){
              if(sqls.length){
                console.log("websql sqls.length",sqls.length,'of',n);
                db.handle.transaction(transaction, (e)=>{
                  who.logErr("Ignoring and contiuing:",e.message)
                  //reject(e);
                  return false;
                }, nextBatch);
              }else{
                resolve(who);
              }
            };
            metrics.evalSqlStart = performance.now();
            nextBatch();
          }catch(e){
            //this.gotErr = e;
            console.error("websql error:",e);
            who.logErr(e.message);
            //reject(e);
          }
        }.bind(this);
      }else{/*sqlite3 db...*/
        runner = function(resolve, reject){
          metrics.evalSqlStart = performance.now();
          try {
            let t;
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
            wasm.setMemValue(pSql + sqlByteLen, 0);
            metrics.strcpy = performance.now() - t;
            let breaker = 0;
            while(pSql && wasm.getMemValue(pSql,'i8')){
              wasm.setPtrValue(ppStmt, 0);
              wasm.setPtrValue(pzTail, 0);
              t = performance.now();
              let rc = capi.sqlite3_prepare_v3(
                db.handle, pSql, sqlByteLen, 0, ppStmt, pzTail
              );
              metrics.prepTotal += performance.now() - t;
              checkSqliteRc(db.handle, rc);
              pStmt = wasm.getPtrValue(ppStmt);
              pSql = wasm.getPtrValue(pzTail);
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
            //this.gotErr = e;
            reject(e);
          }finally{
            capi.sqlite3_exec(db.handle,"rollback;",0,0,0);
            wasm.scopedAllocPop(stack);
          }
        }.bind(this);
      }
      let p;
      if(1){
        p = new Promise(function(res,rej){
          setTimeout(()=>runner(res, rej), 50)/*give UI a chance to output the "running" banner*/;
        });
      }else{
        p = new Promise(runner);
      }
      return p.catch(
        (e)=>this.logErr("Error via execSql("+name+",...):",e.message)
      ).finally(()=>{
        endRun();
        this.blockControls(false);
      });
    },
    
    clearDb: function(){
      const db = this.getSelectedDb();
      if('websql'===db.id){
        this.logErr("TODO: clear websql db.");
        return;
      }
      if(!db.handle) return;
      const capi = this.sqlite3, wasm = this.sqlite3.wasm;
      //const scope = wasm.scopedAllocPush(
      this.logErr("TODO: clear db");
    },
    
    /**
       Loads batch-runner.list and populates the selection list from
       it. Returns a promise which resolves to nothing in particular
       when it completes. Only intended to be run once at the start
       of the app.
     */
    loadSqlList: async function(){
      const sel = this.e.selSql;
      sel.innerHTML = '';
      this.blockControls(true);
      const infile = 'batch-runner.list';
      this.logHtml("Loading list of SQL files:", infile);
      let txt;
      try{
        const r = await fetch(infile);
        if(404 === r.status){
          toss("Missing file '"+infile+"'.");
        }
        if(!r.ok) toss("Loading",infile,"failed:",r.statusText);
        txt = await r.text();
        const warning = E('#warn-list');
        if(warning) warning.remove();
      }catch(e){
        this.logErr(e.message);
        throw e;
      }finally{
        this.blockControls(false);
      }
      const list = txt.split(/\n+/);
      let opt;
      if(0){
        opt = document.createElement('option');
        opt.innerText = "Select file to evaluate...";
        opt.value = '';
        opt.disabled = true;
        opt.selected = true;
        sel.appendChild(opt);
      }
      list.forEach(function(fn){
        if(!fn) return;
        opt = document.createElement('option');
        opt.value = fn;
        opt.innerText = fn.split('/').pop();
        sel.appendChild(opt);
      });
      this.logHtml("Loaded",infile);
    },

    /** Fetch ./fn and return its contents as a Uint8Array. */
    fetchFile: async function(fn, cacheIt=false){
      if(cacheIt && this.cache[fn]) return this.cache[fn];
      this.logHtml("Fetching",fn,"...");
      let sql;
      try {
        const r = await fetch(fn);
        if(!r.ok) toss("Fetch failed:",r.statusText);
        sql = new Uint8Array(await r.arrayBuffer());
      }catch(e){
        this.logErr(e.message);
        throw e;
      }
      this.logHtml("Fetched",sql.length,"bytes from",fn);
      if(cacheIt) this.cache[fn] = sql;
      return sql;
    }/*fetchFile()*/,

    /** Disable or enable certain UI controls. */
    blockControls: function(disable){
      //document.querySelectorAll('.disable-during-eval').forEach((e)=>e.disabled = disable);
      this.e.fsToolbar.disabled = disable;
    },

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
        this.logErr("Metrics are empty. Run something.");
        return;
      }
      ma.forEach(function(row){
        ar.push(row.join(colSeparator),'\n');
      });
      return new Blob(ar);
    },
    
    downloadMetrics: function(){
      const b = this.metricsToBlob();
      if(!b) return;
      const url = URL.createObjectURL(b);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'batch-runner-js-'+((new Date().getTime()/1000) | 0)+'.csv';
      this.logHtml("Triggering download of",a.download);
      document.body.appendChild(a);
      a.click();
      setTimeout(()=>{
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
      }, 500);
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
       Clears all DB tables in all _opened_ databases. Because of
       disparities between backends, we cannot simply "unlink" the
       databases to clean them up.
    */
    clearStorage: function(onlySelectedDb=false){
      const list = onlySelectedDb
            ? [('boolean'===typeof onlySelectedDb)
                ? this.dbs[this.e.selImpl.value]
                : onlySelectedDb]
            : Object.values(this.dbs);
      for(let db of list){
        if(db && db.handle){
          this.logHtml("Clearing db",db.id);
          db.clear();
        }
      }
    },

    /**
       Fetches the handle of the db associated with
       this.e.selImpl.value, opening it if needed.
    */
    getSelectedDb: function(){
      if(!this.dbs.memdb){
        for(let opt of this.e.selImpl.options){
          const d = this.dbs[opt.value] = Object.create(null);
          d.id = opt.value;
          switch(d.id){
              case 'virtualfs':
                d.filename = 'file:/virtualfs.sqlite3?vfs=unix-none';
                break;
              case 'memdb':
                d.filename = ':memory:';
                break;
              case 'wasmfs-opfs':
                d.filename = 'file:'+(
                  this.sqlite3.capi.sqlite3_wasmfs_opfs_dir()
                )+'/wasmfs-opfs.sqlite3b';
                break;
              case 'websql':
                d.filename = 'websql.db';
                break;
              default:
                this.logErr("Unhandled db selection option (see details in the console).",opt);
                toss("Unhandled db init option");
          }
        }
      }/*first-time init*/
      const dbId = this.e.selImpl.value;
      const d = this.dbs[dbId];
      if(d.handle) return d;
      if('websql' === dbId){
        d.handle = self.openDatabase('batch-runner', '0.1', 'foo', 1024 * 1024 * 50);
        d.clear = ()=>clearDbWebSQL(d);
        d.handle.transaction(function(tx){
          tx.executeSql("PRAGMA cache_size="+cacheSize);
          App.logHtml(dbId,"cache_size =",cacheSize);
        });
      }else{
        const capi = this.sqlite3.capi, wasm = this.sqlite3.wasm;
        const stack = wasm.scopedAllocPush();
        let pDb = 0;
        try{
          const oFlags = capi.SQLITE_OPEN_CREATE | capi.SQLITE_OPEN_READWRITE;
          const ppDb = wasm.scopedAllocPtr();
          const rc = capi.sqlite3_open_v2(d.filename, ppDb, oFlags, null);
          pDb = wasm.getPtrValue(ppDb)
          if(rc) toss("sqlite3_open_v2() failed with code",rc);
          capi.sqlite3_exec(pDb, "PRAGMA cache_size="+cacheSize, 0, 0, 0);
          this.logHtml(dbId,"cache_size =",cacheSize);
        }catch(e){
          if(pDb) capi.sqlite3_close_v2(pDb);
        }finally{
          wasm.scopedAllocPop(stack);
        }
        d.handle = pDb;
        d.clear = ()=>clearDbSqlite(d);
      }
      d.clear();
      this.logHtml("Opened db:",dbId,d.filename);
      console.log("db =",d);
      return d;
    },

    run: function(sqlite3){
      delete this.run;
      this.sqlite3 = sqlite3;
      const capi = sqlite3.capi, wasm = sqlite3.wasm;
      this.logHtml("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
      this.logHtml("WASM heap size =",wasm.heap8().length);
      this.loadSqlList();
      if(capi.sqlite3_wasmfs_opfs_dir()){
        E('#warn-opfs').classList.remove('hidden');
      }else{
        E('#warn-opfs').remove();
        E('option[value=wasmfs-opfs]').disabled = true;
      }
      if('function' === typeof self.openDatabase){
        E('#warn-websql').classList.remove('hidden');
      }else{
        E('option[value=websql]').disabled = true;
        E('#warn-websql').remove();
      }
      const who = this;
      if(this.e.cbReverseLog.checked){
        this.e.output.classList.add('reverse');
      }
      this.e.cbReverseLog.addEventListener('change', function(){
        who.e.output.classList[this.checked ? 'add' : 'remove']('reverse');
      }, false);
      this.e.btnClear.addEventListener('click', ()=>this.cls(), false);
      this.e.btnRun.addEventListener('click', function(){
        if(!who.e.selSql.value) return;
        who.evalFile(who.e.selSql.value);
      }, false);
      this.e.btnRunNext.addEventListener('click', function(){
        ++who.e.selSql.selectedIndex;
        if(!who.e.selSql.value) return;
        who.evalFile(who.e.selSql.value);
      }, false);
      this.e.btnReset.addEventListener('click', function(){
        who.clearStorage(true);
      }, false);
      this.e.btnExportMetrics.addEventListener('click', function(){
        who.logHtml2('warning',"Triggering download of metrics CSV. Check your downloads folder.");
        who.downloadMetrics();
        //const m = who.metricsToArrays();
        //console.log("Metrics:",who.metrics, m);
      });
      this.e.selImpl.addEventListener('change', function(){
        who.getSelectedDb();
      });
      this.e.btnRunRemaining.addEventListener('click', async function(){
        let v = who.e.selSql.value;
        const timeStart = performance.now();
        while(v){
          await who.evalFile(v);
          if(who.gotError){
            who.logErr("Error handling script",v,":",who.gotError.message);
            break;
          }
          ++who.e.selSql.selectedIndex;
          v = who.e.selSql.value;
        }
        const timeTotal = performance.now() - timeStart;
        who.logHtml("Run-remaining time:",timeTotal,"ms ("+(timeTotal/1000/60)+" minute(s))");
        who.clearStorage();
      }, false);
    }/*run()*/
  }/*App*/;

  self.sqlite3TestModule.initSqlite3().then(function(sqlite3_){
    sqlite3 = sqlite3_;
    self.App = App /* only to facilitate dev console access */;
    App.run(sqlite3);
  });
})();
