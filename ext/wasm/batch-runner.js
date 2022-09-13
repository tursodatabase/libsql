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

  const App = {
    e: {
      output: document.querySelector('#test-output'),
      selSql: document.querySelector('#sql-select'),
      btnRun: document.querySelector('#sql-run'),
      btnRunNext: document.querySelector('#sql-run-next'),
      btnRunRemaining: document.querySelector('#sql-run-remaining'),
      btnExportMetrics: document.querySelector('#export-metrics'),
      btnClear: document.querySelector('#output-clear'),
      btnReset: document.querySelector('#db-reset'),
      cbReverseLog: document.querySelector('#cb-reverse-log-order')
    },
    db: Object.create(null),
    cache:{},
    metrics:{
      /**
         Map of sql-file to timing metrics. We currently only store
         the most recent run of each file, but we really should store
         all runs so that we can average out certain values which vary
         significantly across runs. e.g. a mandelbrot-generating query
         will have a wide range of runtimes when run 10 times in a
         row.         
      */
    },
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

    openDb: function(fn, unlinkFirst=true){
      if(this.db.ptr){
        toss("Already have an opened db.");
      }
      const capi = this.sqlite3.capi, wasm = capi.wasm;
      const stack = wasm.scopedAllocPush();
      let pDb = 0;
      try{
        if(unlinkFirst && fn){
          if(':'!==fn[0]) capi.wasm.sqlite3_wasm_vfs_unlink(fn);
          this.clearStorage();
        }
        const oFlags = capi.SQLITE_OPEN_CREATE | capi.SQLITE_OPEN_READWRITE;
        const ppDb = wasm.scopedAllocPtr();
        const rc = capi.sqlite3_open_v2(fn, ppDb, oFlags, null);
        pDb = wasm.getPtrValue(ppDb)
        if(rc){
          if(pDb) capi.sqlite3_close_v2(pDb);
          toss("sqlite3_open_v2() failed with code",rc);
        }
      }finally{
        wasm.scopedAllocPop(stack);
      }
      this.db.filename = fn;
      this.db.ptr = pDb;
      this.logHtml("Opened db:",fn);
      return this.db.ptr;
    },

    closeDb: function(unlink=false){
      if(this.db.ptr){
        this.sqlite3.capi.sqlite3_close_v2(this.db.ptr);
        this.logHtml("Closed db",this.db.filename);
        if(unlink){
          capi.wasm.sqlite3_wasm_vfs_unlink(this.db.filename);
          this.clearStorage();
        }
        this.db.ptr = this.db.filename = undefined;
      }
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
        const warning = document.querySelector('#warn-list');
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

    /** Throws if the given sqlite3 result code is not 0. */
    checkRc: function(rc){
      if(this.db.ptr && rc){
        toss("Prepare failed:",this.sqlite3.capi.sqlite3_errmsg(this.db.ptr));
      }
    },

    /** Disable or enable certain UI controls. */
    blockControls: function(disable){
      document.querySelectorAll('.disable-during-eval').forEach((e)=>e.disabled = disable);
    },

    /**
       Converts this.metrics() to a form which is suitable for easy conversion to
       CSV. It returns an array of arrays. The first sub-array is the column names.
       The 2nd and subsequent are the values, one per test file (only the most recent
       metrics are kept for any given file).
    */
    metricsToArrays: function(){
      const rc = [];
      Object.keys(this.metrics).sort().forEach((k)=>{
        const m = this.metrics[k];
        delete m.evalFileStart;
        delete m.evalFileEnd;
        const mk = Object.keys(m).sort();
        if(!rc.length){
          rc.push(['file', ...mk]);
        }
        const row = [k.split('/').pop()/*remove dir prefix from filename*/];
        rc.push(row);
        mk.forEach((kk)=>row.push(m[kk]));
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
      const banner = "========================================";
      this.logHtml(banner,
                   "Running",fn,'('+sql.length,'bytes)...');
      const capi = this.sqlite3.capi, wasm = capi.wasm;
      let pStmt = 0, pSqlBegin;
      const stack = wasm.scopedAllocPush();
      const metrics = this.metrics[fn] = Object.create(null);
      metrics.prepTotal = metrics.stepTotal = 0;
      metrics.stmtCount = 0;
      metrics.malloc = 0;
      metrics.strcpy = 0;
      this.blockControls(true);
      if(this.gotErr){
        this.logErr("Cannot run ["+fn+"]: error cleanup is pending.");
        return;
      }
      // Run this async so that the UI can be updated for the above header...
      const ff = function(resolve, reject){
        metrics.evalFileStart = performance.now();
        try {
          let t;
          let sqlByteLen = sql.byteLength;
          const [ppStmt, pzTail] = wasm.scopedAllocPtr(2);
          t = performance.now();
          pSqlBegin = wasm.alloc( sqlByteLen + 1/*SQL + NUL*/) || toss("alloc(",sqlByteLen,") failed");
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
              this.db.ptr, pSql, sqlByteLen, 0, ppStmt, pzTail
            );
            metrics.prepTotal += performance.now() - t;
            this.checkRc(rc);
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
                default: this.checkRc(rc); toss("Not reached.");
            }
          }
        }catch(e){
          if(pStmt) capi.sqlite3_finalize(pStmt);
          this.gotErr = e;
          //throw e;
          reject(e);
          return;
        }finally{
          wasm.dealloc(pSqlBegin);
          wasm.scopedAllocPop(stack);
          this.blockControls(false);
        }
        metrics.evalFileEnd = performance.now();
        metrics.evalTimeTotal = (metrics.evalFileEnd - metrics.evalFileStart);
        this.logHtml("Metrics:");//,JSON.stringify(metrics, undefined, ' '));
        this.logHtml("prepare() count:",metrics.stmtCount);
        this.logHtml("Time in prepare_v2():",metrics.prepTotal,"ms",
                     "("+(metrics.prepTotal / metrics.stmtCount),"ms per prepare())");
        this.logHtml("Time in step():",metrics.stepTotal,"ms",
                     "("+(metrics.stepTotal / metrics.stmtCount),"ms per step())");
        this.logHtml("Total runtime:",metrics.evalTimeTotal,"ms");
        this.logHtml("Overhead (time - prep - step):",
                     (metrics.evalTimeTotal - metrics.prepTotal - metrics.stepTotal)+"ms");
        this.logHtml(banner,"End of",fn);
        resolve(this);
      }.bind(this);
      let p;
      if(1){
        p = new Promise(function(res,rej){
          setTimeout(()=>ff(res, rej), 50)/*give UI a chance to output the "running" banner*/;
        });
      }else{
        p = new Promise(ff);
      }
      return p.catch((e)=>this.logErr("Error via evalFile("+fn+"):",e.message));
    }/*evalFile()*/,

    clearStorage: function(){
      const sz = sqlite3.capi.sqlite3_web_kvvfs_size();
      const n = sqlite3.capi.sqlite3_web_kvvfs_clear(this.db.filename || '');
      this.logHtml("Cleared kvvfs local/sessionStorage:",
                   n,"entries totaling approximately",sz,"bytes.");
    },

    resetDb: function(){
      if(this.db.ptr){
        const fn = this.db.filename;
        this.closeDb(true);
        this.openDb(fn,false);
      }
    },

    run: function(sqlite3){
      delete this.run;
      this.sqlite3 = sqlite3;
      const capi = sqlite3.capi, wasm = capi.wasm;
      this.logHtml("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
      this.logHtml("WASM heap size =",wasm.heap8().length);
      this.loadSqlList();
      const pDir = 1 ? '' : capi.sqlite3_web_persistent_dir();
      const dbFile = pDir ? pDir+"/speedtest.db" : (
        sqlite3.capi.sqlite3_vfs_find('kvvfs') ? 'local' : ':memory:'
      );
      this.clearStorage();
      if(pDir){
        logHtml("Using persistent storage:",dbFile);
      }else{
        document.querySelector('#warn-opfs').remove();
      }
      this.openDb(dbFile, !!pDir);
      const who = this;
      const eReverseLogNotice = document.querySelector('#reverse-log-notice');
      if(this.e.cbReverseLog.checked){
        eReverseLogNotice.classList.remove('hidden');
        this.e.output.classList.add('reverse');
      }
      this.e.cbReverseLog.addEventListener('change', function(){
        if(this.checked){
          who.e.output.classList.add('reverse');
          eReverseLogNotice.classList.remove('hidden');
        }else{
          who.e.output.classList.remove('reverse');
          eReverseLogNotice.classList.add('hidden');
        }
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
        who.resetDb();
      }, false);
      this.e.btnExportMetrics.addEventListener('click', function(){
        who.logHtml2('warning',"Triggering download of metrics CSV. Check your downloads folder.");
        who.downloadMetrics();
        //const m = who.metricsToArrays();
        //console.log("Metrics:",who.metrics, m);
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

  self.sqlite3TestModule.initSqlite3().then(function(theEmccModule){
    self._MODULE = theEmccModule /* this is only to facilitate testing from the console */;
    sqlite3 = theEmccModule.sqlite3;
    App.run(theEmccModule.sqlite3);
  });
})();
