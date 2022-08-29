/*
  2022-08-29

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  A basic batch SQL running for sqlite3-api.js. This file must be run in
  main JS thread and sqlite3.js must have been loaded before it.
*/
'use strict';
(function(){
  const T = self.SqliteTestUtil;
  const toss = function(...args){throw new Error(args.join(' '))};
  const warn = console.warn.bind(console);

  const App = {
    e: {
      output: document.querySelector('#test-output'),
      selSql: document.querySelector('#sql-select'),
      btnRun: document.querySelector('#sql-run'),
      btnRunNext: document.querySelector('#sql-run-next'),
      btnRunRemaining: document.querySelector('#sql-run-remaining'),
      btnClear: document.querySelector('#output-clear'),
      btnReset: document.querySelector('#db-reset')
    },
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

    openDb: function(fn, unlinkFirst=true){
      if(this.db && this.db.ptr){
        toss("Already have an opened db.");
      }
      const capi = this.sqlite3.capi, wasm = capi.wasm;
      const stack = wasm.scopedAllocPush();
      let pDb = 0;
      try{
        /*if(unlinkFirst && fn && ':memory:'!==fn){
          capi.sqlite3_wasm_vfs_unlink(fn);
        }*/
        const oFlags = capi.SQLITE_OPEN_CREATE | capi.SQLITE_OPEN_READWRITE;
        const ppDb = wasm.scopedAllocPtr();
        const rc = capi.sqlite3_open_v2(fn, ppDb, oFlags, null);
        if(rc) toss("sqlite3_open_v2() failed with code",rc);
        pDb = wasm.getPtrValue(ppDb)
      }finally{
        wasm.scopedAllocPop(stack);
      }
      this.db = Object.create(null);
      this.db.filename = fn;
      this.db.ptr = pDb;
      this.logHtml("Opened db:",fn);
      return this.db.ptr;
    },

    closeDb: function(unlink=false){
      if(this.db && this.db.ptr){
        this.sqlite3.capi.sqlite3_close_v2(this.db.ptr);
        this.logHtml("Closed db",this.db.filename);
        if(unlink) capi.sqlite3_wasm_vfs_unlink(this.db.filename);
        this.db.ptr = this.db.filename = undefined;
      }
    },

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
        opt.value = opt.innerText = fn;
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

    /** Fetch ./fn and eval it as an SQL blob. */
    evalFile: async function(fn){
      const sql = await this.fetchFile(fn);
      const banner = "========================================";
      this.logHtml(banner,
                   "Running",fn,'('+sql.length,'bytes)...');
      const capi = this.sqlite3.capi, wasm = capi.wasm;
      let pStmt = 0, pSqlBegin;
      const stack = wasm.scopedAllocPush();
      const metrics = Object.create(null);
      metrics.prepTotal = metrics.stepTotal = 0;
      metrics.stmtCount = 0;
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
          pSqlBegin = wasm.alloc( sqlByteLen + 1/*SQL + NUL*/) || toss("alloc(",sqlByteLen,") failed");
          let pSql = pSqlBegin;
          const pSqlEnd = pSqlBegin + sqlByteLen;
          wasm.heap8().set(sql, pSql);
          wasm.setMemValue(pSql + sqlByteLen, 0);
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

    run: function(sqlite3){
      delete this.run;
      this.sqlite3 = sqlite3;
      const capi = sqlite3.capi, wasm = capi.wasm;
      this.logHtml("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
      this.logHtml("WASM heap size =",wasm.heap8().length);
      this.loadSqlList();
      const pDir = capi.sqlite3_web_persistent_dir();
      const dbFile = pDir ? pDir+"/speedtest.db" : ":memory:";
      if(!pDir){
        document.querySelector('#warn-opfs').remove();
      }
      this.openDb(dbFile, !!pDir);
      const who = this;
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
        const fn = who.db.filename;
        if(fn){
          who.closeDb(true);
          who.openDb(fn,true);
        }
      }, false);
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
      }, false);
    }/*run()*/
  }/*App*/;

  self.sqlite3TestModule.initSqlite3().then(function(theEmccModule){
    self._MODULE = theEmccModule /* this is only to facilitate testing from the console */;
    App.run(theEmccModule.sqlite3);
  });
})();
