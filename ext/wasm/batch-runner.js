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
  const debug = console.debug.bind(console);

  const App = {
    e: {
      output: document.querySelector('#test-output'),
      selSql: document.querySelector('#sql-select'),
      btnRun: document.querySelector('#sql-run'),
      btnClear: document.querySelector('#output-clear')
    },
    log: console.log.bind(console),
    warn: console.warn.bind(console),
    cls: function(){this.e.output.innerHTML = ''},
    logHtml2: function(cssClass,...args){
      const ln = document.createElement('div');
      if(cssClass) ln.classList.add(cssClass);
      ln.append(document.createTextNode(args.join(' ')));
      this.e.output.append(ln);
    },
    logHtml: function(...args){
      console.log(...args);
      if(1) this.logHtml2('', ...args);
    },
    logErr: function(...args){
      console.error(...args);
      if(1) this.logHtml2('error', ...args);
    },

    openDb: function(fn){
      if(this.pDb){
        toss("Already have an opened db.");
      }
      const capi = this.sqlite3.capi, wasm = capi.wasm;
      const stack = wasm.scopedAllocPush();
      let pDb = 0;
      try{
        const oFlags = capi.SQLITE_OPEN_CREATE | capi.SQLITE_OPEN_READWRITE;
        const ppDb = wasm.scopedAllocPtr();
        const rc = capi.sqlite3_open_v2(fn, ppDb, oFlags, null);
        pDb = wasm.getPtrValue(ppDb)
      }finally{
        wasm.scopedAllocPop(stack);
      }
      this.logHtml("Opened db:",capi.sqlite3_db_filename(pDb, 'main'));
      return this.pDb = pDb;
    },

    closeDb: function(){
      if(this.pDb){
        this.sqlite3.capi.sqlite3_close_v2(this.pDb);
        this.pDb = undefined;
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
      }catch(e){
        this.logErr(e.message);
        throw e;
      }finally{
        this.blockControls(false);
      }
      const list = txt.split('\n');
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
        opt = document.createElement('option');
        opt.value = opt.innerText = fn;
        sel.appendChild(opt);
      });
      this.logHtml("Loaded",infile);
    },

    /** Fetch ./fn and return its contents as a Uint8Array. */
    fetchFile: async function(fn){
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
      return sql;
    },

    checkRc: function(rc){
      if(rc){
        toss("Prepare failed:",this.sqlite3.capi.sqlite3_errmsg(this.pDb));
      }
    },

    blockControls: function(block){
      [
        this.e.selSql, this.e.btnRun, this.e.btnClear
      ].forEach((e)=>e.disabled = block);
    },
    
    /** Fetch ./fn and eval it as an SQL blob. */
    evalFile: async function(fn){
      const sql = await this.fetchFile(fn);
      this.logHtml("Running",fn,'...');
      const capi = this.sqlite3.capi, wasm = capi.wasm;
      let pStmt = 0, pSqlBegin;
      const stack = wasm.scopedAllocPush();
      const metrics = Object.create(null);
      metrics.prepTotal = metrics.stepTotal = 0;
      metrics.stmtCount = 0;
      this.blockControls(true);
      // Use setTimeout() so that the above log messages run before the loop starts.
      setTimeout((function(){
        metrics.timeStart = performance.now();
        try {
          let t;
          let sqlByteLen = sql.byteLength;
          const [ppStmt, pzTail] = wasm.scopedAllocPtr(2);
          pSqlBegin = wasm.alloc( sqlByteLen + 1/*SQL + NUL*/);
          let pSql = pSqlBegin;
          const pSqlEnd = pSqlBegin + sqlByteLen;
          wasm.heap8().set(sql, pSql);
          wasm.setMemValue(pSql + sqlByteLen, 0);
          while(wasm.getMemValue(pSql,'i8')){
            pStmt = 0;
            wasm.setPtrValue(ppStmt, 0);
            wasm.setPtrValue(pzTail, 0);
            t = performance.now();
            let rc = capi.sqlite3_prepare_v3(
              this.pDb, pSql, sqlByteLen, 0, ppStmt, pzTail
            );
            metrics.prepTotal += performance.now() - t;
            this.checkRc(rc);
            ++metrics.stmtCount;
            pStmt = wasm.getPtrValue(ppStmt);
            pSql = wasm.getPtrValue(pzTail);
            sqlByteLen = pSqlEnd - pSql;
            if(!pStmt) continue/*empty statement*/;
            t = performance.now();
            rc = capi.sqlite3_step(pStmt);
            metrics.stepTotal += performance.now() - t;
            switch(rc){
                case capi.SQLITE_ROW:
                case capi.SQLITE_DONE: break;
                default: this.checkRc(rc); toss("Not reached.");
            }
          }
        }catch(e){
          this.logErr(e.message);
          throw e;
        }finally{
          wasm.dealloc(pSqlBegin);
          wasm.scopedAllocPop(stack);
          this.blockControls(false);
        }
        metrics.timeEnd = performance.now();
        metrics.timeTotal = (metrics.timeEnd - metrics.timeStart);
        this.logHtml("Metrics:");//,JSON.stringify(metrics, undefined, ' '));
        this.logHtml("prepare() count:",metrics.stmtCount);
        this.logHtml("Time in prepare_v2():",metrics.prepTotal,"ms",
                     "("+(metrics.prepTotal / metrics.stmtCount),"ms per prepare())");
        this.logHtml("Time in step():",metrics.stepTotal,"ms",
                     "("+(metrics.stepTotal / metrics.stmtCount),"ms per step())");
        this.logHtml("Total runtime:",metrics.timeTotal,"ms");
        this.logHtml("Overhead (time - prep - step):",
                     (metrics.timeTotal - metrics.prepTotal - metrics.stepTotal)+"ms");
      }.bind(this)), 10);
    },

    run: function(sqlite3){
      this.sqlite3 = sqlite3;
      const capi = sqlite3.capi, wasm = capi.wasm;
      this.logHtml("Loaded module:",capi.sqlite3_libversion(), capi.sqlite3_sourceid());
      this.logHtml("WASM heap size =",wasm.heap8().length);
      this.logHtml("WARNING: if the WASMFS/OPFS layer crashes, this page may",
                   "become unresponsive and need to be closed and ",
                   "reloaded to recover.");
      const pDir = capi.sqlite3_web_persistent_dir();
      const dbFile = pDir ? pDir+"/speedtest.db" : ":memory:";
      if(pDir){
        // We initially need a clean db file, so...
        capi.sqlite3_wasm_vfs_unlink(dbFile);
      }
      this.openDb(dbFile);
      this.loadSqlList();
      const who = this;
      this.e.btnClear.addEventListener('click', ()=>this.cls(), false);
      this.e.btnRun.addEventListener('click', function(){
        if(!who.e.selSql.value) return;
        who.evalFile(who.e.selSql.value);
      }, false);
    }
  }/*App*/;

  self.sqlite3TestModule.initSqlite3().then(function(theEmccModule){
    self._MODULE = theEmccModule /* this is only to facilitate testing from the console */;
    App.run(theEmccModule.sqlite3);
  });
})();
