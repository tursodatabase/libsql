(async function(self){

  const logCss = (function(){
    const mapToString = (v)=>{
      switch(typeof v){
          case 'number': case 'string': case 'boolean':
          case 'undefined': case 'bigint':
            return ''+v;
          default: break;
      }
      if(null===v) return 'null';
      if(v instanceof Error){
        v = {
          message: v.message,
          stack: v.stack,
          errorClass: v.name
        };
      }
      return JSON.stringify(v,undefined,2);
    };
    const normalizeArgs = (args)=>args.map(mapToString);
    const logTarget = document.querySelector('#test-output');
    const logCss = function(cssClass,...args){
      const ln = document.createElement('div');
      if(cssClass){
        for(const c of (Array.isArray(cssClass) ? cssClass : [cssClass])){
          ln.classList.add(c);
        }
      }
      ln.append(document.createTextNode(normalizeArgs(args).join(' ')));
      logTarget.append(ln);
    };
    const cbReverse = document.querySelector('#cb-log-reverse');
    const cbReverseKey = 'tester1:cb-log-reverse';
    const cbReverseIt = ()=>{
      logTarget.classList[cbReverse.checked ? 'add' : 'remove']('reverse');
      localStorage.setItem(cbReverseKey, cbReverse.checked ? 1 : 0);
    };
    cbReverse.addEventListener('change', cbReverseIt, true);
    if(localStorage.getItem(cbReverseKey)){
      cbReverse.checked = !!(+localStorage.getItem(cbReverseKey));
    }
    cbReverseIt();
    return logCss;
  })();
  const stdout = (...args)=>logCss('',...args);
  const stderr = (...args)=>logCss('error',...args);

  const wait = async (ms)=>{
    return new Promise((resolve)=>setTimeout(resolve,ms));
  };

  const urlArgsJs = new URL(document.currentScript.src).searchParams;
  const urlArgsHtml = new URL(self.location.href).searchParams;
  const options = Object.create(null);
  options.sqlite3Dir = urlArgsJs.get('sqlite3.dir');
  options.workerCount = (
    urlArgsHtml.has('workers') ? +urlArgsHtml.get('workers') : 3
  ) || 4;
  options.opfsVerbose = (
    urlArgsHtml.has('verbose') ? +urlArgsHtml.get('verbose') : 1
  ) || 1;
  options.interval = (
    urlArgsHtml.has('interval') ? +urlArgsHtml.get('interval') : 1000
  ) || 1000;
  options.iterations = (
    urlArgsHtml.has('iterations') ? +urlArgsHtml.get('iterations') : 10
  ) || 10;
  options.unlockAsap = (
    urlArgsHtml.has('unlock-asap') ? +urlArgsHtml.get('unlock-asap') : 0
  ) || 0;
  options.noUnlink = !!urlArgsHtml.has('no-unlink');
  const workers = [];
  workers.post = (type,...args)=>{
    for(const w of workers) w.postMessage({type, payload:args});
  };
  workers.counts = {loaded: 0, passed: 0, failed: 0};
  const checkFinished = function(){
    if(workers.counts.passed + workers.counts.failed !== workers.length){
      return;
    }
    if(workers.counts.failed>0){
      logCss('tests-fail',"Finished with",workers.counts.failed,"failure(s).");
    }else{
      logCss('tests-pass',"All",workers.length,"workers finished.");
    }
  };
  workers.onmessage = function(msg){
    msg = msg.data;
    const prefix = 'Worker #'+msg.worker+':';
    switch(msg.type){
        case 'loaded':
          stdout(prefix,"loaded");
          if(++workers.counts.loaded === workers.length){
            stdout("All",workers.length,"workers loaded. Telling them to run...");
            workers.post('run');
          }
          break;
        case 'stdout': stdout(prefix,...msg.payload); break;
        case 'stderr': stderr(prefix,...msg.payload); break;
        case 'error': stderr(prefix,"ERROR:",...msg.payload); break;
        case 'finished':
          ++workers.counts.passed;
          logCss('tests-pass',prefix,...msg.payload);
          checkFinished();
          break;
        case 'failed':
          ++workers.counts.failed;
          logCss('tests-fail',prefix,"FAILED:",...msg.payload);
          checkFinished();
          break;
        default: logCss('error',"Unhandled message type:",msg); break;
    }
  };

  stdout("Launching",options.workerCount,"workers. Options:",options);
  workers.uri = (
    'worker.js?'
      + 'sqlite3.dir='+options.sqlite3Dir
      + '&interval='+options.interval
      + '&iterations='+options.iterations
      + '&opfs-verbose='+options.opfsVerbose
      + '&opfs-unlock-asap='+options.unlockAsap
  );
  for(let i = 0; i < options.workerCount; ++i){
    stdout("Launching worker...");
    workers.push(new Worker(
      workers.uri+'&workerId='+(i+1)+(
        (i || options.noUnlink) ? '' : '&unlink-db'
      )
    ));
  }
  // Have to delay onmessage assignment until after the loop
  // to avoid that early workers get an undue head start.
  workers.forEach((w)=>w.onmessage = workers.onmessage);
})(self);
