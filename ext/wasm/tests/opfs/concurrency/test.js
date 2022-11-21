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
  ) || 3;
  options.opfsVerbose = (
    urlArgsHtml.has('verbose') ? +urlArgsHtml.get('verbose') : 1
  ) || 1;
  options.interval = (
    urlArgsHtml.has('interval') ? +urlArgsHtml.get('interval') : 750
  ) || 750;
  const workers = [];
  workers.post = (type,...args)=>{
    for(const w of workers) w.postMessage({type, payload:args});
  };
  workers.loadedCount = 0;
  workers.onmessage = function(msg){
    msg = msg.data;
    const prefix = 'Worker #'+msg.worker+':';
    switch(msg.type){
        case 'loaded':
          stdout(prefix,"loaded");
          if(++workers.loadedCount === workers.length){
            stdout("All workers loaded. Telling them to run...");
            workers.post('run');
          }
          break;
        case 'stdout': stdout(prefix,...msg.payload); break;
        case 'stderr': stderr(prefix,...msg.payload); break;
        case 'error': stderr(prefix,"ERROR:",...msg.payload); break;
        case 'finished':
          logCss('tests-pass',prefix,...msg.payload);
          break;
        case 'failed':
          logCss('tests-fail',prefix,"FAILED:",...msg.payload);
          break;
        default: logCss('error',"Unhandled message type:",msg); break;
    }
  };

  stdout("Launching",options.workerCount,"workers...");
  workers.uri = (
    'worker.js?'
      + 'sqlite3.dir='+options.sqlite3Dir
      + '&interval='+options.interval
      + '&opfs-verbose='+options.opfsVerbose
  );
  for(let i = 0; i < options.workerCount; ++i){
    stdout("Launching worker...");
    workers.push(new Worker(
      workers.uri+'&workerId='+(i+1)+(i ? '' : '&unlink-db')
    ));
  }
  // Have to delay onmessage assignment until after the loop
  // to avoid that early workers get an undue head start.
  workers.forEach((w)=>w.onmessage = workers.onmessage);
})(self);
