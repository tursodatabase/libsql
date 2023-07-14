<!doctype html>
<html lang="en-us">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <link rel="shortcut icon" href="data:image/x-icon;," type="image/x-icon">
    <link rel="stylesheet" href="common/emscripten.css"/>
    <link rel="stylesheet" href="common/testing.css"/>
    <title>speedtest1-wasmfs.wasm</title>
  </head>
  <body>
    <header id='titlebar'><span>speedtest1-wasmfs.wasm</span></header>
    <div>See also: <a href='speedtest1-worker.html'>speedtest1-worker</a></div>
    <div class='warning'>Achtung: running it with the dev tools open may
      <em>drastically</em> slow it down. For faster results, keep the dev
      tools closed when running it!
    </div>
    <div id='test-output'></div>
    <script>
      (function(){
          const eOut = document.querySelector('#test-output');
          const log2 = function(cssClass,...args){
              const ln = document.createElement('div');
              if(cssClass) ln.classList.add(cssClass);
              ln.append(document.createTextNode(args.join(' ')));
              eOut.append(ln);
              //this.e.output.lastElementChild.scrollIntoViewIfNeeded();
          };
          /* can't update DOM while speedtest is running unless we run
             speedtest in a worker thread. */;
          const log = (...args)=>{
              console.log(...args);
              log2('',...args);
          };
          const logErr = function(...args){
              console.error(...args);
              log2('error',...args);
          };
          const W = new Worker(
              'speedtest1-wasmfs.mjs'+globalThis.location.search,{
              type: 'module'
          });
          log("Starting up...");
          W.onmessage = function({data}){
              switch(data.type){
                  case 'log': log(...data.args); break;
                  case 'logErr': logErr(...data.args); break;
                  default:
                      break;
              }
          };
      })();
    </script>
  </body>
</html>
