/*
  2022-05-22

  The author disclaims copyright to this source code.  In place of a
  legal notice, here is a blessing:

  *   May you do good and not evil.
  *   May you find forgiveness for yourself and forgive others.
  *   May you share freely, never taking more than you give.

  ***********************************************************************

  This file contains bootstrapping code used by various test scripts
  which live in this file's directory.
*/
(function(){
    /* querySelectorAll() proxy */
    const EAll = function(/*[element=document,] cssSelector*/){
        return (arguments.length>1 ? arguments[0] : document)
            .querySelectorAll(arguments[arguments.length-1]);
    };
    /* querySelector() proxy */
    const E = function(/*[element=document,] cssSelector*/){
        return (arguments.length>1 ? arguments[0] : document)
            .querySelector(arguments[arguments.length-1]);
    };

    /* emscripten-related bits... */
    self.Module = {
        /* ^^^ cannot declare that const because sqlite3.js
           (auto-generated) includes a decl for it and runs in this
           scope. */
        preRun: [],
        postRun: [],
        //onRuntimeInitialized: function(){},
        print: function(){
            console.log.apply(console, Array.prototype.slice.call(arguments));
        },
        printErr: function(){
            console.error.apply(console, Array.prototype.slice.call(arguments));
        },
        setStatus: function f(text){
            if(!f.last){
                f.last = { text: '', step: 0 };
                f.ui = {
                    status: E('#module-status'),
                    progress: E('#module-progress'),
                    spinner: E('#module-spinner')
                };
            }
            if(text === f.last.text) return;
            f.last.text = text;
            if(f.ui.progress){
                f.ui.progress.value = f.last.step;
                f.ui.progress.max = f.last.step + 1;
            }
            ++f.last.step;
            if(text) {
                f.ui.status.classList.remove('hidden');
                f.ui.status.innerText = text;
            }else{
                if(f.ui.progress){
                    f.ui.progress.remove();
                    f.ui.spinner.remove();
                    delete f.ui.progress;
                    delete f.ui.spinner;
                }
                f.ui.status.classList.add('hidden');
            }
        },
        totalDependencies: 0,
        monitorRunDependencies: function(left) {
            this.totalDependencies = Math.max(this.totalDependencies, left);
            this.setStatus(left
                           ? ('Preparing... (' + (this.totalDependencies-left)
                              + '/' + this.totalDependencies + ')')
                           : 'All downloads complete.');
        },
        /**
           Loads sqlite3-api.js and calls the given callback (if
           provided), passing it an object:

           {
             api:sqlite3_c-like API wrapper,
             SQLite3: OO wrapper
           }

           Whether this is synchronous or async depends on whether
           it's run in the main thread (async) or a worker
           (synchronous).

           If called after the module has been loaded, it uses a
           cached reference, noting that multiple async calls may end
           up loading it multiple times.
        */
        loadSqliteAPI: function f(callback){
            const namespace = self.Module;
            if(namespace.sqlite3){
                if(callback) callback(namespace.sqlite3);
                return;
            }
            const theScript = 'sqlite3-api.js';
            if(self.importScripts){/*worker*/
                importScripts(theScript);
                if(callback) callback(namespace.sqlite3);
            }else{/*main thread*/
                new Promise((resolve, reject) => {
                    const script = document.createElement('script');
                    document.body.appendChild(script);
                    script.onload = resolve;
                    script.onerror = reject;
                    script.async = true;
                    script.src = theScript;
                }).then(() => {
                    if(callback) callback(namespace.sqlite3);
                });
            }
        }
    };

})(self/*window or worker*/);
