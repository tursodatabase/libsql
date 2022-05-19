/* This is the --pre-js file for emcc. It gets prepended to the
   generated fiddle.js. It should contain only code which is relevant
   to the setup and initialization of the wasm module. */
(function(){
    'use strict';

    /**
       What follows is part of the emscripten core setup. Do not
       modify it without understanding what it's doing.
    */
    const statusElement = document.getElementById('status');
    const progressElement = document.getElementById('progress');
    const spinnerElement = document.getElementById('spinner');
    const Module = window.Module = {
        /* Config object. Referenced by certain Module methods and
           app-level code. */
        config: {
            /* If true, the Module.print() impl will auto-scroll
               the output widget to the bottom when it receives output,
               else it won't. */
            autoScrollOutput: true,
            /* If true, the output area will be cleared before each
               command is run, else it will not. */
            autoClearOutput: false,
            /* If true, Module.print() will echo its output to
               the console, in addition to its normal output widget. */
            printToConsole: true,
            /* If true, display input/output areas side-by-side. */
            sideBySide: false,
            /* If true, swap positions of the input/output areas. */
            swapInOut: false
        },
        preRun: [],
        postRun: [],
        //onRuntimeInitialized: function(){},
        print: (function f() {
            /* Maintenance reminder: we currently require/expect a textarea
               output element. It might be nice to extend this to behave
               differently if the output element is a non-textarea element,
               in which case it would need to append the given text as a TEXT
               node and add a line break. */
            const outputElem = document.getElementById('output');
            outputElem.value = ''; // clear browser cache
            return function(text) {
                if(arguments.length > 1) text = Array.prototype.slice.call(arguments).join(' ');
                // These replacements are necessary if you render to raw HTML
                //text = text.replace(/&/g, "&amp;");
                //text = text.replace(/</g, "&lt;");
                //text = text.replace(/>/g, "&gt;");
                //text = text.replace('\n', '<br>', 'g');
                if(null===text){/*special case: clear output*/
                    outputElem.value = '';
                    return;
                }
                if(window.Module.config.printToConsole) console.log(text);
                if(window.Module.jqTerm) window.Module.jqTerm.echo(text);
                outputElem.value += text + "\n";
                if(window.Module.config.autoScrollOutput){
                    outputElem.scrollTop = outputElem.scrollHeight;
                }
            };
        })(),
        setStatus: function f(text) {
            if(!f.last) f.last = { time: Date.now(), text: '' };
            if(text === f.last.text) return;
            const m = text.match(/([^(]+)\((\d+(\.\d+)?)\/(\d+)\)/);
            const now = Date.now();
            if(m && now - f.last.time < 30) return; // if this is a progress update, skip it if too soon
            f.last.time = now;
            f.last.text = text;
            if(m) {
                text = m[1];
                progressElement.value = parseInt(m[2])*100;
                progressElement.max = parseInt(m[4])*100;
                progressElement.hidden = false;
                spinnerElement.hidden = false;
            } else {
                progressElement.remove();
                if(!text) spinnerElement.remove();
            }
            if(text) statusElement.innerText = text;
            else statusElement.remove();
        },
        totalDependencies: 0,
        monitorRunDependencies: function(left) {
            this.totalDependencies = Math.max(this.totalDependencies, left);
            this.setStatus(left
                           ? ('Preparing... (' + (this.totalDependencies-left)
                              + '/' + this.totalDependencies + ')')
                           : 'All downloads complete.');
        }
    };
    Module.printErr = Module.print/*capture stderr output*/;
    Module.setStatus('Downloading...');
    window.onerror = function(/*message, source, lineno, colno, error*/) {
        const err = arguments[4];
        if(err && 'ExitStatus'==err.name){
            Module._isDead = true;
            Module.printErr("FATAL ERROR:", err.message);
            Module.printErr("Restarting the app requires reloading the page.");
            const taOutput = document.querySelector('#output');
            if(taOutput) taOutput.classList.add('error');
        }
        Module.setStatus('Exception thrown, see JavaScript console');
        spinnerElement.style.display = 'none';
        Module.setStatus = function(text) {
            if(text) console.error('[post-exception status] ' + text);
        };
    };
})();
