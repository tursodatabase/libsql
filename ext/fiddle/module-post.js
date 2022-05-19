/* This is the --post-js file for emcc. It gets appended to the
   generated fiddle.js. It should contain all app-level code.

   Maintenance achtung: do not call any wasm-bound functions from
   outside of the onRuntimeInitialized() function. They are not
   permitted to be called until after the module init is complete,
   which does not happen until after this file is processed. Once that
   init is finished, Module.onRuntimeInitialized() will be
   triggered. All app-level init code should go into that callback or
   be triggered via it.  Calling wasm-bound functions before that
   callback is run will trigger an assertion in the wasm environment.
*/
window.Module.onRuntimeInitialized = function(){
    'use strict';
    const Module = window.Module /* wasm module as set up by emscripten */;
    delete Module.onRuntimeInitialized;

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
    
    // Unhide all elements which start out hidden
    EAll('.initially-hidden').forEach((e)=>e.classList.remove('initially-hidden'));
    
    const taInput = E('#input');
    const btnClearIn = E('#btn-clear');
    btnClearIn.addEventListener('click',function(){
        taInput.value = '';
    },false);
    // Ctrl-enter and shift-enter both run the current SQL.
    taInput.addEventListener('keydown',function(ev){
        if((ev.ctrlKey || ev.shiftKey) && 13 === ev.keyCode){
            ev.preventDefault();
            ev.stopPropagation();
            btnRun.click();
        }
    }, false);
    const taOutput = E('#output');
    const btnClearOut = E('#btn-clear-output');
    btnClearOut.addEventListener('click',function(){
        taOutput.value = '';
    },false);
    /* Sends the given text to the shell. If it's null or empty, this
       is a no-op except that the very first call will initialize the
       db and output an informational header. */
    const doExec = function f(sql){
        if(!f._) f._ = Module.cwrap('fiddle_exec', null, ['string']);
        if(Module._isDead){
            Module.printErr("shell module has exit()ed. Cannot run SQL.");
            return;
        }
        if(Module.config.autoClearOutput) taOutput.value='';
        f._(sql);
    };
    const btnRun = E('#btn-run');
    btnRun.addEventListener('click',function(){
        const sql = taInput.value.trim();
        if(sql){
            doExec(sql);
        }
    },false);

    E('#opt-cb-sbs')
        .addEventListener('change', function(){
            E('#main-wrapper').classList[
                this.checked ? 'add' : 'remove'
            ]('side-by-side');
        }, false);
    E('#btn-notes-caveats')
        .addEventListener('click', function(){
            E('#notes-caveats').remove();
        }, false);

    /* For each checkbox with data-config=X, set up a binding to
       Module.config[X]. */
    EAll('input[type=checkbox][data-config]')
        .forEach(function(e){
            e.checked = !!Module.config[e.dataset.config];
            e.addEventListener('change', function(){
                Module.config[this.dataset.config] = this.checked;
            }, false);
        });

    /* For each button with data-cmd=X, map a click handler which
       calls doExec(X). */
    const cmdClick = function(){doExec(this.dataset.cmd);};
    EAll('button[data-cmd]').forEach(
        e => e.addEventListener('click', cmdClick, false)
    );

    doExec(null)/*sets up the db and outputs the header*/;
};
