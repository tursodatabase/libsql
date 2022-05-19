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
        if(Module.jqTerm) Module.jqTerm.clear();
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

    const mainWrapper = E('#main-wrapper');
    /* For each checkboxes with data-csstgt, set up a handler which
       toggles the given CSS class on the element matching
       E(data-csstgt). */
    EAll('input[type=checkbox][data-csstgt]')
        .forEach(function(e){
            const tgt = E(e.dataset.csstgt);
            const cssClass = e.dataset.cssclass || 'error';
            e.checked = tgt.classList.contains(cssClass);
            e.addEventListener('change', function(){
                tgt.classList[
                    this.checked ? 'add' : 'remove'
                ](cssClass)
            }, false);
        });
    /* For each checkbox with data-config=X, set up a binding to
       Module.config[X]. These must be set up AFTER data-csstgt
       checkboxes so that those two states can be synced properly. */
    EAll('input[type=checkbox][data-config]')
        .forEach(function(e){
            const confVal = !!Module.config[e.dataset.config];
            if(e.checked !== confVal){
                /* Ensure that data-csstgt mappings (if any) get
                   synced properly. */
                e.checked = confVal;
                e.dispatchEvent(new Event('change'));
            }
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


    /**
       Given a DOM element, this routine measures its "effective
       height", which is the bounding top/bottom range of this element
       and all of its children, recursively. For some DOM structure
       cases, a parent may have a reported height of 0 even though
       children have non-0 sizes.

       Returns 0 if !e or if the element really has no height.
    */
    const effectiveHeight = function f(e){
        if(!e) return 0;
        if(!f.measure){
            f.measure = function callee(e, depth){
                if(!e) return;
                const m = e.getBoundingClientRect();
                if(0===depth){
                    callee.top = m.top;
                    callee.bottom = m.bottom;
                }else{
                    callee.top = m.top ? Math.min(callee.top, m.top) : callee.top;
                    callee.bottom = Math.max(callee.bottom, m.bottom);
                }
                Array.prototype.forEach.call(e.children,(e)=>callee(e,depth+1));
                if(0===depth){
                    //console.debug("measure() height:",e.className, callee.top, callee.bottom, (callee.bottom - callee.top));
                    f.extra += callee.bottom - callee.top;
                }
                return f.extra;
            };
        }
        f.extra = 0;
        f.measure(e,0);
        return f.extra;
    };

    /**
       Returns a function, that, as long as it continues to be invoked,
       will not be triggered. The function will be called after it stops
       being called for N milliseconds. If `immediate` is passed, call
       the callback immediately and hinder future invocations until at
       least the given time has passed.

       If passed only 1 argument, or passed a falsy 2nd argument,
       the default wait time set in this function's $defaultDelay
       property is used.

       Source: underscore.js, by way of https://davidwalsh.name/javascript-debounce-function
    */
    const debounce = function f(func, wait, immediate) {
        var timeout;
        if(!wait) wait = f.$defaultDelay;
        return function() {
            const context = this, args = Array.prototype.slice.call(arguments);
            const later = function() {
                timeout = undefined;
                if(!immediate) func.apply(context, args);
            };
            const callNow = immediate && !timeout;
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
            if(callNow) func.apply(context, args);
        };
    };
    debounce.$defaultDelay = 500 /*arbitrary*/;

    const ForceResizeKludge = (function(){
        /* Workaround for Safari mayhem regarding use of vh CSS units....
           We cannot use vh units to set the terminal area size because
           Safari chokes on that, so we calculate that height here. Larger
           than ~95% is too big for Firefox on Android, causing the input
           area to move off-screen. */
        const bcl = document.body.classList;
        const appViews = EAll('.app-view');
        const resized = function f(){
            if(f.$disabled) return;
            const wh = window.innerHeight;
            var ht;
            var extra = 0;
            const elemsToCount = [
                E('body > header'),
                E('body > footer')
            ];
            elemsToCount.forEach((e)=>e ? extra += effectiveHeight(e) : false);
            ht = wh - extra;
            appViews.forEach(function(e){
                e.style.height =
                e.style.maxHeight = [
                    "calc(", (ht>=100 ? ht : 100), "px",
                    " - 2em"/*fudge value*/,")"
                    /* ^^^^ hypothetically not needed, but both
                       Chrome/FF on Linux will force scrollbars on the
                       body if this value is too small. */
                ].join('');
            });
        };
        resized.$disabled = true/*gets deleted when setup is finished*/;
        window.addEventListener('resize', debounce(resized, 250), false);
        return resized;
    })();

    Module.print(null/*clear any output generated by the init process*/);
    if(window.jQuery && window.jQuery.terminal){
        /* Set up the terminal-style view... */
        const eTerm = window.jQuery('#jqterminal').empty();
        Module.jqTerm = eTerm.terminal(doExec,{
            prompt: 'sqlite> ',
            greetings: false /* note that the docs incorrectly call this 'greeting' */
        });
        //Module.jqTerm.clear(/*remove the "greeting"*/);
        /* Set up a button to toggle the views... */
        const head = E('header#titlebar');
        const btnToggleView = jQuery("<button>Toggle View</button>")[0];
        head.appendChild(btnToggleView);
        btnToggleView.addEventListener('click',function f(){
            EAll('.app-view').forEach(e=>e.classList.toggle('hidden'));
            if(document.body.classList.toggle('terminal-mode')){
                ForceResizeKludge();
            }
        }, false);
        btnToggleView.click();
    }
    doExec(null/*init the db and output the header*/);
    Module.print('\nThis experimental app is provided in the hope that it',
                 'may prove interesting or useful but is not an officially',
                 'supported deliverable of the sqlite project. It is subject to',
                 'any number of changes or outright removal at any time.\n');
    delete ForceResizeKludge.$disabled;
    ForceResizeKludge();
};
