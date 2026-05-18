<!doctype html>
<html lang="en-us">
  <head>
    <meta charset="utf-8">
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <title>SQLite3 Fiddle</title>
    <link rel="shortcut icon" href="data:image/x-icon;," type="image/x-icon">
    <!--
        To add a terminal-style view using jquery.terminal[^1],
        uncomment the following two HTML lines and ensure that these
        files are on the web server.

        jquery-bundle.min.js is a concatenation of jquery.min.js from
        [^2] and jquery.terminal.min.js from [^1].
        jquery.terminal.min.css is from [^1].

        [^1]: https://github.com/jcubic/jquery.terminal
        [^2]: https://jquery.com
    -->
    <!--script src="jqterm/jqterm-bundle.min.js"></script>
    <link rel="stylesheet" href="jqterm/jquery.terminal.min.css"-->
    <style>
      /* The following styles are for app-level use. */
      :root {
          --sqlite-blue: #044a64;
          --textarea-color1: #000 /*044a64 is nice too*/;
          --textarea-color2: white;
          --size: 1.25 /* used by jqterm to calculate font size and the default is too tiny.*/;
      }
      textarea {
          font-family: monospace;
          flex: 1 1 auto;
          background-color: var(--textarea-color1);
          color: var(--textarea-color2);
      }
      textarea#input {
          color: var(--textarea-color1);
          background-color: var(--textarea-color2);
      }
      header {
          display: flex;
          justify-content: space-between;
          align-items: center;
          background-color: var(--sqlite-blue);
          color: white;
          font-size: 120%;
          font-weight: bold;
          border-radius: 0.25em;
          padding: 0.2em 0.5em;
      }
      header > .powered-by {
          font-size: 80%;
      }
      header a, header a:visited, header a:hover {
          color: inherit;
      }
      #main-wrapper {
          display: flex;
          flex-direction: column-reverse;
          flex: 1 1 auto;
          margin: 0.5em 0;
          overflow: hidden;
      }
      #main-wrapper.side-by-side {
          flex-direction: row;
      }
      #main-wrapper.side-by-side > fieldset {
          margin-left: 0.25em;
          margin-right: 0.25em;
      }
      #main-wrapper:not(.side-by-side) > fieldset {
          margin-bottom: 0.25em;
      }
      #main-wrapper.swapio {
          flex-direction: column;
      }
      #main-wrapper.side-by-side.swapio {
          flex-direction: row-reverse;
      }
      .zone-wrapper{
          display: flex;
          margin: 0;
          flex: 1 1 0%;
          border-radius: 0.5em;
          min-width: inherit/*important: resolves inability to scroll fieldset child element!*/;
          padding: 0.35em 0 0 0;
      }
      .zone-wrapper textarea {
          border-radius: 0.5em;
          flex: 1 1 auto;
          /*min/max width resolve an inexplicable margin on the RHS.  The -1em
            is for the padding, else we overlap the parent boundaries.*/
          /*min-width: calc(100% - 1em);
          max-width: calc(100% - 1em);
          padding: 0 0.5em;*/
      }

      .zone-wrapper.input { flex: 10 1 auto; }
      .zone-wrapper.output { flex: 20 1 auto; }
      .zone-wrapper > div {
          display:flex;
          flex: 1 1 0%;
      }
      .zone-wrapper.output {}
      .button-bar {
          display: flex;
          flex-wrap: wrap;
          align-items: center;
          align-content: space-between;
          justify-content: flex-start;
      }
      .button-bar > * {
          margin: 0.05em 0.5em 0.05em 0;
          flex: 0 1 auto;
          align-self: auto;
      }
      label[for] {
          cursor: pointer;
      }
      .error {
          color: red;
          background-color: yellow;
      }
      .hidden, .initially-hidden {
          position: absolute !important;
          opacity: 0 !important;
          pointer-events: none !important;
          display: none !important;
      }
      fieldset {
          border-radius: 0.5em;
          border: 1px inset;
          padding: 0.25em;
      }
      fieldset.options {
          font-size: 80%;
          margin-top: 0.5em;
      }
      fieldset:not(.options) > legend {
          font-size: 80%;
      }
      fieldset.options > div {
          display: flex;
          flex-wrap: wrap;
      }
      fieldset button {
          font-size: inherit;
      }
      fieldset.collapsible > legend > .fieldset-toggle::after {
          content: " [hide]";
          position: relative;
      }
      fieldset.collapsible.collapsed > legend > .fieldset-toggle::after {
          content: " [show]";
          position: relative;
      }
      span.labeled-input {
          padding: 0.25em;
          margin: 0.05em 0.25em;
          border-radius: 0.25em;
          white-space: nowrap;
          background: #0002;
          display: flex;
          align-items: center;
      }
      span.labeled-input > *:nth-child(2) {
          margin-left: 0.3em;
      }
      .center { text-align: center; }
      body.terminal-mode {
          max-height: calc(100% - 2em);
          display: flex;
          flex-direction: column;
          align-items: stretch;
      }
      #view-terminal {}
      .app-view {
          flex: 20 1 auto;
      }
      #view-split {
          display: flex;
          flex-direction: column-reverse;
      }
      #view-about {
        flex: auto;
        overflow: auto;
      }
      #view-about h1:first-child {
        display: flex;
      }
      #view-about h1:first-child > button {
        align-self: center;
        margin-left: 1em;
      }

      /* emcscript-related styling, used during the module load/intialization processes... */
      .emscripten { padding-right: 0; margin-left: auto; margin-right: auto; display: block; }
      div.emscripten { text-align: center; }
      div.emscripten_border { border: 1px solid black; }
      #module-spinner { overflow: visible; }
      #module-spinner > * {
          margin-top: 1em;
      }
      .spinner {
          height: 50px;
          width: 50px;
          margin: 0px auto;
          animation: rotation 0.8s linear infinite;
          border-left: 10px solid rgb(0,150,240);
          border-right: 10px solid rgb(0,150,240);
          border-bottom: 10px solid rgb(0,150,240);
          border-top: 10px solid rgb(100,0,200);
          border-radius: 100%;
          background-color: rgb(200,100,250);
      }
      @keyframes rotation {
          from {transform: rotate(0deg);}
          to {transform: rotate(360deg);}
      }
    </style>
  </head>
  <body>
    <header id='titlebar'>
      <span>SQLite3 Fiddle</span>
      <span id='titlebar-buttons'>
        <span class='powered-by'>Powered by
          <a href='https://sqlite.org'>SQLite3</a></span>
      </span>
    </header>
    <!-- emscripten bits -->
    <figure id="module-spinner">
      <div class="spinner"></div>
      <div class='center'><strong>Initializing app...</strong></div>
      <div class='center'>
        On a slow internet connection this may take a moment.  If this
        message displays for "a long time", intialization may have
        failed and the JavaScript console may contain clues as to why.
      </div>
    </figure>
    <div class="emscripten" id="module-status">Downloading...</div>
    <div class="emscripten">
      <progress value="0" max="100" id="module-progress" hidden='1'></progress>
    </div><!-- /emscripten bits -->

    <div id='view-terminal' class='app-view hidden initially-hidden'>
      This is a placeholder for a terminal-like view which is not in
      the default build.
    </div>

    <div id='view-split' class='app-view initially-hidden'>
      <div id='main-wrapper' class=''>
        <fieldset class='zone-wrapper input'>
          <legend><div class='button-bar'>
            <button id='btn-shell-exec'>Run</button>
            <button id='btn-clear'>Clear Input</button>
            <!--button data-cmd='.help'>Help</button-->
            <select id='select-examples'></select>
          </div></legend>
          <div><textarea id="input"
                         placeholder="Shell input. Ctrl-enter/shift-enter runs it.">
-- ==================================================
-- Use ctrl-enter or shift-enter to execute sqlite3
-- shell commands and SQL.
-- If a subset of the text is currently selected,
-- only that part is executed.
-- ==================================================
.nullvalue NULL
.headers on
</textarea></div>
        </fieldset>
        <fieldset class='zone-wrapper output'>
          <legend><div class='button-bar'>
            <button id='btn-clear-output'>Clear Output</button>
            <button id='btn-interrupt' class='hidden' disabled>Interrupt</button>
            <!-- interruption cannot work in the current configuration
                 because we cannot send an interrupt message when work
                 is currently underway. At that point the Worker is
                 tied up and will not receive the message. -->
          </div></legend>
          <div><textarea id="output" readonly
                         placeholder="Shell output."></textarea></div>
        </fieldset>
      </div><!-- #main-wrapper -->
    </div> <!-- #view-split -->

<div class='hidden app-view' id='view-about'>
  <h1>About SQLite Fiddle <button id='btn-about-close'>close</button></h1>

  <p>Fiddle is a JavaScript application wrapping a <a href='https://webassembly.org'>WebAssembly</a>
    build of <a href="https://sqlite.org/cli.html">the SQLite CLI shell</a>, slightly
    modified to account for browser-based user input. Aside from the different layout,
    it works just like the CLI shell. This copy was built with SQLite version
    <a id='sqlite-version-link'></a>.
  </p>

  <p>This app is provided in the hope that it may prove interesting or useful
    but it is not an officially-supported deliverable of the SQLite project.
    It is subject to any number of changes or outright removal at any time.
    That said, for as long as it's online we do respond to support requests
    in <a href="https://sqlite.org/forum">the SQLite forum</a>.
  </p>

  <p>This app runs on your device. After loading, it does not interact
    with the remote server at all. Similarly, this app does not use any
    HTTP cookies.</p>

  <p>Fiddle databases are transient in-memory databases unless they
    specifically use a persistent storage option (if available, help
    text in the SQL result output area will indicate how to use
    persistent storage when this app starts up).
  </p>

  <h1>Usage Summary</h1>

  <ul>
    <li class='hidden unhide-if-terminal-available'>In "terminal
      mode" it accepts input just like the CLI shell does.</li>
    <li>In split-view mode:
      <ul>
        <li>Input can be executed with either the Run
          button or tapping one of Ctrl-enter or Shift-enter from within
          the text input field.  If a portion of the input field is
          selected, only that portion will be run.
        </li>
        <li>The various toggle checkboxes can be used to tweak the layout
          and behaviors. Those toggles are persistent if the JS environment
          allows it.
        </li>
      </ul>
    </li>
    <li class='remove-if-terminal-available'>"Terminal mode" is
      not available in this deployment.
    </li>
    <li>Databases can be imported and exported using the buttons in
      the Options toolbar. No specific limit for imported database
      sizes is imposed, but large databases may cause it to fail with
      an out-of-memory error.</li>
    <!--li></li-->
  </ul>

</div><!-- #view-about -->

<fieldset class='options'>
  <legend>Options</legend>
  <div class=''>
    <span class='labeled-input'>
      <input type='file' id='load-db' class='hidden'/>
      <button id='btn-load-db'>Load DB...</button>
    </span>
    <span class='labeled-input'>
      <button id='btn-export'>Download DB</button>
    </span>
    <span class='labeled-input'>
      <button id='btn-reset'>Reset DB</button>
    </span>
    <span id='terminal-button-placeholder' class='hidden'></span>
    <span class='labeled-input'>
      <button id='btn-about'>About...</button>
    </span>
    <span class='labeled-input hide-in-terminal'>
      <input type='checkbox' id='opt-cb-sbs'
             data-csstgt='#main-wrapper'
             data-cssclass='side-by-side'
             data-config='sideBySide'
             >
      <label for='opt-cb-sbs'>Side-by-side</label>
    </span>
    <span class='labeled-input hide-in-terminal'>
      <input type='checkbox' id='opt-cb-swapio'
             data-csstgt='#main-wrapper'
             data-cssclass='swapio'
             data-config='swapInOut'
             >
      <label for='opt-cb-swapio'>Swap in/out</label>
    </span>
    <span class='labeled-input hide-in-terminal'>
      <input type='checkbox' id='opt-cb-autoscroll'
             data-config='autoScrollOutput'
             >
      <label for='opt-cb-autoscroll'>Auto-scroll output</label>
    </span>
    <span class='labeled-input hide-in-terminal'>
      <input type='checkbox' id='opt-cb-autoclear'
             data-config='autoClearOutput'>
      <label for='opt-cb-autoclear'>Auto-clear output</label>
    </span>
    <span class='labeled-input'>
      <span id='opt-wasm-info'>WASM: ???</span>
    </span>
  </div>
</fieldset><!-- .options -->

<script src="fiddle.js"></script>
  </body>
</html>
