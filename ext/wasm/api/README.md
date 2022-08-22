# sqlite3-api.js And Friends

This is the README for the files `sqlite3-*.js` and
`sqlite3-wasm.c`. This collection of files is used to build a
single-file distribution of the sqlite3 WASM API. It is broken into
multiple JS files because:

1. To facilitate including or excluding certain components for
   specific use cases. e.g. by removing `sqlite3-api-oo1.js` if the
   OO#1 API is not needed.

2. To facilitate modularizing the pieces for use in different WASM
   build environments. e.g. the files `post-js-*.js` are for use with
   Emscripten's `--post-js` feature, and nowhere else.

3. Certain components must be in their own standalone files in order
   to be loaded as JS Workers.

Note that the structure described here is the current state of things,
not necessarily the "final" state.

The overall idea is that the following files get concatenated
together, in the listed order, the resulting file is loaded by a
browser client:

- `post-js-header.js`\  
  Emscripten-specific header for the `--post-js` input.
- `sqlite3-api-prologue.js`\  
  Contains the initial bootstrap setup of the sqlite3 API
  objects. This is exposed as a function, rather than objects, so that
  the next step can pass in a config object which abstracts away parts
  of the WASM environment, to facilitate plugging it in to arbitrary
  WASM toolchains.
- `../common/whwasmutil.js`\  
  A semi-third-party collection of JS/WASM utility code intended to
  replace much of the Emscripten glue. The sqlite3 APIs internally use
  these APIs instead of their Emscripten counterparts, in order to be
  more portable to arbitrary WASM toolchains. This API is
  configurable, in principle, for use with arbitrary WASM
  toolchains. It is "semi-third-party" in that it was created in order
  to support this tree but is standalone and maintained together
  with...
- `../jaccwabyt/jaccwabyt.js`\  
  Another semi-third-party API which creates bindings between JS
  and C structs, such that changes to the struct state from either JS
  or C are visible to the other end of the connection. This is also an
  independent spinoff project, conceived for the sqlite3 project but
  maintained separately.
- `sqlite3-api-glue.js`\  
  Invokes the function exposed by `sqlite3-api-prologue.js`, passing
  it a configuration object to configure it for the current WASM
  toolchain (noting that it currently requires Emscripten), then
  removes that function from the global scope. The result of this file
  is a global-scope `sqlite3` object which acts as a namespace for the
  API's functionality. This object gets removed from the global scope
  after the following files have attached their own features to it.
- `sqlite3-api-oo1.js`\  
  Provides a high-level object-oriented wrapper to the lower-level C
  API, colloquially known as OO API #1. Its API is similar to other
  high-level sqlite3 JS wrappers and should feel relatively familiar
  to anyone familiar with such APIs. That said, it is not a "required
  component" and can be elided from builds which do not want it.
- `sqlite3-api-worker.js`\  
  A Worker-thread-based API which uses OO API #1 to provide an
  interface to a database which can be driven from the main Window
  thread via the Worker message-passing interface. Like OO API #1,
  this is an optional component, offering one of any number of
  potential implementations for such an API.
    - `sqlite3-worker.js`\  
      Is not part of the amalgamated sources and is intended to be
      loaded by a client Worker thread. It loads the sqlite3 module
      and runs the Worker API which is implemented in
      `sqlite3-api-worker.js`.
- `sqlite3-api-opfs.js`\  
  is an in-development/experimental sqlite3 VFS wrapper, the goal of
  which being to use Google Chrome's Origin-Private FileSystem (OPFS)
  storage layer to provide persistent storage for database files in a
  browser. It is far from complete.
- `sqlite3-api-cleanup.js`\  
  the previous files temporarily create global objects in order to
  communicate their state to the files which follow them, and _this_
  file connects any final components together and cleans up those
  globals. As of this writing, this code ensures that the previous
  files leave no global symbols installed, and it moves the sqlite3
  namespace object into the in-scope Emscripten module. Abstracting
  this for other WASM toolchains is TODO.
- `post-js-footer.js`\  
  Emscripten-specific footer for the `--post-js` input. This closes
  off the lexical scope opened by `post-js-header.js`.

The build process glues those files together, resulting in
`sqlite3-api.js`, which is everything except for the `post-js-*.js`
files, and `sqlite3.js`, which is the Emscripten-generated amalgamated
output and includes the `post-js-*.js` parts, as well as the
Emscripten-provided module loading pieces.

The non-JS outlier file is `sqlite3-wasm.c`: it is a proxy for
`sqlite3.c` which `#include`'s that file and adds a couple more
WASM-specific helper functions, at least one of which requires access
to private/static `sqlite3.c` internals. `sqlite3.wasm` is compiled
from this file rather than `sqlite3.c`.
