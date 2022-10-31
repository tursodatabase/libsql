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
  Invokes functionality exposed by the previous two files to
  flesh out low-level parts of `sqlite3-api-prologue.js`. Most of
  these pieces related to the `sqlite3.capi.wasm` object.
- `sqlite3-api-build-version.js`\  
  Gets created by the build process and populates the
  `sqlite3.version` object. This part is not critical, but records the
  version of the library against which this module was built.
- `sqlite3-api-oo1.js`\  
  Provides a high-level object-oriented wrapper to the lower-level C
  API, colloquially known as OO API #1. Its API is similar to other
  high-level sqlite3 JS wrappers and should feel relatively familiar
  to anyone familiar with such APIs. That said, it is not a "required
  component" and can be elided from builds which do not want it.
- `sqlite3-api-worker1.js`\  
  A Worker-thread-based API which uses OO API #1 to provide an
  interface to a database which can be driven from the main Window
  thread via the Worker message-passing interface. Like OO API #1,
  this is an optional component, offering one of any number of
  potential implementations for such an API.
    - `sqlite3-worker1.js`\  
      Is not part of the amalgamated sources and is intended to be
      loaded by a client Worker thread. It loads the sqlite3 module
      and runs the Worker #1 API which is implemented in
      `sqlite3-api-worker1.js`.
    - `sqlite3-worker1-promiser.js`\  
      Is likewise not part of the amalgamated sources and provides
      a Promise-based interface into the Worker #1 API. This is
      a far user-friendlier way to interface with databases running
      in a Worker thread.
- `sqlite3-api-opfs.js`\  
  is an sqlite3 VFS implementation which supports Google Chrome's
  Origin-Private FileSystem (OPFS) as a storage layer to provide
  persistent storage for database files in a browser. It requires...
    - `sqlite3-opfs-async-proxy.js`\  
      is the asynchronous backend part of the OPFS proxy. It speaks
      directly to the (async) OPFS API and channels those results back
      to its synchronous counterpart. This file, because it must be
      started in its own Worker, is not part of the amalgamation.
- **`api/sqlite3-api-cleanup.js`**\  
  The previous files do not immediately extend the library. Instead
  they add callback functions to be called during its
  bootstrapping. Some also temporarily create global objects in order
  to communicate their state to the files which follow them. This file
  cleans up any dangling globals and runs the API bootstrapping
  process, which is what finally executes the initialization code
  installed by the previous files. As of this writing, this code
  ensures that the previous files leave no more than a single global
  symbol installed. When adapting the API for non-Emscripten
  toolchains, this "should" be the only file where changes are needed.

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

The following files are part of the build process but are injected
into the build-generated `sqlite3.js` along with `sqlite3-api.js`.

- `extern-pre-js.js`\  
  Emscripten-specific header for Emscripten's `--extern-pre-js`
  flag. As of this writing, that file is only used for experimentation
  purposes and holds no code relevant to the production deliverables.
- `pre-js.js`\  
  Emscripten-specific header for Emscripten's `--pre-js` flag. This
  file is intended as a place to override certain Emscripten behavior
  before it starts up, but corner-case Emscripten bugs keep that from
  being a reality.
- `post-js-header.js`\  
  Emscripten-specific header for the `--post-js` input. It opens up
  a lexical scope by starting a post-run handler for Emscripten.
- `post-js-footer.js`\  
  Emscripten-specific footer for the `--post-js` input. This closes
  off the lexical scope opened by `post-js-header.js`.
- `extern-post-js.js`\  
  Emscripten-specific header for Emscripten's `--extern-post-js`
  flag. This file overwrites the Emscripten-installed
  `sqlite3InitModule()` function with one which, after the module is
  loaded, also initializes the asynchronous parts of the sqlite3
  module. For example, the OPFS VFS support.
