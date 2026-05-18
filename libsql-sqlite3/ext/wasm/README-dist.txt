This is the README for the sqlite3 WASM/JS distribution.

Main project page: https://sqlite.org

Documentation: https://sqlite.org/wasm

This archive contains the following deliverables for the WASM/JS
build:

- jswasm/sqlite3.js is the canonical "vanilla JS" version.

- jswasm/sqlite3.mjs is the same but in ES6 module form

- jswasm/*-bundler-friendly.js and .mjs are variants which are
  intended to be compatible with "bundler" tools commonly seen in
  node.js-based projects. Projects using such tools should use those
  variants, where available, instead of files without the
  "-bundler-friendly" suffix. Some files do not have separate
  variants.

- jswasm/sqlite3.wasm is the binary WASM file imported by all of the
  above-listed JS files.

- The jswasm directory additionally contains a number of supplemental
  JS files which cannot be bundled directly with the main JS files
  but are necessary for certain usages.

- The top-level directory contains various demonstration and test
  applications for sqlite3.js and sqlite3.mjs.
  sqlite3-bundler-friendly.mjs requires client-side build tools to make
  use of and is not demonstrated here.

Browsers will not serve WASM files from file:// URLs, so the test and
demonstration apps require a web server and that server must, for the
OPFS[^1]-related features, include the following headers in its response
when serving the files:

    Cross-Origin-Opener-Policy: same-origin
    Cross-Origin-Embedder-Policy: require-corp

Most functionality will work without those headers but the OPFS[^1]
storage capability will not be available without them.

One simple way to get the demo apps up and running on Unix-style
systems is to install althttpd (https://sqlite.org/althttpd) and run:

    althttpd --enable-sab --page index.html


[^1]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system
