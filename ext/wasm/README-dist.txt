This is the README for the sqlite3 WASM/JS distribution.

Main project page: https://sqlite.org

TODO: link to main WASM/JS docs, once they are online

This archive contains the sqlite3.js and sqlite3.wasm file which make
up the sqlite3 WASM/JS build.

The jswasm directory contains both the main deliverables and small
demonstration and test apps. Browsers will not serve WASM files from
file:// URLs, so the demo/test apps require a web server and that
server must include the following headers in its response when serving
the files:

    Cross-Origin-Opener-Policy: same-origin
    Cross-Origin-Embedder-Policy: require-corp

The files named sqlite3*.js and sqlite3.wasm belong to the core
sqlite3 deliverables and the others are soley for demonstration and
may be discarded. They are not in separate directories from the main
deliverables because a quirk of URI resolution in JS code would then
require that sqlite3.js be duplicated and edited for Worker-loaded
operation.
