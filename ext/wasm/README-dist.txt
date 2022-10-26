This is the README for the sqlite3 WASM/JS distribution.

Main project page: https://sqlite.org

Documentation: https://sqlite.org/wasm

This archive contains the sqlite3.js and sqlite3.wasm file which make
up the sqlite3 WASM/JS build.

The jswasm directory contains the core sqlite3 deliverables and the
top-level directory contains demonstration and test apps. Browsers
will not serve WASM files from file:// URLs, so the demo/test apps
require a web server and that server must include the following
headers in its response when serving the files:

    Cross-Origin-Opener-Policy: same-origin
    Cross-Origin-Embedder-Policy: require-corp

One simple way to get the demo apps up and running on Unix-style
systems is to install althttpd (https://sqlite.org/althttpd) and run:

    althttpd --enable-sab --page index.html

