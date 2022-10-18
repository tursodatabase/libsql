This is the README for the sqlite3 WASM/JS distribution.

Main project page: https://sqlite.org

TODO: link to main WASM/JS docs, once they are online

This archive contains two related deliverables:

- ./main contains the sqlite3.js and sqlite3.wasm file which make up
  the standard sqlite3 WASM/JS build.

- ./wasmfs contains a build of those files which includes the
  Emscripten WASMFS[^1]. It offers an alternative approach
  to accessing the browser-side Origin-Private FileSystem
  but is less portable than the main build, so is provided
  as a separate binary.

Both directories contain small demonstration apps. Browsers will not
server WASM files from file:// URLs, so the demonstrations require a
web server and that server must include the following headers in its
response when serving the files:

    Cross-Origin-Opener-Policy: same-origin
    Cross-Origin-Embedder-Policy: require-corp

[^1]: https://emscripten.org
