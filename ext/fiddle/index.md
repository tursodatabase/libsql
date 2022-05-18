This directory houses a "fiddle"-style application which embeds a
[Web Assembly (WASM)](https://en.wikipedia.org/wiki/WebAssembly)
build of the sqlite3 shell app into an HTML page, effectively running
the shell in a client-side browser.

It requires [emscripten][] and that the build environment be set up for
emscripten. A mini-HOWTO for setting that up follows...

First, install the Emscripten SDK, as documented
[here](https://emscripten.org/docs/getting_started/downloads.html) and summarized
below for Linux environments:

```
# Clone the emscripten repository:
$ git clone https://github.com/emscripten-core/emsdk.git
$ cd emsdk

# Download and install the latest SDK tools:
$ ./emsdk install latest

# Make the "latest" SDK "active" for the current user:
$ ./emsdk activate latest
```

Those parts only need to be run once. The following needs to be run for each
shell instance which needs the `emcc` compiler:

```
# Activate PATH and other environment variables in the current terminal:
$ source ./emsdk_env.sh

$ which emcc
/path/to/emsdk/upstream/emscripten/emcc
```

That `env` script needs to be sourced for building this application from the
top of the sqlite3 build tree:

```
$ make fiddle
```

Or:

```
$ cd ext/fiddle
$ make
```

That will generate the fiddle application under
[ext/fiddle](/dir/ext/fiddle), as `fiddle.html`. That application
cannot, due to XMLHttpRequest security limitations, run if the HTML
file is opened directly in the browser (i.e. if it is opened using a
`file://` URL), so it needs to be served via an HTTP server.  For
example, using [althttpd][]:

```
$ cd ext/fiddle
$ althttpd -debug 1 -jail 0 -port 9090 -root .
```

Then browse to `http://localhost:9090/fiddle.html`.

Note that when serving this app via [althttpd][], it must be a version
from 2022-05-17 or newer so that it recognizes the `.wasm` file
extension and responds with the mimetype `application/wasm`, as the
wasm loader is pedantic about that detail.


[emscripten]: https://emscripten.org
[althttpd]: https://sqlite.org/althttpd
