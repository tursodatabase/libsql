This directory houses the [Web Assembly (WASM)](https://en.wikipedia.org/wiki/WebAssembly)
parts of the sqlite3 build.

It requires [emscripten][] and that the build environment be set up for
emscripten. A mini-HOWTO for setting that up follows...

First, install the Emscripten SDK, as documented
[here](https://emscripten.org/docs/getting_started/downloads.html) and summarized
below for Linux environments:

```
# Clone the emscripten repository:
$ sudo apt install git
$ git clone https://github.com/emscripten-core/emsdk.git
$ cd emsdk

# Download and install the latest SDK tools:
$ ./emsdk install latest

# Make the "latest" SDK "active" for the current user:
$ ./emsdk activate latest
```

Those parts only need to be run once, but the SDK can be updated using:

```
$ git pull
$ ./emsdk install latest
$ ./emsdk activate latest
```

The following needs to be run for each shell instance which needs the
`emcc` compiler:

```
# Activate PATH and other environment variables in the current terminal:
$ source ./emsdk_env.sh

$ which emcc
/path/to/emsdk/upstream/emscripten/emcc
```

Optionally, add that to your login shell's resource file (`~/.bashrc`
or equivalent).

That `env` script needs to be sourced for building this application
from the top of the sqlite3 build tree:

```
$ make fiddle
```

Or:

```
$ cd ext/wasm
$ make
```

That will generate the a number of files required for a handful of
test and demo applications which can be accessed via
`index.html`. WASM content cannot, due to XMLHttpRequest security
limitations, be loaded if the containing HTML file is opened directly
in the browser (i.e. if it is opened using a `file://` URL), so it
needs to be served via an HTTP server.  For example, using
[althttpd][]:

```
$ cd ext/wasm
$ althttpd --enable-sab --max-age 1 --page index.html
```

That will open the system's browser and run the index page, from which
all of the test and demo applications can be accessed.

Note that when serving this app via [althttpd][], it must be a version
from 2022-09-26 or newer so that it recognizes the `--enable-sab`
flag, which causes althttpd to emit two HTTP response headers which
are required to enable JavaScript's `SharedArrayBuffer` and `Atomics`
APIs. Those APIs are required in order to enable the OPFS-related
features in the apps which use them.

# Testing on a remote machine that is accessed via SSH

*NB: The following are developer notes, last validated on 2023-07-19*

  *  Remote: Install git, emsdk, and althttpd
     *  Use a [version of althttpd][althttpd] from
        September 26, 2022 or newer.
  *  Remote: Install the SQLite source tree.  CD to ext/wasm
  *  Remote: "`make`" to build WASM
  *  Remote: `althttpd --enable-sab --port 8080 --popup`
  *  Local:  `ssh -L 8180:localhost:8080 remote`
  *  Local:  Point your web-browser at http://localhost:8180/index.html

In order to enable [SharedArrayBuffer][], the web-browser requires
that the two extra Cross-Origin lines be present in HTTP reply headers
and that the request must come from "localhost" (_or_ over an SSL
connection).  Since the web-server is on a different machine from the
web-broser, the localhost requirement means that the connection must
be tunneled using SSH.


[emscripten]: https://emscripten.org
[althttpd]: https://sqlite.org/althttpd
[SharedArrayBuffer]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer
