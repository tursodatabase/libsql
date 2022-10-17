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

That will generate the fiddle application under
[ext/fiddle](/dir/ext/wasm/fiddle), as `fiddle.html`. That application
cannot, due to XMLHttpRequest security limitations, run if the HTML
file is opened directly in the browser (i.e. if it is opened using a
`file://` URL), so it needs to be served via an HTTP server.  For
example, using [althttpd][]:

```
$ cd ext/wasm/fiddle
$ althttpd -page fiddle.html
```

That will open the system's browser and run the fiddle app's page.

Note that when serving this app via [althttpd][], it must be a version
from 2022-05-17 or newer so that it recognizes the `.wasm` file
extension and responds with the mimetype `application/wasm`, as the
WASM loader is pedantic about that detail.

# Testing on a remote machine that is accessed via SSH

*NB: The following are developer notes, last validated on 2022-08-18*

  *  Remote: Install git, emsdk, and althttpd
     *  Use a [version of althttpd](https://sqlite.org/althttpd/timeline?r=enable-atomics)
        that adds HTTP reply header lines to enable SharedArrayBuffers.  These header
        lines are required:
```
            Cross-Origin-Opener-Policy: same-origin
            Cross-Origin-Embedder-Policy: require-corp
```
  *  Remote: Install the SQLite source tree.  CD to ext/wasm
  *  Remote: "`make`" to build WASM
  *  Remote: althttpd --port 8080 --popup
  *  Local:  ssh -L 8180:localhost:8080 remote
  *  Local:  Point your web-browser at http://localhost:8180/testing1.html

In order to enable [SharedArrayBuffers](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer),
the web-browser requires that the two extra Cross-Origin lines be present
in HTTP reply headers and that the request must come from "localhost".
Since the web-server is on a different machine from
the web-broser, the localhost requirement means that the connection must be tunneled
using SSH.



# Known Quirks and Limitations

Some "impedence mismatch" between C and WASM/JavaScript is to be
expected.

## No I/O

sqlite3 shell commands which require file I/O or pipes are disabled in
the WASM build.

## `exit()` Triggered from C

When C code calls `exit()`, as happens (for example) when running an
"unsafe" command when safe mode is active, WASM's connection to the
sqlite3 shell environment has no sensible choice but to shut down
because `exit()` leaves it in a state we can no longer recover
from. The JavaScript-side application attempts to recognize this and
warn the user that restarting the application is necessary. Currently
the only way to restart it is to reload the page. Restructuring the
shell code such that it could be "rebooted" without restarting the
JS app would require some invasive changes which are not currently
on any TODO list but have not been entirely ruled out long-term.


[emscripten]: https://emscripten.org
[althttpd]: https://sqlite.org/althttpd
