This directory houses the [Web Assembly (WASM)](https://en.wikipedia.org/wiki/WebAssembly)
parts of the sqlite3 build.

It requires [emscripten][] and that the build environment be set up
for emscripten. _Release_ builds also require [the wabt tools](#wabt),
but dev builds do not. A mini-HOWTO for setting that up follows...

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

(Sidebar: Emscripten updates can and do _change things_, i.e. _break
things_, so it's considered _required practice_ to test thoroughly
after upgrading it! Our build process makes no guarantees about which
Emscripten version(s) will or won't work, but it's important that
production builds are built using a compatible version. During active
development, the EMSDK is frequently updated, the goal being to keep
`sqlite3.wasm` working with "the latest" EMSDK.)

The SQLite configure script will search for the EMSDK. One way
to ensure that it finds it is:

```
# Activate PATH and other environment variables in the current terminal:
$ source ./emsdk_env.sh

$ which emcc
/path/to/emsdk/upstream/emscripten/emcc

$ ./configure ...
```

Optionally, add that `source` part to your login shell's resource file
(`~/.bashrc` or equivalent).

Another way is to pass the EMSDK dir to configure:

```
$ ./configure --with-emsdk=/path/to/emsdk
```

The build tree uses a small wrapper for invoking the `emcc` (the
Emscripten compiler): `tool/emcc.sh` is generated from
`tool/emcc.sh.in` using the EMSDK path found by the configure process.

With that in place, the most common build approaches are:

```
# From the top of the tree:
$ make fiddle
```

Or:

```
$ cd ext/wasm
$ make
```

Those will generate the a number of files required for a handful of
test and demo applications which can be accessed via
`index.html`. WASM content cannot, due to XMLHttpRequest security
limitations, be loaded if the containing HTML file is opened directly
in the browser (i.e. if it is opened using a `file://` URL), so it
needs to be served via an HTTP server.  For example, using
[althttpd][]:

```
$ cd ext/wasm
$ althttpd --enable-sab --max-age 1 --page index.html
# Or, more simply, from the ext/wasm dir:
$ make httpd
```

That will open the system's browser and visit the index page, from
which (almost) all of the test and demo applications can be accessed.
(`ext/wasm/SQLTester` is not listed in that page because it's only of
real utility when it's used in conjunction with the project's
proprietary test suite, which most users don't have access to.)

When serving this app via [althttpd][], it must be a version from
2022-09-26 or newer so that it recognizes the `--enable-sab` flag,
which causes althttpd to emit two HTTP response headers which are
required to enable JavaScript's `SharedArrayBuffer` and `Atomics`
APIs. Those APIs are required in order to enable the [OPFS][]-related
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

<a id="wabt"></a>
# The wabt Tools

_Release_ builds require the wabt tools:

<https://github.com/WebAssembly/wabt>

Specifically, we need `wasm-strip` so that the resulting WASM file is not
several megabytes.

Pre-built binaries can be downloaded from:

<https://github.com/WebAssembly/wabt/releases>

As of 2025-10-14, versions 1.36.0 or higher are known to work and
1.34.0 is known to not work with current Emscripten output.


[emscripten]: https://emscripten.org
[althttpd]: https://sqlite.org/althttpd
[SharedArrayBuffer]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/SharedArrayBuffer
[OPFS]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system
