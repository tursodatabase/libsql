# Test Procedures For The SQLite TCL Extension

## 1.0 Background

The SQLite TCL extension logic (in the 
"[tclsqlite.c](/file/src/tclsqlite.c)" source
file) is statically linked into "textfixture" executable
which is the program used to do most of the testing
associated with "make test", "make devtest", and/or
"make releasetest".  So the functionality of the SQLite
TCL extension is thoroughly vetted during normal testing.  The
procedures below are designed to test the loadable extension
aspect of the SQLite TCL extension, and in particular to verify
that the "make tclextension-install" build target works and that
an ordinary tclsh can subsequently run "package require sqlite3".

This procedure can also be used as a template for how to set up
a local TCL+SQLite development environment.  In other words, it
can be be used as a guide on how to compile per-user copies of 
Tcl that are used to develop, test, and debug SQLite.  In that
case, perhaps make minor changes to the procedure such as:

  *  Make TCLBUILD directory is permanent.
  *  Enable debugging symbols on the Tcl library build.
  *  Reduce the optimization level to -O0 for easier debugging.
  *  Also compile "wish" to go with each "tclsh".


<a id="unix"></a>
## 2.0 Testing On Unix-like Systems (Including Mac)

See also the [](./compile-for-unix.md) document which provides another
perspective on how to compile SQLite on unix-like systems.

###  2.1 Setup

<ol type="1">
<li value="1">
    [Fossil][] installed.
<li> Check out source code and set environment variables:
   <ol type="a">
   <li> **TCLSOURCE** &rarr;
    The top-level directory of a [Fossil][] check-out of the
    [TCL source tree][tcl-fossil].
   <li> **SQLITESOURCE** &rarr;
    A Fossil check-out of the SQLite source tree.
   <li> **TCLHOME** &rarr;
    A directory that does not exist at the start of the test and which
    will be deleted at the end of the test, and that will contain the
    test builds of the TCL libraries and the SQLite TCL Extensions.
    It is the top-most installation directory, i.e. the one provided
    to Tcl's `./configure --prefix=/path/to/tcl`.
   <li> **TCLVERSION** &rarr;
    The `X.Y`-form version of Tcl being used: 8.6, 9.0, 9.1...
   </ol>
</ol>

### 2.2 Testing TCL 8.x and 9.x on unix

From a checked-out copy of [the core Tcl tree][tcl-fossil]

<ol type="1">
<li value="3">`TCLVERSION=8.6` <br>
      &uarr; A version of your choice. This process has been tested with
      values of 8.6, 9.0, and 9.1 (as of 2025-04-16). The out-of-life
      version 8.5 fails some of `make devtest` for undetermined reasons.
<li>`TCLHOME=$HOME/tcl/$TCLVERSION`
<li>`TCLSOURCE=/path/to/tcl/checkout`
<li>`SQLITESOURCE=/path/to/sqlite/checkout`
<li>`rm -fr $TCLHOME` <br>
      &uarr; Ensure that no stale Tcl installation is laying around.
<li> `cd $TCLSOURCE`
<li>  `fossil up core-8-6-branch` <br>
      &uarr; The branch corresponding to `$TCLVERSION`, e.g.
      `core-9-0-branch` or `trunk`.
<li>  `fossil clean -x`
<li>  `cd unix`
<li>  `./configure --prefix=$TCLHOME --disable-shared` <br>
      &uarr; The `--disable-shared` is to avoid the need to set `LD_LIBRARY_PATH`
      when using this Tcl build.
<li>  `make install`
<li> `cd $SQLITESOURCE`
<li> `fossil clean -x`
<li> `./configure --with-tcl=$TCLHOME --all`
<li> `make tclextension-install` <br>
     &uarr; Verify extension installed at
     `$TCLHOME/lib/tcl${TCLVERSION}/sqlite<SQLITE_VERSION>`.
<li> `make tclextension-list` <br>
     &uarr; Verify TCL extension correctly installed.
<li> `make tclextension-verify` <br>
     &uarr; Verify that the correct version is installed.
<li> `$TCLHOME/bin/tclsh[89].[0-9] test/testrunner.tcl release --explain` <br>
     &uarr; Verify thousands of lines of output with no errors. Or
     consider running "devtest" without --explain instead of "release".
</ol>

### 2.3 Cleanup

<ol type="1">
<li value="29"> `rm -rf $TCLHOME`
</ol>

<a id="windows"></a>
## 3.0 Testing On Windows

See also the [](./compile-for-windows.md) document which provides another
perspective on how to compile SQLite on Windows.

###  3.1 Setup for Windows

(These docs are not as up-to-date as the Unix docs, above.)

<ol type="1">
<li value="1">
    [Fossil][] installed.
<li>
    Unix-like command-line tools installed.  Example:
    [unxutils](https://unxutils.sourceforge.net/)
<li> [Visual Studio](https://visualstudio.microsoft.com/vs/community/)
     installed.  VS2015 or later required.
<li> Check out source code and set environment variables.
   <ol type="a">
   <li> **TCLSOURCE** &rarr;
    The top-level directory of a Fossil check-out of the TCL source tree.
   <li> **SQLITESOURCE** &rarr;
    A Fossil check-out of the SQLite source tree.
   <li> **TCLBUILD** &rarr;
    A directory that does not exist at the start of the test and which
    will be deleted at the end of the test, and that will contain the
    test builds of the TCL libraries and the SQLite TCL Extensions.
    <li> **ORIGINALPATH** &rarr;
    The original value of %PATH%.  In other words, set as follows:
    `set ORIGINALPATH %PATH%`
   </ol>
</ol>

### 3.2 Testing TCL 8.6 on Windows

<ol type="1">
<li value="5">  `mkdir %TCLBUILD%\tcl86`
<li>  `cd %TCLSOURCE%\win`
<li>  `fossil up core-8-6-16` <br>
      &uarr; Or some other version of Tcl8.6.
<li>  `fossil clean -x`
<li>  `set INSTALLDIR=%TCLBUILD%\tcl86`
<li>  `nmake /f makefile.vc release` <br>
      &udarr; You *must* invoke the "release" and "install" targets
      using separate "nmake" commands or tclsh86t.exe won't be
      installed.
<li>  `nmake /f makefile.vc install`
<li> `cd %SQLITESOURCE%`
<li> `fossil clean -x`
<li> `set TCLDIR=%TCLBUILD%\tcl86`
<li> `set PATH=%TCLBUILD%\tcl86\bin;%ORIGINALPATH%`
<li> `set TCLSH_CMD=%TCLBUILD%\tcl86\bin\tclsh86t.exe`
<li> `nmake /f Makefile.msc tclextension-install` <br>
     &uarr; Verify extension installed at %TCLBUILD%\\tcl86\\lib\\tcl8.6\\sqlite3.*
<li> `nmake /f Makefile.msc tclextension-verify`
<li>`tclsh86t test/testrunner.tcl release --explain` <br>
     &uarr; Verify thousands of lines of output with no errors.  Or
     consider running "devtest" without --explain instead of "release".
</ol>

### 3.3 Testing TCL 9.0 on Windows

<ol>
<li value="20">  `mkdir %TCLBUILD%\tcl90`
<li>  `cd %TCLSOURCE%\win`
<li>  `fossil up core-9-0-0` <br>
      &uarr; Or some other version of Tcl9
<li>  `fossil clean -x`
<li>  `set INSTALLDIR=%TCLBUILD%\tcl90`
<li>  `nmake /f makefile.vc release` <br>
      &udarr; You *must* invoke the "release" and "install" targets
      using separate "nmake" commands or tclsh90.exe won't be
      installed.
<li>  `nmake /f makefile.vc install`
<li> `cd %SQLITESOURCE%`
<li> `fossil clean -x`
<li> `set TCLDIR=%TCLBUILD%\tcl90`
<li> `set PATH=%TCLBUILD%\tcl90\bin;%ORIGINALPATH%`
<li> `set TCLSH_CMD=%TCLBUILD%\tcl90\bin\tclsh90.exe`
<li> `nmake /f Makefile.msc tclextension-install` <br>
     &uarr; Verify extension installed at %TCLBUILD%\\tcl90\\lib\\sqlite3.*
<li> `nmake /f Makefile.msc tclextension-verify`
<li> `tclsh90 test/testrunner.tcl release --explain` <br>
     &uarr; Verify thousands of lines of output with no errors.  Or
     consider running "devtest" without --explain instead of "release".
</ol>

### 3.4 Cleanup

<ol type="1">
<li value="35"> `rm -rf %TCLBUILD%`
</ol>

## 4.0 Testing the TEA(ish) Build (unix only)

This part requires following the setup instructions for Unix systems,
at the top of this document.

The former TEA, now TEA(ish), build of this extension uses the same
code as the builds described above but is provided in a form more
convenient for downstream Tcl users.

It lives in `autoconf/tea` and, as part of the `autoconf` bundle,
_cannot be tested directly from the canonical tree_. Instead it has to
be packaged.

### 4.1 Teaish Setup

Follow the same Tcl- and environment-related related setup described
in the first section of this document, up to and including the
installation of Tcl (unless, of course, it was already installed using
those same instructions).

### 4.2 Teaish Testing

<ol>
<li>`cd $SQLITESOURCE`
<li>Run either `make snapshot-tarball` or `make amalgamation-tarball`
    &uarr;
    Those steps will leave behind a temp dir called `mkpkg_tmp_dir`,
    under which the extension is most readily reached. It can optionally
    be extracted from the generated tarball, but that tarball was
    generated from this dir, and reusing this dir is a time saver
    during development.
<li> `cd mkpkg_tmp/tea`
<li> `./configure --with-tcl=$TCLHOME`
<li> `make test install` <br>
     &uarr; Should run to completion without any errors.
<li> `make uninstall` <br>
     &uarr; Will uninstall the extension. This _can_ be run
     in the same invocation as the `install` target, but only
     if the `-j#` make flag is _not_ used. If it is, the
     install/uninstall steps will race and make a mess of things.
     Parallel builds do not help in this build, anyway, as there's
     only a single C file to compile.
</ol>

When actively developing and testing the teaish build, which requires
going through the tarball generation, there's a caveat about the
`mkpkg_tmp_dir` dir: it will be deleted every time a tarball is
built, the shell console which is parked in that
directory for testing needs to add `cd $PWD &&` to the start of the
build commands, like:

>
```
[user@host:.../mkpkg_tmp_dir/tea]$ \
  cd $PWD && ./configure CFLAGS=-O0 --with-tcl=$TCLHOME \
  && make test install uninstall
```

### 4.3 Teaish Cleanup


<ol type="1">
<li> `rm -rf $TCLHOME`
<li> `cd $SQLITESOURCE; rm -fr mkpkg_tmp_dir; fossil clean -x`
</ol>

[Fossil]: https://fossil-scm.org/home
[tcl-fossil]: https://core.tcl-lang.org/tcl
