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
    [Fossil](https://fossil-scm.org/) installed.
<li> Check out source code and set environment variables:
   <ol type="a">
   <li> **TCLSOURCE** &rarr;
    The top-level directory of a Fossil check-out of the TCL source tree.
   <li> **SQLITESOURCE** &rarr;
    A Fossil check-out of the SQLite source tree.
   <li> **TCLBUILD** &rarr;
    A directory that does not exist at the start of the test and which
    will be deleted at the end of the test, and that will contain the
    test builds of the TCL libraries and the SQLite TCL Extensions.
   </ol>
</ol>

### 2.2 Testing TCL 8.6 on unix

<ol type="1">
<li value="3">  `mkdir -p $TCLBUILD/tcl86`
<li>  `cd $TCLSOURCE/unix`
<li>  `fossil up core-8-6-16` <br>
      &uarr; Or some other version of Tcl8.6.
<li>  `fossil clean -x`
<li>  `./configure --prefix=$TCLBUILD/tcl86 --disable-shared` <br>
      &uarr; The --disable-shared is to avoid the need to set LD_LIBRARY_PATH
      when using this Tcl build.
<li>  `make install`
<li> `cd $SQLITESOURCE`
<li> `fossil clean -x`
<li> `./configure --with-tclsh=$TCLBUILD/tcl86/bin/tclsh8.6 --all`
<li> `make tclextension-install` <br>
     &uarr; Verify extension installed at $TCLBUILD/tcl86/lib/tcl8.6/sqlite3.*
<li> `make tclextension-list` <br>
     &uarr; Verify TCL extension correctly installed.
<li> `make tclextension-verify` <br>
     &uarr; Verify that the correct version is installed.
<li> `$TCLBUILD/tcl86/bin/tclsh8.6 test/testrunner.tcl release --explain` <br>
     &uarr; Verify thousands of lines of output with no errors. Or
     consider running "devtest" without --explain instead of "release".
</ol>

### 2.3 Testing TCL 9.0 on unix

<ol>
<li value="16">  `mkdir -p $TCLBUILD/tcl90`
<li>  `fossil up core-9-0-0` <br>
      &uarr; Or some other version of Tcl9
<li>  `fossil clean -x`
<li>  `./configure --prefix=$TCLBUILD/tcl90 --disable-shared` <br>
      &uarr; The --disable-shared is to avoid the need to set LD_LIBRARY_PATH
      when using this Tcl build.
<li>  `make install`
<li>  `cp -r ../library $TCLBUILD/tcl90/lib/tcl9.0` <br>
      &uarr; The Tcl library is not installed by "make install" for Tcl9.0 unless
      you also include the --disable-zipfs to ./configure.  But if you do that
      then the generated tclsh9.0 is no longer stand-alone.  On the other hand,
      if you don't install the Tcl library, other programs like testfixture
      won't be able to find the Tcl library and hence won't work.  This
      extra installation step resolves the dilemma.
      This step is not required when building Tcl8.6, which lacks support for
      zipfs and hence always installs its Tcl library.
<li> `cd $SQLITESOURCE`
<li> `fossil clean -x`
<li> `./configure --with-tclsh=$TCLBUILD/tcl90/bin/tclsh9.0 --all`
<li> `make tclextension-install` <br>
     &uarr; Verify extension installed at $TCLBUILD/tcl90/lib/sqlite3.*
<li> `make tclextension-list` <br>
     &uarr; Verify TCL extension correctly installed.
<li> `make tclextension-verify`
<li> `$TCLBUILD/tcl90/bin/tclsh9.0 test/testrunner.tcl release --explain` <br>
     &uarr; Verify thousands of lines of output with no errors.  Or
     consider running "devtest" without --explain instead of "release".
</ol>

### 2.4 Cleanup

<ol type="1">
<li value="29"> `rm -rf $TCLBUILD`
</ol>

<a id="windows"></a>
## 3.0 Testing On Windows

See also the [](./compile-for-windows.md) document which provides another
perspective on how to compile SQLite on Windows.

###  3.1 Setup for Windows

<ol type="1">
<li value="1"> 
    [Fossil](https://fossil-scm.org/) installed.
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
