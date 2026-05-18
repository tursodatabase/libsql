# Notes On Compiling SQLite On Windows 11

Here are step-by-step instructions on how to build SQLite from
canonical source on a new Windows 11 PC, as of 2024-10-09:

  1.  Install Microsoft Visual Studio. The free "community edition" 
      will work fine.  Do a standard install for C++ development.
      SQLite only needs the
      "cl" compiler and the "nmake" build tool.

  2.  Under the "Start" menu, find "All Apps" then go to "Visual Studio 20XX"
      and find "x64 Native Tools Command Prompt for VS 20XX".  Pin that
      application to your task bar, as you will use it a lot.  Bring up
      an instance of this command prompt and do all of the subsequent steps
      in that "x64 Native Tools" command prompt.  (Or use "x86" if you want
      a 32-bit build.)  The subsequent steps will not work in a vanilla
      DOS prompt.  Nor will they work in PowerShell.

  3.  Install TCL development libraries.  This note assumes that you will
      install the TCL development libraries in the "`c:\Tcl`" directory.
      Make adjustments
      if you want TCL installed somewhere else.  SQLite needs both the
      "tclsh90.exe" command-line tool as part of the build process, and
      the "tcl90.lib" and "tclstub.lib" libraries in order to run tests.
      This document assumes you are working with <b>TCL version 9.0</b>.
      See versions of this document from prior to 2024-10-10 for
      instructions on how to build using TCL version 8.6.
      <ol type="a">
      <li>Get the TCL source archive, perhaps from
      <https://www.tcl.tk/software/tcltk/download.html>
      or <https://sqlite.org/tmp/tcl9.0.0.tar.gz>.
      <li>Untar or unzip the source archive.  CD into the "win/" subfolder
          of the source tree.
      <li>Run: `nmake /f makefile.vc release`
      <li>Run: `nmake /f makefile.vc INSTALLDIR=c:\Tcl install`
      <li><i>Optional:</i> CD to `c:\Tcl\bin` and make a copy of
          `tclsh90.exe` over into just `tclsh.exe`.
      <li><i>Optional:</i>
          Add `c:\Tcl\bin` to your %PATH%.  To do this, go to Settings
          and search for "path".  Select "edit environment variables for
          your account" and modify your default PATH accordingly.
          You will need to close and reopen your command prompts after
          making this change.
      </ol>

  4.  Download the SQLite source tree and unpack it. CD into the
      toplevel directory of the source tree.

  5.  Set the TCLDIR environment variable to point to your TCL installation.
      Like this:
      <ul>
      <li> `set TCLDIR=c:\Tcl`
      </ul>

      If you install TCL in the "`c:\Tcl`" directory (as recommended
      in step 3 above), then this step is optional because
      "`c:\Tcl`" is the default value for TCLDIR.  You can also skip this
      step by specifying "`TCLDIR=c:\Tcl`" as an argument to the nmake
      commands in step 6 below.

  6.  Run the "`Makefile.msc`" makefile with an appropriate target.
      Examples:
      <ul>
      <li>  `nmake /f makefile.msc`
      <li>  `nmake /f makefile.msc sqlite3.c`
      <li>  `nmake /f makefile.msc sqlite3.exe`
      <li>  `nmake /f makefile.msc sqldiff.exe`
      <li>  `nmake /f makefile.msc sqlite3_rsync.exe`
      <li>  `nmake /f makefile.msc tclextension-install`
      <li>  `nmake /f makefile.msc devtest`
      <li>  `nmake /f makefile.msc releasetest`
      <li>  `nmake /f makefile.msc sqlite3_analyzer.exe`
      </ul>

      It is not required that you run the "tclextension-install" target prior to
      running tests.  However, the tests will run more smoothly if you do.
      The version of SQLite used for the TCL extension does *not* need to
      correspond to the version of SQLite under test.  So you can install the
      SQLite TCL extension once, and then use it to test many different versions
      of SQLite.


  7.  For a debugging build of the CLI, where the ".treetrace" and ".wheretrace"
      commands work, add the DEBUG=3 argument to nmake.  Like this:
      <ul>
      <li> `nmake /f makefile.msc DEBUG=3 clean sqlite3.exe`
      </ul>
   

## 32-bit Builds

Doing a 32-bit build is just like doing a 64-bit build with the
following minor changes:

  1.  Use the "x86 Native Tools Command Prompt" instead of
      "x64 Native Tools Command Prompt".  "**x86**" instead of "**x64**".

  2.  Use a different installation directory for TCL.
      The recommended directory is `c:\tcl32`.  Thus you end up
      with two TCL builds:
      <ul>
      <li> `c:\tcl` &larr;  64-bit (the default)
      <li> `c:\tcl32` &larr;  32-bit
      </ul>

  3.  Ensure that `c:\tcl32\bin` comes before `c:\tcl\bin` on
      your PATH environment variable.  You can achieve this using
      a command like:
      <ul>
      <li>  `set PATH=c:\tcl32\bin;%PATH%`
      </ul>

## Building a DLL

The command the developers use for building the deliverable DLL on the 
[download page](https://sqlite.org/download.html) is as follows:

> ~~~~
nmake /f Makefile.msc sqlite3.dll USE_NATIVE_LIBPATHS=1 "OPTS=-DSQLITE_ENABLE_FTS3=1 -DSQLITE_ENABLE_FTS4=1 -DSQLITE_ENABLE_FTS5=1 -DSQLITE_ENABLE_RTREE=1 -DSQLITE_ENABLE_JSON1=1 -DSQLITE_ENABLE_GEOPOLY=1 -DSQLITE_ENABLE_SESSION=1 -DSQLITE_ENABLE_PREUPDATE_HOOK=1 -DSQLITE_ENABLE_SERIALIZE=1 -DSQLITE_ENABLE_MATH_FUNCTIONS=1"
~~~~

That command generates both the sqlite3.dll and sqlite3.def files.  The same
command works for both 32-bit and 64-bit builds.

## Statically Linking The TCL Library

Some utility programs associated with SQLite need to be linked
with TCL in order to function.  The [sqlite3_analyzer.exe program](https://sqlite.org/sqlanalyze.html)
is an example.  You can build as described above, and then
enter:

> ~~~~
nmake /f Makefile.msc sqlite3_analyzer.exe
~~~~

And you will end up with a working executable.  However, that executable
will depend on having the "tcl98.dll" library somewhere on your %PATH%.
Use the following steps to build an executable that has the TCL library
statically linked so that it does not depend on separate DLL:

  1.  Use the appropriate "Command Prompt" window - either x86 or
      x64, depending on whether you want a 32-bit or 64-bit executable.

  2.  Untar the TCL source tarball into a fresh directory.  CD into
      the "win/" subfolder.

  3.  Run: `nmake /f makefile.vc OPTS=static shell`

  4.  CD into the "Release*" subfolder that is created (note the
      wildcard - the full name of the directory might vary).  There
      you will find the "tcl90s.lib" file.  Copy this file into the
      same directory that you put the "tcl90.lib" on your initial
      installation.  (In this document, that directory is
      "`C:\Tcl32\lib`" for 32-bit builds and
      "`C:\Tcl\lib`" for 64-bit builds.)

  5.  CD into your SQLite source code directory and build the desired
      utility program, but add the following extra argument to the
      nmake command line:
      <blockquote><pre>
      STATICALLY_LINK_TCL=1
      </pre></blockquote>
      <p>So, for example, to build a statically linked version of
      sqlite3_analyzer.exe, you might type:
      <blockquote><pre>
      nmake /f Makefile.msc STATICALLY_LINK_TCL=1 sqlite3_analyzer.exe
      </pre></blockquote>

  6.  After your executable is built, you can verify that it does not
      depend on the TCL DLL by running:
      <blockquote><pre>
      dumpbin /dependents sqlite3_analyzer.exe
      </pre></blockquote>
