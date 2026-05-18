# Notes On Compiling SQLite On All Kinds Of Unix

Here are step-by-step instructions on how to build SQLite from
canonical source on any modern machine that isn't Windows.  These
notes are tested (on 2024-10-11) on Ubuntu and on MacOS, but they
are general and should work on most any modern unix platform.

  1.  Install a C-compiler.  GCC or Clang both work fine.  If you are
      reading this document, you've probably already done that.

  2.  Install TCL9 development libraries.  In this note, we'll do a
      private install in the $HOME/local directory, but you can make
      adjustments to install TCL9 wherever you like.
      <p>
      This document assumes you are working with <b>TCL version 9.0</b>.
      <ol type="a">
      <li>Get the TCL source archive, perhaps from
      <https://www.tcl.tk/software/tcltk/download.html>
      or <https://sqlite.org/tmp/tcl9.0.0.tar.gz>.
      <li>Untar the source archive.  CD into the "unix/" subfolder
          of the source tree.
      <li>Run: `mkdir $HOME/local`
      <li>Run: `./configure --prefix=$HOME/local`
      <li>Run: `make install`
      </ol>

  4.  Download the SQLite source tree and unpack it. CD into the
      toplevel directory of the source tree.

  5.  Run: `./configure --enable-all --with-tclsh=$HOME/local/bin/tclsh9.0`

      You do not need to use --with-tclsh if the tclsh you want to use is the
      first one on your PATH.

  6.  Run the "`Makefile`" makefile with an appropriate target.
      Examples:
      <ul>
      <li>  `make sqlite3.c`
      <li>  `make sqlite3`
      <li>  `make sqldiff`
      <li>  `make sqlite3_rsync`
      <li>  `make tclextension-install`
      <li>  `make devtest`
      <li>  `make releasetest`
      <li>  `make sqlite3_analyzer`
      </ul>

      It is not required that you run the "tclextension-install" target prior to
      running tests.  However, the tests will run more smoothly if you do.
      The version of SQLite used for the TCL extension does *not* need to
      correspond to the version of SQLite under test.  So you can install the
      SQLite TCL extension once, and then use it to test many different versions
      of SQLite.


  7.  For a debugging build of the CLI, where the ".treetrace" and ".wheretrace"
      commands work, add the the --enable-debug argument to configure.
