# Notes On Compiling SQLite On Windows 11

Here are step-by-step instructions on how to build SQLite from
canonical source on a new Windows 11 PC, as of 2023-08-16:

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

  3.  Install TCL development libraries.  This note assumes that you wil
      install the TCL development libraries in the "`c:\Tcl`" directory.
      Make adjustments
      if you want TCL installed somewhere else.  SQLite needs both the
      "tclsh.exe" command-line tool as part of the build process, and
      the "tcl86.lib" library in order to run tests.  You will need
      TCL version 8.6 or later.
      <ol type="a">
      <li>Get the TCL source archive, perhaps from
      [https://www.tcl.tk/software/tcltk/download.html](https://www.tcl.tk/software/tcltk/download.html).
      <li>Untar or unzip the source archive.  CD into the "win/" subfolder
          of the source tree.
      <li>Run: `nmake /f makefile.vc release`
      <li>Run: `nmake /f makefile.vc INSTALLDIR=c:\Tcl install`
      <li>CD to `c:\Tcl\lib`.  In that subfolder make a copy of the
          "`tcl86t.lib`" file to the alternative name "`tcl86.lib`"
          (omitting the second 't').  Leave the copy in the same directory
          as the original.
      <li>CD to `c:\Tcl\bin`.  Make a copy of the "`tclsh86t.exe`"
          file into "`tclsh.exe`" (without the "86t") in the same directory.
      <li>Add `c:\Tcl\bin` to your %PATH%.  To do this, go to Settings
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

  6.  Run the "`Makefile.msc`" makefile with an appropriate target.
      Examples:
      <ul>
      <li>  `nmake /f makefile.msc`
      <li>  `nmake /f makefile.msc sqlite3.c`
      <li>  `nmake /f makefile.msc devtest`
      <li>  `nmake /f makefile.msc releasetest`
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
