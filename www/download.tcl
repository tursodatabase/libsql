#
# Run this TCL script to generate HTML for the download.html file.
#
set rcsid {$Id: download.tcl,v 1.8 2004/05/31 16:04:08 drh Exp $}
source common.tcl
header {SQLite Download Page}

puts {
<h2>SQLite Download Page</h1>
<table width="100%" cellpadding="5">
}


proc Product {file desc} {
  if {![file exists $file]} return
  set size [file size $file]
  puts [subst {
<tr>
<td width="10"></td>
<td align="right" valign="top">
<a href="$file">$file</a><br>($size bytes)</td>
<td width="5"></td>
<td valign="top">[string trim $desc]</td>
</tr>}]
}

proc Heading {title} {
  puts "<tr><td colspan=4><big><b>$title</b></big></td></tr>"
}

Heading {Precompiled Binaries for Linux}

Product sqlite.bin.gz {
  A command-line program for accessing and modifing SQLite databases.
  See <a href="sqlite.html">the documentation</a> for additional information.
}

Product tclsqlite.so.gz {
  Bindings for TCL.  You can import this shared library into either
  tclsh or wish to get SQLite database access from Tcl/Tk.
  See <a href="tclsqlite.html">the documentation</a> for details.
}

Product sqlite.so.gz {
  A precompiled shared-library for Linux.  This is the same as
  <b>tclsqlite.so.gz</b> but without the TCL bindings.
}

cd doc
foreach name [lsort -dict [glob -nocomplain sqlite-*.i386.rpm]] {
  if {[regexp -- -devel- $name]} {
    Product $name {
      RPM containing documentation, header files, and static library.
    }
  } else {
    Product $name {
      RPM containing shared libraries and the <b>sqlite</b> command-line
      program.
    }
  }
}

Heading {Precompiled Binaries For Windows}

Product sqlite.zip {
  A command-line program for accessing and modifing SQLite databases.
  See <a href="sqlite.html">the documentation</a> for additional information.
}
Product tclsqlite.zip {
  Bindings for TCL.  You can import this shared library into either
  tclsh or wish to get SQLite database access from Tcl/Tk.
  See <a href="tclsqlite.html">the documentation</a> for details.
}
Product sqlitedll.zip {
  This is a DLL of the SQLite library without the TCL bindings.
  The only external dependency is MSVCRT.DLL.
}

Heading {Source Code}

Product {sqlite_source.zip} {
  This ZIP archive contains pure C source code for the SQLite library.
  Unlike the tarballs below, all of the preprocessing has already been
  done on these C source code, so you can just hand the files directly to
  your favorite C compiler.  This file is provided as a service to
  MS-Windows users who lack the build support infrastructure of Unix.
}

foreach name [lsort -dict [glob -nocomplain sqlite-*.src.rpm]] {
  Product $name "RPM containing complete source code"
}

foreach name [lsort -dict -decreasing [glob -nocomplain sqlite-*.tar.gz]] {
  regexp {sqlite-(.*)\.tar\.gz} $name match vers
  Product $name "
      Version $vers of the source tree including all documentation.
  "
}

puts {
</table>

<a name="cvs">
<h3>Direct Access To The Sources Via Anonymous CVS</h3>

<p>
All SQLite source code is maintained in a 
<a href="http://www.cvshome.org/">CVS</a> repository that is
available for read-only access by anyone.  You can 
interactively view the
respository contents and download individual files
by visiting
<a href="http://www.sqlite.org/cvstrac/dir?d=sqlite">
http://www.sqlite.org/cvstrac/dir?d=sqlite</a>.
To access the respository directly, use the following
commands:
</p>

<blockquote><pre>
cvs -d :pserver:anonymous@www.sqlite.org:/sqlite login
cvs -d :pserver:anonymous@www.sqlite.org:/sqlite checkout sqlite
</pre></blockquote>

<p>
When the first command prompts you for a password, enter "anonymous".
</p>
}

footer $rcsid
