#
# Run this TCL script to generate HTML for the download.html file.
#
set rcsid {$Id: download.tcl,v 1.3 2002/01/31 15:54:23 drh Exp $}

puts {<html>
<head><title>SQLite Download Page</title></head>
<body bgcolor=white>
<h1 align=center>SQLite Download Page</h1>}
#<p align=center>}
#puts "This page was last modified on [lrange $rcsid 3 4] UTC<br>"
#set vers [lindex $argv 0]
#puts "The latest SQLite version is <b>$vers</b>"
#puts " created on [exec cat last_change] UTC"
#puts {</p>}

puts {<h2>Precompiled Binaries For Linux</h2>}

proc Product {file desc} {
  if {![file exists $file]} return
  set size [file size $file]
  puts [subst {
<table cellpadding="20">
<tr>
<td width="150" align="right" valign="top">
<a href="$file">$file</a><br>($size bytes)
</td>
<td valign="top">[string trim $desc]</td>
</tr>
</table>}]
}

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

puts {<h2>Precompiled Binaries For Windows</h2>}

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

puts {<h2>Source Code</h2>}

Product {sqlite_source.zip} {
  This ZIP archive contains pure C source code for the SQLite library.
  Unlike the tarballs below, all of the preprocessing has already been
  done on these C source code, so you can just hand the files directly to
  your favorite C compiler.  This file is provided as a service to
  MS-Windows users who lack the build support infrastructure of Unix.
}

foreach name [lsort -dict -decreasing [glob -nocomplain sqlite-*.tar.gz]] {
  regexp {sqlite-(.*)\.tar\.gz} $name match vers
  Product $name "
      Version $vers of the source tree including all documentation.
  "
}

puts {
<p><hr /></p>
<p>
<a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite home page</a>
</p>

</body></html>}
