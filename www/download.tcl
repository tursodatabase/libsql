#
# Run this TCL script to generate HTML for the download.html file.
#
set rcsid {$Id: download.tcl,v 1.18 2005/02/15 13:07:39 drh Exp $}
source common.tcl
header {SQLite Download Page}

puts {
<h2>SQLite Download Page</h1>
<table width="100%" cellpadding="5">
}

proc Product {pattern desc} {
  regsub VERSION $pattern {([0-9][0-9a-z._]+)} p2
  set p2 [string map {* .*} $p2]
  regsub VERSION $pattern {*} p3
  set flist [glob -nocomplain $p3]
  foreach file [lsort -dict $flist] {
    if {![regexp ^$p2\$ $file all version]} continue
    regsub -all _ $version . version
    set size [file size $file]
    puts "<tr><td width=\"10\"></td>"
    puts "<td valign=\"top\" align=\"right\">"
    puts "<a href=\"$file\">$file</a><br>($size bytes)</td>"
    puts "<td width=\"5\"></td>"
    regsub -all VERSION $desc $version d2
    puts "<td valign=\"top\">[string trim $d2]</td></tr>"
  }
}
cd doc

proc Heading {title} {
  puts "<tr><td colspan=4><big><b>$title</b></big></td></tr>"
}

Heading {Precompiled Binaries for Linux}

Product sqlite3-VERSION.bin.gz {
  A statically linked command-line program for accessing and modifing
  SQLite databases.
  See <a href="sqlite.html">the documentation</a> for additional information.
}

Product sqlite-VERSION.bin.gz {
  A statically linked command-line program for accessing and modifing
  2 SQLite databases.
  See <a href="sqlite.html">the documentation</a> for additional information.
}

Product tclsqlite-VERSION.so.gz {
  Bindings for TCL.  You can import this shared library into either
  tclsh or wish to get SQLite database access from Tcl/Tk.
  See <a href="tclsqlite.html">the documentation</a> for details.
}

Product sqlite-VERSION.so.gz {
  A precompiled shared-library for Linux.  This is the same as
  <b>tclsqlite.so.gz</b> but without the TCL bindings.
}

Product sqlite-devel-VERSION-1.i386.rpm {
  RPM containing documentation, header files, and static library for
  SQLite version VERSION.
}
Product sqlite-VERSION-1.i386.rpm {
  RPM containing shared libraries and the <b>sqlite</b> command-line
  program for SQLite version VERSION.
}

Product sqlite*_analyzer-VERSION.bin.gz {
  An analysis program for database files compatible with SQLite 
  version VERSION.
}

Heading {Precompiled Binaries For Windows}

Product sqlite-VERSION.zip {
  A command-line program for accessing and modifing SQLite databases.
  See <a href="sqlite.html">the documentation</a> for additional information.
}
Product tclsqlite-VERSION.zip {
  Bindings for TCL.  You can import this shared library into either
  tclsh or wish to get SQLite database access from Tcl/Tk.
  See <a href="tclsqlite.html">the documentation</a> for details.
}
Product sqlitedll-VERSION.zip {
  This is a DLL of the SQLite library without the TCL bindings.
  The only external dependency is MSVCRT.DLL.
}

Product sqlite*_analyzer-VERSION.zip {
  An analysis program for database files compatible with SQLite version
  VERSION.
}


Heading {Source Code}

Product {sqlite-source-VERSION.zip} {
  This ZIP archive contains pure C source code for the SQLite library.
  Unlike the tarballs below, all of the preprocessing and automatic
  code generation has already been done on these C source code, so they
  can be processed directly with any ordinary C compiler.
  This file is provided as a service to
  MS-Windows users who lack the build support infrastructure of Unix.
}

Product {sqlite-VERSION.src.rpm} {
  An RPM containing complete source code for SQLite version VERSION
}

Product {sqlite-VERSION.tar.gz} {
  A tarball of the complete source tree for SQLite version VERSION
  including all of the documentation.
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
repository contents and download individual files
by visiting
<a href="http://www.sqlite.org/cvstrac/dir?d=sqlite">
http://www.sqlite.org/cvstrac/dir?d=sqlite</a>.
To access the repository directly, use the following
commands:
</p>

<blockquote><pre>
cvs -d :pserver:anonymous@www.sqlite.org:/sqlite login
cvs -d :pserver:anonymous@www.sqlite.org:/sqlite checkout sqlite
</pre></blockquote>

<p>
When the first command prompts you for a password, enter "anonymous".
</p>

<p>
To access the SQLite version 2.8 sources, begin by getting the 3.0
tree as described above.  Then update to the "version_2" branch
as follows:
</p>

<blockquote><pre>
cvs update -r version_2
</pre></blockquote>

}

footer $rcsid
