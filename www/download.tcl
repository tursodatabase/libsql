#
# Run this TCL script to generate HTML for the download.html file.
#
set rcsid {$Id: download.tcl,v 1.23 2006/10/08 18:56:57 drh Exp $}
source common.tcl
header {SQLite Download Page}

puts {
<h2>SQLite Download Page</h1>
<table width="100%" cellpadding="5">
}

proc Product {pattern desc} {
  regsub {V[23]} $pattern {*} p3
  regsub V2 $pattern {(2[0-9a-z._]+)} pattern
  regsub V3 $pattern {(3[0-9a-z._]+)} pattern
  set p2 [string map {* .*} $pattern]
  set flist [glob -nocomplain $p3]
  foreach file [lsort -dict $flist] {
    if {![regexp ^$p2\$ $file all version]} continue
    regsub -all _ $version . version
    set size [file size $file]
    set units bytes
    if {$size>1024*1024} {
      set size [format %.2f [expr {$size/(1024.0*1024.0)}]]
      set units MiB
    } elseif {$size>1024} {
      set size [format %.2f [expr {$size/(1024.0)}]]
      set units KiB
    }
    puts "<tr><td width=\"10\"></td>"
    puts "<td valign=\"top\" align=\"right\">"
    puts "<a href=\"$file\">$file</a><br>($size $units)</td>"
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

Product sqlite3-V3.bin.gz {
  A command-line program for accessing and modifying
  SQLite version 3.* databases.
  See <a href="sqlite.html">the documentation</a> for additional information.
}

Product sqlite-V3.bin.gz {
  A command-line program for accessing and modifying
  SQLite databases.
  See <a href="sqlite.html">the documentation</a> for additional information.
}

Product tclsqlite-V3.so.gz {
  Bindings for <a href="http://www.tcl.tk/">Tcl/Tk</a>.
  You can import this shared library into either
  tclsh or wish to get SQLite database access from Tcl/Tk.
  See <a href="tclsqlite.html">the documentation</a> for details.
}

Product sqlite-V3.so.gz {
  A precompiled shared-library for Linux without the TCL bindings.
}

Product fts1-V3.so.gz {
  A precompiled 
  <a href="http://www.sqlite.org/cvstrac/wiki?p=FtsOne">FTS Module</a> 
  for Linux.
}

Product sqlite-devel-V3.i386.rpm {
  RPM containing documentation, header files, and static library for
  SQLite version VERSION.
}
Product sqlite-V3-1.i386.rpm {
  RPM containing shared libraries and the <b>sqlite</b> command-line
  program for SQLite version VERSION.
}

Product sqlite*_analyzer-V3.bin.gz {
  An analysis program for database files compatible with SQLite 
  version VERSION and later.
}

Heading {Precompiled Binaries For Windows}

Product sqlite-V3.zip {
  A command-line program for accessing and modifing SQLite databases.
  See <a href="sqlite.html">the documentation</a> for additional information.
}
Product tclsqlite-V3.zip {
  Bindings for <a href="http://www.tcl.tk/">Tcl/Tk</a>.
  You can import this shared library into either
  tclsh or wish to get SQLite database access from Tcl/Tk.
  See <a href="tclsqlite.html">the documentation</a> for details.
}
Product sqlitedll-V3.zip {
  This is a DLL of the SQLite library without the TCL bindings.
  The only external dependency is MSVCRT.DLL.
}

Product fts1dll-V3.zip {
  A precompiled 
  <a href="http://www.sqlite.org/cvstrac/wiki?p=FtsOne">FTS Module</a> 
  for win32.
}

Product sqlite*_analyzer-V3.zip {
  An analysis program for database files compatible with SQLite version
  VERSION and later.
}


Heading {Source Code}

Product {sqlite-V3.tar.gz} {
  A tarball of the complete source tree for SQLite version VERSION
  including all of the documentation.
}

Product {sqlite-source-V3.zip} {
  This ZIP archive contains pure C source code for the SQLite library.
  Unlike the tarballs below, all of the preprocessing and automatic
  code generation has already been done on these C source code, so they
  can be processed directly with any ordinary C compiler.
  This file is provided as a service to
  MS-Windows users who lack the build support infrastructure of Unix.
}

Product {sqlite-V3-tea.tar.gz} {
  A tarball of proprocessed source code together with a
  <a href="http://www.tcl.tk/doc/tea/">Tcl Extension Architecture (TEA)</a>
  compatible configure script and makefile.
}

Product {sqlite-V3.src.rpm} {
  An RPM containing complete source code for SQLite version VERSION
}

Heading {Cross-Platform Binaries}

Product {sqlite-V3.kit} {
  A <a href="http://www.equi4.com/starkit.html">starkit</a> containing
  precompiled SQLite binaries and Tcl bindings for Linux-x86, Windows,
  and Mac OS-X ppc and x86.
}

Heading {Historical Binaries And Source Code}

Product sqlite-V2.bin.gz {
  A command-line program for accessing and modifying
  SQLite version 2.* databases on Linux-x86.
}
Product sqlite-V2.zip {
  A command-line program for accessing and modifying 
  SQLite version 2.* databases on win32.
}

Product sqlite*_analyzer-V2.bin.gz {
  An analysis program for version 2.* database files on Linux-x86
}
Product sqlite*_analyzer-V2.zip {
  An analysis program for version 2.* database files on win32.
}
Product {sqlite-source-V2.zip} {
  This ZIP archive contains C source code for the SQLite library
  version VERSION.
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
