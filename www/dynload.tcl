#
# Run this Tcl script to generate the dynload.html file.
#
set rcsid {$Id: dynload.tcl,v 1.1 2001/02/11 16:58:22 drh Exp $}

puts {<html>
<head>
  <title>How to build a dynamically loaded Tcl extension for SQLite</title>
</head>
<body bgcolor=white>
<h1 align=center>
How To Build A Dynamically Loaded Tcl Extension
</h1>}
puts {<p>
<i>This note was contributed by 
<a href="bsaunder@tampabay.rr.com.nospam">Bill Saunders</a>.  Thanks, Bill!</i>

<p>
To compile the SQLite Tcl extension into a dynamically loaded module 
I did the following:
</p>

<ol>
<li><p>Do a standard compile
(I had a dir called bld at the same level as sqlite  ie
        /root/bld
        /root/sqlite
I followed the directions and did a standard build in the bld
directory)</p></li>

<li><p>
Now do the following in the bld directory
<blockquote><pre>
gcc -shared -I. -lgdbm ../sqlite/src/tclsqlite.c libsqlite.a -o sqlite.so
</pre></blockquote></p></li>

<li><p>
This should produce the file sqlite.so in the bld directory</p></li>

<li><p>
Create a pkgIndex.tcl file that contains this line

<blockquote><pre>
package ifneeded sqlite 1.0 [list load [file join $dir sqlite.so]]
</pre></blockquote></p></li>

<li><p>
To use this put sqlite.so and pkgIndex.tcl in the same directory</p></li>

<li><p>
From that directory start wish</p></li>

<li><p>
Execute the following tcl command (tells tcl where to fine loadable
modules)
<blockquote><pre>
lappend auto_path [exec pwd]
</pre></blockquote></p></li>

<li><p>
Load the package 
<blockquote><pre>
package require sqlite
</pre></blockquote></p></li>

<li><p>
Have fun....</p></li>
</ul>

</body></html>}
