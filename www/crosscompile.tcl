#
# Run this Tcl script to generate the crosscompile.html file.
#
set rcsid {$Id: crosscompile.tcl,v 1.4 2001/01/31 13:28:09 drh Exp $}

puts {<html>
<head>
  <title>Notes On How To Compile SQLite Using The MinGW Cross-Compiler</title>
</head>
<body bgcolor=white>
<h1 align=center>
Notes On How To Compile SQLite Using The MinGW Cross-Compiler
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] GMT)
</p>"

puts {
<p><a href="http://www.mingw.org/">MinGW</a> or
<a href="http://www.mingw.org/">Minimalist GNU For Windows</a>
is a version of the popular GCC compiler that builds Win95/Win98/WinNT
binaries.  See the website for details.</p>

<p>This page describes how you can use MinGW configured as a
cross-compiler running under RedHat 6.0 Linux to generate a
binary for SQLite that runs under WinNT.
Some additional steps (described <a href="#win95">below</a>)
are needed to target Win95/98.</p>
}

proc Code {body} {
  puts {<blockquote><pre>}
  regsub -all {&} [string trim $body] {\&amp;} body
  regsub -all {>} $body {\&gt;} body
  regsub -all {<} $body {\&lt;} body
  regsub -all {\(\(\(} $body {<font color="#00671f"><u>} body
  regsub -all {\)\)\)} $body {</u></font>} body
  puts $body
  puts {</pre></blockquote>}
}

puts {
<p>Here are the steps:</p>

<ol>
<li>
<p>Get a copy of the MinGW compiler and all
its associated tools that run under Linux.  No binary versions of
MinGW in this configuration are available for net downloads, as far
as I know.  You will probably have to download the source code and
compile it all yourself.
A <a href="mingw.html">separate bulletin</a> describes how this
can be done.
When you are done, make sure the compiler and all its associated tools
are located somewhere on your PATH environment variable.
</p>
</li>

<li>
<p>
Download the Win32 port of GDBM from <a href="http://www.roth.net/libs/gdbm/">
Roth Consulting</a>.  You can FTP a ZIP archive of the sources directly
from <a href="ftp://ftp.roth.net/pub/ntperl/gdbm/source/Win32_GDBM_Source.zip">
ftp://ftp.roth.net/pub/ntperl/gdbm/source/Win32_GDBM_Source.zip</a>.
</p>
</li>

<li>
<p>Make a directory and unpack the Win32 port of GDBM.</p>
<blockquote><pre>
mkdir roth
cd roth
unzip ../Win32_GDBM_Source.zip
</pre></blockquote>
</li>

<li>
<p>Manually build the GDBM library as follows:</p>
<blockquote><pre>
i386-mingw32-gcc -DWIN32=1 -O2 -c *.c
i386-mingw32-ar cr libgdbm.a *.o
i386-mingw32-ranlib libgdbm.a
cd ..
</pre></blockquote>
</li>

<li>
<p>
Download the SQLite tarball from 
<a href="http://www.hwaci.com/sw/sqlite/sqlite.tar.gz">
http://www.hwaci.com/sw/sqlite/sqlite.tar.gz</a>.
Unpack the tarball and create a separate directory in which
to build the executable and library.
</p>
<blockquote><pre>
tar xzf sqlite.tar.gz
mkdir sqlite-bld
cd sqlite-bld
</pre></blockquote>
</li>

<li>
<p>
Create a "hints" file that will tell the SQLite configuration script
to use the MinGW cross-compiler rather than the native linux compiler.
The hints file should looks something like this:</p>
<blockquote><pre>
cat >mingw.hints <<\END
  config_TARGET_CC=i386-mingw32-gcc
  config_TARGET_CFLAGS='-O2'
  config_TARGET_GDBM_LIBS=../roth/libgdbm.a
  config_TARGET_GDBM_INC=-I../roth
  config_TARGET_AR='i386-mingw32-ar cr'
  config_TARGET_RANLIB=i386-mingw32-ranlib
  config_TARGET_EXEEXT='.exe'
END
</pre></blockquote>
</li>

<li>
<p>Configure and build SQLite:</p>
<blockquote><pre>
../sqlite/configure --with-hints=./mingw.hints
make
</pre></blockquote>
</li>
</ol>

<a name="win95">
<h2>Targetting Windows95/98 instead of WindowsNT</h2>

<p>A small amount of additional work is needed to get SQLite running
under Windows95/98.  The first problem is that the LockFile() and
UnlockFile() API that the Roth GDBM port uses does not work under
Windows95.  The easiest workaround is to just disable file locking
in the GDBM library.  You can do so by appending a few lines of code
to the end of one of the GDBM source files and compiling the library
using a special command-line option.  Replace step (4) above as
follows:</p>

<ol>
<li value="4"><p>
Append text to the <b>systems.h</b> source file as follows:</p>

<blockquote><pre>
cat &gt;&gt;systems.h &lt;&lt;\END
#ifdef NO_LOCKS
#undef  UNLOCK_FILE
#define UNLOCK_FILE(x)
#undef  READLOCK_FILE
#define READLOCK_FILE(x)  lock_val=0;
#undef  WRITELOCK_FILE
#define WRITELOCK_FILE(x) lock_val=0;
#endif
END
</pre></blockquote>

<p>Then manually build the GDBM library with the extra
"NO_LOCKS" define as follows:</p>
<blockquote><pre>
i386-mingw32-gcc -DWIN32=1 -DNO_LOCKS -O2 -c *.c
i386-mingw32-ar cr libgdbm.a *.o
i386-mingw32-ranlib libgdbm.a
cd ..
</pre></blockquote>
</p></li>
</ol>

<p>Note that the locking problem has been reported and may actually
be fixed in the Roth GDBM distribution by the time you read this.
You should probably check before you make the above changes.</p>

<p>The above is all you have to do to get SQLite to work on Windows95.
But one more step is required to get it to work <i>well</i>.  It
turns out that SQLite make heavy use of <b>malloc()</b> and
<b>free()</b> and the implementation of this functions
on Windows95 is particular poor.  Large database queries will run
more than 10 times faster if you substitute a better memory allocator
such as the one by
<a href="http://g.oswego.edu/dl/html/malloc.html">Doug Lea</a>.
A copy of Doug's allocator is included in the <b>contrib</b>
directory of the source tree.  Speed improvements are also reported
on WindowsNT when alternative memory allocators are used, though
the speedup is not as dramatic as it is with WIndows95.</p>

}
puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}
