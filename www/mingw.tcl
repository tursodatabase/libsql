#
# Run this Tcl script to generate the mingw.html file.
#
set rcsid {$Id: mingw.tcl,v 1.4 2003/03/30 18:58:58 drh Exp $}

puts {<html>
<head>
  <title>Notes On How To Build MinGW As A Cross-Compiler</title>
</head>
<body bgcolor=white>
<h1 align=center>
Notes On How To Build MinGW As A Cross-Compiler
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"

puts {
<p><a href="http://www.mingw.org/">MinGW</a> or
<a href="http://www.mingw.org/">Minimalist GNU For Windows</a>
is a version of the popular GCC compiler that builds Win95/Win98/WinNT
binaries.  See the website for details.</p>

<p>This page describes how you can build MinGW 
from sources as a cross-compiler
running under Linux.  Doing so will allow you to construct
WinNT binaries from the comfort and convenience of your
Unix desktop.</p>
}

proc Link {path {file {}}} {
  if {$file!=""} {
    set path $path/$file
  } else {
    set file $path
  }
  puts "<a href=\"$path\">$file</a>"
}

puts {
<p>Here are the steps:</p>

<ol>
<li>
<p>Get a copy of source code.  You will need the binutils, the
compiler, and the MinGW runtime.  Each are available separately.
As of this writing, Mumit Khan has collected everything you need
together in one FTP site:
}
set ftpsite \
  ftp://ftp.nanotech.wisc.edu/pub/khan/gnu-win32/mingw32/snapshots/gcc-2.95.2-1
Link $ftpsite
puts {
The three files you will need are:</p>
<ul>
<li>}
Link $ftpsite binutils-19990818-1-src.tar.gz
puts </li><li>
Link $ftpsite gcc-2.95.2-1-src.tar.gz
puts </li><li>
Link $ftpsite mingw-20000203.zip
puts {</li>
</ul>

<p>Put all the downloads in a directory out of the way.  The sequel
will assume all downloads are in a directory named
<b>~/mingw/download</b>.</p>
</li>

<li>
<p>
Create a directory in which to install the new compiler suite and make
the new directory writable.
Depending on what directory you choose, you might need to become
root.  The example shell commands that follow
will assume the installation directory is
<b>/opt/mingw</b> and that your user ID is <b>drh</b>.</p>
<blockquote><pre>
su
mkdir /opt/mingw
chown drh /opt/mingw
exit
</pre></blockquote>
</li>

<li>
<p>Unpack the source tarballs into a separate directory.</p>
<blockquote><pre>
mkdir ~/mingw/src
cd ~/mingw/src
tar xzf ../download/binutils-*.tar.gz
tar xzf ../download/gcc-*.tar.gz
unzip ../download/mingw-*.zip
</pre></blockquote>
</li>

<li>
<p>Create a directory in which to put all the build products.</p>
<blockquote><pre>
mkdir ~/mingw/bld
</pre></blockquote>
</li>

<li>
<p>Configure and build binutils and add the results to your PATH.</p>
<blockquote><pre>
mkdir ~/mingw/bld/binutils
cd ~/mingw/bld/binutils
../../src/binutils/configure --prefix=/opt/mingw --target=i386-mingw32 -v
make 2&gt;&amp;1 | tee make.out
make install 2&gt;&amp;1 | tee make-install.out
export PATH=$PATH:/opt/mingw/bin
</pre></blockquote>
</li>

<li>
<p>Manually copy the runtime include files into the installation directory
before trying to build the compiler.</p>
<blockquote><pre>
mkdir /opt/mingw/i386-mingw32/include
cd ~/mingw/src/mingw-runtime*/mingw/include
cp -r * /opt/mingw/i386-mingw32/include
</pre></blockquote>
</li>

<li>
<p>Configure and build the compiler</p>
<blockquote><pre>
mkdir ~/mingw/bld/gcc
cd ~/mingw/bld/gcc
../../src/gcc-*/configure --prefix=/opt/mingw --target=i386-mingw32 -v
cd gcc
make installdirs
cd ..
make 2&gt;&amp;1 | tee make.out
make install
</pre></blockquote>
</li>

<li>
<p>Configure and build the MinGW runtime</p>
<blockquote><pre>
mkdir ~/mingw/bld/runtime
cd ~/mingw/bld/runtime
../../src/mingw-runtime*/configure --prefix=/opt/mingw --target=i386-mingw32 -v
make install-target-w32api
make install
</pre></blockquote>
</li>
</ol>

<p>And you are done...</p>
}
puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}
