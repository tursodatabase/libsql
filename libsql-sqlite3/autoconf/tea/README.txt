This is the SQLite extension for Tcl using something akin to
the Tcl Extension Architecture (TEA). To build it:

    ./configure ...flags...

e.g.:

    ./configure --with-tcl=/path/to/tcl/install/root

or:

    ./configure --with-tclsh=/path/to/tcl/install/root

Run ./configure --help for the full list of flags.

The configuration process will fail if tclConfig.sh cannot be found.

The makefile will only honor CFLAGS and CPPFLAGS passed to the
configure script, not those directly passed to the makefile.

Then:

    make test install

----------------------- THE PREFERRED WAY ---------------------------

The preferred way to build the TCL extension for SQLite is to use the
canonical source code tarball.  For Unix:

    ./configure --with-tclsh=$(TCLSH)
    make tclextension-install

For Windows:

    nmake /f Makefile.msc tclextension-install TCLSH_CMD=$(TCLSH)

In both of the above, replace $(TCLSH) with the full pathname of
of the tclsh that you want the SQLite extension to work with.  See
step-by-step instructions at the links below for more information:

    https://sqlite.org/src/doc/trunk/doc/compile-for-unix.md
    https://sqlite.org/src/doc/trunk/doc/compile-for-windows.md

And info about the extension's Tcl interface can be found at:

    https://sqlite.org/tclsqlite.html

The whole point of the amalgamation-autoconf tarball (in which this
README.txt file is embedded) is to provide a means of compiling SQLite
that does not require first installing TCL and/or "tclsh".  The
canonical Makefile in the SQLite source tree provides more
capabilities (such as the the ability to run test cases to ensure that
the build worked) and is better maintained.  The only downside of the
canonical Makefile is that it requires a TCL installation.  But if you
are wanting to build the TCL extension for SQLite, then presumably you
already have a TCL installation.  So why not just use the more-capable
and better-maintained canoncal Makefile?

As of version 3.50.0, this build process uses "teaish":

    https://fossil.wanderinghorse.net/r/teaish

which is conceptually derived from the pre-3.50 toolchain, TEA:

    http://core.tcl-lang.org/tclconfig
    http://core.tcl-lang.org/sampleextension

It to works for us.  It might also work for you.  But we cannot
promise that.

If you want to use this TEA builder and it works for you, that's fine.
But if you have trouble, the first thing you should do is go back
to using the canonical Makefile in the SQLite source tree.

------------------------------------------------------------------


UNIX BUILD
==========

Building under most UNIX systems is easy, just run the configure
script and then run make. For example:

	$ cd sqlite-*-tea
	$ ./configure --with-tcl=/path/to/tcl/install/root
	$ make test
	$ make install

WINDOWS BUILD
=============

On Windows this build is known to work on Cygwin and some Msys2
environments. We do not currently support Microsoft makefiles for
native Windows builds.
