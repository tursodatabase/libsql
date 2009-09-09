This directory contains source code to 

    SQLite: An Embeddable SQL Database Engine

To compile the project, first create a directory in which to place
the build products.  It is recommended, but not required, that the
build directory be separate from the source directory.  Cd into the
build directory and then from the build directory run the configure
script found at the root of the source tree.  Then run "make".

For example:

    tar xzf sqlite.tar.gz    ;#  Unpack the source tree into "sqlite"
    mkdir bld                ;#  Build will occur in a sibling directory
    cd bld                   ;#  Change to the build directory
    ../sqlite/configure      ;#  Run the configure script
    make                     ;#  Run the makefile.
    make install             ;#  (Optional) Install the build products

The configure script uses autoconf 2.61 and libtool.  If the configure
script does not work out for you, there is a generic makefile named
"Makefile.linux-gcc" in the top directory of the source tree that you
can copy and edit to suit your needs.  Comments on the generic makefile
show what changes are needed.

The linux binaries on the website are created using the generic makefile,
not the configure script.  The windows binaries on the website are created
using MinGW32 configured as a cross-compiler running under Linux.  For 
details, see the ./publish.sh script at the top-level of the source tree.
The developers do not use teh configure script.

SQLite does not require TCL to run, but a TCL installation is required
by the makefiles.  SQLite contains a lot of generated code and TCL is
used to do much of that code generation.  The makefile also requires
AWK.

Contacts:

   http://www.sqlite.org/
