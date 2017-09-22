## Miscellaneous Extensions

This folder contains a collection of smaller loadable extensions.
See <https://www.sqlite.org/loadext.html> for instructions on how
to compile and use loadable extensions.
Each extension in this folder is implemented in a single file of C code.

Each source file contains a description in its header comment.  See the
header comments for details about each extension.  Additional notes are
as follows:

  *  **carray.c** &mdash;  This module implements the
     [carray](https://www.sqlite.org/carray.html) table-valued function.
     It is a good example of how to go about implementing a custom
     [table-valued function](https://www.sqlite.org/vtab.html#tabfunc2).

  *  **dbdump.c** &mdash;  This is not actually a loadable extension, but
     rather a library that implements an approximate equivalent to the
     ".dump" command of the
     [command-line shell](https://www.sqlite.org/cli.html).

  *  **memvfs.c** &mdash;  This file implements a custom
     [VFS](https://www.sqlite.org/vfs.html) that stores an entire database
     file in a single block of RAM.  It serves as a good example of how
     to implement a simple custom VFS.

  *  **rot13.c** &mdash;  This file implements the very simple rot13()
     substitution function.  This file makes a good template for implementing
     new custom SQL functions for SQLite.

  *  **series.c** &mdash;  This is an implementation of the
     "generate_series" [virtual table](https://www.sqlite.org/vtab.html).
     It can make a good template for new custom virtual table implementations.

  *  **shathree.c** &mdash;  An implementation of the sha3() and
     sha3_query() SQL functions.  The file is named "shathree.c" instead
     of "sha3.c" because the default entry point names in SQLite are based
     on the source filename with digits removed, so if we used the name
     "sha3.c" then the entry point would conflict with the prior "sha1.c"
     extension.
