# JSON Functions Enhancements (2022)

This document summaries enhancements to the SQLite JSON support added in
early 2022.

## 1.0 New feature summary:

  1.  New **->** and **->>** operators that work like MySQL and PostgreSQL (PG).
  2.  New functions: **json_nextract()** and **json_ntype()**.
  3.  JSON functions are built-in rather than being an extension.  They
      are included by default, but can be omitted using the
      -DSQLITE_OMIT_JSON compile-time option.

## 2.0 The **json_nextract()** function.

The new **json_nextract()** function works like **json_extract()** with
one exception: if the input text in the first argument is not well-formed
JSON, then json_nextract() returns NULL whereas json_extract() raises
an error.  The extra "n" in the name of json_nextract() can be throught
of as meaning "null-if-error".

A call to json_nextract($JSON,$PATH) is logically equivalent to:

> ~~~
CASE WHEN json_valid($JSON) THEN json_extract($JSON,$PATH) END
~~~

The json_nextract() function is intended for use in tables where a
column might hold a mixture of datatypes - some rows holding JSON and other
rows holding primitive SQL datatypes such as INT, REAL, and TEXT.  The
json_nextract() function makes it easier to write robust queries 
against such tables.

## 3.0 New operators **->** and **->>**

The SQLite language adds two new binary operators **->** and **->>**.
The -> operator works like the two-argument version of json_nextract()
and the ->> operator works like the two-argument version of json_extract().
The left-hand operand of -> and ->> is JSON.  The right-hand operand
is a JSON path expression.  These operators extract and return a value 
from the left-hand JSON that is specified by right-hand path expression.

The operators work exactly the same if the left-hand side is well-formed
JSON.  The only difference is that if the left-hand side is not well-formed
JSON, the ->> raises an error whereas the -> operator simply returns NULL.

### 3.1 Compatibility with MySQL

The ->> operator should be compatible with MySQL in the sense that
a ->> operator that works in MySQL should work the same way in SQLite.
But (see below) the SQLite ->> operator is also extended to support PG
syntax so not every use of ->> that wworks in SQLite will work for MySQL.

The -> operator is *mostly* compatible with MySQL.  Key differences
between the SQLite -> operator and the MySQL -> operator are:

  *  The SQLite -> operator returns NULL if the left-hand side is
     not well-formed JSON whereas MySQL will raise an error.

  *  When the JSON path expression on the right-hand side selects a
     text value from the JSON, the -> operator in MySQL returns the
     string quoted as if for JSON, whereas the SQLite -> operator
     returns an unquoted SQL text value.

This second difference - the handling of text values extracted from JSON -
is also a difference in the json_extract() function between SQLite and
MySQL.  Because json_extract() has been in active use for 6 years, and
because the SQLite semantics seem to be more useful, there
are no plans to change json_extract() to make it compatible with MySQL.

### 3.2 Compatibility with PostgreSQL (PG)

The ->> operator in PG does not accept a JSON path expression as its
right-hand operand.  Instead, PG looks for either a text string X
(which is then interpreted as the path "$.X") or an integer N (which
is then interpreted as "$[N]").  In order to make the SQLite ->> operator
compatible with the PG ->> operator, the SQLite ->> operator has been
extended so that its right-hand operand can be either a text label or
a integer array index, as it is in PG.  The SQLite ->> operator also
accepts full JSON path expressions as well.

The enhancement of accepting JSON path expression that consist of just
a bare object label or array index is unique to the -> and ->> operators.
All other places in the SQLite JSON interface that require JSON path
expressions continue to require well-formed JSON path expressions.
Only -> and ->> accept the PG-compatible abbreviated path expressions.

The -> operator in SQLite is *mostly* compatible with the -> operator
in PG.  The differences are the same as for MySQL.

## 4.0 The **json_ntype()** function.

The **json_ntype()** function works like **json_type()** except that when
the argument is not well-formed JSON, the json_ntype() function returns
NULL whereas json_type() raises an error.  The extra "n" in the name can
be understood as standing for "null-if-error".

The json_ntype($JSON) function is logically equivalent to:

> ~~~
CASE WHEN json_valid($JSON) THEN json_type($JSON) END
~~~

The json_ntype() function can be seen as an enhanced version of
the json_valid() function, that in addition to indicating whether or
not the string is well-formed JSON, also indicates the top-level type
of that JSON.

## 5.0 JSON moved into the core

The JSON interface is now moved into the SQLite core.

When originally written in 2015, the JSON functions were an extension
that could be optionally included at compile-time, or loaded at run-time.
The implementation was in a source file named ext/misc/json1.c in the
source tree.  JSON functions were only compiled in if the
-DSQLITE_ENABLE_JSON1 compile-time option was used.

After these enhancements, the JSON functions are now built-ins.
The source file that implements the JSON functions is moved to src/json.c.
No special compile-time options are needed to load JSON into the build.
Instead, there is a new -DSQLITE_OMIT_JSON compile-time option to leave
them out.
