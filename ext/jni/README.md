SQLite3 via JNI
========================================================================

This directory houses a Java Native Interface (JNI) binding for the
sqlite3 API. If you are reading this from the distribution ZIP file,
links to resources in the canonical source tree will note work. The
canonical copy of this file can be browsed at:

  <https://sqlite.org/src/doc/trunk/ext/jni/README.md>

Technical support is available in the forum:

  <https://sqlite.org/forum>


> **FOREWARNING:** this subproject is very much in development and
  subject to any number of changes. Please do not rely on any
  information about its API until this disclaimer is removed.

Project goals/requirements:

- A [1-to-1(-ish) mapping of the C API](#1to1ish) to Java via JNI,
  insofar as cross-language semantics allow for. A closely-related
  goal is that [the C documentation](https://sqlite.org/c3ref/intro.html)
  should be usable as-is, insofar as possible, for the JNI binding.

- Support Java as far back as version 8 (2014).

- Environment-independent. Should work everywhere both Java
  and SQLite3 do.

- No 3rd-party dependencies beyond the JDK. That includes no
  build-level dependencies for specific IDEs and toolchains.  We
  welcome the addition of build files for arbitrary environments
  insofar as they neither interfere with each other nor become
  a maintenance burden for the sqlite developers.

Non-goals:

- Creation of high-level OO wrapper APIs. Clients are free to create
  them off of the C-style API.


Significant TODOs
========================================================================

- The initial beta release with version 3.43 has severe threading
  limitations.  Namely, two threads cannot call into the JNI-bound API
  at once. This limitation will be remove in a subsequent release.


Building
========================================================================

The canonical builds assumes a Linux-like environment and requires:

- GNU Make
- A JDK supporting Java 8 or higher
- A modern C compiler. gcc and clang should both work.

Put simply:

```
$ export JAVA_HOME=/path/to/jdk/root
$ make
$ make test
$ make clean
```

<a id='1to1ish'></a>
One-to-One(-ish) Mapping to C
========================================================================

This JNI binding aims to provide as close to a 1-to-1 experience with
the C API as cross-language semantics allow. Exceptions are
necessarily made where cross-language semantics do not allow a 1-to-1,
and judiciously made where a 1-to-1 mapping would be unduly cumbersome
to use in Java.

Golden Rule: _Never_ Throw from Callbacks
------------------------------------------------------------------------

JNI bindings which accept client-defined functions _must never throw
exceptions_ unless _very explicitly documented_ as being
throw-safe. Exceptions are generally reserved for higher-level
bindings which are constructed to specifically deal with them and
ensure that they do not leak C-level resources. Some of the JNI
bindings are provided as Java functions which expect this rule to
always hold.

UTF-8(-ish)
------------------------------------------------------------------------

SQLite internally uses UTF-8 encoding, whereas Java natively uses
UTF-16.  Java JNI has routines for converting to and from UTF-8, _but_
Java uses what its docs call "[modified UTF-8][modutf8]." Care must be
taken when converting Java strings to UTF-8 to ensure that the proper
conversion is performed. In short,
`String.getBytes(StandardCharsets.UTF_8)` performs the proper
conversion in Java, and there is no JNI C API for that conversion
(JNI's `NewStringUTF()` returns MUTF-8).

Known consequences and limitations of this discrepancy include:

- Names of databases, tables, and collations must not contain
  characters which differ in MUTF-8 and UTF-8, or certain APIs will
  mis-translate them on their way between languages. APIs which
  transfer other client-side data to Java take extra care to
  convert the data at the cost of performance.

[modutf8]: https://docs.oracle.com/javase/8/docs/api/java/io/DataInput.html#modified-utf-8


Unwieldy Constructs are Re-mapped
------------------------------------------------------------------------

Some constructs, when modelled 1-to-1 from C to Java, are unduly
clumsy to work with in Java because they try to shoehorn C's way of
doing certain things into Java's wildly different ways. The following
subsections cover those, starting with a verbose explanation and
demonstration of where such changes are "really necessary"...

### Custom Collations

A prime example of where interface changes for Java are necessary for
usability is [registration of a custom
collation](https://sqlite.org/c3ref/create_collation.html):

```
// C:
int sqlite3_create_collation(sqlite3 * db, const char * name, int eTextRep,
                             void *pUserData,
                             int (*xCompare)(void*,int,void const *,int,void const *));

int sqlite3_create_collation_v2(sqlite3 * db, const char * name, int eTextRep,
                                void *pUserData,
                                int (*xCompare)(void*,int,void const *,int,void const *),
                                void (*xDestroy)(void*));
```

The `pUserData` object is optional client-defined state for the
`xCompare()` and/or `xDestroy()` callback functions, both of which are
passed that object as their first argument. That data is passed around
"externally" in C because that's how C models the world. If we were to
bind that part as-is to Java, the result would be awkward to use (^Yes,
we tried this.):

```
// Java:
int sqlite3_create_collation(sqlite3 db, String name, int eTextRep,
                             Object pUserData, xCompareType xCompare);

int sqlite3_create_collation_v2(sqlite3 db, String name, int eTextRep,
                                Object pUserData,
                                xCompareType xCompare, xDestroyType xDestroy);
```

The awkwardness comes from (A) having two distinctly different objects
for callbacks and (B) having their internal state provided separately,
which is ill-fitting in Java. For the sake of usability, C APIs which
follow that pattern use a slightly different Java interface:

```
int sqlite3_create_collation(sqlite3 db, String name, int eTextRep,
                             Collation collation);
```

Where the `Collation` class has an abstract `xCompare()` method and
no-op `xDestroy()` method which can be overridden if needed, leading to
a much more Java-esque usage:

```
int rc = sqlite3_create_collation(db, "mycollation", SQLITE_UTF8, new Collation(){

  // Required comparison function:
  @Override public int xCompare(byte[] lhs, byte[] rhs){ ... }

  // Optional finalizer function:
  @Override public void xDestroy(){ ... }

  // Optional local state:
  private String localState1 =
    "This is local state. There are many like it, but this one is mine.";
  private MyStateType localState2 = new MyStateType();
  ...
});
```

Noting that:

- It is still possible to bind in call-scope-local state via closures,
  if desired.

- No capabilities of the C API are lost or unduly obscured via the
  above API reshaping, so power users need not make any compromises.

- In the specific example above, `sqlite3_create_collation_v2()`
  becomes superfluous because the provided interface effectively
  provides both the v1 and v2 interfaces, the difference being that
  overriding the `xDestroy()` method effectively gives it v2
  semantics.

### User-defined SQL Functions (a.k.a. UDFs)

The [`sqlite3_create_function()`](https://sqlite.org/c3ref/create_function.html)
family of APIs make heavy use of function pointers to provide
client-defined callbacks, necessitating interface changes in the JNI
binding. The Java API has only one core function-registration function:

```
int sqlite3_create_function(sqlite3 db, String funcName, int nArgs,
                            int encoding, SQLFunction func);
```

> Design question: does the encoding argument serve any purpose in JS?

`SQLFunction` is not used directly, but is instead instantiated via
one of its three subclasses:

- `SQLFunction.Scalar` implements simple scalar functions using but a
  single callback.
- `SQLFunction.Aggregate` implements aggregate functions using two
  callbacks.
- `SQLFunction.Window` implements window functions using four
  callbacks.

Search [`Tester1.java`](/file/ext/jni/src/org/sqlite/jni/Tester1.java) for
`SQLFunction` for how it's used.

Reminder: see the disclaimer at the top of this document regarding the
in-flux nature of this API.

[jsrc]: /file/
[www]: https://sqlite.org
