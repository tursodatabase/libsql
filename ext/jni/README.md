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
  information about its API until this disclaimer is removed.  The JNI
  bindings released with version 3.43 are a "tech preview" and 3.44
  will be "final," at which point strong backward compatibility
  guarantees will apply.

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

- Support for mixed-mode operation, where client code accesses SQLite
  both via the Java-side API and the C API via their own native
  code. In such cases, proxy functionalities (primarily callback
  handler wrappers of all sorts) may fail because the C-side use of
  the SQLite APIs will bypass those proxies.


Hello World
-----------------------------------------------------------------------

```java
import org.sqlite.jni.*;
import static org.sqlite.jni.CApi.*;

...

final sqlite3 db = sqlite3_open(":memory:");
try {
  final int rc = sqlite3_errcode(db);
  if( 0 != rc ){
    if( null != db ){
      System.out.print("Error opening db: "+sqlite3_errmsg(db));
    }else{
      System.out.print("Error opening db: rc="+rc);
    }
    ... handle error ...
  }
  // ... else use the db ...
}finally{
  // ALWAYS close databases using sqlite3_close() or sqlite3_close_v2()
  // when done with them. All of their active statement handles must
  // first have been passed to sqlite3_finalize().
  sqlite3_close_v2(db);
}
```


Building
========================================================================

The canonical builds assumes a Linux-like environment and requires:

- GNU Make
- A JDK supporting Java 8 or higher
- A modern C compiler. gcc and clang should both work.

Put simply:

```console
$ export JAVA_HOME=/path/to/jdk/root
$ make
$ make test
$ make clean
```

The jar distribution can be created with `make jar`, but note that it
does not contain the binary DLL file. A different DLL is needed for
each target platform.


<a id='1to1ish'></a>
One-to-One(-ish) Mapping to C
========================================================================

This JNI binding aims to provide as close to a 1-to-1 experience with
the C API as cross-language semantics allow. Interface changes are
necessarily made where cross-language semantics do not allow a 1-to-1,
and judiciously made where a 1-to-1 mapping would be unduly cumbersome
to use in Java. In all cases, this binding makes every effort to
provide semantics compatible with the C API documentation even if the
interface to those semantics is slightly different.  Any cases which
deviate from those semantics (either removing or adding semantics) are
clearly documented.

Where it makes sense to do so for usability, Java-side overloads are
provided which accept or return data in alternative forms or provide
sensible default argument values. In all such cases they are thin
proxies around the corresponding C APIs and do not introduce new
semantics.

In some very few cases, Java-specific capabilities have been added in
new APIs, all of which have "_java" somewhere in their names.
Examples include:

- `sqlite3_result_java_object()`
- `sqlite3_column_java_object()`
- `sqlite3_column_java_casted()`
- `sqlite3_value_java_object()`
- `sqlite3_value_java_casted()`

which, as one might surmise, collectively enable the passing of
arbitrary Java objects from user-defined SQL functions through to the
caller.


Golden Rule: Garbage Collection Cannot Free SQLite Resources
------------------------------------------------------------------------

It is important that all databases and prepared statement handles get
cleaned up by client code. A database cannot be closed if it has open
statement handles. `sqlite3_close()` fails if the db cannot be closed
whereas `sqlite3_close_v2()` recognizes that case and marks the db as
a "zombie," pending finalization when the library detects that all
pending statements have been closed. Be aware that Java garbage
collection _cannot_ close a database or finalize a prepared statement.
Those things require explicit API calls.


Golden Rule #2: _Never_ Throw from Callbacks (Unless...)
------------------------------------------------------------------------

All routines in this API, barring explicitly documented exceptions,
retain C-like semantics. For example, they are not permitted to throw
or propagate exceptions and must return error information (if any) via
result codes or `null`. The only cases where the C-style APIs may
throw is through client-side misuse, e.g. passing in a null where it
shouldn't be used. The APIs clearly mark function parameters which
should not be null, but does not actively defend itself against such
misuse. Some C-style APIs explicitly accept `null` as a no-op for
usability's sake, and some of the JNI APIs deliberately return an
error code, instead of segfaulting, when passed a `null`.

Client-defined callbacks _must never throw exceptions_ unless _very
explicitly documented_ as being throw-safe. Exceptions are generally
reserved for higher-level bindings which are constructed to
specifically deal with them and ensure that they do not leak C-level
resources. In some cases, callback handlers are permitted to throw, in
which cases they get translated to C-level result codes and/or
messages. If a callback which is not permitted to throw throws, its
exception may trigger debug output but will otherwise be suppressed.

The reason some callbacks are permitted to throw and others not is
because all such callbacks act as proxies for C function callback
interfaces and some of those interfaces have no error-reporting
mechanism. Those which are capable of propagating errors back through
the library convert exceptions from callbacks into corresponding
C-level error information. Those which cannot propagate errors
necessarily suppress any exceptions in order to maintain the C-style
semantics of the APIs.


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

```c
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

```java
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

```java
int sqlite3_create_collation(sqlite3 db, String name, int eTextRep,
                             SomeCallbackType collation);
```

Where the `Collation` class has an abstract `call()` method and
no-op `xDestroy()` method which can be overridden if needed, leading to
a much more Java-esque usage:

```java
int rc = sqlite3_create_collation(db, "mycollation", SQLITE_UTF8, new SomeCallbackType(){

  // Required comparison function:
  @Override public int call(byte[] lhs, byte[] rhs){ ... }

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

- It is possible to bind in call-scope-local state via closures, if
  desired, as opposed to packing it into the Collation object.

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

```java
int sqlite3_create_function(sqlite3 db, String funcName, int nArgs,
                            int encoding, SQLFunction func);
```

> Design question: does the encoding argument serve any purpose in
  Java? That's as-yet undetermined. If not, it will be removed.

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

### And so on...

Various APIs which accept callbacks, e.g. `sqlite3_trace_v2()` and
`sqlite3_update_hook()`, use interfaces similar to those shown above.
Despite the changes in signature, the JNI layer makes every effort to
provide the same semantics as the C API documentation suggests.
