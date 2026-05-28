Maintaining Autosetup in the SQLite Tree
========================================================================

This document provides some tips and reminders for the SQLite
developers regarding using and maintaining the [Autosetup][]-based
build infrastructure. It is not an [Autosetup][] reference.

**Table of Contents**:

- [Autosetup API Reference](#apiref)
- [API Tips](#apitips)
- [Ensuring TCL Compatibility](#tclcompat)
- [Design Conventions](#conventions)
  - Symbolic Names of Feature Flags
  - Do Not Update Global Shared State
- [Updating Autosetup](#updating)
  - [Patching Autosetup for Project-local changes](#patching)

------------------------------------------------------------------------

<a name="apiref"></a>
Autosetup API Reference
========================================================================

The Autosetup API is quite extensive and can be read either in
the [files in the `autosetup` dir](/dir/autosetup) or using:

>
```
$ ./configure --reference | less
```

That will include any docs from any TCL files in the `./autosetup` dir
which contain certain (simple) markup defined by autosetup.

This project's own autosetup-related APIs are in [proj.tcl][] or
[auto.def][].  The former contains helper APIs which are, more or
less, portable across projects (that file is re-used as-is in other
projects) and all have a `proj-` name prefix. The latter is the main
configure script driver and contains related functions which are
specific to this tree.


<a name="apitips"></a>
Autosetup API Tips
========================================================================

This section briefly covers only APIs which are frequently useful in
day-to-day maintenance and might not be immediately recognized as such
obvious from a casual perusal of [auto.def][]. The complete docs of
those with `proj-` prefix can be found in [proj.tcl][]. The others are
scattered around [the TCL files in ./autosetup](/dir/autosetup).

In (mostly) alphabetical order:

- **`file-isexec filename`**\  
  Should be used in place of `[file executable]`, as it will also
  check for `${filename}.exe` on Windows platforms. However, on such
  platforms it also assumes that _any_ existing file is executable.

- **`get-env VAR ?default?`**\  
  Will fetch an "environment variable" from the first of either: (1) a
  KEY=VALUE passed to the configure script or (2) the system's
  environment variables. Not to be confused with `getenv`, which only
  does the latter and is rarely, if ever, useful in this tree.
  - **`proj-get-env VAR ?default?`**\  
    Works like `get-env` but will, if that function finds no match,
    look for a file named `./.env-$VAR` and, if found, return its
    trimmed contents. This can be used, e.g., to set a developer's
    local preferences for the default `CFLAGS`.

- **`define-for-opt flag defineName ?checkingMsg? ?yesVal=1? ?noVal=0?`**\  
  `[define $defineName]` to either `$yesVal` or `$noVal`, depending on
  whether `--$flag` is truthy or not. `$checkingMsg` is a
  human-readable description of the check being made, e.g. "enable foo?"
  If no `checkingMsg` is provided, the operation is silent.\  
  Potential TODO: change the final two args to `-yes` and `-no`
  flags. They're rarely needed, though: search [auto.def][] for
  `TSTRNNR_OPTS` for an example of where they are used.

- **`proj-fatal msg`**\  
  Emits `$msg` to stderr and exits with non-zero.

- **`proj-if-opt-truthy flag thenScript ?elseScript?`**\  
  Evals `thenScript` if the given `--flag` is truthy, else it
  evals the optional `elseScript`.

- **`proj-indented-notice ?-error? ?-notice? msg`**\  
  Breaks its `msg` argument into lines, trims them, and emits them
  with consistent indentation. Exactly how it emits depends on the
  flags passed to it (or not), as covered in its docs. This will stick
  out starkly from normal output and is intended to be used only for
  important notices.

- **`proj-opt-truthy flag`**\  
  Returns 1 if `--flag`'s value is "truthy," i.e. one of (1, on,
  enabled, yes, true).

- **`proj-opt-was-provided FLAG`**\  
  Returns 1 if `--FLAG` was explicitly provided to configure,
  else 0. This distinction can be used to determine, e.g., whether
  `--with-readline` was provided or whether we're searching for
  readline by default. In the former case, failure to find it should
  be treated as fatal, where in the latter case it's not.

- **`proj-val-truthy value`**\  
  Returns 1 if `$value` is "truthy," See `proj-opt-truthy` for the definition
  of "truthy."

- **`proj-warn msg`**\  
  Emits `$msg` to stderr. Closely-related is autosetup's `user-notice`
  (described below).

- **`sqlite-add-feature-flag ?-shell? FLAG...`**\  
  Adds the given feature flag to the CFLAGS which are specific to
  building libsqlite3. It's intended to be passed one or more
  `-DSQLITE_ENABLE_...`, or similar, flags. If the `-shell` flag is
  used then it also passes its arguments to
  `sqlite-add-shell-opt`. This is a no-op if `FLAG` is not provided or
  is empty.

- **`sqlite-add-shell-opt FLAG...`**\  
  The shell-specific counterpart of `sqlite-add-feature-flag` which
  only adds the given flag(s) to the CLI-shell-specific CFLAGS.

- **`user-notice msg`**\  
  Queues `$msg` to be sent to stderr, but does not emit it until
  either `show-notices` is called or the next time autosetup would
  output something (it internally calls `show-notices`). This can be
  used to generate warnings between a "checking for..." message and
  its resulting "yes/no/whatever" message in such a way as to not
  spoil the layout of such messages.


<a name="tclcompat"></a>
Ensuring TCL Compatibility
========================================================================

It is important that any TCL files used by the configure process
remain compatible with both [JimTCL][] and the canonical TCL. Though
JimTCL has outstanding compatibility with canonical TCL, it does have
a few corners with incompatibilities, e.g. regular expressions. If a
script runs in JimTCL without using any JimTCL-specific features, then
it's a certainty that it will run in canonical TCL as well. The
opposite, however, is not _always_ the case.

When [`./configure`](/file/configure) is run, it goes through a
bootstrapping process to find a suitable TCL with which to run the
autosetup framework. The first step involves [finding or building a
TCL shell](/file/autosetup/autosetup-find-tclsh).  That will first
search for an available `tclsh` (under several common names,
e.g. `tclsh8.6`) before falling back to compiling the copy of
`jimsh0.c` included in the source tree. i.e. it will prefer to use a
system-installed TCL for running the configure script. Once it finds
(or builds) a TCL shell, it then runs [a sanity test to ensure that
the shell is suitable](/file/autosetup/autosetup-test-tclsh) before
using it to run the main autosetup app.

There are two simple ways to ensure that running of the configure
process uses JimTCL instead of the canonical `tclsh`, and either
approach provides equally high assurances about configure script
compatibility across TCL implementations:

1. Build on a system with no `tclsh` installed in the `$PATH`. In that
   case, the configure process will fall back to building the in-tree
   copy of JimTCL.

2. Manually build `./jimsh0` in the top of the checkout with:\  
   `cc -o jimsh0 autosetup/jimsh0.c`\  
   With that in place, the configure script will prefer to use that
   before looking for a system-level `tclsh`. Be aware, though, that
   `make distclean` will remove that file.

**Note that `jimsh0` is distinctly different from the `jimsh`** which
gets built for code-generation purposes.  The latter requires
non-default build flags to enable features which are
platform-dependent, most notably to make its `[file normalize]` work.
This means, for example, that the configure script and its utility
APIs must not use `[file normalize]`, but autosetup provides a
TCL-only implementation of `[file-normalize]` (note the dash) for
portable use in the configure script.


<a name="conventions"></a>
Design Conventions
========================================================================

This section describes the motivations for the most glaring of the
build's design decisions, in particular how they deviate from
historical, or even widely-conventional, practices.

Symbolic Names of Feature Flags
------------------------------------------------------------------------

Historically, the project's makefile has exclusively used
`UPPER_UNDERSCORE` form for makefile variables. This build, however,
primarily uses `X.y` format, where `X` is often a category label,
e.g. `CFLAGS`, and `y` is the specific instance of that category,
e.g. `CFLAGS.readline`.

When the configure script exports flags for consumption by filtered
files, e.g. [Makefile.in][] and the generated
`sqlite_cfg.h`, it does so in the more conventional `X_Y` form because
those flags get exported as as C `#define`s to `sqlite_cfg.h`, where
dots are not permitted.

The `X.y` convention is used in the makefiles primarily because the
person who did the initial port finds that considerably easier on the
eyes and fingers. In practice, the `X_Y` form of such exports is used
exactly once in [Makefile.in][], where it's translated into into `X.y`
form for consumption by [Makefile.in][] and [main.mk][]. For example:

>
```
LDFLAGS.shobj = @SHOBJ_LDFLAGS@
LDFLAGS.zlib = @LDFLAGS_ZLIB@
LDFLAGS.math = @LDFLAGS_MATH@
```

(That first one is defined by autosetup, and thus applies "LDFLAGS" as
the suffix rather than the prefix. Which is more legible is a matter
of taste, for which there is no accounting.)


Do Not Update Global Shared State
------------------------------------------------------------------------

In both the legacy Autotools-driven build and in common Autosetup
usage, feature tests performed by the configure script may amend
global flags such as `LIBS`, `LDFLAGS`, and `CFLAGS`[^as-cflags].  That's
appropriate for a makefile which builds a single deliverable, but less
so for makefiles which produce multiple deliverables. Drawbacks of
that approach include:

- It's unlikely that every single deliverable will require the same
  core set of those flags.
- It can be difficult to determine the origin of any given change to
  that global state because those changes are hidden behind voodoo performed
  outside the immediate visibility of the configure script's
  maintainer.
- It can force the maintainers of the configure script to place tests
  in a specific order so that the resulting flags get applied at
  the correct time and/or in the correct order.\  
  (A real-life example: before the approach described below was taken
  to collecting build-time flags, the test for `-rpath` had to come
  _after_ the test for zlib because the results of the `-rpath` test
  implicitly modified global state which broke the zlib feature
  test. Because the feature tests no longer (intentionally) modify
  shared global state, that is not an issue.)

In this build, cases where feature tests modify global state in such a
way that it may impact later feature tests are either (A) very
intentionally defined to do so (e.g. the `--with-wasi-sdk` flag has
invasive side-effects) or (B) are oversights (i.e. bugs).

This tree's [configure script][auto.def], [utility APIs][proj.tcl],
[Makefile.in][], and [main.mk][] therefore strive to separate the
results of any given feature test into its own well-defined
variables. For example:

- The linker flags for zlib are exported from the configure script as
  `LDFLAGS_ZLIB`, which [Makefile.in][] and [main.mk][] then expose as
  `LDFLAGS.zlib`.
- `CFLAGS_READLINE` (a.k.a. `CFLAGS.readline`) contains the `CFLAGS`
  needed for including `libreadline`, `libedit`, or `linenoise`, and
  `LDFLAGS_READLINE` (a.k.a. `LDFLAGS.readline`) is its link-time
  counterpart.

It is then up to the Makefile to apply and order the flags however is
appropriate.

At the end of the configure script, the global `CFLAGS` _ideally_
holds only flags which are either relevant to all targets or, failing
that, will have no unintended side-effects on any targets. That said:
clients frequently pass custom `CFLAGS` to `./configure` or `make` to
set library-level feature toggles, e.g. `-DSQLITE_OMIT_FOO`, in which
case there is no practical way to avoid "polluting" the builds of
arbitrary makefile targets with those. _C'est la vie._


[^as-cflags]: But see this article for a detailed discussion of how
    autosetup currently deals specifically with CFLAGS:
    <https://msteveb.github.io/autosetup/articles/handling-cflags/>


<a name="updating"></a>
Updating Autosetup
========================================================================

Updating autosetup is, more often than not, painless. It requires having
a checked-out copy of [the autosetup git repository][autosetup-git]:

>
```
$ git clone https://github.com/msteveb/autosetup
$ cd autosetup
# Or, if it's already checked out:
$ git pull
```

Then, from the top-most directory of an SQLite checkout:

>
```
$ /path/to/autosetup-checkout/autosetup --install .
$ fossil status # show the modified files
```

Unless the upgrade made any incompatible changes (which is exceedingly
rare), that's all there is to it.  After that's done, **apply a patch
for the change described in the following section**, test the
configure process, and check it in.

<a name="patching"></a>
Patching Autosetup for Project-local Changes
------------------------------------------------------------------------

Autosetup reserves the flag name **`--debug`** for its own purposes,
and its own special handling of `--enable-...` flags makes `--debug`
an alias for `--enable-debug`. As we have a long history of using
`--enable-debug` for this project's own purposes, we patch autosetup
to use the name `--autosetup-debug` in place of `--debug`. That
requires (as of this writing) four small edits in
[](/file/autosetup/autosetup), as demonstrated in [check-in
3296c8d3](/info/3296c8d3).

If autosetup is upgraded and this patch is _not_ applied the invoking
`./configure` will fail loudly because of the declaration of the
`debug` flag in `auto.def` - duplicated flags are not permitted.


[Autosetup]: https://msteveb.github.io/autosetup/
[auto.def]: /file/auto.def
[autosetup-git]: https://github.com/msteveb/autosetup
[proj.tcl]: /file/autosetup/proj.tcl
[Makefile.in]: /file/Makefile.in
[main.mk]: /file/main.mk
[JimTCL]: https://jim.tcl.tk
