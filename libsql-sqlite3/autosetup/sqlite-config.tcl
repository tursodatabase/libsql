# This file holds functions for autosetup which are specific to the
# sqlite build tree.  They are in this file, instead of auto.def, so
# that they can be reused in the autoconf sub-tree. This file requires
# functions from the project-agnostic proj.tcl.

if {[string first " " $autosetup(srcdir)] != -1} {
  user-error "The pathname of the source tree\
              may not contain space characters"
}
if {[string first " " $autosetup(builddir)] != -1} {
  user-error "The pathname of the build directory\
              may not contain space characters"
}

use proj
#
# We want the package version info to be emitted early on, but doing
# so requires a bit of juggling. We have to [use system] for
# --prefix=... to work and to emit the Host/Build system info, but we
# don't want those to interfere with --help output.
define PACKAGE_VERSION [proj-file-content -trim $::autosetup(srcdir)/VERSION]
if {"--help" ni $::argv} {
  msg-result "Configuring SQLite version [get-define PACKAGE_VERSION]"
}
use system ; # Will output "Host System" and "Build System" lines
if {"--help" ni $::argv} {
  proj-tweak-default-env-dirs
  msg-result "Source dir = $::autosetup(srcdir)"
  msg-result "Build dir  = $::autosetup(builddir)"
  use cc cc-db cc-shared cc-lib pkg-config
}

#
# Object for communicating certain config-time state across various
# auto.def-related pieces.
array set sqliteConfig [subst [proj-strip-hash-comments {
  #
  # Gets set by [sqlite-configure] (the main configure script driver).
  build-mode unknown
  #
  # Gets set to 1 when using jimsh for code generation. May affect
  # later decisions.
  use-jim-for-codegen  0
  #
  # Set to 1 when cross-compiling This value may be changed by certain
  # build options, so it's important that config code which checks for
  # cross-compilation uses this var instead of
  # [proj-is-cross-compiling].
  is-cross-compiling [proj-is-cross-compiling]
  #
  # Pass msg-debug=1 to configure to enable obnoxiously loud output
  # from [msg-debug].
  msg-debug-enabled    0
  #
  # Output file for --dump-defines. Intended only for build debugging
  # and not part of the public build interface.
  dump-defines-txt   ./config.defines.txt
  #
  # If not empty then --dump-defines will dump not only
  # (dump-defines-txt) but also a JSON file named after this option's
  # value.
  dump-defines-json  ""

  #
  # The list of feature --flags which the --all flag implies. This
  # requires special handling in a few places.
  #
  all-flag-enables {fts4 fts5 rtree geopoly session dbpage dbstat carray}

  #
  # Default value for the --all flag. Can hypothetically be modified
  # by non-canonical builds (it was added for a Tcl extension build
  # mode which was eventually removed).
  #
  all-flag-default 0
}]]

########################################################################
# Processes all configure --flags for this build, run build-specific
# config checks, then finalize the configure process. $buildMode must
# be one of (canonical, autoconf), and others may be added in the
# future. After bootstrapping, $configScript is eval'd in the caller's
# scope, then post-configuration finalization is run. $configScript is
# intended to hold configure code which is specific to the given
# $buildMode, with the caveat that _some_ build-specific code is
# encapsulated in the configuration finalization step.
#
# The intent is that all (or almost all) build-mode-specific
# configuration goes inside the $configScript argument to this
# function, and that an auto.def file contains only two commands:
#
#  use sqlite-config
#  sqlite-configure BUILD_NAME { build-specific configure script }
#
# There are snippets of build-mode-specific decision-making in
# [sqlite-configure-finalize], which gets run after $configScript.
proc sqlite-configure {buildMode configScript} {
  proj-assert {$::sqliteConfig(build-mode) eq "unknown"} \
    "sqlite-configure must not be called more than once"
  set allBuildModes {canonical autoconf}
  if {$buildMode ni $allBuildModes} {
    user-error "Invalid build mode: $buildMode. Expecting one of: $allBuildModes"
  }
  if {$::sqliteConfig(all-flag-default)} {
    set allFlagHelp "Disable these extensions: $::sqliteConfig(all-flag-enables)"
  } else {
    set allFlagHelp "Enable these extensions: $::sqliteConfig(all-flag-enables)"
  }

  set ::sqliteConfig(build-mode) $buildMode
  ########################################################################
  # A gentle introduction to flags handling in autosetup
  #
  # Reference: https://msteveb.github.io/autosetup/developer/
  #
  # All configure flags must be described in one or more calls to
  # autosetup's [options] and [options-add] functions. The general
  # syntax of the single argument to those functions is a list contain
  # a mapping of flags to help text:
  #
  #  FLAG => {Help text}
  #
  # Where FLAG can have any of the following formats:
  #
  #   boolopt            => "a boolean option which defaults to disabled"
  #   boolopt2=1         => "a boolean option which defaults to enabled"
  #   stringopt:         => "an option which takes an argument, e.g. --stringopt=value"
  #   stringopt:DESCR    => As for stringopt: with a description for the value
  #   stringopt2:=value  => "an option where the argument is optional and defaults to 'value'"
  #   optalias booltopt3 => "a boolean with a hidden alias. --optalias is not shown in --help"
  #
  # Autosetup does no small amount of specialized handling for flags,
  # especially booleans. Each bool-type --FLAG implicitly gets
  # --enable-FLAG and --disable-FLAG forms. That can lead lead to some
  # confusion when writing help text. For example:
  #
  #   options { json=1 {Disable JSON functions} }
  #
  # The reason the help text says "disable" is because a boolean option
  # which defaults to true is, in the --help text, rendered as:
  #
  #   --disable-json          Disable JSON functions
  #
  # Whereas a bool flag which defaults to false will instead render as:
  #
  #   --enable-FLAG
  #
  # Non-boolean flags, in contrast, use the names specifically given to
  # them in the [options] invocation. e.g. "with-tcl" is the --with-tcl
  # flag.
  #
  # Fetching values for flags:
  #
  #   booleans: use one of:
  #     - [opt-bool FLAG] is autosetup's built-in command for this, but we
  #       have some convenience variants:
  #     - [proj-opt-truthy FLAG]
  #     - [proj-opt-if-truthy FLAG {THEN} {ELSE}]
  #
  #   Non-boolean (i.e. string) flags:
  #     - [opt-val FLAG ?default?]
  #     - [opt-str ...] - see the docs in ./autosetup/autosetup
  #
  # [proj-opt-was-provided] can be used to determine whether a flag was
  # explicitly provided, which is often useful for distinguishing from
  # the case of a default value.
  ########################################################################
  set allFlags {
    # Structure: a list of M {Z} pairs, where M is a descriptive
    # option group name and Z is a list of X Y pairs. X is a list of
    # $buildMode name(s) to which the Y flags apply, or {*} to apply
    # to all builds. Y is a {block} in the form expected by
    # autosetup's [options] and [options-add] command.  Each block
    # which is applicable to $buildMode is passed on to
    # [options-add]. The order of each Y and sub-Y is retained, which
    # is significant for rendering of --help.
    #
    # Maintenance note: [options] does not support comments in
    # options, but we filter this object through
    # [proj-strip-hash-comments] to remove them before passing them on
    # to [options].

    # When writing {help text blocks}, be aware that:
    #
    # A) autosetup formats them differently if the {block} starts with
    # a newline: it starts left-aligned, directly under the --flag, and
    # the rest of the block is pasted verbatim rather than
    # pretty-printed.
    #
    # B) Vars and commands are NOT expanded, but we use a [subst] call
    # below which will replace (only) $var refs.

    # Options for how to build the library
    build-modes {
      {canonical autoconf} {
        shared=1             => {Disable build of shared library}
        static=1             => {Disable build of static library}
      }
      {canonical} {
        amalgamation=1       => {Disable the amalgamation and instead build all files separately}
      }
    }

    # Library-level features and defaults
    lib-features {
      {*} {
        threadsafe=1         => {Disable mutexing}
        with-tempstore:=no   => {Use an in-RAM database for temporary tables: never,no,yes,always}
        load-extension=1     => {Disable loading of external extensions}
        # ^^^ one of the downstream custom builds overrides the load-extension default to 0, which
        # confuses the --help text generator. https://github.com/msteveb/autosetup/issues/77
        math=1               => {Disable math functions}
        json=1               => {Disable JSON functions}
        memsys5              => {Enable MEMSYS5}
        memsys3              => {Enable MEMSYS3}
        fts3                 => {Enable the FTS3 extension}
        fts4                 => {Enable the FTS4 extension}
        fts5                 => {Enable the FTS5 extension}
        wasm-runtime         => {Enable libSQL WebAssembly runtime integration}
        wasm-runtime-dynamic => {Link libSQL WebAssembly runtime dynamically}
        wasm-runtime-wasmedge
          => {Enable libSQL WebAssembly runtime integration using WasmEdge}
        update-limit         => {Enable the UPDATE/DELETE LIMIT clause}
        geopoly              => {Enable the GEOPOLY extension}
        rtree                => {Enable the RTREE extension}
        session              => {Enable the SESSION extension}
        dbpage               => {Enable the sqlite3_dbpage extension}
        dbstat               => {Enable the sqlite3_dbstat extension}
        carray=1             => {Disable the CARRAY extension}
        all=$::sqliteConfig(all-flag-default) => {$allFlagHelp}
        largefile=1
          => {This legacy flag has no effect on the library but may influence
              the generated sqlite_cfg.h by adding #define HAVE_LFS}
      }
      {canonical} {
        column-metadata      => {Enable the column metadata APIs}
        # ^^^ Affects how sqlite3.c is generated, so is not available in
        # the autoconf build.
      }
    }

    # Options for TCL support
    tcl {
      {canonical} {
        tcl=1
          => {Disable components which require TCL, including all tests.
              This tree requires TCL for code generation but can use the in-tree
              copy of autosetup/jimsh0.c for that. The SQLite TCL extension and the
              test code require a canonical tclsh.}
        with-tcl:DIR
          => {Directory containing tclConfig.sh or a directory one level up from
              that, from which we can derive a directory containing tclConfig.sh.
              A dir name of "prefix" is equivalent to the directory specified by
              the --prefix flag.}
        with-tclsh:PATH
          => {Full pathname of tclsh to use.  It is used for (A) trying to find
              tclConfig.sh and (B) all TCL-based code generation. Use --with-tcl
              unless you have a specific need for this flag. Warning: if its
              containing dir has multiple tclsh versions, it may select the
              wrong tclConfig.sh!}
        static-tclsqlite3=0
          => {Statically-link tclsqlite3. This only works if TCL support is
              enabled and all requisite libraries are available in
              static form. Note that glibc is unable to fully statically
              link certain libraries required by tclsqlite3, so this won't
              work on most Linux environments.}
      }
    }

    # Options for line-editing modes for the CLI shell
    line-editing {
      {canonical autoconf} {
        readline=1
          => {Disable readline support}
        # --with-readline-lib is a backwards-compatible alias for
        # --with-readline-ldflags
        with-readline-lib:
        with-readline-ldflags:=auto
          => {Readline LDFLAGS, e.g. -lreadline -lncurses}
        # --with-readline-inc is a backwards-compatible alias for
        # --with-readline-cflags.
        with-readline-inc:
        with-readline-cflags:=auto
          => {Readline CFLAGS, e.g. -I/path/to/includes}
        with-readline-header:PATH
          => {Full path to readline.h, from which --with-readline-cflags will be derived}
        with-linenoise:DIR
          => {Source directory for linenoise.c and linenoise.h}
        editline=0
          => {Enable BSD editline support}
      }
    }

    # Options for ICU: International Components for Unicode
    icu {
      {*} {
        with-icu-ldflags:LDFLAGS
          => {Enable SQLITE_ENABLE_ICU and add the given linker flags for the
              ICU libraries. e.g. on Ubuntu systems, try '-licui18n -licuuc -licudata'.}
        with-icu-cflags:CFLAGS
          => {Apply extra CFLAGS/CPPFLAGS necessary for building with ICU.
              e.g. -I/usr/local/include}
        with-icu-config:=auto
          => {Enable SQLITE_ENABLE_ICU. Value must be one of: auto, pkg-config,
              /path/to/icu-config}
        icu-collations=0
          => {Enable SQLITE_ENABLE_ICU_COLLATIONS. Requires --with-icu-ldflags=...
              or --with-icu-config}
      }
    }

    # Options for exotic/alternative build modes
    alternative-builds {
      {canonical autoconf} {
        with-wasi-sdk:=/opt/wasi-sdk
          => {Top-most dir of the wasi-sdk for a WASI build}
      }

      {*} {
        # Note that --static-cli-shell has a completely different
        # meaning from --static-shell in the autoconf build!
        # --[disable-]static-shell is a legacy flag which we can't
        # remove without breaking downstream builds.
        static-cli-shell=0
          => {Statically-link the sqlite3 CLI shell.
              This only works if the requisite libraries are all available in
              static form.}
      }

      {canonical} {
        static-shells=0
          => {Shorthand for --static-cli-shell --static-tclsqlite3}

        with-emsdk:=auto
          => {Top-most dir of the Emscripten SDK installation.
              Needed only by ext/wasm. Default=EMSDK env var.}

        amalgamation-extra-src:FILES
          => {Space-separated list of source files to append as-is to the resulting
              sqlite3.c amalgamation file. May be provided multiple times.}
      }
    }

    # Options primarily for downstream packagers/package maintainers
    packaging {
      {autoconf} {
        # --disable-static-shell: https://sqlite.org/forum/forumpost/cc219ee704
        # Note that this has a different meaning from --static-cli-shell in the
        # canonical build!
        static-shell=1
          => {Link the sqlite3 shell app against the DLL instead of embedding sqlite3.c}
      }
      {canonical autoconf} {
        rpath=1 => {Disable use of the rpath linker flag}
        # soname: https://sqlite.org/src/forumpost/5a3b44f510df8ded
        soname:=legacy
          => {SONAME for libsqlite3.so. "none", or not using this flag, sets no
              soname. "legacy" sets it to its historical value of
              libsqlite3.so.0.  A value matching the glob "libsqlite3.*" sets
              it to that literal value. Any other value is assumed to be a
              suffix which gets applied to "libsqlite3.so.",
              e.g. --soname=9.10 equates to "libsqlite3.so.9.10".}
        # dll-basename: https://sqlite.org/forum/forumpost/828fdfe904
        dll-basename:=auto
          => {Specifies the base name of the resulting DLL file.
              If not provided, "libsqlite3" is usually assumed but on some platforms
              a platform-dependent default is used. On some platforms this flag
              gets automatically enabled if it is not provided. Use "default" to
              explicitly disable platform-dependent activation on such systems.}
        # out-implib: https://sqlite.org/forum/forumpost/0c7fc097b2
        out-implib:=auto
          => {Enable use of --out-implib linker flag to generate an
              "import library" for the DLL. The output's base name is
              specified by this flag's value, with "auto" meaning to figure
              out a name automatically. On some platforms this flag gets
              automatically enabled if it is not provided. Use "none" to
              explicitly disable this feature on such platforms.}
      }
    }

    # Options mostly for sqlite's own development
    developer {
      {*} {
        # Note that using the --debug/--enable-debug flag here
        # requires patching autosetup/autosetup to rename its builtin
        # --debug to --autosetup-debug. See details in
        # autosetup/README.md#patching.
        with-debug=0
        debug=0
          => {Enable debug build flags. This option will impact performance by
              as much as 4x, as it includes large numbers of assert()s in
              performance-critical loops.  Never use --debug for production
              builds.}
        scanstatus
          => {Enable the SQLITE_ENABLE_STMT_SCANSTATUS feature flag}
      }
      {canonical} {
        dev
          => {Enable dev-mode build: automatically enables certain other flags}
        test-status
          => {Enable status of tests}
        gcov=0
          => {Enable coverage testing using gcov}
        linemacros
          => {Enable #line macros in the amalgamation}
        dynlink-tools
          => {Dynamically link libsqlite3 to certain tools which normally statically embed it}
        asan-fsanitize:=auto
          => {Comma- or space-separated list of -fsanitize flags for use with the
              fuzzcheck-asan tool. Only those which the compiler claims to support
              will actually be used. May be provided multiple times.}
      }
      {*} {
        dump-defines=0
          => {Dump autosetup defines to $::sqliteConfig(dump-defines-txt)
              (for build debugging)}
      }
    }
  }; # $allFlags

  set allFlags [proj-strip-hash-comments $allFlags]
  # ^^^ lappend of [sqlite-custom-flags] introduces weirdness if
  # we delay [proj-strip-hash-comments] until after that.

  ########################################################################
  # sqlite-custom.tcl is intended only for vendor-branch-specific
  # customization.  See autosetup/README.md#branch-customization for
  # details.
  if {[file exists $::autosetup(libdir)/sqlite-custom.tcl]} {
    uplevel 1 {source $::autosetup(libdir)/sqlite-custom.tcl}
  }

  if {[llength [info proc sqlite-custom-flags]] > 0} {
    # sqlite-custom-flags is assumed to be imported via
    # autosetup/sqlite-custom.tcl.
    set scf [sqlite-custom-flags]
    if {"" ne $scf} {
      lappend allFlags sqlite-custom-flags $scf
    }
  }

  #lappend allFlags just-testing {{*} {soname:=duplicateEntry => {x}}}

  # Filter allFlags to create the set of [options] legal for this build
  foreach {group XY} [subst -nobackslashes -nocommands $allFlags] {
    foreach {X Y} $XY {
      if { $buildMode in $X || "*" in $X } {
        options-add $Y
      }
    }
  }

  if {[catch {options {}} msg xopts]} {
    # Workaround for <https://github.com/msteveb/autosetup/issues/73>
    # where [options] behaves oddly on _some_ TCL builds when it's
    # called from deeper than the global scope.
    dict incr xopts -level
    return {*}$xopts $msg
  }
  sqlite-configure-phase1 $buildMode
  uplevel 1 $configScript
  sqlite-configure-finalize
}; # sqlite-configure

########################################################################
# Runs "phase 1" of the configure process: after initial --flags
# handling but before sqlite-configure's $configScript argument is
# run. $buildMode must be the mode which was passed to
# [sqlite-configure].
proc sqlite-configure-phase1 {buildMode} {
  define PACKAGE_NAME sqlite
  define PACKAGE_URL {https://sqlite.org}
  define PACKAGE_BUGREPORT [get-define PACKAGE_URL]/forum
  define PACKAGE_STRING "[get-define PACKAGE_NAME] [get-define PACKAGE_VERSION]"
  proj-xfer-options-aliases {
    # Carry values from hidden --flag aliases over to their canonical
    # flag forms. This list must include only options which are common
    # to all build modes supported by [sqlite-configure].
    with-readline-inc => with-readline-cflags
    with-readline-lib => with-readline-ldflags
    with-debug => debug
  }
  set ::sqliteConfig(msg-debug-enabled) [proj-val-truthy [get-env msg-debug 0]]
  proc-debug "msg-debug is enabled"
  proj-setup-autoreconfig SQLITE_AUTORECONFIG
  proj-file-extensions
  if {".exe" eq [get-define TARGET_EXEEXT]} {
    define SQLITE_OS_UNIX 0
    define SQLITE_OS_WIN 1
  } else {
    define SQLITE_OS_UNIX 1
    define SQLITE_OS_WIN 0
  }
  sqlite-setup-default-cflags
  define HAVE_LFS 0
  if {[opt-bool largefile]} {
    #
    # Insofar as we can determine HAVE_LFS has no effect on the
    # library.  Perhaps it did back in the early 2000's. The
    # --enable/disable-largefile flag is retained because it's
    # harmless, but it doesn't do anything useful. It does have
    # visible side-effects, though: the generated sqlite_cfg.h may (or
    # may not) define HAVE_LFS.
    cc-check-lfs
  }
  set srcdir $::autosetup(srcdir)
  proj-dot-ins-append $srcdir/Makefile.in
  if {[file exists $srcdir/sqlite3.pc.in]} {
    proj-dot-ins-append $srcdir/sqlite3.pc.in
  }
  sqlite-handle-hpux; # must be relatively early so that other config tests can work
}; # sqlite-configure-phase1

########################################################################
# Performs late-stage config steps common to all supported
# $::sqliteConfig(build-mode) values.
proc sqlite-configure-finalize {} {
  sqlite-handle-rpath
  sqlite-handle-soname
  sqlite-handle-threadsafe
  sqlite-handle-tempstore
  sqlite-handle-load-extension
  sqlite-handle-math
  sqlite-handle-icu
  if {[proj-opt-exists readline]} {
    sqlite-handle-line-editing
  }
  if {[proj-opt-exists shared]} {
    proj-define-for-opt shared ENABLE_LIB_SHARED "Build shared library?"
  }
  if {[proj-opt-exists static]} {
    if {![proj-define-for-opt static ENABLE_LIB_STATIC "Build static library?"]} {
      # This notice really only applies to the canonical build...
      proj-indented-notice {
        NOTICE: static lib build may be implicitly re-activated by
        other components, e.g. some test apps.
      }
    }
  }
  sqlite-handle-env-quirks
  sqlite-handle-common-feature-flags
  sqlite-finalize-feature-flags
  sqlite-process-dot-in-files; # do not [define] anything after this
  sqlite-dump-defines
}

########################################################################
# Internal config-time debugging output routine. It generates no
# output unless msg-debug=1 is passed to the configure script.
proc msg-debug {msg} {
  if {$::sqliteConfig(msg-debug-enabled)} {
    puts stderr [proj-bold "** DEBUG: $msg"]
  }
}
########################################################################
# A [msg-debug] proxy which prepends the name of the current proc to
# the debug message. It is not legal to call this from the global
# scope.
proc proc-debug {msg} {
  msg-debug "\[[proj-scope 1]\]: $msg"
}

define OPT_FEATURE_FLAGS {} ; # -DSQLITE_OMIT/ENABLE flags.
define OPT_SHELL {}         ; # Feature-related CFLAGS for the sqlite3 CLI app
########################################################################
# Adds $args, if not empty, to OPT_FEATURE_FLAGS.  If the first arg is
# -shell then it strips that arg and passes the remaining args the
# sqlite-add-shell-opt in addition to adding them to
# OPT_FEATURE_FLAGS. This is intended only for holding
# -DSQLITE_ENABLE/OMIT/... flags, but that is not enforced here.
proc sqlite-add-feature-flag {args} {
  set shell ""
  if {"-shell" eq [lindex $args 0]} {
    set args [lassign $args shell]
  }
  if {"" ne $args} {
    if {"" ne $shell} {
      sqlite-add-shell-opt {*}$args
    }
    define-append OPT_FEATURE_FLAGS {*}$args
  }
}

########################################################################
# Appends $args, if not empty, to OPT_SHELL.
proc sqlite-add-shell-opt {args} {
  if {"" ne $args} {
    define-append OPT_SHELL {*}$args
  }
}

########################################################################
# Check for log(3) in libm and die with an error if it is not
# found. $featureName should be the feature name which requires that
# function (it's used only in error messages). defines LDFLAGS_MATH to
# the required linker flags (which may be empty even if the math APIs
# are found, depending on the OS).
proc sqlite-affirm-have-math {featureName} {
  if {"" eq [get-define LDFLAGS_MATH ""]} {
    if {![msg-quiet proj-check-function-in-lib log m]} {
      user-error "Missing math APIs for $featureName"
    }
    set lfl [get-define lib_log ""]
    undefine lib_log
    if {"" ne $lfl} {
      user-notice "Forcing requirement of $lfl for $featureName"
    }
    define LDFLAGS_MATH $lfl
  }
}

########################################################################
# Run checks for required binaries, like ld and ar. In the canonical
# build this must come before [sqlite-handle-wasi-sdk].
proc sqlite-check-common-bins {} {
  cc-check-tools ld ar ; # must come before [sqlite-handle-wasi-sdk]
  if {"" eq [proj-bin-define install]} {
    proj-warn "Cannot find install binary, so 'make install' will not work."
    define BIN_INSTALL false
  }
}

########################################################################
# Run checks for system-level includes and libs which are common to
# both the canonical build and the "autoconf" bundle.
#
# For the canonical build this must come after
# [sqlite-handle-wasi-sdk], as that function may change the
# environment in ways which affect this.
proc sqlite-check-common-system-deps {} {
  # Check for needed/wanted data types
  cc-with {-includes stdint.h} \
    {cc-check-types int8_t int16_t int32_t int64_t intptr_t \
       uint8_t uint16_t uint32_t uint64_t uintptr_t}

  # Check for needed/wanted functions
  cc-check-functions gmtime_r isnan localtime_r localtime_s \
    usleep utime pread pread64 pwrite pwrite64

  apply {{} {
    set ldrt ""
    # Collapse funcs from librt into LDFLAGS_RT.
    # Some systems (ex: SunOS) require -lrt in order to use nanosleep
    foreach func {fdatasync nanosleep} {
      if {[proj-check-function-in-lib $func rt]} {
        set ldrt [get-define lib_${func} ""]
        undefine lib_${func}
        if {"" ne $ldrt} {
          break
        }
      }
    }
    define LDFLAGS_RT $ldrt
  }}

  # Check for needed/wanted headers
  cc-check-includes \
    sys/types.h sys/stat.h dlfcn.h unistd.h \
    stdlib.h malloc.h memory.h \
    string.h strings.h \
    inttypes.h

  if {[cc-check-includes zlib.h] && [proj-check-function-in-lib deflate z]} {
    # TODO? port over the more sophisticated zlib search from the fossil auto.def
    define HAVE_ZLIB 1
    define LDFLAGS_ZLIB -lz
    sqlite-add-shell-opt -DSQLITE_HAVE_ZLIB=1
  } else {
    define HAVE_ZLIB 0
    define LDFLAGS_ZLIB ""
  }
}

########################################################################
# Move -DSQLITE_OMIT... and -DSQLITE_ENABLE... flags from CFLAGS and
# CPPFLAGS to OPT_FEATURE_FLAGS and remove them from BUILD_CFLAGS.
proc sqlite-munge-cflags {} {
  # Move CFLAGS and CPPFLAGS entries matching -DSQLITE_OMIT* and
  # -DSQLITE_ENABLE* to OPT_FEATURE_FLAGS. This behavior is derived
  # from the legacy build and was missing the 3.48.0 release (the
  # initial Autosetup port).
  # https://sqlite.org/forum/forumpost/9801e54665afd728
  #
  # Handling of CPPFLAGS, as well as removing ENABLE/OMIT from
  # CFLAGS/CPPFLAGS, was missing in the 3.49.0 release as well.
  #
  # If any configure flags for features are in conflict with
  # CFLAGS/CPPFLAGS-specified feature flags, all bets are off.  There
  # are no guarantees about which one will take precedence.
  foreach flagDef {CFLAGS CPPFLAGS} {
    set tmp ""
    foreach cf [get-define $flagDef ""] {
      switch -glob -- $cf {
        -DSQLITE_OMIT* -
        -DSQLITE_ENABLE* {
          sqlite-add-feature-flag $cf
        }
        default {
          lappend tmp $cf
        }
      }
    }
    define $flagDef $tmp
  }

  # Strip all SQLITE_ENABLE/OMIT flags from BUILD_CFLAGS,
  # for compatibility with the legacy build.
  set tmp ""
  foreach cf [get-define BUILD_CFLAGS ""] {
    switch -glob -- $cf {
      -DSQLITE_OMIT* -
      -DSQLITE_ENABLE* {}
      default {
        lappend tmp $cf
      }
    }
  }
  define BUILD_CFLAGS $tmp
}

#########################################################################
# Set up the default CFLAGS and BUILD_CFLAGS values.
proc sqlite-setup-default-cflags {} {
  ########################################################################
  # We differentiate between two C compilers: the one used for binaries
  # which are to run on the build system (in autosetup it's called
  # CC_FOR_BUILD and in Makefile.in it's $(B.cc)) and the one used for
  # compiling binaries for the target system (CC a.k.a. $(T.cc)).
  # Normally they're the same, but they will differ when
  # cross-compiling.
  #
  # When cross-compiling we default to not using the -g flag, based on a
  # /chat discussion prompted by
  # https://sqlite.org/forum/forumpost/9a67df63eda9925c
  set defaultCFlags {-O2}
  if {!$::sqliteConfig(is-cross-compiling)} {
    lappend defaultCFlags -g
  }
  define CFLAGS [proj-get-env CFLAGS $defaultCFlags]
  # BUILD_CFLAGS is the CFLAGS for CC_FOR_BUILD.
  define BUILD_CFLAGS [proj-get-env BUILD_CFLAGS {-g}]
  sqlite-munge-cflags
}

########################################################################
# Handle various SQLITE_ENABLE/OMIT_... feature flags.
proc sqlite-handle-common-feature-flags {} {
  msg-result "Feature flags..."
  if {![opt-bool all]} {
    # Special handling for --disable-all
    foreach flag $::sqliteConfig(all-flag-enables) {
      if {![proj-opt-was-provided $flag]} {
        proj-opt-set $flag 0
      }
    }
  }
  foreach {boolFlag featureFlag ifSetEvalThis} [proj-strip-hash-comments {
    all         {} {
      # The 'all' option must be first in this list.  This impl makes
      # an effort to only apply flags which the user did not already
      # apply, so that combinations like (--all --disable-geopoly)
      # will indeed disable geopoly. There are corner cases where
      # flags which depend on each other will behave in non-intuitive
      # ways:
      #
      # --all --disable-rtree
      #
      # Will NOT disable geopoly, though geopoly depends on rtree.
      # The --geopoly flag, though, will automatically re-enable
      # --rtree, so --disable-rtree won't actually disable anything in
      # that case.
      foreach k $::sqliteConfig(all-flag-enables) {
        if {![proj-opt-was-provided $k]} {
          proj-opt-set $k 1
        }
      }
    }
    fts3         -DSQLITE_ENABLE_FTS3    {sqlite-affirm-have-math fts3}
    fts4         -DSQLITE_ENABLE_FTS4    {sqlite-affirm-have-math fts4}
    fts5         -DSQLITE_ENABLE_FTS5    {sqlite-affirm-have-math fts5}
    geopoly      -DSQLITE_ENABLE_GEOPOLY {proj-opt-set rtree}
    rtree        -DSQLITE_ENABLE_RTREE   {}
    session      {-DSQLITE_ENABLE_SESSION -DSQLITE_ENABLE_PREUPDATE_HOOK} {}
    update-limit -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT {}
    memsys5      -DSQLITE_ENABLE_MEMSYS5 {}
    memsys3      {} {
      if {[opt-bool memsys5]} {
        proj-warn "not enabling memsys3 because memsys5 is enabled."
        expr 0
      } else {
        sqlite-add-feature-flag -DSQLITE_ENABLE_MEMSYS3
      }
    }
    scanstatus      -DSQLITE_ENABLE_STMT_SCANSTATUS {}
    column-metadata -DSQLITE_ENABLE_COLUMN_METADATA {}
    dbpage          -DSQLITE_ENABLE_DBPAGE_VTAB {}
    dbstat          -DSQLITE_ENABLE_DBSTAT_VTAB {}
    carray          -DSQLITE_ENABLE_CARRAY {}
  }] {
    if {$boolFlag ni $::autosetup(options)} {
      # Skip flags which are in the canonical build but not
      # the autoconf bundle.
      continue
    }
    proj-if-opt-truthy $boolFlag {
      sqlite-add-feature-flag $featureFlag
      if {0 != [eval $ifSetEvalThis] && "all" ne $boolFlag} {
        msg-result "  + $boolFlag"
      }
    } {
      if {"all" ne $boolFlag} {
        msg-result "  - $boolFlag"
      }
    }
  }
  ########################################################################
  # Invert the above loop's logic for some SQLITE_OMIT_...  cases. If
  # config option $boolFlag is false, [sqlite-add-feature-flag
  # $featureFlag], where $featureFlag is intended to be
  # -DSQLITE_OMIT_...
  foreach {boolFlag featureFlag} {
    json        -DSQLITE_OMIT_JSON
  } {
    if {[proj-opt-truthy $boolFlag]} {
      msg-result "  + $boolFlag"
    } else {
      sqlite-add-feature-flag $featureFlag
      msg-result "  - $boolFlag"
    }
  }
}

#########################################################################
# Remove duplicates from the final feature flag sets and show them to
# the user.
proc sqlite-finalize-feature-flags {} {
  set oFF [get-define OPT_FEATURE_FLAGS]
  if {"" ne $oFF} {
    define OPT_FEATURE_FLAGS [lsort -unique $oFF]
    msg-result "Library feature flags: [get-define OPT_FEATURE_FLAGS]"
  }
  set oFF [get-define OPT_SHELL]
  if {"" ne $oFF} {
    define OPT_SHELL [lsort -unique $oFF]
    msg-result "Shell options: [get-define OPT_SHELL]"
  }
  if {"" ne [set extraSrc [get-define AMALGAMATION_EXTRA_SRC ""]]} {
    proj-assert {"canonical" eq $::sqliteConfig(build-mode)}
    msg-result "Appending source files to amalgamation: $extraSrc"
  }
  if {[lsearch [get-define TARGET_DEBUG ""] -DSQLITE_DEBUG=1] > -1} {
    msg-result "Note: this is a debug build, so performance will suffer."
  }
}

########################################################################
# Checks for the --debug flag and [define]s TARGET_DEBUG based on
# that.  TARGET_DEBUG is unused in the autoconf build but that is
# arguably a bug.
proc sqlite-handle-debug {} {
  msg-checking "SQLITE_DEBUG build? "
  proj-if-opt-truthy debug {
    define TARGET_DEBUG {-g -DSQLITE_DEBUG=1 -O0 -Wall}
    sqlite-add-feature-flag -DSQLITE_ENABLE_SELECTTRACE -DSQLITE_ENABLE_WHERETRACE
    proj-opt-set memsys5
    msg-result yes
  } {
    define TARGET_DEBUG {-DNDEBUG}
    msg-result no
  }
}

########################################################################
# "soname" for libsqlite3.so. See discussion at:
# https://sqlite.org/src/forumpost/5a3b44f510df8ded
proc sqlite-handle-soname {} {
  define LDFLAGS_LIBSQLITE3_SONAME ""
  if {[proj-opt-was-provided soname]} {
    set soname [join [opt-val soname] ""]
  } else {
    # Enabling soname breaks linking for the --dynlink-tools feature,
    # and this project has no direct use for soname, so default to
    # none. Package maintainers, on the other hand, like to have an
    # soname.
    set soname none
  }
  switch -exact -- $soname {
    none - "" { return 0 }
    legacy    { set soname libsqlite3.so.0 }
    default {
      if {[string match libsqlite3.* $soname]} {
        # use it as-is
      } else {
        # Assume it's a suffix
        set soname "libsqlite3.so.${soname}"
      }
    }
  }
  proc-debug "soname=$soname"
  if {[proj-check-soname $soname]} {
    define LDFLAGS_LIBSQLITE3_SONAME [get-define LDFLAGS_SONAME_PREFIX]$soname
    msg-result "Setting SONAME using: [get-define LDFLAGS_LIBSQLITE3_SONAME]"
  } elseif {[proj-opt-was-provided soname]} {
    # --soname was explicitly requested but not available, so fail fatally
    proj-fatal "This environment does not support SONAME."
  } else {
    # --soname was not explicitly requested but not available, so just warn
    msg-result "This environment does not support SONAME."
  }
}

########################################################################
# If --enable-threadsafe is set, this adds -DSQLITE_THREADSAFE=1 to
# OPT_FEATURE_FLAGS and sets LDFLAGS_PTHREAD to the linker flags
# needed for linking pthread (possibly an empty string). If
# --enable-threadsafe is not set, adds -DSQLITE_THREADSAFE=0 to
# OPT_FEATURE_FLAGS and sets LDFLAGS_PTHREAD to an empty string.
proc sqlite-handle-threadsafe {} {
  msg-checking "Support threadsafe operation? "
  define LDFLAGS_PTHREAD ""
  set enable 0
  proj-if-opt-truthy threadsafe {
    msg-result "Checking for libs..."
    if {[proj-check-function-in-lib pthread_create pthread]
        && [proj-check-function-in-lib pthread_mutexattr_init pthread]} {
      set enable 1
      define LDFLAGS_PTHREAD [get-define lib_pthread_create]
      undefine lib_pthread_create
      undefine lib_pthread_mutexattr_init
    } elseif {[proj-opt-was-provided threadsafe]} {
      user-error "Missing required pthread libraries. Use --disable-threadsafe to disable this check."
    } else {
      msg-result "pthread support not detected"
    }
    # Recall that LDFLAGS_PTHREAD might be empty even if pthreads if
    # found because it's in -lc on some platforms.
  } {
    msg-result "Disabled using --disable-threadsafe"
  }
  sqlite-add-feature-flag -DSQLITE_THREADSAFE=${enable}
  return $enable
}

########################################################################
# Handles the --with-tempstore flag.
#
# The test fixture likes to set SQLITE_TEMP_STORE on its own, so do
# not set that feature flag unless it was explicitly provided to the
# configure script.
proc sqlite-handle-tempstore {} {
  if {[proj-opt-was-provided with-tempstore]} {
    set ts [opt-val with-tempstore no]
    set tsn 1
    msg-checking "Use an in-RAM database for temporary tables? "
    switch -exact -- $ts {
      never  { set tsn 0 }
      no     { set tsn 1 }
      yes    { set tsn 2 }
      always { set tsn 3 }
      default {
        user-error "Invalid --with-tempstore value '$ts'. Use one of: never, no, yes, always"
      }
    }
    msg-result $ts
    sqlite-add-feature-flag -DSQLITE_TEMP_STORE=$tsn
  }
}

########################################################################
# Check for the Emscripten SDK for building the web-based wasm
# components.  The core lib and tools do not require this but ext/wasm
# does. Most of the work is done via [proj-check-emsdk], then this
# function adds the following defines:
#
# - EMCC_WRAPPER = "" or top-srcdir/tool/emcc.sh
# - BIN_WASM_OPT = "" or path to wasm-opt
# - BIN_WASM_STRIP = "" or path to wasm-strip
#
# Noting that:
#
# 1) Not finding the SDK is not fatal at this level, nor is failure to
#    find one of the related binaries.
#
# 2) wasm-strip is part of the wabt package:
#
#   https://github.com/WebAssembly/wabt
#
# and this project requires it for production-mode builds but not dev
# builds.
#
proc sqlite-handle-emsdk {} {
  define EMCC_WRAPPER ""
  define BIN_WASM_STRIP ""
  define BIN_WASM_OPT ""
  set srcdir $::autosetup(srcdir)
  if {$srcdir ne $::autosetup(builddir)} {
    # The EMSDK pieces require writing to the original source tree
    # even when doing an out-of-tree build. The ext/wasm pieces do not
    # support an out-of-tree build so we treat that case as if EMSDK
    # were not found.
    msg-result "Out-of tree build: not checking for EMSDK."
    return
  }
  set emccSh $srcdir/tool/emcc.sh
  set extWasmConfig $srcdir/ext/wasm/config.make
  if {![get-define HAVE_WASI_SDK] && [proj-check-emsdk]} {
    define EMCC_WRAPPER $emccSh
    set emsdkHome [get-define EMSDK_HOME ""]
    proj-assert {"" ne $emsdkHome}
    #define EMCC_WRAPPER ""; # just for testing
    proj-bin-define wasm-strip
    proj-bin-define bash; # ext/wasm/GNUmakefile requires bash
    if {[file-isexec $emsdkHome/upstream/bin/wasm-opt]} {
      define BIN_WASM_OPT $emsdkHome/upstream/bin/wasm-opt
    } else {
      # Maybe there's a copy in the path?
      proj-bin-define wasm-opt BIN_WASM_OPT
    }
    proj-dot-ins-append $emccSh.in $emccSh {
      catch {exec chmod u+x $dotInsOut}
    }
    proj-dot-ins-append $extWasmConfig.in $extWasmConfig
  } else {
    define EMCC_WRAPPER ""
    file delete -force -- $emccSh $extWasmConfig
  }
}

########################################################################
# Internal helper for [sqlite-check-line-editing]. Returns a list of
# potential locations under which readline.h might be found.
#
# On some environments this function may perform extra work to help
# sqlite-check-line-editing figure out how to find libreadline and
# friends. It will communicate those results via means other than the
# result value, e.g. by modifying configure --flags.
proc sqlite-get-readline-dir-list {} {
  # Historical note: the dirs list, except for the inclusion of
  # $prefix and some platform-specific dirs, originates from the
  # legacy configure script.
  set dirs [list [get-define prefix]]
  switch -glob -- [get-define host] {
    *-linux-android {
      # Possibly termux
      lappend dirs /data/data/com.termux/files/usr
    }
    *-mingw32 {
      lappend dirs /mingw32 /mingw
    }
    *-mingw64 {
      lappend dirs /mingw64 /mingw
    }
    *-haiku {
      lappend dirs /boot/system/develop/headers
      if {[opt-val with-readline-ldflags] in {auto ""}} {
        # If the user did not supply their own --with-readline-ldflags
        # value, hijack that flag to inject options which are known to
        # work on Haiku OS installations.
        if {"" ne [glob -nocomplain /boot/system/lib/libreadline*]} {
          proj-opt-set with-readline-ldflags {-L/boot/system/lib -lreadline}
        }
      }
    }
  }
  lappend dirs /usr /usr/local /usr/local/readline /usr/contrib
  set rv {}
  foreach d $dirs {
    if {[file isdir $d]} {lappend rv $d}
  }
  #proc-debug "dirs=$rv"
  return $rv
}

########################################################################
# sqlite-check-line-editing jumps through proverbial hoops to try to
# find a working line-editing library, setting:
#
#   - HAVE_READLINE to 0 or 1
#   - HAVE_LINENOISE to 0, 1, or 2
#   - HAVE_EDITLINE to 0 or 1
#
# Only one of ^^^ those will be set to non-0.
#
#   - LDFLAGS_READLINE = linker flags or empty string
#
#   - CFLAGS_READLINE = compilation flags for clients or empty string.
#
# Note that LDFLAGS_READLINE and CFLAGS_READLINE may refer to
# linenoise or editline, not necessarily libreadline.  In some cases
# it will set HAVE_READLINE=1 when it's really using editline, for
# reasons described in this function's comments.
#
# Returns a string describing which line-editing approach to use, or
# "none" if no option is available.
#
# Order of checks:
#
#  1) --with-linenoise trumps all others and skips all of the
#     complexities involved with the remaining options.
#
#  2) --editline trumps --readline
#
#  3) --disable-readline trumps --readline
#
#  4) Default to automatic search for optional readline
#
#  5) Try to find readline or editline. If it's not found AND the
#     corresponding --FEATURE flag was explicitly given then fail
#     fatally, else fail non-fatally.
proc sqlite-check-line-editing {} {
  msg-result "Checking for line-editing capability..."
  define HAVE_READLINE 0
  define HAVE_LINENOISE 0
  define HAVE_EDITLINE 0
  define LDFLAGS_READLINE ""
  define CFLAGS_READLINE ""
  set failIfNotFound 0 ; # Gets set to 1 for explicit --FEATURE requests
                         # so that we know whether to fail fatally or not
                         # if the library is not found.
  set libsForReadline {readline edit} ; # -l<LIB> names to check for readline().
                                        # The libedit check changes this.
  set editLibName "readline"     ; # "readline" or "editline"
  set editLibDef "HAVE_READLINE" ; # "HAVE_READLINE" or "HAVE_EDITLINE"
  set dirLn [opt-val with-linenoise]
  if {"" ne $dirLn} {
    # Use linenoise from a copy of its sources (not a library)...
    if {![file isdir $dirLn]} {
      proj-fatal "--with-linenoise value is not a directory"
    }
    set lnH $dirLn/linenoise.h
    if {![file exists $lnH] } {
      proj-fatal "Cannot find linenoise.h in $dirLn"
    }
    set lnC ""
    set lnCOpts {linenoise-ship.c linenoise.c}
    foreach f $lnCOpts {
      if {[file exists $dirLn/$f]} {
        set lnC $dirLn/$f
        break
      }
    }
    if {"" eq $lnC} {
      proj-fatal "Cannot find any of $lnCOpts in $dirLn"
    }
    set flavor ""
    set lnVal [proj-which-linenoise $lnH]
    switch -- $lnVal {
      1 { set flavor "antirez" }
      2 { set flavor "msteveb" }
      default {
        proj-fatal "Cannot determine the flavor of linenoise from $lnH"
      }
    }
    define CFLAGS_READLINE "-I$dirLn $lnC"
    define HAVE_LINENOISE $lnVal
    sqlite-add-shell-opt -DHAVE_LINENOISE=$lnVal
    if {$::sqliteConfig(use-jim-for-codegen) && 2 == $lnVal} {
      define-append CFLAGS_JIMSH -DUSE_LINENOISE [get-define CFLAGS_READLINE]
      user-notice "Adding linenoise support to jimsh."
    }
    return "linenoise ($flavor)"
  } elseif {[opt-bool editline]} {
    # libedit mimics libreadline and on some systems does not have its
    # own header installed (instead, that of libreadline is used).
    #
    # shell.c historically expects HAVE_EDITLINE to be set for
    # libedit, but it then expects to see <editline/readline.h>, which
    # some system's don't actually have despite having libedit.  If we
    # end up finding <editline/readline.h> below, we will use
    # -DHAVE_EDITLINE=1, else we will use -DHAVE_READLINE=1. In either
    # case, we will link against libedit.
    set failIfNotFound 1
    set libsForReadline {edit}
    set editLibName editline
  } elseif {![opt-bool readline]} {
    msg-result "Readline support explicitly disabled with --disable-readline"
    return "none"
  } elseif {[proj-opt-was-provided readline]} {
    # If an explicit --[enable-]readline was used, fail if it's not
    # found, else treat the feature as optional.
    set failIfNotFound 1
  }

  # Transform with-readline-header=X to with-readline-cflags=-I...
  set v [opt-val with-readline-header]
  proj-opt-set with-readline-header ""
  if {"" ne $v} {
    if {"auto" eq $v} {
      proj-opt-set with-readline-cflags auto
    } else {
      set v [file dirname $v]
      if {[string match */readline $v]} {
        # Special case: if the path includes .../readline/readline.h,
        # set the -I to one dir up from that because our sources
        # #include <readline/readline.h> or <editline/readline.h>.
        set v [file dirname $v]
      }
      proj-opt-set with-readline-cflags "-I$v"
    }
  }

  # Look for readline.h
  set rlInc [opt-val with-readline-cflags auto]
  if {"auto" eq $rlInc} {
    set rlInc ""
    if {$::sqliteConfig(is-cross-compiling)} {
      # ^^^ this check is derived from the legacy configure script.
      proj-warn "Skipping check for readline.h because we're cross-compiling."
    } else {
      set dirs [sqlite-get-readline-dir-list]
      set subdirs [list \
                     include/$editLibName \
                     readline]
      if {"editline" eq $editLibName} {
        lappend subdirs include/readline
        # ^^^ editline, on some systems, does not have its own header,
        # and uses libreadline's header.
      }
      lappend subdirs include
      set rlInc [proj-search-for-header-dir readline.h \
                   -dirs $dirs -subdirs $subdirs]
      #proc-debug "rlInc=$rlInc"
      if {"" ne $rlInc} {
        if {[string match */readline $rlInc]} {
          set rlInc [file dirname $rlInc]; # CLI shell: #include <readline/readline.h>
        } elseif {[string match */editline $rlInc]} {
          set editLibDef HAVE_EDITLINE
          set rlInc [file dirname $rlInc]; # CLI shell: #include <editline/readline.h>
        }
        set rlInc "-I${rlInc}"
      }
    }
  } elseif {"" ne $rlInc && ![string match *-I* $rlInc]} {
    proj-fatal "Argument to --with-readline-cflags is intended to be CFLAGS and contain -I..."
  }

  # If readline.h was found/specified, look for lib(readline|edit)...
  #
  # This is not quite straightforward because both libreadline and
  # libedit typically require some other library which (according to
  # legacy autotools-generated tests) provides tgetent(3). On some
  # systems that's built into libreadline/edit, on some (most?) its in
  # lib[n]curses, and on some it's in libtermcap.
  set rlLib ""
  if {"" ne $rlInc} {
    set rlLib [opt-val with-readline-ldflags]
    #proc-debug "rlLib=$rlLib"
    if {$rlLib in {auto ""}} {
      set rlLib ""  ; # make sure it's not "auto", as we may append to it below
      set libTerm ""; # lib with tgetent(3)
      if {[proj-check-function-in-lib tgetent [list $editLibName ncurses curses termcap]]} {
        # ^^^ that libs list comes from the legacy configure script ^^^
        set libTerm [get-define lib_tgetent]
        undefine lib_tgetent
      }
      if {$editLibName eq $libTerm} {
        # tgetent(3) was found in the editing library
        set rlLib $libTerm
      } elseif {[proj-check-function-in-lib readline $libsForReadline $libTerm]} {
        # tgetent(3) was found in an external lib
        set rlLib [get-define lib_readline]
        lappend rlLib $libTerm
        undefine lib_readline
      }
    }
  }

  # If we found a library, configure the build to use it...
  if {"" ne $rlLib} {
    if {"editline" eq $editLibName && "HAVE_READLINE" eq $editLibDef} {
      # Alert the user that, despite outward appearances, we won't be
      # linking to the GPL'd libreadline. Presumably that distinction is
      # significant for those using --editline.
      proj-indented-notice {
        NOTE: the local libedit uses <readline/readline.h> so we
        will compile with -DHAVE_READLINE=1 but will link with
        libedit.
      }
    }
    set rlLib [join $rlLib]
    set rlInc [join $rlInc]
    define LDFLAGS_READLINE $rlLib
    define CFLAGS_READLINE $rlInc
    proj-assert {$editLibDef in {HAVE_READLINE HAVE_EDITLINE}}
    proj-assert {$editLibName in {readline editline}}
    sqlite-add-shell-opt -D${editLibDef}=1
    msg-result "Using $editLibName flags: $rlInc $rlLib"
    # Check whether rl_completion_matches() has a signature we can use
    # and disable that sub-feature if it doesn't.
    if {![cctest -cflags "$rlInc -D${editLibDef}" -libs $rlLib -nooutput 1 \
            -source {
             #include <stdio.h>
             #ifdef HAVE_EDITLINE
             #include <editline/readline.h>
             #else
             #include <readline/readline.h>
             #endif
             static char * rcg(const char *z, int i){(void)z; (void)i; return 0;}
             int main(void) {
               char ** x = rl_completion_matches("one", rcg);
               (void)x;
               return 0;
             }
           }]} {
      proj-warn "readline-style completion disabled due to rl_completion_matches() signature mismatch"
      sqlite-add-shell-opt -DSQLITE_OMIT_READLINE_COMPLETION
    }
    return $editLibName
  }

  if {$failIfNotFound} {
    proj-fatal "Explicit --$editLibName failed to find a matching library."
  }
  return "none"
}; # sqlite-check-line-editing

########################################################################
# Runs sqlite-check-line-editing and adds a message around it. In the
# canonical build this must not be called before
# sqlite-determine-codegen-tcl for reasons now lost to history (and
# might not still be applicable).
proc sqlite-handle-line-editing {} {
  msg-result "Line-editing support for the sqlite3 shell: [sqlite-check-line-editing]"
}


########################################################################
# ICU - International Components for Unicode
#
# Handles these flags:
#
#  --with-icu-ldflags=LDFLAGS
#  --with-icu-cflags=CFLAGS
#  --with-icu-config[=auto | pkg-config | /path/to/icu-config]
#  --enable-icu-collations
#
# --with-icu-config values:
#
#   - auto: use the first one of (pkg-config, icu-config) found on the
#     system.
#   - pkg-config: use only pkg-config to determine flags
#   - /path/to/icu-config: use that to determine flags
#
# If --with-icu-config is used and neither pkg-config nor icu-config
# are found, fail fatally.
#
# If both --with-icu-ldflags and --with-icu-config are provided, they
# are cumulative.  If neither are provided, icu-collations is not
# honored and a warning is emitted if it is provided.
#
# Design note: though we could automatically enable ICU if the
# icu-config binary or (pkg-config icu-io) are found, we specifically
# do not. ICU is always an opt-in feature.
proc sqlite-handle-icu {} {
  define LDFLAGS_ICU [join [opt-val with-icu-ldflags ""]]
  define CFLAGS_ICU [join [opt-val with-icu-cflags ""]]
  if {[proj-opt-was-provided with-icu-config]} {
    msg-result "Checking for ICU support..."
    set icuConfigBin [opt-val with-icu-config]
    set tryIcuConfigBin 1; # set to 0 if we end up using pkg-config
    if {$icuConfigBin in {auto pkg-config}} {
      if {[pkg-config-init 0] && [pkg-config icu-io]} {
        # Maintenance reminder: historical docs say to use both of
        # (icu-io, icu-uc). icu-uc lacks a required lib and icu-io has
        # all of them on tested OSes.
        set tryIcuConfigBin 0
        define LDFLAGS_ICU [get-define PKG_ICU_IO_LDFLAGS]
        define-append LDFLAGS_ICU [get-define PKG_ICU_IO_LIBS]
        define CFLAGS_ICU [get-define PKG_ICU_IO_CFLAGS]
      } elseif {"pkg-config" eq $icuConfigBin} {
        proj-fatal "pkg-config cannot find package icu-io"
      } else {
        proj-assert {"auto" eq $icuConfigBin}
      }
    }
    if {$tryIcuConfigBin} {
      if {"auto" eq $icuConfigBin} {
        set icuConfigBin [proj-first-bin-of \
                            /usr/local/bin/icu-config \
                            /usr/bin/icu-config]
        if {"" eq $icuConfigBin} {
          proj-indented-notice -error {
            --with-icu-config=auto cannot find (pkg-config icu-io) or icu-config binary.
            On Ubuntu-like systems try:
            --with-icu-ldflags='-licui18n -licuuc -licudata'
          }
        }
      }
      if {[file-isexec $icuConfigBin]} {
        set x [exec $icuConfigBin --ldflags]
        if {"" eq $x} {
          proj-indented-notice -error \
            [subst {
              $icuConfigBin --ldflags returned no data.
              On Ubuntu-like systems try:
              --with-icu-ldflags='-licui18n -licuuc -licudata'
            }]
        }
        define-append LDFLAGS_ICU $x
        set x [exec $icuConfigBin --cppflags]
        define-append CFLAGS_ICU $x
      } else {
        proj-fatal "--with-icu-config=$icuConfigBin does not refer to an executable"
      }
    }
  }
  set ldflags [define LDFLAGS_ICU [string trim [get-define LDFLAGS_ICU]]]
  set cflags [define CFLAGS_ICU [string trim [get-define CFLAGS_ICU]]]
  if {"" ne $ldflags} {
    sqlite-add-feature-flag -shell -DSQLITE_ENABLE_ICU
    msg-result "Enabling ICU support with flags: $ldflags $cflags"
    if {[opt-bool icu-collations]} {
      msg-result "Enabling ICU collations."
      sqlite-add-feature-flag -shell -DSQLITE_ENABLE_ICU_COLLATIONS
      # Recall that shell.c builds with sqlite3.c except in the case
      # of --disable-static-shell, a combination we do not
      # specifically attempt to account for.
    }
  } elseif {[opt-bool icu-collations]} {
    proj-warn "ignoring --enable-icu-collations because neither --with-icu-ldflags nor --with-icu-config provided any linker flags"
  } else {
    msg-result "ICU support is disabled."
  }
}; # sqlite-handle-icu


########################################################################
# Handles the --enable-load-extension flag. Returns 1 if the support
# is enabled, else 0. If support for that feature is not found, a
# fatal error is triggered if --enable-load-extension is explicitly
# provided, else a loud warning is instead emitted. If
# --disable-load-extension is used, no check is performed.
#
# Makes the following environment changes:
#
# - defines LDFLAGS_DLOPEN to any linker flags needed for this
#   feature.  It may legally be empty on (A) some systems where
#   dlopen() is in libc and (B) certain Unix-esque Windows
#   environments which identify as Windows for SQLite's purposes so
#   use LoadLibrary().
#
# - If the feature is not available, adds
#   -DSQLITE_OMIT_LOAD_EXTENSION=1 to the feature flags list.
proc sqlite-handle-load-extension {} {
  define LDFLAGS_DLOPEN ""
  set found 0
  set suffix ""
  proj-if-opt-truthy load-extension {
    switch -glob -- [get-define host] {
      *-*-mingw* - *windows* {
        incr found
        set suffix "Using LoadLibrary()"
      }
      default {
        set found [proj-check-function-in-lib dlopen dl]
        if {$found} {
          set suffix [define LDFLAGS_DLOPEN [get-define lib_dlopen]]
          undefine lib_dlopen
        } else {
          if {[proj-opt-was-provided load-extension]} {
            # Explicit --enable-load-extension: fail if not found
            proj-indented-notice -error {
              --enable-load-extension was provided but dlopen()
              not found. Use --disable-load-extension to bypass this
              check.
            }
          } else {
            # It was implicitly enabled: warn if not found
            proj-indented-notice {
              WARNING: dlopen() not found, so loadable module support will
              be disabled. Use --disable-load-extension to bypass this
              check.
            }
          }
        }
      }
    }
  }
  if {$found} {
    msg-result "Loadable extension support enabled. $suffix"
  } else {
    msg-result "Disabling loadable extension support. Use --enable-load-extension to enable them."
    sqlite-add-feature-flag -DSQLITE_OMIT_LOAD_EXTENSION=1
  }
  return $found
}

########################################################################
# Handles the --enable-math flag.
proc sqlite-handle-math {} {
  proj-if-opt-truthy math {
    if {![proj-check-function-in-lib ceil m]} {
      user-error "Cannot find libm functions. Use --disable-math to bypass this."
    }
    define LDFLAGS_MATH [get-define lib_ceil]
    undefine lib_ceil
    sqlite-add-feature-flag -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_PERCENTILE
    msg-result "Enabling math SQL functions"
  } {
    define LDFLAGS_MATH ""
    msg-result "Disabling math SQL functions"
  }
}

########################################################################
# If this OS looks like a Mac, checks for the Mac-specific
# -current_version and -compatibility_version linker flags. Defines
# LDFLAGS_MAC_CVERSION to an empty string and returns 0 if they're not
# supported, else defines that to the linker flags and returns 1.
#
# We don't check this on non-Macs because this whole thing is a
# libtool compatibility kludge to account for a version stamp which
# libtool applied only on Mac platforms.
#
# Based on https://sqlite.org/forum/forumpost/9dfd5b8fd525a5d7.
proc sqlite-handle-mac-cversion {} {
  define LDFLAGS_MAC_CVERSION ""
  set rc 0
  if {[proj-looks-like-mac]} {
    cc-with {-link 1} {
      # These version numbers are historical libtool-defined values, not
      # library-defined ones
      if {[cc-check-flags "-Wl,-current_version,9.6.0"]
          && [cc-check-flags "-Wl,-compatibility_version,9.0.0"]} {
        define LDFLAGS_MAC_CVERSION "-Wl,-compatibility_version,9.0.0 -Wl,-current_version,9.6.0"
        set rc 1
      } elseif {[cc-check-flags "-compatibility_version 9.0.0"]
                && [cc-check-flags "-current_version 9.6.0"]} {
        define LDFLAGS_MAC_CVERSION "-compatibility_version 9.0.0 -current_version 9.6.0"
        set rc 1
      }
    }
  }
  return $rc
}

########################################################################
# If this is a Mac platform, check for support for
# -Wl,-install_name,...  and, if it's available, define
# LDFLAGS_MAC_INSTALL_NAME to a variant of that string which is
# intended to expand at make-time, else set LDFLAGS_MAC_INSTALL_NAME
# to an empty string.
#
# https://sqlite.org/forum/forumpost/5651662b8875ec0a
proc sqlite-handle-mac-install-name {} {
  define LDFLAGS_MAC_INSTALL_NAME ""; # {-Wl,-install_name,"$(install-dir.lib)/$(libsqlite3.DLL)"}
  set rc 0
  if {[proj-looks-like-mac]} {
    cc-with {-link 1} {
      if {[cc-check-flags "-Wl,-install_name,/usr/local/lib/libsqlite3.dylib"]} {
        define LDFLAGS_MAC_INSTALL_NAME {-Wl,-install_name,"$(install-dir.lib)/$(libsqlite3.DLL)"}
        set rc 1
      }
    }
  }
  return $rc
}

#
# Checks specific to HP-UX.
#
proc sqlite-handle-hpux {} {
  switch -glob -- [get-define host] {
    *hpux* {
      if {[cc-check-flags "-Ae"]} {
        define-append CFLAGS -Ae
      }
    }
  }
}

########################################################################
# Handles the --dll-basename configure flag. [define]'s
# SQLITE_DLL_BASENAME to the DLL's preferred base name (minus
# extension). If --dll-basename is not provided (or programmatically
# set - see [sqlite-handle-env-quirks]) then this is always
# "libsqlite3", otherwise it may use a different value based on the
# value of [get-define host].
proc sqlite-handle-dll-basename {} {
  if {[proj-opt-was-provided dll-basename]} {
    set dn [join [opt-val dll-basename] ""]
    if {$dn in {none default}} { set dn libsqlite3 }
  } else {
    set dn libsqlite3
  }
  if {$dn in {auto ""}} {
    switch -glob -- [get-define host] {
      *-*-cygwin  { set dn cygsqlite3-0 }
      *-*-ming*   { set dn libsqlite3-0 }
      *-*-msys    { set dn msys-sqlite3-0 }
      default     { set dn libsqlite3 }
    }
  }
  define SQLITE_DLL_BASENAME $dn
}

########################################################################
# [define]s LDFLAGS_OUT_IMPLIB to either an empty string or to a
# -Wl,... flag for the platform-specific --out-implib flag, which is
# used for building an "import library .dll.a" file on some platforms
# (e.g. msys2, mingw). SQLITE_OUT_IMPLIB is defined to the name of the
# import lib or an empty string. Returns 1 if supported, else 0.
#
# The name of the import library is [define]d in SQLITE_OUT_IMPLIB.
#
# If the configure flag --out-implib is not used (or programmatically
# set) then this simply sets the above-listed defines to empty strings
# (but see [sqlite-handle-env-quirks]).  If that flag is used but the
# capability is not available, a fatal error is triggered.
#
# This feature is specifically opt-in because it's supported on far
# more platforms than actually need it and enabling it causes creation
# of libsqlite3.so.a files which are unnecessary in most environments.
#
# Added in response to: https://sqlite.org/forum/forumpost/0c7fc097b2
#
# Platform notes:
#
# - cygwin sqlite packages historically install no .dll.a file.
#
# - msys2 and mingw sqlite packages historically install
#   /usr/lib/libsqlite3.dll.a despite the DLL being in
#   /usr/bin.
proc sqlite-handle-out-implib {} {
  define LDFLAGS_OUT_IMPLIB ""
  define SQLITE_OUT_IMPLIB ""
  set rc 0
  if {[proj-opt-was-provided out-implib]} {
    set olBaseName [join [opt-val out-implib] ""]
    if {$olBaseName in {auto ""}} {
      set olBaseName "libsqlite3" ;# [get-define SQLITE_DLL_BASENAME]
      # Based on discussions with mingw/msys users, the import lib
      # should always be called libsqlite3.dll.a even on platforms
      # which rename libsqlite3.dll to something else.
    }
    if {$olBaseName ne "none"} {
      cc-with {-link 1} {
        set dll "${olBaseName}[get-define TARGET_DLLEXT]"
        set flags [proj-cc-check-Wl-flag --out-implib ${dll}.a]
        if {"" ne $flags} {
          define LDFLAGS_OUT_IMPLIB $flags
          define SQLITE_OUT_IMPLIB ${dll}.a
          set rc 1
        }
      }
      if {!$rc} {
        user-error "--out-implib is not supported on this platform"
      }
    }
  }
  return $rc
}

########################################################################
# If the given platform identifier (defaulting to [get-define host])
# appears to be one of the Unix-on-Windows environments, returns a
# brief symbolic name for that environment, else returns an empty
# string.
#
# It does not distinguish between msys and msys2, returning msys for
# both. The build does not, as of this writing, specifically support
# msys v1. Similarly, this function returns "mingw" for both "mingw32"
# and "mingw64".
proc sqlite-env-is-unix-on-windows {{envTuple ""}} {
  if {"" eq $envTuple} {
    set envTuple [get-define host]
  }
  set name ""
  switch -glob -- $envTuple {
    *-*-cygwin { set name cygwin }
    *-*-ming*  { set name mingw }
    *-*-msys   { set name msys }
  }
  return $name
}

########################################################################
# Performs various tweaks to the build which are only relevant on
# certain platforms, e.g. Mac and "Unix on Windows" platforms (msys2,
# cygwin, ...).
#
# 1) DLL installation:
#
# [define]s SQLITE_DLL_INSTALL_RULES to a symbolic name suffix for a
# set of "make install" rules to use for installation of the DLL
# deliverable. The makefile is tasked with providing rules named
# install-dll-NAME which runs the installation for that set, as well
# as providing a rule named install-dll which resolves to
# install-dll-NAME (perhaps indirectly, depending on whether the DLL
# is (de)activated).
#
# The default value is "unix-generic".
#
# 2) --out-implib:
#
# On platforms where an "import library" is conventionally used but
# --out-implib was not explicitly used, automatically add that flag.
# This conventionally applies only to the "Unix on Windows"
# environments like msys and cygwin.
#
# 3) --dll-basename:
#
# On the same platforms addressed by --out-implib, if --dll-basename
# is not explicitly specified, --dll-basename=auto is implied.
proc sqlite-handle-env-quirks {} {
  set instName unix-generic; # name of installation rules set
  set autoDll 0; # true if --out-implib/--dll-basename should be implied
  set host [get-define host]
  switch -glob -- $host {
    *apple* -
    *darwin*    { set instName darwin }
    default {
      set x [sqlite-env-is-unix-on-windows $host]
      if {"" ne $x} {
        set instName $x
        set autoDll 1
      }
    }
  }
  define SQLITE_DLL_INSTALL_RULES $instName
  if {$autoDll} {
    if {![proj-opt-was-provided out-implib]} {
      # Imply --out-implib=auto
      proj-indented-notice [subst -nocommands -nobackslashes {
        NOTICE: auto-enabling --out-implib for environment [$host].
        Use --out-implib=none to disable this special case
        or --out-implib=auto to squelch this notice.
      }]
      proj-opt-set out-implib auto
    }
    if {![proj-opt-was-provided dll-basename]} {
      # Imply --dll-basename=auto
      proj-indented-notice [subst -nocommands -nobackslashes {
        NOTICE: auto-enabling --dll-basename for environment [$host].
        Use --dll-basename=default to disable this special case
        or --dll-basename=auto to squelch this notice.
      }]
      proj-opt-set dll-basename auto
    }
  }
  sqlite-handle-dll-basename
  sqlite-handle-out-implib
  sqlite-handle-mac-cversion
  sqlite-handle-mac-install-name
  if {[llength [info proc sqlite-custom-handle-flags]] > 0} {
    # sqlite-custom-handle-flags is assumed to be imported via a
    # client-specific import: autosetup/sqlite-custom.tcl.
    sqlite-custom-handle-flags
  }
}

########################################################################
# Perform some late-stage work and generate the configure-process
# output file(s).
proc sqlite-process-dot-in-files {} {
  ########################################################################
  # "Re-export" the autoconf-conventional --XYZdir flags into something
  # which is more easily overridable from a make invocation. See the docs
  # for [proj-remap-autoconf-dir-vars] for the explanation of why.
  #
  # We do this late in the config process, immediately before we export
  # the Makefile and other generated files, so that configure tests
  # which make make use of the autotools-conventional flags
  # (e.g. [proj-check-rpath]) may do so before we "mangle" them here.
  proj-remap-autoconf-dir-vars

  proj-dot-ins-process -validate
  make-config-header sqlite_cfg.h \
    -bare {SIZEOF_* HAVE_DECL_*} \
    -none {HAVE_CFLAG_* LDFLAGS_* SH_* SQLITE_AUTORECONFIG
      TARGET_* USE_GCOV TCL_*} \
    -auto {HAVE_* PACKAGE_*} \
    -none *
  proj-touch sqlite_cfg.h ; # help avoid frequent unnecessary @SQLITE_AUTORECONFIG@
}

########################################################################
# Handle --with-wasi-sdk[=DIR]
#
# This must be run relatively early on because it may change the
# toolchain and disable a number of config options. However, in the
# canonical build this must come after [sqlite-check-common-bins].
proc sqlite-handle-wasi-sdk {} {
  set wasiSdkDir [opt-val with-wasi-sdk] ; # ??? [lindex [opt-val with-wasi-sdk] end]
  define HAVE_WASI_SDK 0
  if {$wasiSdkDir eq ""} {
    return 0
  } elseif {$::sqliteConfig(is-cross-compiling)} {
    proj-fatal "Cannot combine --with-wasi-sdk with cross-compilation"
  }
  msg-result "Checking WASI SDK directory \[$wasiSdkDir]... "
  proj-affirm-files-exist -v {*}[prefix "$wasiSdkDir/bin/" {clang wasm-ld ar}]
  define HAVE_WASI_SDK 1
  define WASI_SDK_DIR $wasiSdkDir
  # Disable numerous options which we know either can't work or are
  # not useful in this build...
  msg-result "Using wasi-sdk clang. Disabling CLI shell and modifying config flags:"
  # Boolean (--enable-/--disable-) flags which must be switched off:
  foreach opt {
    dynlink-tools
    editline
    gcov
    icu-collations
    load-extension
    readline
    shared
    tcl
    threadsafe
  } {
    if {[proj-opt-exists $opt] && [opt-bool $opt]} {
      # -^^^^ not all builds define all of these flags
      msg-result "  --disable-$opt"
      proj-opt-set $opt 0
    }
  }
  # Non-boolean flags which need to be cleared:
  foreach opt {
    with-emsdk
    with-icu-config
    with-icu-ldflags
    with-icu-cflags
    with-linenoise
    with-tcl
  } {
    if {[proj-opt-was-provided $opt]} {
      msg-result "  removing --$opt"
      proj-opt-set $opt ""
    }
  }
  # Remember that we now have a discrepancy between
  # $::sqliteConfig(is-cross-compiling) and [proj-is-cross-compiling].
  set ::sqliteConfig(is-cross-compiling) 1

  #
  # Changing --host and --target have no effect here except to
  # possibly cause confusion. Autosetup has finished processing them
  # by this point.
  #
  #  host_alias=wasm32-wasi
  #  target=wasm32-wasi
  #
  # Merely changing CC, LD, and AR to the wasi-sdk's is enough to get
  # sqlite3.o building in WASM format.
  #
  define CC "${wasiSdkDir}/bin/clang"
  define LD "${wasiSdkDir}/bin/wasm-ld"
  define AR "${wasiSdkDir}/bin/ar"
  #define STRIP "${wasiSdkDir}/bin/strip"
  return 1
}; # sqlite-handle-wasi-sdk

########################################################################
# TCL...
#
# sqlite-check-tcl performs most of the --with-tcl and --with-tclsh
# handling. Some related bits and pieces are performed before and
# after that function is called.
#
# Important [define]'d vars:
#
#  - HAVE_TCL indicates whether we have a tclsh suitable for building
#    the TCL SQLite extension and, by extension, the testing
#    infrastructure. This must only be 1 for environments where
#    tclConfig.sh can be found.
#
#  - TCLSH_CMD is the path to the canonical tclsh or "". It never
#    refers to jimtcl.
#
#  - TCL_CONFIG_SH is the path to tclConfig.sh or "".
#
#  - TCLLIBDIR is the dir to which libtclsqlite3 gets installed.
#
#  - BTCLSH = the path to the tcl interpreter used for in-tree code
#    generation.  It may be jimtcl or the canonical tclsh but may not
#    be empty - this tree requires TCL to generated numerous
#    components.
#
# If --tcl or --with-tcl are provided but no TCL is found, this
# function fails fatally. If they are not explicitly provided then
# failure to find TCL is not fatal but a loud warning will be emitted.
#
proc sqlite-check-tcl {} {
  define TCLSH_CMD false ; # Significant is that it exits with non-0
  define HAVE_TCL 0      ; # Will be enabled via --tcl or a successful search
  define TCLLIBDIR ""    ; # Installation dir for TCL extension lib
  define TCL_CONFIG_SH ""; # full path to tclConfig.sh

  # Clear out all vars which would harvest from tclConfig.sh so that
  # the late-config validation of @VARS@ works even if --disable-tcl
  # is used.
  proj-tclConfig-sh-to-autosetup ""

  file delete -force ".tclenv.sh"; # ensure no stale state from previous configures.
  if {![opt-bool tcl]} {
    proj-indented-notice {
      NOTE: TCL is disabled via --disable-tcl. This means that none
      of the TCL-based components will be built, including tests
      and sqlite3_analyzer.
    }
    return
  }
  # TODO: document the steps this is taking.
  set srcdir $::autosetup(srcdir)
  msg-result "Checking for a suitable tcl... "
  proj-assert [proj-opt-truthy tcl]
  set use_tcl 1
  set with_tclsh [opt-val with-tclsh]
  set with_tcl [opt-val with-tcl]
  if {"prefix" eq $with_tcl} {
    set with_tcl [get-define prefix]
  }
  proc-debug "use_tcl ${use_tcl}"
  proc-debug "with_tclsh=${with_tclsh}"
  proc-debug "with_tcl=$with_tcl"
  if {"" eq $with_tclsh && "" eq $with_tcl} {
    # If neither --with-tclsh nor --with-tcl are provided, try to find
    # a workable tclsh.
    set with_tclsh [proj-first-bin-of tclsh9.1 tclsh9.0 tclsh8.6 tclsh]
    proc-debug "with_tclsh=${with_tclsh}"
  }

  set doConfigLookup 1 ; # set to 0 to test the tclConfig.sh-not-found cases
  if {"" ne $with_tclsh} {
    # --with-tclsh was provided or found above. Validate it and use it
    # to trump any value passed via --with-tcl=DIR.
    if {![file-isexec $with_tclsh]} {
      proj-fatal "TCL shell $with_tclsh is not executable"
    } else {
      define TCLSH_CMD $with_tclsh
      #msg-result "Using tclsh: $with_tclsh"
    }
    if {$doConfigLookup &&
        [catch {exec $with_tclsh $::autosetup(libdir)/find_tclconfig.tcl} result] == 0} {
      set with_tcl $result
    }
    if {"" ne $with_tcl && [file isdir $with_tcl]} {
      msg-result "$with_tclsh recommends the tclConfig.sh from $with_tcl"
    } else {
      proj-warn "$with_tclsh is unable to recommend a tclConfig.sh"
      set use_tcl 0
    }
  }
  set cfg ""
  set tclSubdirs {tcl9.1 tcl9.0 tcl8.6 lib}
  while {$use_tcl} {
    if {"" ne $with_tcl} {
      # Ensure that we can find tclConfig.sh under ${with_tcl}/...
      if {$doConfigLookup} {
        if {[file readable "${with_tcl}/tclConfig.sh"]} {
          set cfg "${with_tcl}/tclConfig.sh"
        } else {
          foreach i $tclSubdirs {
            if {[file readable "${with_tcl}/$i/tclConfig.sh"]} {
              set cfg "${with_tcl}/$i/tclConfig.sh"
              break
            }
          }
        }
      }
      if {"" eq $cfg} {
        proj-fatal "No tclConfig.sh found under ${with_tcl}"
      }
    } else {
      # If we have not yet found a tclConfig.sh file, look in $libdir
      # which is set automatically by autosetup or via the --prefix
      # command-line option.  See
      # https://sqlite.org/forum/forumpost/e04e693439a22457
      set libdir [get-define libdir]
      if {[file readable "${libdir}/tclConfig.sh"]} {
        set cfg "${libdir}/tclConfig.sh"
      } else {
        foreach i $tclSubdirs {
          if {[file readable "${libdir}/$i/tclConfig.sh"]} {
            set cfg "${libdir}/$i/tclConfig.sh"
            break
          }
        }
      }
      if {![file readable $cfg]} {
        break
      }
    }
    msg-result "Using tclConfig.sh: $cfg"
    break
  }
  define TCL_CONFIG_SH $cfg
  # Export a subset of tclConfig.sh to the current TCL-space.  If $cfg
  # is an empty string, this emits empty-string entries for the
  # various options we're interested in.
  proj-tclConfig-sh-to-autosetup $cfg

  if {"" eq $with_tclsh && $cfg ne ""} {
    # We have tclConfig.sh but no tclsh. Attempt to locate a tclsh
    # based on info from tclConfig.sh.
    set tclExecPrefix [get-define TCL_EXEC_PREFIX]
    proj-assert {"" ne $tclExecPrefix}
    set tryThese [list \
                    $tclExecPrefix/bin/tclsh[get-define TCL_VERSION] \
                    $tclExecPrefix/bin/tclsh ]
    foreach trySh $tryThese {
      if {[file-isexec $trySh]} {
        set with_tclsh $trySh
        break
      }
    }
    if {![file-isexec $with_tclsh]} {
      proj-warn "Cannot find a usable tclsh (tried: $tryThese)
    }
  }
  define TCLSH_CMD $with_tclsh
  if {$use_tcl} {
    # Set up the TCLLIBDIR
    #
    # 2024-10-28: calculation of TCLLIBDIR is now done via the shell
    # in main.mk (search it for T.tcl.env.sh) so that
    # static/hand-written makefiles which import main.mk do not have
    # to define that before importing main.mk. Even so, we export
    # TCLLIBDIR from here, which will cause the canonical makefile to
    # use this one rather than to re-calculate it at make-time.
    set tcllibdir [get-env TCLLIBDIR ""]
    set sq3Ver [get-define PACKAGE_VERSION]
    if {"" eq $tcllibdir} {
      # Attempt to extract TCLLIBDIR from TCL's $auto_path
      if {"" ne $with_tclsh &&
          [catch {exec echo "puts stdout \$auto_path" | "$with_tclsh"} result] == 0} {
        foreach i $result {
          if {[file isdir $i]} {
            set tcllibdir $i/sqlite${sq3Ver}
            break
          }
        }
      } else {
        proj-warn "Cannot determine TCLLIBDIR."
        # The makefile will fail fatally in this case if a target is
        # invoked which requires TCLLIBDIR.
      }
    }
    #if {"" ne $tcllibdir} { msg-result "TCLLIBDIR = ${tcllibdir}"; }
    define TCLLIBDIR $tcllibdir
  }; # find TCLLIBDIR

  if {[file-isexec $with_tclsh]} {
    msg-result "Using tclsh: $with_tclsh"
    if {$cfg ne ""} {
      define HAVE_TCL 1
    } else {
      proj-warn "Found tclsh but no tclConfig.sh."
    }
  }
  show-notices
  # If TCL is not found: if it was explicitly requested then fail
  # fatally, else just emit a warning. If we can find the APIs needed
  # to generate a working JimTCL then that will suffice for build-time
  # TCL purposes (see: proc sqlite-determine-codegen-tcl).
  if {![get-define HAVE_TCL] &&
      ([proj-opt-was-provided tcl] || [proj-opt-was-provided with-tcl])} {
    proj-fatal "TCL support was requested but no tclConfig.sh could be found."
  }
  if {"" eq $cfg} {
    proj-assert {0 == [get-define HAVE_TCL]}
    proj-indented-notice {
      WARNING: Cannot find a usable tclConfig.sh file.  Use
      --with-tcl=DIR to specify a directory where tclConfig.sh can be
      found.  SQLite does not use TCL internally, but some optional
      components require TCL, including tests and sqlite3_analyzer.
    }
  }
}; # sqlite-check-tcl

########################################################################
# sqlite-determine-codegen-tcl checks which TCL to use as a code
# generator.  By default, prefer jimsh simply because we have it
# in-tree (it's part of autosetup) unless --with-tclsh=X is used, in
# which case prefer X.
#
# Returns the human-readable name of the TCL it selects. Fails fatally
# if it cannot detect a TCL appropriate for code generation.
#
# Defines:
#
#   - BTCLSH = the TCL shell used for code generation. It may set this
#     to an unexpanded makefile var name.
#
#   - CFLAGS_JIMSH = any flags needed for buildng a BTCLSH-compatible
#     jimsh. The defaults may be passed on to configure as
#     CFLAGS_JIMSH=...
proc sqlite-determine-codegen-tcl {} {
  msg-result "Checking for TCL to use for code generation... "
  define CFLAGS_JIMSH [proj-get-env CFLAGS_JIMSH {-O1}]
  set cgtcl [opt-val with-tclsh jimsh]
  if {"jimsh" ne $cgtcl} {
    # When --with-tclsh=X is used, use that for all TCL purposes,
    # including in-tree code generation, per developer request.
    define BTCLSH "\$(TCLSH_CMD)"
    return $cgtcl
  }
  set flagsToRestore {CC CFLAGS AS_CFLAGS CPPFLAGS AS_CPPFLAGS LDFLAGS LINKFLAGS LIBS CROSS}
  define-push $flagsToRestore {
    # We have to swap CC to CC_FOR_BUILD for purposes of the various
    # [cc-...] tests below. Recall that --with-wasi-sdk may have
    # swapped out CC with one which is not appropriate for this block.
    # Per consulation with autosetup's creator, doing this properly
    # requires us to [define-push] the whole $flagsToRestore list
    # (plus a few others which are not relevant in this tree).
    #
    # These will get set to their previous values at the end of this
    # block.
    foreach flag $flagsToRestore {define $flag ""}
    define CC [get-define CC_FOR_BUILD]
    # These headers are technically optional for JimTCL but necessary if
    # we want to use it for code generation:
    set sysh [cc-check-includes dirent.h sys/time.h]
    # jimsh0.c hard-codes #define's for HAVE_DIRENT_H and
    # HAVE_SYS_TIME_H on the platforms it supports, so we do not
    # need to add -D... flags for those. We check for them here only
    # so that we can avoid the situation that we later, at
    # make-time, try to compile jimsh but it then fails due to
    # missing headers (i.e. fail earlier rather than later).
    if {$sysh && [cc-check-functions realpath]} {
      define-append CFLAGS_JIMSH -DHAVE_REALPATH
      define BTCLSH "\$(JIMSH)"
      set ::sqliteConfig(use-jim-for-codegen) 1
    } elseif {$sysh && [cc-check-functions _fullpath]} {
      # _fullpath() is a Windows API. It's not entirely clear
      # whether we need to add {-DHAVE_SYS_TIME_H -DHAVE_DIRENT_H}
      # to CFLAGS_JIMSH in this case. On MinGW32 we definitely do
      # not want to because it already hard-codes them. On _MSC_VER
      # builds it does not.
      define-append CFLAGS_JIMSH -DHAVE__FULLPATH
      define BTCLSH "\$(JIMSH)"
      set ::sqliteConfig(use-jim-for-codegen) 1
    } elseif {[file-isexec [get-define TCLSH_CMD]]} {
      set cgtcl [get-define TCLSH_CMD]
      define BTCLSH "\$(TCLSH_CMD)"
    } else {
      # One last-ditch effort to find TCLSH_CMD: use info from
      # tclConfig.sh to try to find a tclsh
      if {"" eq [get-define TCLSH_CMD]} {
        set tpre [get-define TCL_EXEC_PREFIX]
        if {"" ne $tpre} {
          set tv [get-define TCL_VERSION]
          if {[file-isexec "${tpre}/bin/tclsh${tv}"]} {
            define TCLSH_CMD "${tpre}/bin/tclsh${tv}"
          } elseif {[file-isexec "${tpre}/bin/tclsh"]} {
            define TCLSH_CMD "${tpre}/bin/tclsh"
          }
        }
      }
      set cgtcl [get-define TCLSH_CMD]
      if {![file-isexec $cgtcl]} {
        proj-fatal "Cannot find a tclsh to use for code generation."
      }
      define BTCLSH "\$(TCLSH_CMD)"
    }
  }; # /define-push $flagsToRestore
  return $cgtcl
}; # sqlite-determine-codegen-tcl

########################################################################
# Runs sqlite-check-tcl and, if this is the canonical build,
# sqlite-determine-codegen-tcl.
proc sqlite-handle-tcl {} {
  sqlite-check-tcl
  if {"canonical" ne $::sqliteConfig(build-mode)} return
  msg-result "TCL for code generation: [sqlite-determine-codegen-tcl]"

  # Determine the base name of the Tcl extension's DLL
  #
  if {[get-define HAVE_TCL]} {
    if {[string match *-cygwin [get-define host]]} {
      set libname cyg
    } else {
      set libname lib
    }
    if {[get-define TCL_MAJOR_VERSION] > 8} {
      append libname tcl9
    }
    append libname sqlite
  } else {
    set libname ""
  }
  define TCL_EXT_DLL_BASENAME $libname
  # The extension is added in the makefile
}

########################################################################
# Handle the --enable/disable-rpath flag.
proc sqlite-handle-rpath {} {
  # autosetup/cc-shared.tcl sets the rpath flag definition in
  # [get-define SH_LINKRPATH], but it does so on a per-platform basis
  # rather than as a compiler check. Though we should do a proper
  # compiler check (as proj-check-rpath does), we may want to consider
  # adopting its approach of clearing the rpath flags for environments
  # for which sqlite-env-is-unix-on-windows returns a non-empty
  # string.

  # https://sqlite.org/forum/forumpost/13cac3b56516f849
  if {[proj-opt-truthy rpath]} {
    proj-check-rpath
  } else {
    msg-result "Disabling use of rpath."
    define LDFLAGS_RPATH ""
  }
}

########################################################################
# If the --dump-defines configure flag is provided then emit a list of
# all [define] values to config.defines.txt, else do nothing.
proc sqlite-dump-defines {} {
  proj-if-opt-truthy dump-defines {
    make-config-header $::sqliteConfig(dump-defines-txt) \
      -bare {SQLITE_OS* SQLITE_DEBUG USE_*} \
      -str {BIN_* CC LD AR LDFLAG* OPT_*} \
      -auto {*}
    # achtung: ^^^^ whichever SQLITE_OS_foo flag which is set to 0 will
    # get _undefined_ here unless it's part of the -bare set.
    if {"" ne $::sqliteConfig(dump-defines-json)} {
      msg-result "--dump-defines is creating $::sqliteConfig(dump-defines-json)"
      ########################################################################
      # Dump config-defines.json...
      # Demonstrate (mis?)handling of spaces in JSON-export array values:
      # define-append OPT_FOO.list {"-DFOO=bar baz" -DBAR="baz barre"}
      define OPT_FEATURE_FLAGS.list [get-define OPT_FEATURE_FLAGS]
      define OPT_SHELL.list [get-define OPT_SHELL]
      set dumpDefsOpt {
        -bare {SIZEOF_* HAVE_DECL_*}
        -none {HAVE_CFLAG_* LDFLAGS_* SH_* SQLITE_AUTORECONFIG TARGET_* USE_GCOV TCL_*}
        -array {*.list}
        -auto {OPT_* PACKAGE_* HAVE_*}
      }
#      if {$::sqliteConfig(dump-defines-json-include-lowercase)} {
#        lappend dumpDefsOpt -none {lib_*} ; # remnants from proj-check-function-in-lib and friends
#        lappend dumpDefsOpt -auto {[a-z]*}
#      }
      lappend dumpDefsOpt -none *
      proj-dump-defs-json $::sqliteConfig(dump-defines-json) {*}$dumpDefsOpt
      undefine OPT_FEATURE_FLAGS.list
      undefine OPT_SHELL.list
    }
  }
}
