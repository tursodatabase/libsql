# Teaish configure script for the SQLite Tcl extension

#
# State for disparate config-time pieces.
#
array set sqlite__Config [proj-strip-hash-comments {
  #
  # The list of feature --flags which the --all flag implies. This
  # requires special handling in a few places.
  #
  all-flag-enables {fts3 fts4 fts5 rtree geopoly}

  # >0 if building in the canonical tree. -1=undetermined
  is-canonical -1
}]

#
# Set up the package info for teaish...
#
apply {{dir} {
  # Figure out the version number...
  set version ""
  if {[file exists $dir/../VERSION]} {
    # The canonical SQLite TEA(ish) build
    set version [proj-file-content -trim $dir/../VERSION]
    set ::sqlite__Config(is-canonical) 1
    set distname sqlite-tcl
  } elseif {[file exists $dir/generic/tclsqlite3.c]} {
    # The copy from the teaish tree, used as a dev/test bed before
    # updating SQLite's tree.
    set ::sqlite__Config(is-canonical) 0
    set fd [open $dir/generic/tclsqlite3.c rb]
    while {[gets $fd line] >=0} {
      if {[regexp {^#define[ ]+SQLITE_VERSION[ ]+"(3.+)"} \
             $line - version]} {
        set distname sqlite-teaish
        break
      }
    }
    close $fd
  }

  if {"" eq $version} {
    proj-fatal "Cannot determine the SQLite version number"
  }

  proj-assert {$::sqlite__Config(is-canonical) > -1}
  proj-assert {[string match 3.*.* $version]} \
    "Unexpected SQLite version: $version"

  set pragmas {}
  if {$::sqlite__Config(is-canonical)} {
    # Disable "make dist" in the canonical tree.  That tree is
    # generated from several pieces and creating/testing working
    # "dist" rules for that sub-build currently feels unnecessary. The
    # copy in the teaish tree, though, should be able to "make dist".
    lappend pragmas no-dist
  } else {
    lappend pragmas full-dist
  }

  teaish-pkginfo-set -vars {
    -name sqlite
    -name.pkg sqlite3
    -version $version
    -name.dist $distname
    -libDir sqlite$version
    -pragmas $pragmas
    -src generic/tclsqlite3.c
  }
  # We should also have:
  #    -vsatisfies 8.6-
  # But at least one platform is failing this vsatisfies check
  # for no apparent reason:
  # https://sqlite.org/forum/forumpost/fde857fb8101a4be
}} [teaish-get -dir]


#
# Must return either an empty string or a list in the form accepted by
# autosetup's [options] function.
#
proc teaish-options {} {
  # These flags and defaults mostly derive from the historical TEA
  # build.  Some, like ICU, are taken from the canonical SQLite tree.
  return [subst -nocommands -nobackslashes {
    with-system-sqlite=0
      => {Use the system-level SQLite instead of the copy in this tree.
          Also requires use of --override-sqlite-version so that the build
          knows what version number to associate with the system-level SQLite.}
    override-sqlite-version:VERSION
      => {For use with --with-system-sqlite to set the version number.}
    threadsafe=1         => {Disable mutexing}
    with-tempstore:=no   => {Use an in-RAM database for temporary tables: never,no,yes,always}
    load-extension=0     => {Enable loading of external extensions}
    math=1               => {Disable math functions}
    json=1               => {Disable JSON functions}
    fts3                 => {Enable the FTS3 extension}
    fts4                 => {Enable the FTS4 extension}
    fts5                 => {Enable the FTS5 extension}
    update-limit         => {Enable the UPDATE/DELETE LIMIT clause}
    geopoly              => {Enable the GEOPOLY extension}
    rtree                => {Enable the RTREE extension}
    session              => {Enable the SESSION extension}
    all=1                => {Disable $::sqlite__Config(all-flag-enables)}
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
  }]
}

#
# Gets called by tea-configure-core. Must perform any configuration
# work needed for this extension.
#
proc teaish-configure {} {
  use teaish/feature

  if {[proj-opt-was-provided override-sqlite-version]} {
    teaish-pkginfo-set -version [opt-val override-sqlite-version]
    proj-warn "overriding sqlite version number:" [teaish-pkginfo-get -version]
  } elseif {[proj-opt-was-provided with-system-sqlite]
            && [opt-val with-system-sqlite] ne "0"} {
    proj-fatal "when using --with-system-sqlite also use" \
      "--override-sqlite-version to specify a library version number."
  }

  define CFLAGS [proj-get-env CFLAGS {-O2}]
  sqlite-munge-cflags

  #
  # Add feature flags from legacy configure.ac which are not covered by
  # --flags.
  #
  sqlite-add-feature-flag {
    -DSQLITE_3_SUFFIX_ONLY=1
    -DSQLITE_ENABLE_DESERIALIZE=1
    -DSQLITE_ENABLE_DBPAGE_VTAB=1
    -DSQLITE_ENABLE_BYTECODE_VTAB=1
    -DSQLITE_ENABLE_DBSTAT_VTAB=1
  }

  if {[opt-bool with-system-sqlite]} {
    msg-result "Using system-level sqlite3."
    teaish-cflags-add -DUSE_SYSTEM_SQLITE
    teaish-ldflags-add -lsqlite3
  } elseif {$::sqlite__Config(is-canonical)} {
    teaish-cflags-add -I[teaish-get -dir]/..
  }

  teaish-check-librt
  teaish-check-libz
  sqlite-handle-threadsafe
  sqlite-handle-tempstore
  sqlite-handle-load-extension
  sqlite-handle-math
  sqlite-handle-icu

  sqlite-handle-common-feature-flags; # must be late in the process
}; # teaish-configure

define OPT_FEATURE_FLAGS {} ; # -DSQLITE_OMIT/ENABLE flags.
#
# Adds $args, if not empty, to OPT_FEATURE_FLAGS. This is intended only for holding
# -DSQLITE_ENABLE/OMIT/... flags, but that is not enforced here.
proc sqlite-add-feature-flag {args} {
  if {"" ne $args} {
    define-append OPT_FEATURE_FLAGS {*}$args
  }
}

#
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
    teaish-ldflags-prepend $lfl
  }
}

#
# Handle various SQLITE_ENABLE/OMIT_... feature flags.
proc sqlite-handle-common-feature-flags {} {
  msg-result "Feature flags..."
  if {![opt-bool all]} {
    # Special handling for --disable-all
    foreach flag $::sqlite__Config(all-flag-enables) {
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
      foreach k $::sqlite__Config(all-flag-enables) {
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
    scanstatus     -DSQLITE_ENABLE_STMT_SCANSTATUS {}
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
  #
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

  ##
  # Remove duplicates from the final feature flag sets and show them
  # to the user.
  set oFF [get-define OPT_FEATURE_FLAGS]
  if {"" ne $oFF} {
    define OPT_FEATURE_FLAGS [lsort -unique $oFF]
    msg-result "Library feature flags: [get-define OPT_FEATURE_FLAGS]"
  }
  if {[lsearch [get-define TARGET_DEBUG ""] -DSQLITE_DEBUG=1] > -1} {
    msg-result "Note: this is a debug build, so performance will suffer."
  }
  teaish-cflags-add -define OPT_FEATURE_FLAGS
}; # sqlite-handle-common-feature-flags

#
# If --enable-threadsafe is set, this adds -DSQLITE_THREADSAFE=1 to
# OPT_FEATURE_FLAGS and sets LDFLAGS_PTHREAD to the linker flags
# needed for linking pthread (possibly an empty string). If
# --enable-threadsafe is not set, adds -DSQLITE_THREADSAFE=0 to
# OPT_FEATURE_FLAGS and sets LDFLAGS_PTHREAD to an empty string.
#
# It prepends the flags to the global LDFLAGS.
proc sqlite-handle-threadsafe {} {
  msg-checking "Support threadsafe operation? "
  define LDFLAGS_PTHREAD ""
  set enable 0
  if {[proj-opt-was-provided threadsafe]} {
    proj-if-opt-truthy threadsafe {
      if {[proj-check-function-in-lib pthread_create pthread]
          && [proj-check-function-in-lib pthread_mutexattr_init pthread]} {
        incr enable
        set ldf [get-define lib_pthread_create]
        define LDFLAGS_PTHREAD $ldf
        teaish-ldflags-prepend $ldf
        undefine lib_pthread_create
        undefine lib_pthread_mutexattr_init
      } else {
        user-error "Missing required pthread libraries. Use --disable-threadsafe to disable this check."
      }
      # Recall that LDFLAGS_PTHREAD might be empty even if pthreads if
      # found because it's in -lc on some platforms.
    } {
      msg-result "Disabled using --disable-threadsafe"
    }
  } else {
    #
    # If user does not specify --[disable-]threadsafe then select a
    # default based on whether it looks like Tcl has threading
    # support.
    #
    catch {
      scan [exec echo {puts [tcl::pkgconfig get threaded]} | [get-define TCLSH_CMD]] \
        %d enable
    }
    if {$enable} {
      set flagName "--threadsafe"
      set lblAbled "enabled"
      msg-result yes
    } else {
      set flagName "--disable-threadsafe"
      set lblAbled "disabled"
      msg-result no
    }
    msg-result "Defaulting to ${flagName} because Tcl has threading ${lblAbled}."
    # ^^^ We (probably) don't need to link against -lpthread in the
    # is-enabled case. We might in the case of static linking. Unsure.
  }
  sqlite-add-feature-flag -DSQLITE_THREADSAFE=${enable}
  return $enable
}

#
# Handles the --enable-load-extension flag. Returns 1 if the support
# is enabled, else 0. If support for that feature is not found, a
# fatal error is triggered if --enable-load-extension is explicitly
# provided, else a loud warning is instead emitted. If
# --disable-load-extension is used, no check is performed.
#
# Makes the following environment changes:
#
# - defines LDFLAGS_DLOPEN to any linker flags needed for this
#   feature.  It may legally be empty on some systems where dlopen()
#   is in libc.
#
# - If the feature is not available, adds
#   -DSQLITE_OMIT_LOAD_EXTENSION=1 to the feature flags list.
proc sqlite-handle-load-extension {} {
  define LDFLAGS_DLOPEN ""
  set found 0
  proj-if-opt-truthy load-extension {
    set found [proj-check-function-in-lib dlopen dl]
    if {$found} {
      set ldf [get-define lib_dlopen]
      define LDFLAGS_DLOPEN $ldf
      teaish-ldflags-prepend $ldf
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
  if {$found} {
    msg-result "Loadable extension support enabled."
  } else {
    msg-result "Disabling loadable extension support. Use --enable-load-extension to enable them."
    sqlite-add-feature-flag -DSQLITE_OMIT_LOAD_EXTENSION=1
  }
  return $found
}

#
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
# If --with-icu-config is used as neither pkg-config nor icu-config
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
  define LDFLAGS_LIBICU [join [opt-val with-icu-ldflags ""]]
  define CFLAGS_LIBICU [join [opt-val with-icu-cflags ""]]
  if {[proj-opt-was-provided with-icu-config]} {
    msg-result "Checking for ICU support..."
    set icuConfigBin [opt-val with-icu-config]
    set tryIcuConfigBin 1; # set to 0 if we end up using pkg-config
    if {$icuConfigBin in {auto pkg-config}} {
      uplevel 3 { use pkg-config }
      if {[pkg-config-init 0] && [pkg-config icu-io]} {
        # Maintenance reminder: historical docs say to use both of
        # (icu-io, icu-uc). icu-uc lacks a required lib and icu-io has
        # all of them on tested OSes.
        set tryIcuConfigBin 0
        define LDFLAGS_LIBICU [get-define PKG_ICU_IO_LDFLAGS]
        define-append LDFLAGS_LIBICU [get-define PKG_ICU_IO_LIBS]
        define CFLAGS_LIBICU [get-define PKG_ICU_IO_CFLAGS]
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
        define-append LDFLAGS_LIBICU $x
        set x [exec $icuConfigBin --cppflags]
        define-append CFLAGS_LIBICU $x
      } else {
        proj-fatal "--with-icu-config=$icuConfigBin does not refer to an executable"
      }
    }
  }
  set ldflags [define LDFLAGS_LIBICU [string trim [get-define LDFLAGS_LIBICU]]]
  set cflags [define CFLAGS_LIBICU [string trim [get-define CFLAGS_LIBICU]]]
  if {"" ne $ldflags} {
    sqlite-add-feature-flag -DSQLITE_ENABLE_ICU
    msg-result "Enabling ICU support with flags: $ldflags $cflags"
    if {[opt-bool icu-collations]} {
      msg-result "Enabling ICU collations."
      sqlite-add-feature-flag -DSQLITE_ENABLE_ICU_COLLATIONS
    }
    teaish-ldflags-prepend $ldflags
    teaish-cflags-add $cflags
  } elseif {[opt-bool icu-collations]} {
    proj-warn "ignoring --enable-icu-collations because neither --with-icu-ldflags nor --with-icu-config provided any linker flags"
  } else {
    msg-result "ICU support is disabled."
  }
}; # sqlite-handle-icu


#
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

#
# Handles the --enable-math flag.
proc sqlite-handle-math {} {
  proj-if-opt-truthy math {
    if {![proj-check-function-in-lib ceil m]} {
      user-error "Cannot find libm functions. Use --disable-math to bypass this."
    }
    set lfl [get-define lib_ceil]
    undefine lib_ceil
    define LDFLAGS_MATH $lfl
    teaish-ldflags-prepend $lfl
    sqlite-add-feature-flag -DSQLITE_ENABLE_MATH_FUNCTIONS
    msg-result "Enabling math SQL functions"
  } {
    define LDFLAGS_MATH ""
    msg-result "Disabling math SQL functions"
  }
}

#
# Move -DSQLITE_OMIT... and -DSQLITE_ENABLE... flags from CFLAGS and
# CPPFLAGS to OPT_FEATURE_FLAGS and remove them from BUILD_CFLAGS.
proc sqlite-munge-cflags {} {
  # Move CFLAGS and CPPFLAGS entries matching -DSQLITE_OMIT* and
  # -DSQLITE_ENABLE* to OPT_FEATURE_FLAGS. This behavior is derived
  # from the pre-3.48 build.
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
}
