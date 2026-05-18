########################################################################
# 2025 April 5
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#  * May you do good and not evil.
#  * May you find forgiveness for yourself and forgive others.
#  * May you share freely, never taking more than you give.
#
########################################################################
# ----- @module teaish.tcl -----
# @section TEA-ish ((TCL Extension Architecture)-ish)
#
# Functions in this file with a prefix of teaish__ are
# private/internal APIs. Those with a prefix of teaish- are
# public APIs.
#
# Teaish has a hard dependency on proj.tcl, and any public API members
# of that module are considered legal for use by teaish extensions.
#
# Project home page: https://fossil.wanderinghorse.net/r/teaish

use proj

#
# API-internal settings and shared state.
array set teaish__Config [proj-strip-hash-comments {
  #
  # Teaish's version number, not to be confused with
  # teaish__PkgInfo(-version).
  #
  version 0.1-beta

  # set to 1 to enable some internal debugging output
  debug-enabled 0

  #
  # 0     = don't yet have extension's pkgindex
  # 0x01  = found TEAISH_EXT_DIR/pkgIndex.tcl.in
  # 0x02  = found srcdir/pkgIndex.tcl.in
  # 0x10  = found TEAISH_EXT_DIR/pkgIndex.tcl (static file)
  # 0x20  = static-pkgIndex.tcl pragma: behave as if 0x10
  # 0x100 = disabled by -tm.tcl.in
  # 0x200 = disabled by -tm.tcl
  #
  # Reminder: it's significant that the bottom 4 bits be
  # cases where teaish manages ./pkgIndex.tcl.
  #
  pkgindex-policy 0

  #
  # The pkginit counterpart of pkgindex-policy:
  #
  # 0    = no pkginit
  # 0x01 = found default X.in: generate X from X.in
  # 0x10 = found static pkginit file X
  # 0x02 = user-provided X.in generates ./X.
  # 0x20 = user-provided static pkginit file X
  #
  # The 0x0f bits indicate that teaish is responsible for cleaning up
  # the (generated) pkginit file.
  #
  pkginit-policy 0
  #
  # 0    = no tm.tcl
  # 0x01 = tm.tcl.in
  # 0x10 = static tm.tcl
  tm-policy 0

  #
  # If 1+ then teaish__verbose will emit messages.
  #
  verbose 0

  #
  # Mapping of pkginfo -flags to their TEAISH_xxx define (if any).
  # This must not be modified after initialization.
  #
  pkginfo-f2d {
    -name            TEAISH_NAME
    -name.dist       TEAISH_DIST_NAME
    -name.pkg        TEAISH_PKGNAME
    -version         TEAISH_VERSION
    -libDir          TEAISH_LIBDIR_NAME
    -loadPrefix      TEAISH_LOAD_PREFIX
    -vsatisfies      TEAISH_VSATISFIES
    -pkgInit.tcl     TEAISH_PKGINIT_TCL
    -pkgInit.tcl.in  TEAISH_PKGINIT_TCL_IN
    -url             TEAISH_URL
    -tm.tcl          TEAISH_TM_TCL
    -tm.tcl.in       TEAISH_TM_TCL_IN
    -options         {}
    -pragmas         {}
    -src             {}
  }

  #
  # Queues for use with teaish-checks-queue and teaish-checks-run.
  #
  queued-checks-pre {}
  queued-checks-post {}

  # Whether or not "make dist" parts are enabled. They get enabled
  # when building from an extension's dir, disabled when building
  # elsewhere.
  dist-enabled 1
  # Whether or not "make install" parts are enabled. By default
  # they are, but we have a single use case where they're
  # both unnecessary and unhelpful, so...
  install-enabled 1

  # By default we enable compilation of a native extension but if the
  # extension has no native code or the user wants to take that over
  # via teaish.make.in or provide a script-only extension, we will
  # elide the default compilation rules if this is 0.
  dll-enabled 1

  # Files to include in the "make dist" bundle.
  dist-files {}

  # List of source files for the extension.
  extension-src {}

  # Path to the teaish.tcl file.
  teaish.tcl {}

  # Dir where teaish.tcl is found.
  extension-dir {}

  # Whether the generates TEASH_VSATISFIES_CODE should error out on a
  # satisfies error. If 0, it uses return instead of error.
  vsatisfies-error 1

  # Whether or not to allow a "full dist" - a "make dist" build which
  # includes both the extension and teaish. By default this is only on
  # if the extension dir is teaish's dir.
  dist-full-enabled 0
}]
set teaish__Config(core-dir) $::autosetup(libdir)/teaish

#
# Array of info managed by teaish-pkginfo-get and friends.  Has the
# same set of keys as $teaish__Config(pkginfo-f2d).
#
array set teaish__PkgInfo {}

#
# Runs {*}$args if $lvl is <= the current verbosity level, else it has
# no side effects.
#
proc teaish__verbose {lvl args} {
  if {$lvl <= $::teaish__Config(verbose)} {
    {*}$args
  }
}

#
# @teaish-argv-has flags...
#
# Returns true if any arg in $::argv matches any of the given globs,
# else returns false.
#
proc teaish-argv-has {args} {
  foreach glob $args {
    foreach arg $::argv {
      if {[string match $glob $arg]} {
        return 1
      }
    }
  }
  return 0
}

if {[teaish-argv-has --teaish-verbose --t-v]} {
  # Check this early so that we can use verbose-only messages in the
  # pre-options-parsing steps.
  set ::teaish__Config(verbose) 1
  #teaish__verbose 1 msg-result "--teaish-verbose activated"
}

msg-quiet use system ; # Outputs "Host System" and "Build System" lines
if {"--help" ni $::argv} {
  teaish__verbose 1 msg-result "TEA(ish) Version = $::teaish__Config(version)"
  teaish__verbose 1 msg-result "Source dir       = $::autosetup(srcdir)"
  teaish__verbose 1 msg-result "Build dir        = $::autosetup(builddir)"
}

#
# @teaish-configure-core
#
# Main entry point for the TEA-ish configure process. auto.def's primary
# (ideally only) job should be to call this.
#
proc teaish-configure-core {} {
  proj-tweak-default-env-dirs

  set ::teaish__Config(install-mode) [teaish-argv-has --teaish-install*]
  set ::teaish__Config(create-ext-mode) \
    [teaish-argv-has --teaish-create-extension=* --t-c-e=*]
  set gotExt 0; # True if an extension config is found
  if {!$::teaish__Config(create-ext-mode)
      && !$::teaish__Config(install-mode)} {
    # Don't look for an extension if we're in --t-c-e or --t-i mode
    set gotExt [teaish__find_extension]
  }

  #
  # Set up the core --flags. This needs to come before teaish.tcl is
  # sourced so that that file can use teaish-pkginfo-set to append
  # options.
  #
  options-add [proj-strip-hash-comments {
    with-tcl:DIR
      => {Directory containing tclConfig.sh or a directory one level up from
          that, from which we can derive a directory containing tclConfig.sh.
          Defaults to the $TCL_HOME environment variable.}

    with-tclsh:PATH
      => {Full pathname of tclsh to use.  It is used for trying to find
          tclConfig.sh.  Warning: if its containing dir has multiple tclsh
          versions, it may select the wrong tclConfig.sh!
        Defaults to the $TCLSH environment variable.}

    tcl-stubs=0 => {Enable use of Tcl stubs library.}

    # TEA has --with-tclinclude but it appears to only be useful for
    # building an extension against an uninstalled copy of TCL's own
    # source tree. The policy here is that either we get that info
    # from tclConfig.sh or we give up.
    #
    # with-tclinclude:DIR
    #   => {Specify the directory which contains the tcl.h. This should not
    #       normally be required, as that information comes from tclConfig.sh.}

    # We _generally_ want to reduce the possibility of flag collisions with
    # extensions, and thus use a teaish-... prefix on most flags. However,
    # --teaish-extension-dir is frequently needed, so...
    #
    # As of this spontaneous moment, we'll settle on using --t-A-X to
    # abbreviate --teaish-A...-X... flags when doing so is
    # unambiguous...
    ted: t-e-d:
    teaish-extension-dir:DIR
      => {Looks for an extension in the given directory instead of the current
          dir.}

    t-c-e:
    teaish-create-extension:TARGET_DIRECTORY
      => {Writes stub files for creating an extension. Will refuse to overwrite
          existing files without --teaish-force.}

    t-f
    teaish-force
      => {Has a context-dependent meaning (autosetup defines --force for its
          own use).}

    t-d-d
    teaish-dump-defines
      => {Dump all configure-defined vars to config.defines.txt}

    t-v:=0
    teaish-verbose:=0
      => {Enable more (often extraneous) messages from the teaish core.}

    t-d
    teaish-debug=0 => {Enable teaish-specific debug output}

    t-i
    teaish-install:=auto
      => {Installs a copy of teaish, including autosetup, to the target dir.
          When used with --teaish-create-extension=DIR, a value of "auto"
          (no no value) will inherit that directory.}

    #TODO: --teaish-install-extension:=dir as short for
    # --t-c-e=dir --t-i

    t-e-p:
    teaish-extension-pkginfo:pkginfo
      => {For use with --teaish-create-extension. If used, it must be a
          list of arguments for use with teaish-pkginfo-set, e.g.
          --teaish-extension-pkginfo="-name Foo -version 2.3"}

    t-v-c
    teaish-vsatisfies-check=1
      => {Disable the configure-time "vsatisfies" check on the target tclsh.}

  }]; # main options.

  if {$gotExt} {
    # We found an extension. Source it...
    set ttcl $::teaish__Config(teaish.tcl)
    proj-assert {"" ne [teaish-pkginfo-get -name]}
    proj-assert {[file exists $ttcl]} \
      "Expecting to have found teaish.(tcl|config) by now"
    if {[string match *.tcl $ttcl]} {
      uplevel 1 {source $::teaish__Config(teaish.tcl)}
    } else {
      teaish-pkginfo-set {*}[proj-file-content -trim $ttcl]
    }
    unset ttcl
    # Set up some default values if the extension did not set them.
    # This must happen _after_ it's sourced but before
    # teaish-configure is called.
    array set f2d $::teaish__Config(pkginfo-f2d)
    foreach {pflag key type val} {
      - TEAISH_CFLAGS            -v ""
      - TEAISH_LDFLAGS           -v ""
      - TEAISH_MAKEFILE          -v ""
      - TEAISH_MAKEFILE_CODE     -v ""
      - TEAISH_MAKEFILE_IN       -v ""
      - TEAISH_PKGINDEX_TCL      -v ""
      - TEAISH_PKGINDEX_TCL_IN   -v ""
      - TEAISH_PKGINIT_TCL       -v ""
      - TEAISH_PKGINIT_TCL_IN    -v ""
      - TEAISH_PKGINIT_TCL_TAIL  -v ""
      - TEAISH_TEST_TCL          -v ""
      - TEAISH_TEST_TCL_IN       -v ""

      -version          -       -v 0.0.0
      -name.pkg         -       -e {set ::teaish__PkgInfo(-name)}
      -name.dist        -       -e {set ::teaish__PkgInfo(-name)}
      -libDir           -       -e {
        join [list \
                $::teaish__PkgInfo(-name.pkg) \
                $::teaish__PkgInfo(-version)] ""
      }
      -loadPrefix       -       -e {
        string totitle $::teaish__PkgInfo(-name.pkg)
      }
      -vsatisfies       -       -v {{Tcl 8.5-}}
      -pkgInit.tcl      -       -v ""
      -pkgInit.tcl.in   -       -v ""
      -url              -       -v ""
      -tm.tcl           -       -v ""
      -tm.tcl.in        -       -v ""
      -src              -       -v ""
    } {
      #proj-assert 0 {Just testing}
      set isPIFlag [expr {"-" ne $pflag}]
      if {$isPIFlag} {
        if {[info exists ::teaish__PkgInfo($pflag)]} {
          # Was already set - skip it.
          continue;
        }
        proj-assert {{-} eq $key};# "Unexpected pflag=$pflag key=$key type=$type val=$val"
        set key $f2d($pflag)
      }
      if {"" ne $key} {
        if {"<nope>" ne [get-define $key "<nope>"]} {
          # Was already set - skip it.
          continue
        }
      }
      switch -exact -- $type {
        -v {}
        -e { set val [eval $val] }
        default { proj-error "Invalid type flag: $type" }
      }
      #puts "***** defining default $pflag $key {$val} isPIFlag=$isPIFlag"
      if {$key ne ""} {
        define $key $val
      }
      if {$isPIFlag} {
        set ::teaish__PkgInfo($pflag) $val
      }
    }
    unset isPIFlag pflag key type val
    array unset f2d
  }; # sourcing extension's teaish.tcl

  if {[llength [info proc teaish-options]] > 0} {
    # Add options defined by teaish-options, which is assumed to be
    # imported via [teaish-get -teaish-tcl].
    set o [teaish-options]
    if {"" ne $o} {
      options-add $o
    }
  }
  #set opts [proj-options-combine]
  #lappend opts teaish-debug => {x}; #testing dupe entry handling
  if {[catch {options {}} msg xopts]} {
    # Workaround for <https://github.com/msteveb/autosetup/issues/73>
    # where [options] behaves oddly on _some_ TCL builds when it's
    # called from deeper than the global scope.
    dict incr xopts -level
    return {*}$xopts $msg
  }

  proj-xfer-options-aliases {
    t-c-e  => teaish-create-extension
    t-d    => teaish-debug
    t-d-d  => teaish-dump-defines
    ted    => teaish-extension-dir
    t-e-d  => teaish-extension-dir
    t-e-p  => teaish-extension-pkginfo
    t-f    => teaish-force
    t-i    => teaish-install
    t-v    => teaish-verbose
    t-v-c  => teaish-vsatisfies-check
  }

  scan [opt-val teaish-verbose 0] %d ::teaish__Config(verbose)
  set ::teaish__Config(debug-enabled) [opt-bool teaish-debug]

  set exitEarly 0
  if {[proj-opt-was-provided teaish-create-extension]} {
    teaish__create_extension [opt-val teaish-create-extension]
    incr exitEarly
  }
  if {$::teaish__Config(install-mode)} {
    teaish__install
    incr exitEarly
  }

  if {$exitEarly} {
    file delete -force config.log
    return
  }
  proj-assert {1==$gotExt} "Else we cannot have gotten this far"

  teaish__configure_phase1
}


#
# Internal config-time debugging output routine. It is not legal to
# call this from the global scope.
#
proc teaish-debug {msg} {
  if {$::teaish__Config(debug-enabled)} {
    puts stderr [proj-bold "** DEBUG: \[[proj-scope 1]\]: $msg"]
  }
}

#
# Runs "phase 1" of the configuration, immediately after processing
# --flags. This is what will import the client-defined teaish.tcl.
#
proc teaish__configure_phase1 {} {
  msg-result \
    [join [list "Configuring build of Tcl extension" \
       [proj-bold [teaish-pkginfo-get -name] \
          [teaish-pkginfo-get -version]] "..."]]

  uplevel 1 {
    use cc cc-db cc-shared cc-lib; # pkg-config
  }
  teaish__check_tcl
  apply {{} {
    #
    # If --prefix or --exec-prefix are _not_ provided, use their
    # TCL_... counterpart from tclConfig.sh.  Caveat: by the time we can
    # reach this point, autosetup's system.tcl will have already done
    # some non-trivial amount of work with these to create various
    # derived values from them, so we temporarily end up with a mishmash
    # of autotools-compatibility var values. That will be straightened
    # out in the final stage of the configure script via
    # [proj-remap-autoconf-dir-vars].
    #
    foreach {flag uflag tclVar} {
      prefix      prefix      TCL_PREFIX
      exec-prefix exec_prefix TCL_EXEC_PREFIX
    } {
      if {![proj-opt-was-provided $flag]} {
        if {"exec-prefix" eq $flag} {
          # If --exec-prefix was not used, ensure that --exec-prefix
          # derives from the --prefix we may have just redefined.
          set v {${prefix}}
        } else {
          set v [get-define $tclVar "???"]
          teaish__verbose 1 msg-result "Using \$$tclVar for --$flag=$v"
        }
        proj-assert {"???" ne $v} "Expecting teaish__check_tcl to have defined $tclVar"
        #puts "*** $flag $uflag $tclVar = $v"
        proj-opt-set $flag $v
        define $uflag $v

        # ^^^ As of here, all autotools-compatibility vars which derive
        # from --$flag, e.g. --libdir, still derive from the default
        # --$flag value which was active when system.tcl was
        # included. So long as those flags are not explicitly passed to
        # the configure script, those will be straightened out via
        # [proj-remap-autoconf-dir-vars].
      }
    }
  }}; # --[exec-]prefix defaults
  teaish__check_common_bins
  #
  # Set up library file names
  #
  proj-file-extensions
  teaish__define_pkginfo_derived *

  teaish-checks-run -pre
  if {[llength [info proc teaish-configure]] > 0} {
    # teaish-configure is assumed to be imported via
    # teaish.tcl
    teaish-configure
  }
  teaish-checks-run -post

  define TEAISH_USE_STUBS [opt-bool tcl-stubs]

  apply {{} {
    # Set up "vsatisfies" code for pkgIndex.tcl.in,
    # _teaish.tester.tcl.in, and for a configure-time check.  We would
    # like to put this before [teaish-checks-run -pre] but it's
    # marginally conceivable that a client may need to dynamically
    # calculate the vsatisfies and set it via [teaish-configure].
    set vs [get-define TEAISH_VSATISFIES ""]
    if {"" eq $vs} return
    set code {}
    set n 0
    # Treat $vs as a list-of-lists {{Tcl 8.5-} {Foo 1.0- -3.0} ...}
    # and generate Tcl which will run package vsatisfies tests with
    # that info.
    foreach pv $vs {
      set n [llength $pv]
      if {$n < 2} {
        proj-error "-vsatisfies: {$pv} appears malformed. Whole list is: $vs"
      }
      set pkg [lindex $pv 0]
      set vcheck {}
      for {set i 1} {$i < $n} {incr i} {
        lappend vcheck [lindex $pv $i]
      }
      if {[opt-bool teaish-vsatisfies-check]} {
        set tclsh [get-define TCLSH_CMD]
        set vsat "package vsatisfies \[ package provide $pkg \] $vcheck"
        set vputs "puts \[ $vsat \]"
        #puts "*** vputs = $vputs"
        scan [exec echo $vputs | $tclsh] %d vvcheck
        if {![info exists vvcheck] || 0 == $vvcheck} {
          proj-fatal -up $tclsh "check failed:" $vsat
        }
      }
      if {$::teaish__Config(vsatisfies-error)} {
        set vunsat \
          [list error [list Package \
                         $::teaish__PkgInfo(-name) $::teaish__PkgInfo(-version) \
                         requires $pv]]
      } else {
        set vunsat return
      }
      lappend code \
        [string trim [subst -nocommands \
          {if { ![package vsatisfies [package provide $pkg] $vcheck] } {\n  $vunsat\n}}]]
    }; # foreach pv
    define TEAISH_VSATISFIES_CODE [join $code "\n"]
  }}; # vsatisfies

  if {[proj-looks-like-windows]} {
    # Without this, linking of an extension will not work on Cygwin or
    # Msys2.
    msg-result "Using USE_TCL_STUBS for Unix(ish)-on-Windows environment"
    teaish-cflags-add -DUSE_TCL_STUBS=1
  }

  #define AS_LIBDIR $::autosetup(libdir)
  define TEAISH_TESTUTIL_TCL $::teaish__Config(core-dir)/tester.tcl

  apply {{} {
    #
    # Ensure we have a pkgIndex.tcl and don't have a stale generated one
    # when rebuilding for different --with-tcl=... values.
    #
    if {!$::teaish__Config(pkgindex-policy)} {
      proj-error "Cannot determine which pkgIndex.tcl to use"
    }
    if {0x300 & $::teaish__Config(pkgindex-policy)} {
      teaish__verbose 1 msg-result "pkgIndex disabled by -tm.tcl(.in)"
    } else {
      set tpi [proj-coalesce \
                 [get-define TEAISH_PKGINDEX_TCL_IN] \
                 [get-define TEAISH_PKGINDEX_TCL]]
      proj-assert {$tpi ne ""} \
        "TEAISH_PKGINDEX_TCL should have been set up by now"
      teaish__verbose 1 msg-result "Using pkgIndex from $tpi"
      if {0x0f & $::teaish__Config(pkgindex-policy)} {
        # Don't leave stale pkgIndex.tcl laying around yet don't delete
        # or overwrite a user-managed static pkgIndex.tcl.
        file delete -force -- [get-define TEAISH_PKGINDEX_TCL]
        proj-dot-ins-append [get-define TEAISH_PKGINDEX_TCL_IN]
      } else {
        teaish-dist-add [file tail $tpi]
      }
    }
  }}; # $::teaish__Config(pkgindex-policy)

  #
  # Ensure we clean up TEAISH_PKGINIT_TCL if needed and @-process
  # TEAISH_PKGINIT_TCL_IN if needed.
  #
  if {0x0f & $::teaish__Config(pkginit-policy)} {
    file delete -force -- [get-define TEAISH_PKGINIT_TCL]
    proj-dot-ins-append [get-define TEAISH_PKGINIT_TCL_IN] \
      [get-define TEAISH_PKGINIT_TCL]
  }
  if {0x0f & $::teaish__Config(tm-policy)} {
    file delete -force -- [get-define TEAISH_TM_TCL]
    proj-dot-ins-append [get-define TEAISH_TM_TCL_IN]
  }

  apply {{} {
    # Queue up any remaining dot-in files
    set dotIns [list]
    foreach {dIn => dOut} {
      TEAISH_TESTER_TCL_IN => TEAISH_TESTER_TCL
      TEAISH_TEST_TCL_IN   => TEAISH_TEST_TCL
      TEAISH_MAKEFILE_IN   => TEAISH_MAKEFILE
    } {
      lappend dotIns [get-define $dIn ""] [get-define $dOut ""]
    }
    lappend dotIns $::autosetup(srcdir)/Makefile.in Makefile; # must be after TEAISH_MAKEFILE_IN.
    # Much later: probably because of timestamps for deps purposes :-?
    #puts "dotIns=$dotIns"
    foreach {i o} $dotIns {
      if {"" ne $i && "" ne $o} {
        #puts " pre-dot-ins-append:  \[$i\] -> \[$o\]"
        proj-dot-ins-append $i $o
      }
    }
  }}

  define TEAISH_DIST_FULL \
    [expr {
           $::teaish__Config(dist-enabled)
           && $::teaish__Config(dist-full-enabled)
         }]

  define TEAISH_AUTOSETUP_DIR  $::teaish__Config(core-dir)
  define TEAISH_ENABLE_DIST    $::teaish__Config(dist-enabled)
  define TEAISH_ENABLE_INSTALL $::teaish__Config(install-enabled)
  define TEAISH_ENABLE_DLL     $::teaish__Config(dll-enabled)
  define TEAISH_TCL            $::teaish__Config(teaish.tcl)

  define TEAISH_DIST_FILES     [join $::teaish__Config(dist-files)]
  define TEAISH_EXT_DIR        [join $::teaish__Config(extension-dir)]
  define TEAISH_EXT_SRC        [join $::teaish__Config(extension-src)]
  proj-setup-autoreconfig TEAISH_AUTORECONFIG
  foreach f {
    TEAISH_CFLAGS
    TEAISH_LDFLAGS
  } {
    # Ensure that any of these lists are flattened
    define $f [join [get-define $f]]
  }
  proj-remap-autoconf-dir-vars
  set tdefs [teaish__defines_to_list]
  define TEAISH__DEFINES_MAP $tdefs; # injected into _teaish.tester.tcl

  #
  # NO [define]s after this point!
  #
  proj-if-opt-truthy teaish-dump-defines {
    proj-file-write config.defines.txt $tdefs
  }
  proj-dot-ins-process -validate

}; # teaish__configure_phase1

#
# Run checks for required binaries.
#
proc teaish__check_common_bins {} {
  if {"" eq [proj-bin-define install]} {
    proj-warn "Cannot find install binary, so 'make install' will not work."
    define BIN_INSTALL false
  }
  if {"" eq [proj-bin-define zip]} {
    proj-warn "Cannot find zip, so 'make dist.zip' will not work."
  }
  if {"" eq [proj-bin-define tar]} {
    proj-warn "Cannot find tar, so 'make dist.tgz' will not work."
  }
}

#
# TCL...
#
# teaish__check_tcl performs most of the --with-tcl and --with-tclsh
# handling. Some related bits and pieces are performed before and
# after that function is called.
#
# Important [define]'d vars:
#
#  - TCLSH_CMD is the path to the canonical tclsh or "".
#
#  - TCL_CONFIG_SH is the path to tclConfig.sh or "".
#
#  - TCLLIBDIR is the dir to which the extension library gets
#  - installed.
#
proc teaish__check_tcl {} {
  define TCLSH_CMD false ; # Significant is that it exits with non-0
  define TCLLIBDIR ""    ; # Installation dir for TCL extension lib
  define TCL_CONFIG_SH ""; # full path to tclConfig.sh

  # Clear out all vars which would harvest from tclConfig.sh so that
  # the late-config validation of @VARS@ works even if --disable-tcl
  # is used.
  proj-tclConfig-sh-to-autosetup ""

  # TODO: better document the steps this is taking.
  set srcdir $::autosetup(srcdir)
  msg-result "Checking for a suitable tcl... "
  set use_tcl 1
  set withSh [opt-val with-tclsh [proj-get-env TCLSH]]
  set tclHome [opt-val with-tcl [proj-get-env TCL_HOME]]
  if {[string match */lib $tclHome]} {
    # TEA compatibility kludge: its --with-tcl wants the lib
    # dir containing tclConfig.sh.
    #proj-warn "Replacing --with-tcl=$tclHome for TEA compatibility"
    regsub {/lib^} $tclHome "" tclHome
    msg-result "NOTE: stripped /lib suffix from --with-tcl=$tclHome (a TEA-ism)"
  }
  if {0} {
    # This misinteracts with the $TCL_PREFIX default: it will use the
    # autosetup-defined --prefix default
    if {"prefix" eq $tclHome} {
      set tclHome [get-define prefix]
    }
  }
  teaish-debug "use_tcl ${use_tcl}"
  teaish-debug "withSh=${withSh}"
  teaish-debug "tclHome=$tclHome"
  if {"" eq $withSh && "" eq $tclHome} {
    # If neither --with-tclsh nor --with-tcl are provided, try to find
    # a workable tclsh.
    set withSh [proj-first-bin-of tclsh9.1 tclsh9.0 tclsh8.6 tclsh]
    teaish-debug "withSh=${withSh}"
  }

  set doConfigLookup 1 ; # set to 0 to test the tclConfig.sh-not-found cases
  if {"" ne $withSh} {
    # --with-tclsh was provided or found above. Validate it and use it
    # to trump any value passed via --with-tcl=DIR.
    if {![file-isexec $withSh]} {
      proj-error "TCL shell $withSh is not executable"
    } else {
      define TCLSH_CMD $withSh
      #msg-result "Using tclsh: $withSh"
    }
    if {$doConfigLookup &&
        [catch {exec $withSh $::autosetup(libdir)/find_tclconfig.tcl} result] == 0} {
      set tclHome $result
    }
    if {"" ne $tclHome && [file isdirectory $tclHome]} {
      teaish__verbose 1 msg-result "$withSh recommends the tclConfig.sh from $tclHome"
    } else {
      proj-warn "$withSh is unable to recommend a tclConfig.sh"
      set use_tcl 0
    }
  }
  set cfg ""
  set tclSubdirs {tcl9.1 tcl9.0 tcl8.6 tcl8.5 lib}
  while {$use_tcl} {
    if {"" ne $tclHome} {
      # Ensure that we can find tclConfig.sh under ${tclHome}/...
      if {$doConfigLookup} {
        if {[file readable "${tclHome}/tclConfig.sh"]} {
          set cfg "${tclHome}/tclConfig.sh"
        } else {
          foreach i $tclSubdirs {
            if {[file readable "${tclHome}/$i/tclConfig.sh"]} {
              set cfg "${tclHome}/$i/tclConfig.sh"
              break
            }
          }
        }
      }
      if {"" eq $cfg} {
        proj-error "No tclConfig.sh found under ${tclHome}"
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
    teaish__verbose 1 msg-result "Using tclConfig.sh = $cfg"
    break
  }; # while {$use_tcl}
  define TCL_CONFIG_SH $cfg
  # Export a subset of tclConfig.sh to the current TCL-space.  If $cfg
  # is an empty string, this emits empty-string entries for the
  # various options we're interested in.
  proj-tclConfig-sh-to-autosetup $cfg

  if {"" eq $withSh && $cfg ne ""} {
    # We have tclConfig.sh but no tclsh. Attempt to locate a tclsh
    # based on info from tclConfig.sh.
    set tclExecPrefix [get-define TCL_EXEC_PREFIX]
    proj-assert {"" ne $tclExecPrefix}
    set tryThese [list \
                    $tclExecPrefix/bin/tclsh[get-define TCL_VERSION] \
                    $tclExecPrefix/bin/tclsh ]
    foreach trySh $tryThese {
      if {[file-isexec $trySh]} {
        set withSh $trySh
        break
      }
    }
    if {![file-isexec $withSh]} {
      proj-warn "Cannot find a usable tclsh (tried: $tryThese)"
    }
  }
  define TCLSH_CMD $withSh
  if {$use_tcl} {
    # Set up the TCLLIBDIR
    set tcllibdir [get-env TCLLIBDIR ""]
    set extDirName [teaish-pkginfo-get -libDir]
    if {"" eq $tcllibdir} {
      # Attempt to extract TCLLIBDIR from TCL's $auto_path
      if {"" ne $withSh &&
          [catch {exec echo "puts stdout \$auto_path" | "$withSh"} result] == 0} {
        foreach i $result {
          if {![string match //zip* $i] && [file isdirectory $i]} {
            # isdirectory actually passes on //zipfs:/..., but those are
            # useless for our purposes
            set tcllibdir $i/$extDirName
            break
          }
        }
      } else {
        proj-error "Cannot determine TCLLIBDIR."
      }
    }
    define TCLLIBDIR $tcllibdir
  }; # find TCLLIBDIR

  set gotSh [file-isexec $withSh]
  set tmdir ""; # first tcl::tm::list entry
  if {$gotSh} {
    catch {
      set tmli [exec echo {puts [tcl::tm::list]} | $withSh]
      # Reminder: this list contains many names of dirs which do not
      # exist but are legitimate. If we rely only on an is-dir check,
      # we can end up not finding any of the many candidates.
      set firstDir ""
      foreach d $tmli {
        if {"" eq $firstDir && ![string match //*:* $d]} {
          # First non-VFS entry, e.g. not //zipfs:
          set firstDir $d
        }
        if {[file isdirectory $d]} {
          set tmdir $d
          break
        }
      }
      if {"" eq $tmdir} {
        set tmdir $firstDir
      }
    }; # find tcl::tm path
  }
  define TEAISH_TCL_TM_DIR $tmdir

  # Finally, let's wrap up...
  if {$gotSh} {
    teaish__verbose 1 msg-result "Using tclsh        = $withSh"
    if {$cfg ne ""} {
      define HAVE_TCL 1
    } else {
      proj-warn "Found tclsh but no tclConfig.sh."
    }
    if {"" eq $tmdir} {
      proj-warn "Did not find tcl::tm directory."
    }
  }
  show-notices
  # If TCL is not found: if it was explicitly requested then fail
  # fatally, else just emit a warning. If we can find the APIs needed
  # to generate a working JimTCL then that will suffice for build-time
  # TCL purposes (see: proc sqlite-determine-codegen-tcl).
  if {!$gotSh} {
    proj-error "Did not find tclsh"
  } elseif {"" eq $cfg} {
    proj-indented-notice -error {
      Cannot find a usable tclConfig.sh file.  Use --with-tcl=DIR to
      specify a directory near which tclConfig.sh can be found, or
      --with-tclsh=/path/to/tclsh to allow the tclsh binary to locate
      its tclConfig.sh, with the caveat that a symlink to tclsh, or
      wrapper script around it, e.g. ~/bin/tclsh ->
      $HOME/tcl/9.0/bin/tclsh9.1, may not work because tclsh emits
      different library paths for the former than the latter.
    }
  }
  msg-result "Using Tcl [get-define TCL_VERSION] from [get-define TCL_PREFIX]."
  teaish__tcl_platform_quirks
}; # teaish__check_tcl

#
# Perform last-minute platform-specific tweaks to account for quirks.
#
proc teaish__tcl_platform_quirks {} {
  define TEAISH_POSTINST_PREREQUIRE ""
  switch -glob -- [get-define host] {
    *-haiku {
      # Haiku's default TCLLIBDIR is "all wrong": it points to a
      # read-only virtual filesystem mount-point. We bend it back to
      # fit under $TCL_PACKAGE_PATH here.
      foreach {k d} {
        vj TCL_MAJOR_VERSION
        vn TCL_MINOR_VERSION
        pp TCL_PACKAGE_PATH
        ld TCLLIBDIR
      } {
        set $k [get-define $d]
      }
      if {[string match /packages/* $ld]} {
        set old $ld
        set tail [file tail $ld]
        if {8 == $vj} {
          set ld "${pp}/tcl${vj}.${vn}/${tail}"
        } else {
          proj-assert {9 == $vj}
          set ld "${pp}/${tail}"
        }
        define TCLLIBDIR $ld
        # [load foo.so], without a directory part, does not work via
        # automated tests on Haiku (but works when run
        # manually). Similarly, the post-install [package require ...]
        # test fails, presumably for a similar reason. We work around
        # the former in _teaish.tester.tcl.in. We work around the
        # latter by amending the post-install check's ::auto_path (in
        # Makefile.in). This code MUST NOT contain any single-quotes.
        define TEAISH_POSTINST_PREREQUIRE \
          [join [list set ::auto_path \
                   \[ linsert \$::auto_path 0 $ld \] \; \
                  ]]
        proj-indented-notice [subst -nocommands -nobackslashes {
          Haiku users take note: patching target installation dir to match
          Tcl's home because Haiku's is not writable.

          Original  : $old
          Substitute: $ld
        }]
      }
    }
  }
}; # teaish__tcl_platform_quirks

#
# Searches $::argv and/or the build dir and/or the source dir for
# teaish.tcl and friends. Fails if it cannot find teaish.tcl or if
# there are other irreconcilable problems. If it returns 0 then it did
# not find an extension but the --help flag was seen, in which case
# that's not an error.
#
# This does not _load_ the extension, it primarily locates the files
# which make up an extension and fills out no small amount of teaish
# state related to that.
#
proc teaish__find_extension {} {
  proj-assert {!$::teaish__Config(install-mode)}
  teaish__verbose 1 msg-result "Looking for teaish extension..."

  # Helper for the foreach loop below.
  set checkTeaishTcl {{mustHave fid dir} {
    set f [file join $dir $fid]
    if {[file readable $f]} {
      file-normalize $f
    } elseif {$mustHave} {
      proj-error "Missing required $dir/$fid"
    }
  }}

  #
  # We have to handle some flags manually because the extension must
  # be loaded before [options] is run (so that the extension can
  # inject its own options).
  #
  set dirBld $::autosetup(builddir); # dir we're configuring under
  set dirSrc $::autosetup(srcdir);   # where teaish's configure script lives
  set extT ""; # teaish.tcl
  set largv {}; # rewritten $::argv
  set gotHelpArg 0; # got the --help
  foreach arg $::argv {
    #puts "*** arg=$arg"
    switch -glob -- $arg {
      --ted=* -
      --t-e-d=* -
      --teaish-extension-dir=* {
        # Ensure that $extD refers to a directory and contains a
        # teaish.tcl.
        regexp -- {--[^=]+=(.+)} $arg - extD
        set extD [file-normalize $extD]
        if {![file isdirectory $extD]} {
          proj-error "--teaish-extension-dir value is not a directory: $extD"
        }
        set extT [apply $checkTeaishTcl 0 teaish.config $extD]
        if {"" eq $extT} {
          set extT [apply $checkTeaishTcl 1 teaish.tcl $extD]
        }
        set ::teaish__Config(extension-dir) $extD
      }
      --help {
        incr gotHelpArg
        lappend largv $arg
      }
      default {
        lappend largv $arg
      }
    }
  }
  set ::argv $largv

  set dirExt $::teaish__Config(extension-dir); # dir with the extension
  #
  # teaish.tcl is a TCL script which implements various
  # interfaces described by this framework.
  #
  # We use the first one we find in the builddir or srcdir.
  #
  if {"" eq $extT} {
    set flist [list]
    proj-assert {$dirExt eq ""}
    lappend flist $dirBld/teaish.tcl $dirBld/teaish.config $dirSrc/teaish.tcl
    if {![proj-first-file-found extT $flist]} {
      if {$gotHelpArg} {
        # Tell teaish-configure-core that the lack of extension is not
        # an error when --help or --teaish-install is used.
        return 0;
      }
      proj-indented-notice -error "
Did not find any of: $flist

If you are attempting an out-of-tree build, use
 --teaish-extension-dir=/path/to/extension"
    }
  }
  if {![file readable $extT]} {
    proj-error "extension tcl file is not readable: $extT"
  }
  set ::teaish__Config(teaish.tcl) $extT
  set dirExt [file dirname $extT]

  set ::teaish__Config(extension-dir) $dirExt
  set ::teaish__Config(blddir-is-extdir) [expr {$dirBld eq $dirExt}]
  set ::teaish__Config(dist-enabled) $::teaish__Config(blddir-is-extdir); # may change later
  set ::teaish__Config(dist-full-enabled) \
    [expr {[file-normalize $::autosetup(srcdir)]
           eq [file-normalize $::teaish__Config(extension-dir)]}]

  set addDist {{file} {
    teaish-dist-add [file tail $file]
  }}
  apply $addDist $extT

  teaish__verbose 1 msg-result "Extension dir            = [teaish-get -dir]"
  teaish__verbose 1 msg-result "Extension config         = $extT"

  teaish-pkginfo-set -name [file tail [file dirname $extT]]

  #
  # teaish.make[.in] provides some of the info for the main makefile,
  # like which source(s) to build and their build flags.
  #
  # We use the first one of teaish.make.in or teaish.make we find in
  # $dirExt.
  #
  if {[proj-first-file-found extM \
         [list \
            $dirExt/teaish.make.in \
            $dirExt/teaish.make \
         ]]} {
    if {[string match *.in $extM]} {
      define TEAISH_MAKEFILE_IN $extM
      define TEAISH_MAKEFILE _[file rootname [file tail $extM]]
    } else {
      define TEAISH_MAKEFILE_IN ""
      define TEAISH_MAKEFILE $extM
    }
    apply $addDist $extM
    teaish__verbose 1 msg-result "Extension makefile       = $extM"
  } else {
    define TEAISH_MAKEFILE_IN ""
    define TEAISH_MAKEFILE ""
  }

  # Look for teaish.pkginit.tcl[.in]
  set piPolicy 0
  if {[proj-first-file-found extI \
         [list \
            $dirExt/teaish.pkginit.tcl.in \
            $dirExt/teaish.pkginit.tcl \
           ]]} {
    if {[string match *.in $extI]} {
      # Generate teaish.pkginit.tcl from $extI.
      define TEAISH_PKGINIT_TCL_IN $extI
      define TEAISH_PKGINIT_TCL [file rootname [file tail $extI]]
      set piPolicy 0x01
    } else {
      # Assume static $extI.
      define TEAISH_PKGINIT_TCL_IN ""
      define TEAISH_PKGINIT_TCL $extI
      set piPolicy 0x10
    }
    apply $addDist $extI
    teaish__verbose 1 msg-result "Extension post-load init = $extI"
    define TEAISH_PKGINIT_TCL_TAIL \
      [file tail [get-define TEAISH_PKGINIT_TCL]]; # for use in pkgIndex.tcl.in
  }
  set ::teaish__Config(pkginit-policy) $piPolicy

  # Look for pkgIndex.tcl[.in]...
  set piPolicy 0
  if {[proj-first-file-found extPI $dirExt/pkgIndex.tcl.in]} {
    # Generate ./pkgIndex.tcl from $extPI.
    define TEAISH_PKGINDEX_TCL_IN $extPI
    define TEAISH_PKGINDEX_TCL [file rootname [file tail $extPI]]
    apply $addDist $extPI
    set piPolicy 0x01
  } elseif {$dirExt ne $dirSrc
            && [proj-first-file-found extPI $dirSrc/pkgIndex.tcl.in]} {
    # Generate ./pkgIndex.tcl from $extPI.
    define TEAISH_PKGINDEX_TCL_IN $extPI
    define TEAISH_PKGINDEX_TCL [file rootname [file tail $extPI]]
    set piPolicy 0x02
  } elseif {[proj-first-file-found extPI $dirExt/pkgIndex.tcl]} {
    # Assume $extPI's a static file and use it.
    define TEAISH_PKGINDEX_TCL_IN ""
    define TEAISH_PKGINDEX_TCL $extPI
    apply $addDist $extPI
    set piPolicy 0x10
  }
  # Reminder: we have to delay removal of stale TEAISH_PKGINDEX_TCL
  # and the proj-dot-ins-append of TEAISH_PKGINDEX_TCL_IN until much
  # later in the process.
  set ::teaish__Config(pkgindex-policy) $piPolicy

  # Look for teaish.test.tcl[.in]
  proj-assert {"" ne $dirExt}
  set flist [list $dirExt/teaish.test.tcl.in $dirExt/teaish.test.tcl]
  if {[proj-first-file-found ttt $flist]} {
    if {[string match *.in $ttt]} {
      # Generate _teaish.test.tcl from $ttt
      set xt _[file rootname [file tail $ttt]]
      file delete -force -- $xt; # ensure no stale copy is used
      define TEAISH_TEST_TCL $xt
      define TEAISH_TEST_TCL_IN $ttt
    } else {
      define TEAISH_TEST_TCL $ttt
      define TEAISH_TEST_TCL_IN ""
    }
    apply $addDist $ttt
  } else {
    define TEAISH_TEST_TCL ""
    define TEAISH_TEST_TCL_IN ""
  }

  # Look for _teaish.tester.tcl[.in]
  set flist [list $dirExt/_teaish.tester.tcl.in $dirSrc/_teaish.tester.tcl.in]
  if {[proj-first-file-found ttt $flist]} {
    # Generate teaish.test.tcl from $ttt
    set xt [file rootname [file tail $ttt]]
    file delete -force -- $xt; # ensure no stale copy is used
    define TEAISH_TESTER_TCL $xt
    define TEAISH_TESTER_TCL_IN $ttt
    if {[lindex $flist 0] eq $ttt} {
      apply $addDist $ttt
    }
    unset ttt xt
  } else {
    if {[file exists [set ttt [file join $dirSrc _teaish.tester.tcl.in]]]} {
      set xt [file rootname [file tail $ttt]]
      define TEAISH_TESTER_TCL $xt
      define TEAISH_TESTER_TCL_IN $ttt
    } else {
      define TEAISH_TESTER_TCL ""
      define TEAISH_TESTER_TCL_IN ""
    }
  }
  unset flist

  # TEAISH_OUT_OF_EXT_TREE = 1 if we're building from a dir other
  # than the extension's home dir.
  define TEAISH_OUT_OF_EXT_TREE \
    [expr {[file-normalize $::autosetup(builddir)] ne \
             [file-normalize $::teaish__Config(extension-dir)]}]
  return 1
}; # teaish__find_extension

#
# @teaish-cflags-add ?-p|prepend? ?-define? cflags...
#
# Equivalent to [proj-define-amend TEAISH_CFLAGS {*}$args].
#
proc teaish-cflags-add {args} {
  proj-define-amend TEAISH_CFLAGS {*}$args
}

#
# @teaish-define-to-cflag ?flags? defineName...|{defineName...}
#
# Uses [proj-define-to-cflag] to expand a list of [define] keys, each
# one a separate argument, to CFLAGS-style -D... form then appends
# that to the current TEAISH_CFLAGS.
#
# It accepts these flags from proj-define-to-cflag: -quote,
# -zero-undef. It does _not_ support its -list flag.
#
# It accepts its non-flag argument(s) in 2 forms: (1) each arg is a
# single [define] key or (2) its one arg is a list of such keys.
#
# TODO: document teaish's well-defined (as it were) defines for this
# purpose. At a bare minimum:
#
#  - TEAISH_NAME
#  - TEAISH_PKGNAME
#  - TEAISH_VERSION
#  - TEAISH_LIBDIR_NAME
#  - TEAISH_LOAD_PREFIX
#  - TEAISH_URL
#
proc teaish-define-to-cflag {args} {
  set flags {}
  while {[string match -* [lindex $args 0]]} {
    set arg [lindex $args 0]
    switch -exact -- $arg {
      -quote -
      -zero-undef {
        lappend flags $arg
        set args [lassign $args -]
      }
      default break
    }
  }
  if {1 == [llength $args]} {
    set args [list {*}[lindex $args 0]]
  }
  #puts "***** flags=$flags args=$args"
  teaish-cflags-add [proj-define-to-cflag {*}$flags {*}$args]
}

#
# @teaish-cflags-for-tea ?...CFLAGS?
#
# Adds several -DPACKAGE_... CFLAGS using the extension's metadata,
# all as quoted strings. Those symbolic names are commonly used in
# TEA-based builds, and this function is intended to simplify porting
# of such builds. The -D... flags added are:
#
#  -DPACKAGE_VERSION=...
#  -DPACKAGE_NAME=...
#  -DPACKAGE_URL=...
#  -DPACKAGE_STRING=...
#
# Any arguments are passed-on as-is to teaish-cflags-add.
#
proc teaish-cflags-for-tea {args} {
  set name $::teaish__PkgInfo(-name)
  set version $::teaish__PkgInfo(-version)
  set pstr [join [list $name $version]]
  teaish-cflags-add \
    {*}$args \
    '-DPACKAGE_VERSION="$version"' \
    '-DPACKAGE_NAME="$name"' \
    '-DPACKAGE_STRING="$pstr"' \
    '-DPACKAGE_URL="[teaish-get -url]"'
}

#
# @teaish-ldflags-add ?-p|-prepend? ?-define? ldflags...
#
# Equivalent to [proj-define-amend TEAISH_LDFLAGS {*}$args].
#
# Typically, -lXYZ flags need to be in "reverse" order, with each -lY
# resolving symbols for -lX's to its left. This order is largely
# historical, and not relevant on all environments, but it is
# technically correct and still relevant on some environments.
#
# See: teaish-ldflags-prepend
#
proc teaish-ldflags-add {args} {
  proj-define-amend TEAISH_LDFLAGS {*}$args
}

#
# @teaish-ldflags-prepend args...
#
# Functionally equivalent to [teaish-ldflags-add -p {*}$args]
#
proc teaish-ldflags-prepend {args} {
  teaish-ldflags-add -p {*}$args
}

#
# @teaish-src-add ?-dist? ?-dir? src-files...
#
# Appends all non-empty $args to the project's list of C/C++ source or
# (in some cases) object files.
#
# If passed -dist then it also passes each filename, as-is, to
# [teaish-dist-add].
#
# If passed -dir then each src-file has [teaish-get -dir] prepended to
# it before they're added to the list. As often as not, that will be
# the desired behavior so that out-of-tree builds can find the
# sources, but there are cases where it's not desired (e.g. when using
# a source file from outside of the extension's dir, or when adding
# object files (which are typically in the build tree)).
#
proc teaish-src-add {args} {
  proj-parse-simple-flags args flags {
    -dist 0 {expr 1}
    -dir  0 {expr 1}
  }
  if {$flags(-dist)} {
    teaish-dist-add {*}$args
  }
  if {$flags(-dir)} {
    set xargs {}
    foreach arg $args {
      if {"" ne $arg} {
        lappend xargs [file join $::teaish__Config(extension-dir) $arg]
      }
    }
    set args $xargs
  }
  lappend ::teaish__Config(extension-src) {*}$args
}

#
# @teaish-dist-add files-or-dirs...
#
# Adds the given files to the list of files to include with the "make
# dist" rules.
#
# This is a no-op when the current build is not in the extension's
# directory, as dist support is disabled in out-of-tree builds.
#
# It is not legal to call this until [teaish-get -dir] has been
# reliably set (via teaish__find_extension).
#
proc teaish-dist-add {args} {
  if {$::teaish__Config(blddir-is-extdir)} {
    # ^^^ reminder: we ignore $::teaish__Config(dist-enabled) here
    # because the client might want to implement their own dist
    # rules.
    #proj-warn "**** args=$args"
    lappend ::teaish__Config(dist-files) {*}$args
  }
}

# teaish-install-add files...
# Equivalent to [proj-define-apend TEAISH_INSTALL_FILES ...].
#proc teaish-install-add {args} {
#  proj-define-amend TEAISH_INSTALL_FILES {*}$args
#}

#
# @teash-make-add args...
#
# Appends makefile code to the TEAISH_MAKEFILE_CODE define. Each
# arg may be any of:
#
# -tab: emit a literal tab
# -nl: emit a literal newline
# -nltab: short for -nl -tab
# -bnl: emit a backslash-escaped end-of-line
# -bnltab: short for -eol -tab
#
# Anything else is appended verbatim. This function adds no additional
# spacing between each argument nor between subsequent invocations.
# Generally speaking, a series of calls to this function need to
# be sure to end the series with a newline.
proc teaish-make-add {args} {
  set out [get-define TEAISH_MAKEFILE_CODE ""]
  foreach a $args {
    switch -exact -- $a {
      -bnl    { set a " \\\n" }
      -bnltab { set a " \\\n\t" }
      -tab    { set a "\t" }
      -nl     { set a "\n" }
      -nltab  { set a "\n\t" }
    }
    append out $a
  }
  define TEAISH_MAKEFILE_CODE $out
}

# Internal helper to generate a clean/distclean rule name
proc teaish__cleanup_rule {{tgt clean}} {
  set x [incr ::teaish__Config(teaish__cleanup_rule-counter-${tgt})]
  return ${tgt}-_${x}_
}

# @teaish-make-obj ?flags? ?...args?
#
# Uses teaish-make-add to inject makefile rules for $objfile from
# $srcfile, which is assumed to be C code which uses libtcl. Unless
# -recipe is used (see below) it invokes the compiler using the
# makefile-defined $(CC.tcl) which, in the default Makefile.in
# template, includes any flags needed for building against the
# configured Tcl.
#
# This always terminates the resulting code with a newline.
#
# Any arguments after the 2nd may be flags described below or, if no
# -recipe is provided, flags for the compiler call.
#
#   -obj obj-filename.o
#
#   -src src-filename.c
#
#   -recipe {...}
#   Uses the trimmed value of {...} as the recipe, prefixing it with
#   a single hard-tab character.
#
#   -deps {...}
#   List of extra files to list as dependencies of $o.
#
#   -clean
#   Generate cleanup rules as well.
proc teaish-make-obj {args} {
  proj-parse-simple-flags args flags {
    -clean 0 {expr 1}
    -recipe => {}
    -deps => {}
    -obj => {}
    -src => {}
  }
  #parray flags
  if {"" eq $flags(-obj)} {
    set args [lassign $args flags(-obj)]
    if {"" eq $flags(-obj)} {
      proj-error "Missing -obj flag."
    }
  }
  foreach f {-deps -src} {
    set flags($f) [string trim [string map {\n " "} $flags($f)]]
  }
  foreach f {-deps -src} {
    set flags($f) [string trim $flags($f)]
  }
  #parray flags
  #puts "-- args=$args"
  teaish-make-add \
    "# [proj-scope 1] -> [proj-scope] $flags(-obj) $flags(-src)" -nl \
    "$flags(-obj): $flags(-src) $::teaish__Config(teaish.tcl)"
  if {[info exists flags(-deps)]} {
    teaish-make-add " " [join $flags(-deps)]
  }
  teaish-make-add -nltab
  if {[info exists flags(-recipe)]} {
    teaish-make-add [string trim $flags(-recipe)] -nl
  } else {
    teaish-make-add [join [list \$(CC.tcl) -c $flags(-src) {*}$args]] -nl
  }
  if {$flags(-clean)} {
    set rule [teaish__cleanup_rule]
    teaish-make-add \
      "clean: $rule\n$rule:\n\trm -f \"$flags(-obj)\"\n"
  }
}

#
# @teaish-make-clean ?-r? ?-dist? ...files|{...files}
#
# Adds makefile rules for cleaning up the given files via the "make
# clean" or (if -dist is used) "make distclean" makefile rules. The -r
# flag uses "rm -fr" instead of "rm -f", so be careful with that.
#
# The file names are taken literally as arguments to "rm", so they may
# be shell wildcards to be resolved at cleanup-time. To clean up whole
# directories, pass the -r flag. Each name gets quoted in
# double-quotes, so spaces in names should not be a problem (but
# double-quotes in names will be).
#
proc teaish-make-clean {args} {
  if {1 == [llength $args]} {
    set args [list {*}[lindex $args 0]]
  }

  set tgt clean
  set rmflags "-f"
  proj-parse-simple-flags args flags {
    -dist 0 {
      set tgt distclean
    }
    -r 0 {
      set rmflags "-fr"
    }
  }
  set rule [teaish__cleanup_rule $tgt]
  teaish-make-add "# [proj-scope 1] -> [proj-scope]: [join $args]\n"
  teaish-make-add "${rule}:\n\trm ${rmflags}"
  foreach a $args {
    teaish-make-add " \"$a\""
  }
  teaish-make-add "\n${tgt}: ${rule}\n"
}

#
# @teaish-make-config-header filename
#
# Invokes autosetup's [make-config-header] and passes it $filename and
# a relatively generic list of options for controlling which defined
# symbols get exported. Clients which need more control over the
# exports can copy/paste/customize this.
#
# The exported file is then passed to [proj-touch] because, in
# practice, that's sometimes necessary to avoid build dependency
# issues.
#
proc teaish-make-config-header {filename} {
  make-config-header $filename \
    -none {HAVE_CFLAG_* LDFLAGS_* SH_* TEAISH__* TEAISH_*_CODE} \
    -auto {SIZEOF_* HAVE_* TEAISH_*  TCL_*} \
    -none *
  proj-touch $filename; # help avoid frequent unnecessary auto-reconfig
}

#
# @teaish-feature-cache-set $key value
#
# Sets a feature-check cache entry with the given key.
# See proj-cache-set for the key's semantics. $key should
# normally be 0.
#
proc teaish-feature-cache-set {key val} {
  proj-cache-set -key $key -level 1 $val
}

#
# @teaish-feature-cache-check key tgtVarName
#
# Checks for a feature-check cache entry with the given key.
# See proj-cache-set for the key's semantics.
#
# $key should also almost always be 0 but, due to a tclsh
# incompatibility in 1 OS, it cannot have a default value unless it's
# the second argument (but it should be the first one).
#
# If the feature-check cache has a matching entry then this function
# assigns its value to tgtVar and returns 1, else it assigns tgtVar to
# "" and returns 0.
#
# See proj-cache-check for $key's semantics.
#
proc teaish-feature-cache-check {key tgtVar} {
  upvar $tgtVar tgt
  proj-cache-check -key $key -level 1 tgt
}

#
# @teaish-check-cached@ ?flags? msg script...
#
# A proxy for feature-test impls which handles caching of a feature
# flag check on per-function basis, using the calling scope's name as
# the cache key.
#
# It emits [msg-checking $msg]. If $msg is empty then it defaults to
# the name of the caller's scope. The -nomsg flag suppresses the
# message for non-cache-hit checks. At the end, it will [msg-result
# "ok"] [msg-result "no"] unless -nostatus is used, in which case the
# caller is responsible for emitting at least a newline when it's
# done. The -msg-0 and -msg-1 flags can be used to change the ok/no
# text.
#
# This function checks for a cache hit before running $script and
# caching the result. If no hit is found then $script is run in the
# calling scope and its result value is stored in the cache. This
# routine will intercept a 'return' from $script.
#
# $script may be a command and its arguments, as opposed to a single
# script block.
#
# Flags:
#
#   -nostatus = do not emit "ok" or "no" at the end. This presumes
#    that either $script will emit at least one newline before
#    returning or the caller will account for it. Because of how this
#    function is typically used, -nostatus is not honored when the
#    response includes a cached result.
#
#   -quiet = disable output from Autosetup's msg-checking and
#    msg-result for the duration of the $script check. Note that when
#    -quiet is in effect, Autosetup's user-notice can be used to queue
#    up output to appear after the check is done. Also note that
#    -quiet has no effect on _this_ function, only the $script part.
#
#   -nomsg = do not emit $msg for initial check. Like -nostatus, this
#    flag is not honored when the response includes a cached result
#    because it would otherwise produce no output (which is confusing
#    in this context). This is useful when a check runs several other
#    verbose checks and they emit all the necessary info.
#
#   -msg-0 and -msg-1 MSG = strings to show when the check has failed
#    resp. passed. Defaults are "no" and "ok". The 0 and 1 refer to the
#    result value from teaish-feature-cache-check.
#
#   -key cachekey = set the cache context key. Only needs to be
#    explicit when using this function multiple times from a single
#    scope. See proj-cache-check and friends for details on the key
#    name. Its default is the name of the scope which calls this
#    function.
#
proc teaish-check-cached {args} {
  proj-parse-simple-flags args flags {
    -nostatus 0 {expr 1}
    -quiet    0 {expr 1}
    -key      => 1
    -nomsg    0 {expr 1}
    -msg-0    => no
    -msg-1    => ok
  }
  set args [lassign $args msg]
  set script [join $args]
  if {"" eq $msg} {
    set msg [proj-scope 1]
  }
  if {[teaish-feature-cache-check $flags(-key) check]} {
    #if {0 == $flags(-nomsg)} {
    msg-checking "${msg} ... (cached) "
    #}
    #if {!$flags(-nostatus)} {
    msg-result $flags(-msg-[expr {0 != ${check}}])
    #}
    return $check
  } else {
    if {0 == $flags(-nomsg)} {
      msg-checking "${msg} ... "
    }
    if {$flags(-quiet)} {
      incr ::autosetup(msg-quiet)
    }
    set code [catch {uplevel 1 $script} rc xopt]
    if {$flags(-quiet)} {
      incr ::autosetup(msg-quiet) -1
    }
    #puts "***** cached-check got code=$code rc=$rc"
    if {$code in {0 2}} {
      teaish-feature-cache-set 1 $rc
      if {!$flags(-nostatus)} {
        msg-result $flags(-msg-[expr {0 != ${rc}}])
      } else {
        #show-notices; # causes a phantom newline because we're in a
        #msg-checking scope, so...
        if {[info exists ::autosetup(notices)]} {
          show-notices
        }
      }
    } else {
      #puts "**** code=$code rc=$rc xopt=$xopt"
      teaish-feature-cache-set 1 0
    }
    #puts "**** code=$code rc=$rc"
    return {*}$xopt $rc
  }
}

#
# Internal helper for teaish__defs_format_: returns a JSON-ish quoted
# form of the given string-type values.
#
# If $asList is true then the return value is in {$value} form.  If
# $asList is false it only performs the most basic of escaping and
# the input must not contain any control characters.
#
proc teaish__quote_str {asList value} {
  if {$asList} {
    return "{${value}}"
  }
  return \"[string map [list \\ \\\\ \" \\\"] $value]\"
}

#
# Internal helper for teaish__defines_to_list. Expects to be passed
# a name and the variadic $args which are passed to
# teaish__defines_to_list.. If it finds a pattern match for the
# given $name in the various $args, it returns the type flag for that
# $name, e.g. "-str" or "-bare", else returns an empty string.
#
proc teaish__defs_type {name spec} {
  foreach {type patterns} $spec {
    foreach pattern $patterns {
      if {[string match $pattern $name]} {
        return $type
      }
    }
  }
  return ""
}

#
# An internal impl detail. Requires a data type specifier, as used by
# Autosetup's [make-config-header], and a value. Returns the formatted
# value or the value $::teaish__Config(defs-skip) if the caller should
# skip emitting that value.
#
# In addition to -str, -auto, etc., as defined by make-config-header,
# it supports:
#
#  -list {...} will cause non-integer values to be quoted in {...}
#  instead of quotes.
#
#  -autolist {...} works like -auto {...} except that it falls back to
#   -list {...} type instead of -str {...} style for non-integers.
#
#  -jsarray {...} emits the output in something which, for
#   conservative inputs, will be a valid JSON array. It can only
#   handle relatively simple values with no control characters in
#   them.
#
set teaish__Config(defs-skip) "-teaish__defs_format sentinel"
proc teaish__defs_format {type value} {
  switch -exact -- $type {
    -bare {
      # Just output the value unchanged
    }
    -none {
      set value $::teaish__Config(defs-skip)
    }
    -str {
      set value [teaish__quote_str 0 $value]
    }
    -auto {
      # Automatically determine the type
      if {![string is integer -strict $value]} {
        set value [teaish__quote_str 0 $value]
      }
    }
    -autolist {
      if {![string is integer -strict $value]} {
        set value [teaish__quote_str 1 $value]
      }
    }
    -list {
      set value [teaish__quote_str 1 $value]
    }
    -jsarray {
      set ar {}
      foreach v $value {
        if {![string is integer -strict $v]} {
          set v [teaish__quote_str 0 $v]
        }
        if {$::teaish__Config(defs-skip) ne $v} {
          lappend ar $v
        }
      }
      set value [concat \[ [join $ar {, }] \]]
    }
    "" {
      # (Much later:) Why do we do this?
      set value $::teaish__Config(defs-skip)
    }
    default {
      proj-error \
        "Unknown [proj-scope] -type ($type) called from" \
        [proj-scope 1]
    }
  }
  return $value
}

#
# Returns Tcl code in the form of code which evaluates to a list of
# configure-time DEFINEs in the form {key val key2 val...}. It may
# misbehave for values which are not numeric or simple strings.  Some
# defines are specifically filtered out of the result, either because
# their irrelevant to teaish or because they may be arbitrarily large
# (e.g. makefile content).
#
# The $args are explained in the docs for internal-use-only
# [teaish__defs_format]. The default mode is -autolist.
#
proc teaish__defines_to_list {args} {
  set lines {}
  lappend lines "\{"
  set skipper $::teaish__Config(defs-skip)
  set args [list \
              -none {
                TEAISH__*
                TEAISH_*_CODE
                AM_* AS_*
              } \
              {*}$args \
              -autolist *]
  foreach d [lsort [dict keys [all-defines]]] {
    set type [teaish__defs_type $d $args]
    set value [teaish__defs_format $type [get-define $d]]
    if {$skipper ne $value} {
      lappend lines "$d $value"
    }
  }
  lappend lines "\}"
  tailcall join $lines "\n"
}

#
# teaish__pragma ...flags
#
# Offers a way to tweak how teaish's core behaves in some cases, in
# particular those which require changing how the core looks for an
# extension and its files.
#
# Accepts the following flags. Those marked with [L] are safe to use
# during initial loading of tclish.tcl (recall that most teaish APIs
# cannot be used until [teaish-configure] is called).
#
#    static-pkgIndex.tcl [L]: Tells teaish that ./pkgIndex.tcl is not
#    a generated file, so it will not try to overwrite or delete
#    it. Errors out if it does not find pkgIndex.tcl in the
#    extension's dir.
#
#    no-dist [L]: tells teaish to elide the 'make dist' recipe
#    from the generated Makefile.
#
#    no-dll [L]: tells teaish to elide the DLL-building recipe
#    from the generated Makefile.
#
#    no-vsatisfies-error [L]: tells teaish that failure to match the
#    -vsatisfies value should simply "return" instead of "error".
#
#    no-tester [L]: disables automatic generation of teaish.test.tcl
#    even if a copy of _teaish.tester.tcl.in is found.
#
#    no-full-dist [L]: changes the "make dist" rules to never include
#    a copy of teaish itself. By default it will include itself only
#    if the extension lives in the same directory as teaish.
#
#    full-dist [L]: changes the "make dist" rules to always include
#    a copy of teaish itself.
#
# Emits a warning message for unknown arguments.
#
proc teaish__pragma {args} {
  foreach arg $args {
    switch -exact -- $arg {

      static-pkgIndex.tcl {
        if {$::teaish__Config(tm-policy)} {
          proj-fatal -up "Cannot use pragma $arg together with -tm.tcl or -tm.tcl.in."
        }
        set tpi [file join $::teaish__Config(extension-dir) pkgIndex.tcl]
        if {[file exists $tpi]} {
          define TEAISH_PKGINDEX_TCL_IN ""
          define TEAISH_PKGINDEX_TCL $tpi
          set ::teaish__Config(pkgindex-policy) 0x20
        } else {
          proj-error "pragma $arg: found no package-local pkgIndex.tcl\[.in]"
        }
      }

      no-dist {
        set ::teaish__Config(dist-enabled) 0
      }

      no-install {
        set ::teaish__Config(install-enabled) 0
      }

      full-dist {
        set ::teaish__Config(dist-full-enabled) 1
      }

      no-full-dist {
        set ::teaish__Config(dist-full-enabled) 0
      }

      no-dll {
        set ::teaish__Config(dll-enabled) 0
      }

      no-vsatisfies-error {
        set ::teaish__Config(vsatisfies-error) 0
      }

      no-tester {
        define TEAISH_TESTER_TCL_IN ""
        define TEAISH_TESTER_TCL ""
      }

      default {
        proj-error "Unknown flag: $arg"
      }
    }
  }
}

#
# @teaish-pkginfo-set ...flags
#
# The way to set up the initial package state. Used like:
#
#   teaish-pkginfo-set -name foo -version 0.1.2
#
# Or:
#
#   teaish-pkginfo-set ?-vars|-subst? {-name foo -version 0.1.2}
#
# The latter may be easier to write for a multi-line invocation.
#
# For the second call form, passing the -vars flag tells it to perform
# a [subst] of (only) variables in the {...} part from the calling
# scope. The -subst flag will cause it to [subst] the {...} with
# command substitution as well (but no backslash substitution). When
# using -subst for string concatenation, e.g.  with -libDir
# foo[get-version-number], be sure to wrap the value in braces:
# -libDir {foo[get-version-number]}.
#
# Each pkginfo flag corresponds to one piece of extension package
# info.  Teaish provides usable default values for all of these flags,
# but at least the -name and -version should be set by clients.
# e.g. the default -name is the directory name the extension lives in,
# which may change (e.g. when building it from a "make dist" bundle).
#
# The flags:
#
#    -name theName: The extension's name. It defaults to the name of the
#     directory containing the extension. (In TEA this would be the
#     PACKAGE_NAME, not to be confused with...)
#
#    -name.pkg pkg-provide-name: The extension's name for purposes of
#     Tcl_PkgProvide(), [package require], and friends. It defaults to
#     the `-name`, and is normally the same, but some projects (like
#     SQLite) have a different name here than they do in their
#     historical TEA PACKAGE_NAME.
#
#    -version version: The extension's package version. Defaults to
#     0.0.0.
#
#    -libDir dirName: The base name of the directory into which this
#     extension should be installed. It defaults to a concatenation of
#     `-name.pkg` and `-version`.
#
#    -loadPrefix prefix: For use as the second argument passed to
#     Tcl's `load` command in the package-loading process. It defaults
#     to title-cased `-name.pkg` because Tcl's `load` plugin system
#     expects it in that form.
#
#    -options {...}: If provided, it must be a list compatible with
#     Autosetup's `options-add` function. These can also be set up via
#     `teaish-options`.
#
#    -vsatisfies {{...} ...}: Expects a list-of-lists of conditions
#     for Tcl's `package vsatisfies` command: each list entry is a
#     sub-list of `{PkgName Condition...}`.  Teaish inserts those
#     checks via its default pkgIndex.tcl.in and _teaish.tester.tcl.in
#     templates to verify that the system's package dependencies meet
#     these requirements. The default value is `{{Tcl 8.5-}}` (recall
#     that it's a list-of-lists), as 8.5 is the minimum Tcl version
#     teaish will run on, but some extensions may require newer
#     versions or dependencies on other packages. As a special case,
#     if `-vsatisfies` is given a single token, e.g. `8.6-`, then it
#     is transformed into `{Tcl $thatToken}`, i.e. it checks the Tcl
#     version which the package is being run with.  If given multiple
#     lists, each `package provides` check is run in the given
#     order. Failure to meet a `vsatisfies` condition triggers an
#     error.
#
#    -url {...}: an optional URL for the extension.
#
#    -pragmas {...}  A list of infrequently-needed lower-level
#     directives which can influence teaish, including:
#
#      static-pkgIndex.tcl: tells teaish that the client manages their
#      own pkgIndex.tcl, so that teaish won't try to overwrite it
#      using a template.
#
#      no-dist: tells teaish to elide the "make dist" recipe from the
#      makefile so that the client can implement it.
#
#      no-dll: tells teaish to elide the makefile rules which build
#      the DLL, as well as any templated test script and pkgIndex.tcl
#      references to them. The intent here is to (A) support
#      client-defined build rules for the DLL and (B) eventually
#      support script-only extensions.
#
# Unsupported flags or pragmas will trigger an error.
#
# Potential pothole: setting certain state, e.g. -version, after the
# initial call requires recalculating of some [define]s. Any such
# changes should be made as early as possible in teaish-configure so
# that any later use of those [define]s gets recorded properly (not
# with the old value).  This is particularly relevant when it is not
# possible to determine the -version or -name until teaish-configure
# has been called, and it's updated dynamically from
# teaish-configure. Notably:
#
#   - If -version or -name are updated, -libDir will almost certainly
#     need to be explicitly set along with them.
#
#   - If -name is updated, -loadPrefix probably needs to be as well.
#
proc teaish-pkginfo-set {args} {
  set doVars 0
  set doCommands 0
  set xargs $args
  set recalc {}
  foreach arg $args {
    switch -exact -- $arg {
      -vars {
        incr doVars
        set xargs [lassign $xargs -]
      }
      -subst {
        incr doVars
        incr doCommands
        set xargs [lassign $xargs -]
      }
      default {
        break
      }
    }
  }
  set args $xargs
  unset xargs
  if {1 == [llength $args] && [llength [lindex $args 0]] > 1} {
    # Transform a single {...} arg into the canonical call form
    set a [list {*}[lindex $args 0]]
    if {$doVars || $doCommands} {
      set sflags -nobackslashes
      if {!$doCommands} {
        lappend sflags -nocommands
      }
      set a [uplevel 1 [list subst {*}$sflags $a]]
    }
    set args $a
  }
  set sentinel "<nope>"
  set flagDefs [list]
  foreach {f d} $::teaish__Config(pkginfo-f2d) {
    lappend flagDefs $f => $sentinel
  }
  proj-parse-simple-flags args flags $flagDefs
  if {[llength $args]} {
    proj-error -up "Too many (or unknown) arguments to [proj-scope]: $args"
  }
  foreach {f d} $::teaish__Config(pkginfo-f2d) {
    if {$sentinel eq [set v $flags($f)]} continue
    switch -exact -- $f {

      -options {
        proj-assert {"" eq $d}
        options-add $v
      }

      -pragmas {
        teaish__pragma {*}$v
      }

      -vsatisfies {
        if {1 == [llength $v] && 1 == [llength [lindex $v 0]]} {
          # Transform X to {Tcl $X}
          set v [list [join [list Tcl $v]]]
        }
        define $d $v
      }

      -pkgInit.tcl -
      -pkgInit.tcl.in {
        if {0x22 & $::teaish__Config(pkginit-policy)} {
          proj-fatal "Cannot use -pkgInit.tcl(.in) more than once."
        }
        set x [file join $::teaish__Config(extension-dir) $v]
        set tTail [file tail $v]
        if {"-pkgInit.tcl.in" eq $f} {
          # Generate pkginit file X from X.in
          set pI 0x02
          set tIn $x
          set tOut [file rootname $tTail]
          set other -pkgInit.tcl
        } else {
          # Static pkginit file X
          set pI 0x20
          set tIn ""
          set tOut $x
          set other -pkgInit.tcl.in
        }
        set ::teaish__Config(pkginit-policy) $pI
        set ::teaish__PkgInfo($other) {}
        define TEAISH_PKGINIT_TCL_IN $tIn
        define TEAISH_PKGINIT_TCL $tOut
        define TEAISH_PKGINIT_TCL_TAIL $tTail
        teaish-dist-add $v
        set v $x
      }

      -src {
        set d $::teaish__Config(extension-dir)
        foreach f $v {
          lappend ::teaish__Config(dist-files) $f
          lappend ::teaish__Config(extension-src) $d/$f
          lappend ::teaish__PkgInfo(-src) $f
          # ^^^ so that default-value initialization in
          # teaish-configure-core recognizes that it's been set.
        }
      }

      -tm.tcl -
      -tm.tcl.in {
        if {0x30 & $::teaish__Config(pkgindex-policy)} {
          proj-fatal "Cannot use $f together with a pkgIndex.tcl."
        } elseif {$::teaish__Config(tm-policy)} {
          proj-fatal "Cannot use -tm.tcl(.in) more than once."
        }
        set x [file join $::teaish__Config(extension-dir) $v]
        if {"-tm.tcl.in" eq $f} {
          # Generate tm file X from X.in
          set pT 0x02
          set pI 0x100
          set tIn $x
          set tOut [file rootname [file tail $v]]
          set other -tm.tcl
        } else {
          # Static tm file X
          set pT 0x20
          set pI 0x200
          set tIn ""
          set tOut $x
          set other -tm.tcl.in
        }
        set ::teaish__Config(pkgindex-policy) $pI
        set ::teaish__Config(tm-policy) $pT
        set ::teaish__PkgInfo($other) {}
        define TEAISH_TM_TCL_IN $tIn
        define TEAISH_TM_TCL $tOut
        define TEAISH_PKGINDEX_TCL ""
        define TEAISH_PKGINDEX_TCL_IN ""
        define TEAISH_PKGINDEX_TCL_TAIL ""
        teaish-dist-add $v
        teaish__pragma no-dll
        set v $x
      }

      default {
        proj-assert {"" ne $d}
        define $d $v
      }
    }
    set ::teaish__PkgInfo($f) $v
    if {$f in {-name -version -libDir -loadPrefix}} {
      lappend recalc $f
    }
  }
  if {"" ne $recalc} {
    teaish__define_pkginfo_derived $recalc
  }
}

#
# @teaish-pkginfo-get ?arg?
#
# If passed no arguments, it returns the extension config info in the
# same form accepted by teaish-pkginfo-set.
#
# If passed one -flagname arg then it returns the value of that config
# option.
#
# Else it treats arg as the name of caller-scoped variable to
# which this function assigns an array containing the configuration
# state of this extension, in the same structure accepted by
# teaish-pkginfo-set. In this case it returns an empty string.
#
proc teaish-pkginfo-get {args} {
  set cases {}
  set argc [llength $args]
  set rv {}
  switch -exact $argc {
    0 {
      # Return a list of (-flag value) pairs
      lappend cases default {{
        if {[info exists ::teaish__PkgInfo($flag)]} {
          lappend rv $flag $::teaish__PkgInfo($flag)
        } else {
          lappend rv $flag [get-define $defName]
        }
      }}
    }

    1 {
      set arg $args
      if {[string match -* $arg]} {
        # Return the corresponding -flag's value
        lappend cases $arg {{
          if {[info exists ::teaish__PkgInfo($flag)]} {
            return $::teaish__PkgInfo($flag)
          } else {
            return [get-define $defName]
          }
        }}
      } else {
        # Populate target with an array of (-flag value).
        upvar $arg tgt
        array set tgt {}
        lappend cases default {{
          if {[info exists ::teaish__PkgInfo($flag)]} {
            set tgt($flag) $::teaish__PkgInfo($flag)
          } else {
            set tgt($flag) [get-define $defName]
          }
        }}
      }
    }

    default {
      proj-error "invalid arg count from [proj-scope 1]"
    }
  }

  foreach {flag defName} $::teaish__Config(pkginfo-f2d) {
    switch -exact -- $flag [join $cases]
  }
  if {0 == $argc} { return $rv }
}

# (Re)set some defines based on pkginfo state. $flags is the list of
# pkginfo -flags which triggered this, or "*" for the initial call.
proc teaish__define_pkginfo_derived {flags} {
  set all [expr {{*} in $flags}]
  if {$all || "-version" in $flags || "-name" in $flags} {
    set name $::teaish__PkgInfo(-name) ; # _not_ -name.pkg
    if {[info exists ::teaish__PkgInfo(-version)]} {
      set pkgver $::teaish__PkgInfo(-version)
      set libname "lib"
      if {[string match *-cygwin [get-define host]]} {
        set libname cyg
      }
      define TEAISH_DLL8_BASENAME $libname$name$pkgver
      define TEAISH_DLL9_BASENAME ${libname}tcl9$name$pkgver
      set ext [get-define TARGET_DLLEXT]
      define TEAISH_DLL8 [get-define TEAISH_DLL8_BASENAME]$ext
      define TEAISH_DLL9 [get-define TEAISH_DLL9_BASENAME]$ext
    }
  }
  if {$all || "-libDir" in $flags} {
    if {[info exists ::teaish__PkgInfo(-libDir)]} {
      define TCLLIBDIR \
        [file dirname [get-define TCLLIBDIR]]/$::teaish__PkgInfo(-libDir)
    }
  }
}

#
# @teaish-checks-queue -pre|-post args...
#
# Queues one or more arbitrary "feature test" functions to be run when
# teaish-checks-run is called. $flag must be one of -pre or -post to
# specify whether the tests should be run before or after
# teaish-configure is run. Each additional arg is the name of a
# feature-test proc.
#
proc teaish-checks-queue {flag args} {
  if {$flag ni {-pre -post}} {
    proj-error "illegal flag: $flag"
  }
  lappend ::teaish__Config(queued-checks${flag}) {*}$args
}

#
# @teaish-checks-run -pre|-post
#
# Runs all feature checks queued using teaish-checks-queue
# then cleares the queue.
#
proc teaish-checks-run {flag} {
  if {$flag ni {-pre -post}} {
    proj-error "illegal flag: $flag"
  }
  #puts "*** running $flag: $::teaish__Config(queued-checks${flag})"
  set foo 0
  foreach f $::teaish__Config(queued-checks${flag}) {
    if {![teaish-feature-cache-check $f foo]} {
      set v [$f]
      teaish-feature-cache-set $f $v
    }
  }
  set ::teaish__Config(queued-checks${flag}) {}
}

#
# A general-purpose getter for various teaish state. Requires one
# flag, which determines its result value. Flags marked with [L] below
# are safe for using at load-time, before teaish-pkginfo-set is called
#
#   -dir [L]: returns the extension's directory, which may differ from
#    the teaish core dir or the build dir.
#
#   -teaish-home [L]: the "home" dir of teaish itself, which may
#   differ from the extension dir or build dir.
#
#   -build-dir [L]: the build directory (typically the current working
#   -dir).
#
#   Any of the teaish-pkginfo-get/get flags: returns the same as
#   teaish-pkginfo-get. Not safe for use until teaish-pkginfo-set has
#   been called.
#
# Triggers an error if passed an unknown flag.
#
proc teaish-get {flag} {
  #-teaish.tcl {return $::teaish__Config(teaish.tcl)}
  switch -exact -- $flag {
    -dir {
      return $::teaish__Config(extension-dir)
    }
    -teaish-home {
      return $::autosetup(srcdir)
    }
    -build-dir {
      return $::autosetup(builddir)
    }
    default {
      if {[info exists ::teaish__PkgInfo($flag)]} {
        return $::teaish__PkgInfo($flag)
      }
    }
  }
  proj-error "Unhandled flag: $flag"
}

#
# Handles --teaish-create-extension=TARGET-DIR
#
proc teaish__create_extension {dir} {
  set force [opt-bool teaish-force]
  if {"" eq $dir} {
    proj-error "--teaish-create-extension=X requires a directory name."
  }
  file mkdir $dir/generic
  set cwd [pwd]
  #set dir [file-normalize [file join $cwd $dir]]
  teaish__verbose 1 msg-result "Created dir $dir"
  cd $dir
  if {!$force} {
    # Ensure that we don't blindly overwrite anything
    foreach f {
      generic/teaish.c
      teaish.tcl
      teaish.make.in
      teaish.test.tcl
    } {
      if {[file exists $f]} {
        error "Cowardly refusing to overwrite $dir/$f. Use --teaish-force to overwrite."
      }
    }
  }

  set name [file tail $dir]
  set pkgName $name
  set version 0.0.1
  set loadPrefix [string totitle $pkgName]
  set content {teaish-pkginfo-set }
  #puts "0 content=$content"
  if {[opt-str teaish-extension-pkginfo epi]} {
    set epi [string trim $epi]
    if {[string match "*\n*" $epi]} {
      set epi "{$epi}"
    } elseif {![string match "{*}" $epi]} {
      append content "\{" $epi "\}"
    } else {
      append content $epi
    }
    #puts "2 content=$content\nepi=$epi"
  } else {
    append content [subst -nocommands -nobackslashes {{
      -name ${name}
      -name.pkg ${pkgName}
      -name.dist ${pkgName}
      -version ${version}
      -loadPrefix $loadPrefix
      -libDir ${name}${version}
      -vsatisfies {{Tcl 8.5-}}
      -url {}
      -options {}
      -pragmas {full-dist}
    }}]
    #puts "3 content=$content"
  }
  #puts "1 content=$content"
  append content "\n" {
#proc teaish-options {} {
  # Return a list and/or use \[options-add\] to add new
  # configure flags. This is called before teaish's
  # bootstrapping is finished, so only teaish-*
  # APIs which are explicitly noted as being safe
  # early on may be used here. Any autosetup-related
  # APIs may be used here.
  #
  # Return an empty string if there are no options to
  # add or if they are added using \[options-add\].
  #
  # If there are no options to add, this proc need
  # not be defined.
#}

# Called by teaish once bootstrapping is complete.
# This function is responsible for the client-specific
# parts of the configuration process.
proc teaish-configure {} {
  teaish-src-add -dir -dist generic/teaish.c
  teaish-define-to-cflag -quote TEAISH_PKGNAME TEAISH_VERSION

  # TODO: your code goes here..
}
}; # $content
  proj-file-write teaish.tcl $content
  teaish__verbose 1 msg-result "Created teaish.tcl"

  set content "# Teaish test script.
# When this tcl script is invoked via 'make test' it will have loaded
# the package, run any teaish.pkginit.tcl code, and loaded
# autosetup/teaish/tester.tcl.
"
  proj-file-write teaish.test.tcl $content
  teaish__verbose 1 msg-result "Created teaish.test.tcl"

  set content [subst -nocommands -nobackslashes {
#include <tcl.h>
static int
${loadPrefix}_Cmd(ClientData cdata, Tcl_Interp *interp, int objc, Tcl_Obj *const objv[]){
  Tcl_SetObjResult(interp, Tcl_NewStringObj("this is the ${name} extension", -1));
  return TCL_OK;
}

extern int DLLEXPORT ${loadPrefix}_Init(Tcl_Interp *interp){
  if (Tcl_InitStubs(interp, TCL_VERSION, 0) == NULL) {
    return TCL_ERROR;
  }
  if (Tcl_PkgProvide(interp, TEAISH_PKGNAME, TEAISH_VERSION) == TCL_ERROR) {
    return TCL_ERROR;
  }
  Tcl_CreateObjCommand(interp, TEAISH_PKGNAME, ${loadPrefix}_Cmd, NULL, NULL);
  return TCL_OK;
}
}]
  proj-file-write generic/teaish.c $content
  teaish__verbose 1 msg-result "Created generic/teaish.c"

  set content "# teaish makefile for the ${name} extension
# tx.src      = \$(tx.dir)/generic/teaish.c
# tx.LDFLAGS  =
# tx.CFLAGS   =
"
  proj-file-write teaish.make.in $content
  teaish__verbose 1 msg-result "Created teaish.make.in"

  msg-result "Created new extension \[$dir\]."

  cd $cwd
  set ::teaish__Config(install-ext-dir) $dir
}

#
# Internal helper for teaish__install
#
proc teaish__install_file {f destDir force} {
  set dest $destDir/[file tail $f]
  if {[file isdirectory $f]} {
    file mkdir $dest
  } elseif {!$force && [file exists $dest]} {
    array set st1 [file stat $f]
    array set st2 [file stat $dest]
    if {($st1(mtime) == $st2(mtime))
        && ($st1(size) == $st2(size))} {
      if {[file tail $f] in {
        pkgIndex.tcl.in
        _teaish.tester.tcl.in
      }} {
        # Assume they're the same. In the scope of the "make dist"
        # rules, this happens legitimately when an extension with a
        # copy of teaish installed in the same dir assumes that the
        # pkgIndex.tcl.in and _teaish.tester.tcl.in belong to the
        # extension, whereas teaish believes they belong to teaish.
        # So we end up with dupes of those.
        return
      }
    }
    proj-error -up "Cowardly refusing to overwrite \[$dest\]." \
      "Use --teaish-force to enable overwriting."
  } else {
    # file copy -force $f $destDir; # loses +x bit
    #
    # JimTcl doesn't have [file attribute], so we can't use that here
    # (in the context of an autosetup configure script).
    exec cp -p $f $dest
  }
}

#
# Installs a copy of teaish, with autosetup, to $dDest, which defaults
# to the --teaish-install=X or --teash-create-extension=X dir. Won't
# overwrite files unless --teaish-force is used.
#
proc teaish__install {{dDest ""}} {
  if {$dDest in {auto ""}} {
    set dDest [opt-val teaish-install]
    if {$dDest in {auto ""}} {
      if {[info exists ::teaish__Config(install-ext-dir)]} {
        set dDest $::teaish__Config(install-ext-dir)
      }
    }
  }
  set force [opt-bool teaish-force]
  if {$dDest in {auto ""}} {
    proj-error "Cannot determine installation directory."
  } elseif {!$force && [file exists $dDest/auto.def]} {
    proj-error \
      "Target dir looks like it already contains teaish and/or autosetup: $dDest" \
      "\nUse --teaish-force to overwrite it."
  }

  set dSrc $::autosetup(srcdir)
  set dAS $::autosetup(libdir)
  set dAST $::teaish__Config(core-dir)
  set dASTF $dAST/feature
  teaish__verbose 1 msg-result "Installing teaish to \[$dDest\]..."
  if {$::teaish__Config(verbose)>1} {
    msg-result "dSrc  = $dSrc"
    msg-result "dAS   = $dAS"
    msg-result "dAST  = $dAST"
    msg-result "dASTF = $dASTF"
    msg-result "dDest = $dDest"
  }

  # Dest subdirs...
  set ddAS   $dDest/autosetup
  set ddAST  $ddAS/teaish
  set ddASTF $ddAST/feature
  foreach {srcDir destDir} [list \
                              $dAS   $ddAS \
                              $dAST  $ddAST \
                              $dASTF $ddASTF \
                             ] {
    teaish__verbose 1 msg-result "Copying files to $destDir..."
    file mkdir $destDir
    foreach f  [glob -nocomplain -directory $srcDir *] {
      if {[string match {*~} $f] || [string match "#*#" [file tail $f]]} {
        # Editor-generated backups and emacs lock files
        continue
      }
      teaish__verbose 2 msg-result "\t$f"
      teaish__install_file $f $destDir $force
    }
  }
  teaish__verbose 1 msg-result "Copying files to $dDest..."
  foreach f {
    auto.def configure Makefile.in pkgIndex.tcl.in
    _teaish.tester.tcl.in
  } {
    teaish__verbose 2 msg-result "\t$f"
    teaish__install_file $dSrc/$f $dDest $force
  }
  set ::teaish__Config(install-self-dir) $dDest
  msg-result "Teaish $::teaish__Config(version) installed in \[$dDest\]."
}
