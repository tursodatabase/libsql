########################################################################
# 2024 September 25
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#  * May you do good and not evil.
#  * May you find forgiveness for yourself and forgive others.
#  * May you share freely, never taking more than you give.
#

#
# ----- @module proj.tcl -----
# @section Project-agnostic Helper APIs
#

#
# Routines for Steve Bennett's autosetup which are common to trees
# managed in and around the umbrella of the SQLite project.
#
# The intent is that these routines be relatively generic, independent
# of a given project.
#
# For practical purposes, the copy of this file hosted in the SQLite
# project is the "canonical" one:
#
# https://sqlite.org/src/file/autosetup/proj.tcl
#
# This file was initially derived from one used in the libfossil
# project, authored by the same person who ported it here, and this is
# noted here only as an indication that there are no licensing issues
# despite this code having a handful of near-twins running around a
# handful of third-party source trees.
#
# Design notes:
#
# - Symbols with _ separators are intended for internal use within
#   this file, and are not part of the API which auto.def files should
#   rely on. Symbols with - separators are public APIs.
#
# - By and large, autosetup prefers to update global state with the
#   results of feature checks, e.g. whether the compiler supports flag
#   --X.  In this developer's opinion that (A) causes more confusion
#   than it solves[^1] and (B) adds an unnecessary layer of "voodoo"
#   between the autosetup user and its internals. This module, in
#   contrast, instead injects the results of its own tests into
#   well-defined variables and leaves the integration of those values
#   to the caller's discretion.
#
# [1]: As an example: testing for the -rpath flag, using
# cc-check-flags, can break later checks which use
# [cc-check-function-in-lib ...] because the resulting -rpath flag
# implicitly becomes part of those tests. In the case of an rpath
# test, downstream tests may not like the $prefix/lib path added by
# the rpath test. To avoid such problems, we avoid (intentionally)
# updating global state via feature tests.
#

#
# $proj__Config is an internal-use-only array for storing whatever generic
# internal stuff we need stored.
#
array set ::proj__Config [subst {
  self-tests [get-env proj.self-tests 0]
  verbose-assert [get-env proj.assert-verbose 0]
  isatty [isatty? stdout]
}]

#
# List of dot-in files to filter in the final stages of
# configuration. Some configuration steps may append to this.  Each
# one in this list which exists will trigger the generation of a
# file with that same name, minus the ".in", in the build directory
# (which differ from the source dir in out-of-tree builds).
#
# See: proj-dot-ins-append and proj-dot-ins-process
#
set ::proj__Config(dot-in-files) [list]

#
# @proj-warn msg
#
# Emits a warning message to stderr. All args are appended with a
# space between each.
#
proc proj-warn {args} {
  show-notices
  puts stderr [join [list "WARNING:" \[ [proj-scope 1] \]: {*}$args] " "]
}


#
# Internal impl of [proj-fatal] and [proj-error]. It must be called
# using tailcall.
#
proc proj__faterr {failMode args} {
  show-notices
  set lvl 1
  while {"-up" eq [lindex $args 0]} {
    set args [lassign $args -]
    incr lvl
  }
  if {$failMode} {
    puts stderr [join [list "FATAL:" \[ [proj-scope $lvl] \]: {*}$args]]
    exit 1
  } else {
    error [join [list in \[ [proj-scope $lvl] \]: {*}$args]]
  }
}

#
# @proj-fatal ?-up...? msg...
#
# Emits an error message to stderr and exits with non-0. All args are
# appended with a space between each.
#
# The calling scope's name is used in the error message. To instead
# use the name of a call higher up in the stack, use -up once for each
# additional level.
#
proc proj-fatal {args} {
  tailcall proj__faterr 1 {*}$args
}

#
# @proj-error ?-up...? msg...
#
# Works like proj-fatal but uses [error] intead of [exit].
#
proc proj-error {args} {
  tailcall proj__faterr 0 {*}$args
}

#
# @proj-assert script ?message?
#
# Kind of like a C assert: if uplevel of [list expr $script] is false,
# a fatal error is triggered. The error message, by default, includes
# the body of the failed assertion, but if $msg is set then that is
# used instead.
#
proc proj-assert {script {msg ""}} {
  if {1 eq $::proj__Config(verbose-assert)} {
    msg-result [proj-bold "asserting: $script"]
  }
  if {![uplevel 1 [list expr $script]]} {
    if {"" eq $msg} {
      set msg $script
    }
    tailcall proj__faterr 1 "Assertion failed:" $msg
  }
}

#
# @proj-bold str
#
# If this function believes that the current console might support
# ANSI escape sequences then this returns $str wrapped in a sequence
# to bold that text, else it returns $str as-is.
#
proc proj-bold {args} {
  if {$::autosetup(iswin) || !$::proj__Config(isatty)} {
    return [join $args]
  }
  return "\033\[1m${args}\033\[0m"
}

#
# @proj-indented-notice ?-error? ?-notice? msg
#
# Takes a multi-line message and emits it with consistent indentation.
# It does not perform any line-wrapping of its own. Which output
# routine it uses depends on its flags, defaulting to msg-result.
# For -error and -notice it uses user-notice.
#
# If the -notice flag it used then it emits using [user-notice], which
# means its rendering will (A) go to stderr and (B) be delayed until
# the next time autosetup goes to output a message.
#
# If the -error flag is provided then it renders the message
# immediately to stderr and then exits.
#
# If neither -notice nor -error are used, the message will be sent to
# stdout without delay.
#
proc proj-indented-notice {args} {
  set fErr ""
  set outFunc "msg-result"
  while {[llength $args] > 1} {
    switch -exact -- [lindex $args 0] {
      -error  {
        set args [lassign $args fErr]
        set outFunc "user-notice"
      }
      -notice {
        set args [lassign $args -]
        set outFunc "user-notice"
      }
      default {
        break
      }
    }
  }
  set lines [split [join $args] \n]
  foreach line $lines {
    set line [string trimleft $line]
    if {"" eq $line} {
      $outFunc $line
    } else {
      $outFunc "    $line"
    }
  }
  if {"" ne $fErr} {
    show-notices
    exit 1
  }
}

#
# @proj-is-cross-compiling
#
# Returns 1 if cross-compiling, else 0.
#
proc proj-is-cross-compiling {} {
  expr {[get-define host] ne [get-define build]}
}

#
# @proj-strip-hash-comments value
#
# Expects to receive string input, which it splits on newlines, strips
# out any lines which begin with any number of whitespace followed by
# a '#', and returns a value containing the [append]ed results of each
# remaining line with a \n between each. It does not strip out
# comments which appear after the first non-whitespace character.
#
proc proj-strip-hash-comments {val} {
  set x {}
  foreach line [split $val \n] {
    if {![string match "#*" [string trimleft $line]]} {
      append x $line \n
    }
  }
  return $x
}

#
# @proj-cflags-without-werror
#
# Fetches [define $var], strips out any -Werror entries, and returns
# the new value. This is intended for temporarily stripping -Werror
# from CFLAGS or CPPFLAGS within the scope of a [define-push] block.
#
proc proj-cflags-without-werror {{var CFLAGS}} {
  set rv {}
  foreach f [get-define $var ""] {
    switch -exact -- $f {
      -Werror {}
      default { lappend rv $f }
    }
  }
  join $rv " "
}

#
# @proj-check-function-in-lib
#
# A proxy for cc-check-function-in-lib with the following differences:
#
# - Does not make any global changes to the LIBS define.
#
# - Strips out the -Werror flag from CFLAGS before running the test,
#   as these feature tests will often fail if -Werror is used.
#
# Returns the result of cc-check-function-in-lib (i.e. true or false).
# The resulting linker flags are stored in the [define] named
# lib_${function}.
#
proc proj-check-function-in-lib {function libs {otherlibs {}}} {
  set found 0
  define-push {LIBS CFLAGS} {
    #puts "CFLAGS before=[get-define CFLAGS]"
    define CFLAGS [proj-cflags-without-werror]
    #puts "CFLAGS after =[get-define CFLAGS]"
    set found [cc-check-function-in-lib $function $libs $otherlibs]
  }
  return $found
}

#
# @proj-search-for-header-dir ?-dirs LIST? ?-subdirs LIST? header
#
# Searches for $header in a combination of dirs and subdirs, specified
# by the -dirs {LIST} and -subdirs {LIST} flags (each of which have
# sane defaults). Returns either the first matching dir or an empty
# string.  The return value does not contain the filename part.
#
proc proj-search-for-header-dir {header args} {
  set subdirs {include}
  set dirs {/usr /usr/local /mingw}
# Debatable:
#  if {![proj-is-cross-compiling]} {
#    lappend dirs [get-define prefix]
#  }
  while {[llength $args]} {
    switch -exact -- [lindex $args 0] {
      -dirs     { set args [lassign $args - dirs] }
      -subdirs  { set args [lassign $args - subdirs] }
      default   {
        proj-error "Unhandled argument: $args"
      }
    }
  }
  foreach dir $dirs {
    foreach sub $subdirs {
      if {[file exists $dir/$sub/$header]} {
        return "$dir/$sub"
      }
    }
  }
  return ""
}

#
# @proj-find-executable-path ?-v? binaryName
#
# Works similarly to autosetup's [find-executable-path $binName] but:
#
# - If the first arg is -v, it's verbose about searching, else it's quiet.
#
# Returns the full path to the result or an empty string.
#
proc proj-find-executable-path {args} {
  set binName $args
  set verbose 0
  if {[lindex $args 0] eq "-v"} {
    set verbose 1
    set args [lassign $args - binName]
    msg-checking "Looking for $binName ... "
  }
  set check [find-executable-path $binName]
  if {$verbose} {
    if {"" eq $check} {
      msg-result "not found"
    } else {
      msg-result $check
    }
  }
  return $check
}

#
# @proj-bin-define binName ?defName?
#
# Uses [proj-find-executable-path $binName] to (verbosely) search for
# a binary, sets a define (see below) to the result, and returns the
# result (an empty string if not found).
#
# The define'd name is: If $defName is not empty, it is used as-is. If
# $defName is empty then "BIN_X" is used, where X is the upper-case
# form of $binName with any '-' characters replaced with '_'.
#
proc proj-bin-define {binName {defName {}}} {
  set check [proj-find-executable-path -v $binName]
  if {"" eq $defName} {
    set defName "BIN_[string toupper [string map {- _} $binName]]"
  }
  define $defName $check
  return $check
}

#
# @proj-first-bin-of bin...
#
# Looks for the first binary found of the names passed to this
# function.  If a match is found, the full path to that binary is
# returned, else "" is returned.
#
# Despite using cc-path-progs to do the search, this function clears
# any define'd name that function stores for the result (because the
# caller has no sensible way of knowing which [define] name it has
# unless they pass only a single argument).
#
proc proj-first-bin-of {args} {
  set rc ""
  foreach b $args {
    set u [string toupper $b]
    # Note that cc-path-progs defines $u to "false" if it finds no
    # match.
    if {[cc-path-progs $b]} {
      set rc [get-define $u]
    }
    undefine $u
    if {"" ne $rc} break
  }
  return $rc
}

#
# @proj-opt-was-provided key
#
# Returns 1 if the user specifically provided the given configure flag
# or if it was specifically set using proj-opt-set, else 0. This can
# be used to distinguish between options which have a default value
# and those which were explicitly provided by the user, even if the
# latter is done in a way which uses the default value.
#
# For example, with a configure flag defined like:
#
#   { foo-bar:=baz => {its help text} }
#
# This function will, when passed foo-bar, return 1 only if the user
# passes --foo-bar to configure, even if that invocation would resolve
# to the default value of baz. If the user does not explicitly pass in
# --foo-bar (with or without a value) then this returns 0.
#
# Calling [proj-opt-set] is, for purposes of the above, equivalent to
# explicitly passing in the flag.
#
# Note: unlike most functions which deal with configure --flags, this
# one does not validate that $key refers to a pre-defined flag. i.e.
# it accepts arbitrary keys, even those not defined via an [options]
# call. [proj-opt-set] manipulates the internal list of flags, such
# that new options set via that function will cause this function to
# return true. (That's an unintended and unavoidable side-effect, not
# specifically a feature which should be made use of.)
#
proc proj-opt-was-provided {key} {
  dict exists $::autosetup(optset) $key
}

#
# @proj-opt-set flag ?val?
#
# Force-set autosetup option $flag to $val. The value can be fetched
# later with [opt-val], [opt-bool], and friends.
#
# Returns $val.
#
proc proj-opt-set {flag {val 1}} {
  if {$flag ni $::autosetup(options)} {
    # We have to add this to autosetup(options) or else future calls
    # to [opt-bool $flag] will fail validation of $flag.
    lappend ::autosetup(options) $flag
  }
  dict set ::autosetup(optset) $flag $val
  return $val
}

#
# @proj-opt-exists flag
#
# Returns 1 if the given flag has been defined as a legal configure
# option, else returns 0. Options set via proj-opt-set "exist" for
# this purpose even if they were not defined via autosetup's
# [options] function.
#
proc proj-opt-exists {flag} {
  expr {$flag in $::autosetup(options)};
}

#
# @proj-val-truthy val
#
# Returns 1 if $val appears to be a truthy value, else returns
# 0. Truthy values are any of {1 on true yes enabled}
#
proc proj-val-truthy {val} {
  expr {$val in {1 on true yes enabled}}
}

#
# @proj-opt-truthy flag
#
# Returns 1 if [opt-val $flag] appears to be a truthy value or
# [opt-bool $flag] is true. See proj-val-truthy.
#
proc proj-opt-truthy {flag} {
  if {[proj-val-truthy [opt-val $flag]]} { return 1 }
  set rc 0
  catch {
    # opt-bool will throw if $flag is not a known boolean flag
    set rc [opt-bool $flag]
  }
  return $rc
}

#
# @proj-if-opt-truthy boolFlag thenScript ?elseScript?
#
# If [proj-opt-truthy $flag] is true, eval $then, else eval $else.
#
proc proj-if-opt-truthy {boolFlag thenScript {elseScript {}}} {
  if {[proj-opt-truthy $boolFlag]} {
    uplevel 1 $thenScript
  } else {
    uplevel 1 $elseScript
  }
}

#
# @proj-define-for-opt flag def ?msg? ?iftrue? ?iffalse?
#
# If [proj-opt-truthy $flag] then [define $def $iftrue] else [define
# $def $iffalse]. If $msg is not empty, output [msg-checking $msg] and
# a [msg-results ...] which corresponds to the result. Returns 1 if
# the opt-truthy check passes, else 0.
#
proc proj-define-for-opt {flag def {msg ""} {iftrue 1} {iffalse 0}} {
  if {"" ne $msg} {
    msg-checking "$msg "
  }
  set rcMsg ""
  set rc 0
  if {[proj-opt-truthy $flag]} {
    define $def $iftrue
    set rc 1
  } else {
    define $def $iffalse
  }
  switch -- [proj-val-truthy [get-define $def]] {
    0 { set rcMsg no }
    1 { set rcMsg yes }
  }
  if {"" ne $msg} {
    msg-result $rcMsg
  }
  return $rc
}

#
# @proj-opt-define-bool ?-v? optName defName ?descr?
#
# Checks [proj-opt-truthy $optName] and calls [define $defName X]
# where X is 0 for false and 1 for true. $descr is an optional
# [msg-checking] argument which defaults to $defName. Returns X.
#
# If args[0] is -v then the boolean semantics are inverted: if
# the option is set, it gets define'd to 0, else 1. Returns the
# define'd value.
#
proc proj-opt-define-bool {args} {
  set invert 0
  if {[lindex $args 0] eq "-v"} {
    incr invert
    lassign $args - optName defName descr
  } else {
    lassign $args optName defName descr
  }
  if {"" eq $descr} {
    set descr $defName
  }
  #puts "optName=$optName defName=$defName descr=$descr"
  set rc 0
  msg-checking "[join $descr] ... "
  set rc [proj-opt-truthy $optName]
  if {$invert} {
    set rc [expr {!$rc}]
  }
  msg-result [string map {0 no 1 yes} $rc]
  define $defName $rc
  return $rc
}

#
# @proj-check-module-loader
#
# Check for module-loading APIs (libdl/libltdl)...
#
# Looks for libltdl or dlopen(), the latter either in -ldl or built in
# to libc (as it is on some platforms). Returns 1 if found, else
# 0. Either way, it `define`'s:
#
#  - HAVE_LIBLTDL to 1 or 0 if libltdl is found/not found
#  - HAVE_LIBDL to 1 or 0 if dlopen() is found/not found
#  - LDFLAGS_MODULE_LOADER one of ("-lltdl", "-ldl", or ""), noting
#    that -ldl may legally be empty on some platforms even if
#    HAVE_LIBDL is true (indicating that dlopen() is available without
#    extra link flags). LDFLAGS_MODULE_LOADER also gets "-rdynamic" appended
#    to it because otherwise trying to open DLLs will result in undefined
#    symbol errors.
#
# Note that if it finds LIBLTDL it does not look for LIBDL, so will
# report only that is has LIBLTDL.
#
proc proj-check-module-loader {} {
  msg-checking "Looking for module-loader APIs... "
  if {99 ne [get-define LDFLAGS_MODULE_LOADER 99]} {
    if {1 eq [get-define HAVE_LIBLTDL 0]} {
      msg-result "(cached) libltdl"
      return 1
    } elseif {1 eq [get-define HAVE_LIBDL 0]} {
      msg-result "(cached) libdl"
      return 1
    }
    # else: wha???
  }
  set HAVE_LIBLTDL 0
  set HAVE_LIBDL 0
  set LDFLAGS_MODULE_LOADER ""
  set rc 0
  puts "" ;# cosmetic kludge for cc-check-XXX
  if {[cc-check-includes ltdl.h] && [cc-check-function-in-lib lt_dlopen ltdl]} {
    set HAVE_LIBLTDL 1
    set LDFLAGS_MODULE_LOADER "-lltdl -rdynamic"
    msg-result " - Got libltdl."
    set rc 1
  } elseif {[cc-with {-includes dlfcn.h} {
    cctest -link 1 -declare "extern char* dlerror(void);" -code "dlerror();"}]} {
    msg-result " - This system can use dlopen() without -ldl."
    set HAVE_LIBDL 1
    set LDFLAGS_MODULE_LOADER ""
    set rc 1
  } elseif {[cc-check-includes dlfcn.h]} {
    set HAVE_LIBDL 1
    set rc 1
    if {[cc-check-function-in-lib dlopen dl]} {
      msg-result " - dlopen() needs libdl."
      set LDFLAGS_MODULE_LOADER "-ldl -rdynamic"
    } else {
      msg-result " - dlopen() not found in libdl. Assuming dlopen() is built-in."
      set LDFLAGS_MODULE_LOADER "-rdynamic"
    }
  }
  define HAVE_LIBLTDL $HAVE_LIBLTDL
  define HAVE_LIBDL $HAVE_LIBDL
  define LDFLAGS_MODULE_LOADER $LDFLAGS_MODULE_LOADER
  return $rc
}

#
# @proj-no-check-module-loader
#
# Sets all flags which would be set by proj-check-module-loader to
# empty/falsy values, as if those checks had failed to find a module
# loader. Intended to be called in place of that function when
# a module loader is explicitly not desired.
#
proc proj-no-check-module-loader {} {
  define HAVE_LIBDL 0
  define HAVE_LIBLTDL 0
  define LDFLAGS_MODULE_LOADER ""
}

#
# @proj-file-content ?-trim? filename
#
# Opens the given file, reads all of its content, and returns it.  If
# the first arg is -trim, the contents of the file named by the second
# argument are trimmed before returning them.
#
proc proj-file-content {args} {
  set trim 0
  set fname $args
  if {"-trim" eq [lindex $args 0]} {
    set trim 1
    lassign $args - fname
  }
  set fp [open $fname rb]
  set rc [read $fp]
  close $fp
  if {$trim} { return [string trim $rc] }
  return $rc
}

#
# @proj-file-conent filename
#
# Returns the contents of the given file as an array of lines, with
# the EOL stripped from each input line.
#
proc proj-file-content-list {fname} {
  set fp [open $fname rb]
  set rc {}
  while { [gets $fp line] >= 0 } {
    lappend rc $line
  }
  close $fp
  return $rc
}

#
# @proj-file-write ?-ro? fname content
#
# Works like autosetup's [writefile] but explicitly uses binary mode
# to avoid EOL translation on Windows. If $fname already exists, it is
# overwritten, even if it's flagged as read-only.
#
proc proj-file-write {args} {
  if {"-ro" eq [lindex $args 0]} {
    lassign $args ro fname content
  } else {
    set ro ""
    lassign $args fname content
  }
  file delete -force -- $fname; # in case it's read-only
  set f [open $fname wb]
  puts -nonewline $f $content
  close $f
  if {"" ne $ro} {
    catch {
      exec chmod -w $fname
      #file attributes -w $fname; #jimtcl has no 'attributes'
    }
  }
}

#
# @proj-check-compile-commands ?-assume-for-clang? ?configFlag?
#
# Checks the compiler for compile_commands.json support. If
# $configFlag is not empty then it is assumed to be the name of an
# autosetup boolean config which controls whether to run/skip this
# check.
#
# If -assume-for-clang is provided and $configFlag is not empty and CC
# matches *clang* and no --$configFlag was explicitly provided to the
# configure script then behave as if --$configFlag had been provided.
# To disable that assumption, either don't pass -assume-for-clang or
# pass --$configFlag=0 to the configure script. (The reason for this
# behavior is that clang supports compile-commands but some other
# compilers report false positives with these tests.)
#
# Returns 1 if supported, else 0, and defines HAVE_COMPILE_COMMANDS to
# that value. Defines MAKE_COMPILATION_DB to "yes" if supported, "no"
# if not. The use of MAKE_COMPILATION_DB is deprecated/discouraged:
# HAVE_COMPILE_COMMANDS is preferred.
#
# ACHTUNG: this test has a long history of false positive results
# because of compilers reacting differently to the -MJ flag.  Because
# of this, it is recommended that this support be an opt-in feature,
# rather than an on-by-default default one. That is: in the
# configure script define the option as
# {--the-flag-name=0 => {Enable ....}}
#
proc proj-check-compile-commands {args} {
  set i 0
  set configFlag {}
  set fAssumeForClang 0
  set doAssume 0
  msg-checking "compile_commands.json support... "
  if {"-assume-for-clang" eq [lindex $args 0]} {
    lassign $args - configFlag
    incr fAssumeForClang
  } elseif {1 == [llength $args]} {
    lassign $args configFlag
  } else {
    proj-error "Invalid arguments"
  }
  if {1 == $fAssumeForClang && "" ne $configFlag} {
    if {[string match *clang* [get-define CC]]
        && ![proj-opt-was-provided $configFlag]
        && ![proj-opt-truthy $configFlag]} {
      proj-indented-notice [subst -nocommands -nobackslashes {
        CC appears to be clang, so assuming that --$configFlag is likely
        to work. To disable this assumption use --$configFlag=0.}]
      incr doAssume
    }
  }
  if {!$doAssume && "" ne $configFlag && ![proj-opt-truthy $configFlag]} {
    msg-result "check disabled. Use --${configFlag} to enable it."
    define HAVE_COMPILE_COMMANDS 0
    define MAKE_COMPILATION_DB no
    return 0
  } else {
    if {[cctest -lang c -cflags {/dev/null -MJ} -source {}]} {
      # This test reportedly incorrectly succeeds on one of
      # Martin G.'s older systems. drh also reports a false
      # positive on an unspecified older Mac system.
      msg-result "compiler supports -MJ. Assuming it's useful for compile_commands.json"
      define MAKE_COMPILATION_DB yes; # deprecated
      define HAVE_COMPILE_COMMANDS 1
      return 1
    } else {
      msg-result "compiler does not support compile_commands.json"
      define MAKE_COMPILATION_DB no
      define HAVE_COMPILE_COMMANDS 0
      return 0
    }
  }
}

#
# @proj-touch filename
#
# Runs the 'touch' external command on one or more files, ignoring any
# errors.
#
proc proj-touch {filename} {
  catch { exec touch {*}$filename }
}

#
# @proj-make-from-dot-in ?-touch? infile ?outfile?
#
# Uses [make-template] to create makefile(-like) file(s) $outfile from
# $infile but explicitly makes the output read-only, to avoid
# inadvertent editing (who, me?).
#
# If $outfile is empty then:
#
# - If $infile is a 2-element list, it is assumed to be an in/out pair,
#   and $outfile is set from the 2nd entry in that list. Else...
#
# - $outfile is set to $infile stripped of its extension.
#
# If the first argument is -touch then the generated file is touched
# to update its timestamp. This can be used as a workaround for
# cases where (A) autosetup does not update the file because it was
# not really modified and (B) the file *really* needs to be updated to
# please the build process.
#
# Failures when running chmod or touch are silently ignored.
#
proc proj-make-from-dot-in {args} {
  set fIn ""
  set fOut ""
  set touch 0
  if {[lindex $args 0] eq "-touch"} {
    set touch 1
    lassign $args - fIn fOut
  } else {
    lassign $args fIn fOut
  }
  if {"" eq $fOut} {
    if {[llength $fIn]>1} {
      lassign $fIn fIn fOut
    } else {
      set fOut [file rootname $fIn]
    }
  }
  #puts "filenames=$filename"
  if {[file exists $fOut]} {
    catch { exec chmod u+w $fOut }
  }
  #puts "making template: $fIn ==> $fOut"
  #define-push {top_srcdir} {
    #puts "--- $fIn $fOut top_srcdir=[get-define top_srcdir]"
    make-template $fIn $fOut
    #puts "--- $fIn $fOut top_srcdir=[get-define top_srcdir]"
    # make-template modifies top_srcdir
  #}
  if {$touch} {
    proj-touch $fOut
  }
  catch {
    exec chmod -w $fOut
    #file attributes -w $f; #jimtcl has no 'attributes'
  }
}

#
# @proj-check-profile-flag ?flagname?
#
# Checks for the boolean configure option named by $flagname. If set,
# it checks if $CC seems to refer to gcc. If it does (or appears to)
# then it defines CC_PROFILE_FLAG to "-pg" and returns 1, else it
# defines CC_PROFILE_FLAG to "" and returns 0.
#
# Note that the resulting flag must be added to both CFLAGS and
# LDFLAGS in order for binaries to be able to generate "gmon.out".  In
# order to avoid potential problems with escaping, space-containing
# tokens, and interfering with autosetup's use of these vars, this
# routine does not directly modify CFLAGS or LDFLAGS.
#
proc proj-check-profile-flag {{flagname profile}} {
  #puts "flagname=$flagname ?[proj-opt-truthy $flagname]?"
  if {[proj-opt-truthy $flagname]} {
    set CC [get-define CC]
    regsub {.*ccache *} $CC "" CC
    # ^^^ if CC="ccache gcc" then [exec] treats "ccache gcc" as a
    # single binary name and fails. So strip any leading ccache part
    # for this purpose.
    if { ![catch { exec $CC --version } msg]} {
      if {[string first gcc $CC] != -1} {
        define CC_PROFILE_FLAG "-pg"
        return 1
      }
    }
  }
  define CC_PROFILE_FLAG ""
  return 0
}

#
# @proj-looks-like-windows ?key?
#
# Returns 1 if this appears to be a Windows environment (MinGw,
# Cygwin, MSys), else returns 0. The optional argument is the name of
# an autosetup define which contains platform name info, defaulting to
# "host" (meaning, somewhat counterintuitively, the target system, not
# the current host). The other legal value is "build" (the build
# machine, i.e. the local host). If $key == "build" then some
# additional checks may be performed which are not applicable when
# $key == "host".
#
proc proj-looks-like-windows {{key host}} {
  global autosetup
  switch -glob -- [get-define $key] {
    *-*-ming* - *-*-cygwin - *-*-msys - *windows* {
      return 1
    }
  }
  if {$key eq "build"} {
    # These apply only to the local OS, not a cross-compilation target,
    # as the above check potentially can.
    if {$::autosetup(iswin)} { return 1 }
    if {[find-an-executable cygpath] ne "" || $::tcl_platform(os) eq "Windows NT"} {
      return 1
    }
  }
  return 0
}

#
# @proj-looks-like-mac ?key?
#
# Looks at either the 'host' (==compilation target platform) or
# 'build' (==the being-built-on platform) define value and returns if
# if that value seems to indicate that it represents a Mac platform,
# else returns 0.
#
proc proj-looks-like-mac {{key host}} {
  switch -glob -- [get-define $key] {
    *-*-darwin* {
      # https://sqlite.org/forum/forumpost/7b218c3c9f207646
      # There's at least one Linux out there which matches *apple*.
      return 1
    }
    default {
      return 0
    }
  }
}

#
# @proj-exe-extension
#
# Checks autosetup's "host" and "build" defines to see if the build
# host and target are Windows-esque (Cygwin, MinGW, MSys). If the
# build environment is then BUILD_EXEEXT is [define]'d to ".exe", else
# "". If the target, a.k.a. "host", is then TARGET_EXEEXT is
# [define]'d to ".exe", else "".
#
proc proj-exe-extension {} {
  set rH ""
  set rB ""
  if {[proj-looks-like-windows host]} {
    set rH ".exe"
  }
  if {[proj-looks-like-windows build]} {
    set rB ".exe"
  }
  define BUILD_EXEEXT $rB
  define TARGET_EXEEXT $rH
}

#
# @proj-dll-extension
#
# Works like proj-exe-extension except that it defines BUILD_DLLEXT
# and TARGET_DLLEXT to one of (.so, ,dll, .dylib).
#
# Trivia: for .dylib files, the linker needs the -dynamiclib flag
# instead of -shared.
#
proc proj-dll-extension {} {
  set inner {{key} {
    if {[proj-looks-like-mac $key]} {
      return ".dylib"
    }
    if {[proj-looks-like-windows $key]} {
      return ".dll"
    }
    return ".so"
  }}
  define BUILD_DLLEXT [apply $inner build]
  define TARGET_DLLEXT [apply $inner host]
}

#
# @proj-lib-extension
#
# Static-library counterpart of proj-dll-extension. Defines
# BUILD_LIBEXT and TARGET_LIBEXT to the conventional static library
# extension for the being-built-on resp. the target platform.
#
proc proj-lib-extension {} {
  set inner {{key} {
    switch -glob -- [get-define $key] {
      *-*-ming* - *-*-cygwin - *-*-msys {
        return ".a"
        # ^^^ this was ".lib" until 2025-02-07. See
        # https://sqlite.org/forum/forumpost/02db2d4240
      }
      default {
        return ".a"
      }
    }
  }}
  define BUILD_LIBEXT [apply $inner build]
  define TARGET_LIBEXT [apply $inner host]
}

#
# @proj-file-extensions
#
# Calls all of the proj-*-extension functions.
#
proc proj-file-extensions {} {
  proj-exe-extension
  proj-dll-extension
  proj-lib-extension
}

#
# @proj-affirm-files-exist ?-v? filename...
#
# Expects a list of file names. If any one of them does not exist in
# the filesystem, it fails fatally with an informative message.
# Returns the last file name it checks. If the first argument is -v
# then it emits msg-checking/msg-result messages for each file.
#
proc proj-affirm-files-exist {args} {
  set rc ""
  set verbose 0
  if {[lindex $args 0] eq "-v"} {
    set verbose 1
    set args [lrange $args 1 end]
  }
  foreach f $args {
    if {$verbose} { msg-checking "Looking for $f ... " }
    if {![file exists $f]} {
      user-error "not found: $f"
    }
    if {$verbose} { msg-result "" }
    set rc $f
  }
  return rc
}

#
# @proj-check-emsdk
#
# Emscripten is used for doing in-tree builds of web-based WASM stuff,
# as opposed to WASI-based WASM or WASM binaries we import from other
# places. This is only set up for Unix-style OSes and is untested
# anywhere but Linux. Requires that the --with-emsdk flag be
# registered with autosetup.
#
# It looks for the SDK in the location specified by --with-emsdk.
# Values of "" or "auto" mean to check for the environment var EMSDK
# (which gets set by the emsdk_env.sh script from the SDK) or that
# same var passed to configure.
#
# If the given directory is found, it expects to find emsdk_env.sh in
# that directory, as well as the emcc compiler somewhere under there.
#
# If the --with-emsdk[=DIR] flag is explicitly provided and the SDK is
# not found then a fatal error is generated, otherwise failure to find
# the SDK is not fatal.
#
# Defines the following:
#
# - HAVE_EMSDK = 0 or 1 (this function's return value)
# - EMSDK_HOME = "" or top dir of the emsdk
# - EMSDK_ENV_SH = "" or $EMSDK_HOME/emsdk_env.sh
# - BIN_EMCC = "" or $EMSDK_HOME/upstream/emscripten/emcc
#
# Returns 1 if EMSDK_ENV_SH is found, else 0.  If EMSDK_HOME is not empty
# but BIN_EMCC is then emcc was not found in the EMSDK_HOME, in which
# case we have to rely on the fact that sourcing $EMSDK_ENV_SH from a
# shell will add emcc to the $PATH.
#
proc proj-check-emsdk {} {
  set emsdkHome [opt-val with-emsdk]
  define EMSDK_HOME ""
  define EMSDK_ENV_SH ""
  define BIN_EMCC ""
  set hadValue [llength $emsdkHome]
  msg-checking "Emscripten SDK? "
  if {$emsdkHome in {"" "auto"}} {
    # Check the environment. $EMSDK gets set by sourcing emsdk_env.sh.
    set emsdkHome [get-env EMSDK ""]
  }
  set rc 0
  if {$emsdkHome ne ""} {
    define EMSDK_HOME $emsdkHome
    set emsdkEnv "$emsdkHome/emsdk_env.sh"
    if {[file exists $emsdkEnv]} {
      msg-result "$emsdkHome"
      define EMSDK_ENV_SH $emsdkEnv
      set rc 1
      set emcc "$emsdkHome/upstream/emscripten/emcc"
      if {[file exists $emcc]} {
        define BIN_EMCC $emcc
      }
    } else {
      msg-result "emsdk_env.sh not found in $emsdkHome"
    }
  } else {
    msg-result "not found"
  }
  if {$hadValue && 0 == $rc} {
    # Fail if it was explicitly requested but not found
    proj-fatal "Cannot find the Emscripten SDK"
  }
  define HAVE_EMSDK $rc
  return $rc
}

#
# @proj-cc-check-Wl-flag ?flag ?args??
#
# Checks whether the given linker flag (and optional arguments) can be
# passed from the compiler to the linker using one of these formats:
#
# - -Wl,flag[,arg1[,...argN]]
# - -Wl,flag -Wl,arg1 ...-Wl,argN
#
# If so, that flag string is returned, else an empty string is
# returned.
#
proc proj-cc-check-Wl-flag {args} {
  cc-with {-link 1} {
    # Try -Wl,flag,...args
    set fli "-Wl"
    foreach f $args { append fli ",$f" }
    if {[cc-check-flags $fli]} {
      return $fli
    }
    # Try -Wl,flag -Wl,arg1 ...-Wl,argN
    set fli ""
    foreach f $args { append fli "-Wl,$f " }
    if {[cc-check-flags $fli]} {
      return [string trim $fli]
    }
    return ""
  }
}

#
# @proj-check-rpath
#
# Tries various approaches to handling the -rpath link-time
# flag. Defines LDFLAGS_RPATH to that/those flag(s) or an empty
# string. Returns 1 if it finds an option, else 0.
#
# By default, the rpath is set to $prefix/lib. However, if either of
# --exec-prefix=... or --libdir=...  are explicitly passed to
# configure then [get-define libdir] is used (noting that it derives
# from exec-prefix by default).
#
proc proj-check-rpath {} {
  if {[proj-opt-was-provided libdir]
      || [proj-opt-was-provided exec-prefix]} {
    set lp "[get-define libdir]"
  } else {
    set lp "[get-define prefix]/lib"
  }
  # If we _don't_ use cc-with {} here (to avoid updating the global
  # CFLAGS or LIBS or whatever it is that cc-check-flags updates) then
  # downstream tests may fail because the resulting rpath gets
  # implicitly injected into them.
  cc-with {-link 1} {
    if {[cc-check-flags "-rpath $lp"]} {
      define LDFLAGS_RPATH "-rpath $lp"
    } else {
      set wl [proj-cc-check-Wl-flag -rpath $lp]
      if {"" eq $wl} {
        set wl [proj-cc-check-Wl-flag -R$lp]
      }
      if {"" eq $wl} {
        # HP-UX: https://sqlite.org/forum/forumpost/d80ecdaddd
        set wl [proj-cc-check-Wl-flag +b $lp]
      }
      define LDFLAGS_RPATH $wl
    }
  }
  expr {"" ne [get-define LDFLAGS_RPATH]}
}

#
# @proj-check-soname ?libname?
#
# Checks whether CC supports the -Wl,-soname,lib... flag. If so, it
# returns 1 and defines LDFLAGS_SONAME_PREFIX to the flag's prefix, to
# which the client would need to append "libwhatever.N".  If not, it
# returns 0 and defines LDFLAGS_SONAME_PREFIX to an empty string.
#
# The libname argument is only for purposes of running the flag
# compatibility test, and is not included in the resulting
# LDFLAGS_SONAME_PREFIX. It is provided so that clients may
# potentially avoid some end-user confusion by using their own lib's
# name here (which shows up in the "checking..." output).
#
proc proj-check-soname {{libname "libfoo.so.0"}} {
  cc-with {-link 1} {
    if {[cc-check-flags "-Wl,-soname,${libname}"]} {
      define LDFLAGS_SONAME_PREFIX "-Wl,-soname,"
      return 1
    } elseif {[cc-check-flags "-Wl,+h,${libname}"]} {
      # HP-UX: https://sqlite.org/forum/forumpost/d80ecdaddd
      define LDFLAGS_SONAME_PREFIX "-Wl,+h,"
      return 1
    } else {
      define LDFLAGS_SONAME_PREFIX ""
      return 0
    }
  }
}

#
# @proj-check-fsanitize ?list-of-opts?
#
# Checks whether CC supports -fsanitize=X, where X is each entry of
# the given list of flags. If any of those flags are supported, it
# returns the string "-fsanitize=X..." where X... is a comma-separated
# list of all flags from the original set which are supported. If none
# of the given options are supported then it returns an empty string.
#
# Example:
#
#  set f [proj-check-fsanitize {address bounds-check just-testing}]
#
# Will, on many systems, resolve to "-fsanitize=address,bounds-check",
# but may also resolve to "-fsanitize=address".
#
proc proj-check-fsanitize {{opts {address bounds-strict}}} {
  set sup {}
  foreach opt $opts {
    # -nooutput is used because -fsanitize=hwaddress will otherwise
    # pass this test on x86_64, but then warn at build time that
    # "hwaddress is not supported for this target".
    cc-with {-nooutput 1} {
      if {[cc-check-flags "-fsanitize=$opt"]} {
        lappend sup $opt
      }
    }
  }
  if {[llength $sup] > 0} {
    return "-fsanitize=[join $sup ,]"
  }
  return ""
}

#
# Internal helper for proj-dump-defs-json. Expects to be passed a
# [define] name and the variadic $args which are passed to
# proj-dump-defs-json. If it finds a pattern match for the given
# $name in the various $args, it returns the type flag for that $name,
# e.g. "-str" or "-bare", else returns an empty string.
#
proc proj-defs-type_ {name spec} {
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
# Internal helper for proj-defs-format_: returns a JSON-ish quoted
# form of the given string-type values. It only performs the most
# basic of escaping. The input must not contain any control
# characters.
#
proc proj-quote-str_ {value} {
  return \"[string map [list \\ \\\\ \" \\\"] $value]\"
}

#
# An internal impl detail of proj-dump-defs-json. Requires a data
# type specifier, as used by make-config-header, and a value. Returns
# the formatted value or the value $::proj__Config(defs-skip) if the caller
# should skip emitting that value.
#
set ::proj__Config(defs-skip) "-proj-defs-format_ sentinel"
proc proj-defs-format_ {type value} {
  switch -exact -- $type {
    -bare {
      # Just output the value unchanged
    }
    -none {
      set value $::proj__Config(defs-skip)
    }
    -str {
      set value [proj-quote-str_ $value]
    }
    -auto {
      # Automatically determine the type
      if {![string is integer -strict $value]} {
        set value [proj-quote-str_ $value]
      }
    }
    -array {
      set ar {}
      foreach v $value {
        set v [proj-defs-format_ -auto $v]
        if {$::proj__Config(defs-skip) ne $v} {
          lappend ar $v
        }
      }
      set value "\[ [join $ar {, }] \]"
    }
    "" {
      set value $::proj__Config(defs-skip)
    }
    default {
      proj-fatal "Unknown type in proj-dump-defs-json: $type"
    }
  }
  return $value
}

#
# @proj-dump-defs-json outfile ...flags
#
# This function works almost identically to autosetup's
# make-config-header but emits its output in JSON form. It is not a
# fully-functional JSON emitter, and will emit broken JSON for
# complicated outputs, but should be sufficient for purposes of
# emitting most configure vars (numbers and simple strings).
#
# In addition to the formatting flags supported by make-config-header,
# it also supports:
#
#  -array {patterns...}
#
# Any defines matching the given patterns will be treated as a list of
# values, each of which will be formatted as if it were in an -auto {...}
# set, and the define will be emitted to JSON in the form:
#
#  "ITS_NAME": [ "value1", ...valueN ]
#
# Achtung: if a given -array pattern contains values which themselves
# contains spaces...
#
#   define-append foo {"-DFOO=bar baz" -DBAR="baz barre"}
#
# will lead to:
#
#  ["-DFOO=bar baz", "-DBAR=\"baz", "barre\""]
#
# Neither is especially satisfactory (and the second is useless), and
# handling of such values is subject to change if any such values ever
# _really_ need to be processed by our source trees.
#
proc proj-dump-defs-json {file args} {
  file mkdir [file dirname $file]
  set lines {}
  lappend args -bare {SIZEOF_* HAVE_DECL_*} -auto HAVE_*
  foreach n [lsort [dict keys [all-defines]]] {
    set type [proj-defs-type_ $n $args]
    set value [proj-defs-format_ $type [get-define $n]]
    if {$::proj__Config(defs-skip) ne $value} {
      lappend lines "\"$n\": ${value}"
    }
  }
  set buf {}
  lappend buf [join $lines ",\n"]
  write-if-changed $file $buf {
    msg-result "Created $file"
  }
}

#
# @proj-xfer-option-aliases map
#
# Expects a list of pairs of configure flags which have been
# registered with autosetup, in this form:
#
#  { alias1 => canonical1
#    aliasN => canonicalN ... }
#
# The names must not have their leading -- part and must be in the
# form which autosetup will expect for passing to [opt-val NAME] and
# friends.
#
# Comment lines are permitted in the input.
#
# For each pair of ALIAS and CANONICAL, if --ALIAS is provided but
# --CANONICAL is not, the value of the former is copied to the
# latter. If --ALIAS is not provided, this is a no-op. If both have
# explicitly been provided a fatal usage error is triggered.
#
# Motivation: autosetup enables "hidden aliases" in [options] lists,
# and elides the aliases from --help output but does no further
# handling of them. For example, when --alias is a hidden alias of
# --canonical and a user passes --alias=X, [opt-val canonical] returns
# no value. i.e. the script must check both [opt-val alias] and
# [opt-val canonical].  The intent here is that this function be
# passed such mappings immediately after [options] is called, to carry
# over any values from hidden aliases into their canonical names, such
# that [opt-value canonical] will return X if --alias=X is passed to
# configure.
#
# That said: autosetup's [opt-str] does support alias forms, but it
# requires that the caller know all possible aliases. It's simpler, in
# terms of options handling, if there's only a single canonical name
# which each down-stream call of [opt-...] has to know.
#
proc proj-xfer-options-aliases {mapping} {
  foreach {hidden - canonical} [proj-strip-hash-comments $mapping] {
    if {[proj-opt-was-provided $hidden]} {
      if {[proj-opt-was-provided $canonical]} {
        proj-fatal "both --$canonical and its alias --$hidden were used. Use only one or the other."
      } else {
        proj-opt-set $canonical [opt-val $hidden]
      }
    }
  }
}

#
# Arguable/debatable...
#
# When _not_ cross-compiling and CC_FOR_BUILD is _not_ explicitly
# specified, force CC_FOR_BUILD to be the same as CC, so that:
#
# ./configure CC=clang
#
# will use CC_FOR_BUILD=clang, instead of cc, for building in-tree
# tools. This is based off of an email discussion and is thought to
# be likely to cause less confusion than seeing 'cc' invocations
# when when the user passes CC=clang.
#
# Sidebar: if we do this before the cc package is installed, it gets
# reverted by that package. Ergo, the cc package init will tell the
# user "Build C compiler...cc" shortly before we tell them otherwise.
#
proc proj-redefine-cc-for-build {} {
  if {![proj-is-cross-compiling]
      && [get-define CC] ne [get-define CC_FOR_BUILD]
      && "nope" eq [get-env CC_FOR_BUILD "nope"]} {
    user-notice "Re-defining CC_FOR_BUILD to CC=[get-define CC]. To avoid this, explicitly pass CC_FOR_BUILD=..."
    define CC_FOR_BUILD [get-define CC]
  }
}

#
# @proj-which-linenoise headerFile
#
# Attempts to determine whether the given linenoise header file is of
# the "antirez" or "msteveb" flavor. It returns 2 for msteveb, else 1
# (it does not validate that the header otherwise contains the
# linenoise API).
#
proc proj-which-linenoise {dotH} {
  set srcHeader [proj-file-content $dotH]
  if {[string match *userdata* $srcHeader]} {
    return 2
  } else {
    return 1
  }
}

#
# @proj-remap-autoconf-dir-vars
#
# "Re-map" the autoconf-conventional --XYZdir flags into something
# which is more easily overridable from a make invocation.
#
# Based off of notes in <https://sqlite.org/forum/forumpost/00d12a41f7>.
#
# Consider:
#
# $ ./configure --prefix=/foo
# $ make install prefix=/blah
#
# In that make invocation, $(libdir) would, at make-time, normally be
# hard-coded to /foo/lib, rather than /blah/lib. That happens because
# autosetup exports conventional $prefix-based values for the numerous
# autoconfig-compatible XYZdir vars at configure-time.  What we would
# normally want, however, is that --libdir derives from the make-time
# $(prefix).  The distinction between configure-time and make-time is
# the significant factor there.
#
# This function attempts to reconcile those vars in such a way that
# they will derive, at make-time, from $(prefix) in a conventional
# manner unless they are explicitly overridden at configure-time, in
# which case those overrides takes precedence.
#
# Each autoconf-relvant --XYZ flag which is explicitly passed to
# configure is exported as-is, as are those which default to some
# top-level system directory, e.g. /etc or /var.  All which derive
# from either $prefix or $exec_prefix are exported in the form of a
# Makefile var reference, e.g.  libdir=${exec_prefix}/lib. Ergo, if
# --exec-prefix=FOO is passed to configure, libdir will still derive,
# at make-time, from whatever exec_prefix is passed to make, and will
# use FOO if exec_prefix is not overridden at make-time.  Without this
# post-processing, libdir would be cemented in as FOO/lib at
# configure-time, so could be tedious to override properly via a make
# invocation.
#
proc proj-remap-autoconf-dir-vars {} {
  set prefix [get-define prefix]
  set exec_prefix [get-define exec_prefix $prefix]
  # The following var derefs must be formulated such that they are
  # legal for use in (A) makefiles, (B) pkgconfig files, and (C) TCL's
  # [subst] command.  i.e. they must use the form ${X}.
  foreach {flag makeVar makeDeref} {
    exec-prefix     exec_prefix    ${prefix}
    datadir         datadir        ${prefix}/share
    mandir          mandir         ${datadir}/man
    includedir      includedir     ${prefix}/include
    bindir          bindir         ${exec_prefix}/bin
    libdir          libdir         ${exec_prefix}/lib
    sbindir         sbindir        ${exec_prefix}/sbin
    sysconfdir      sysconfdir     /etc
    sharedstatedir  sharedstatedir ${prefix}/com
    localstatedir   localstatedir  /var
    runstatedir     runstatedir    /run
    infodir         infodir        ${datadir}/info
    libexecdir      libexecdir     ${exec_prefix}/libexec
  } {
    if {[proj-opt-was-provided $flag]} {
      define $makeVar [join [opt-val $flag]]
    } else {
      define $makeVar [join $makeDeref]
    }
    # Maintenance reminder: the [join] call is to avoid {braces}
    # around the output when someone passes in,
    # e.g. --libdir=\${prefix}/foo/bar. Debian's SQLite package build
    # script does that.
  }
}

#
# @proj-env-file flag ?default?
#
# If a file named .env-$flag exists, this function returns a
# trimmed copy of its contents, else it returns $dflt. The intended
# usage is that things like developer-specific CFLAGS preferences can
# be stored in .env-CFLAGS.
#
proc proj-env-file {flag {dflt ""}} {
  set fn ".env-${flag}"
  if {[file readable $fn]} {
    return [proj-file-content -trim $fn]
  }
  return $dflt
}

#
# @proj-get-env var ?default?
#
# Extracts the value of "environment" variable $var from the first of
# the following places where it's defined:
#
# - Passed to configure as $var=...
# - Exists as an environment variable
# - A file named .env-$var (see [proj-env-file])
#
# If none of those are set, $dflt is returned.
#
proc proj-get-env {var {dflt ""}} {
  get-env $var [proj-env-file $var $dflt]
}

#
# @proj-scope ?lvl?
#
# Returns the name of the _calling_ proc from ($lvl + 1) levels up the
# call stack (where the caller's level will be 1 up from _this_
# call). If $lvl would resolve to global scope "global scope" is
# returned and if it would be negative then a string indicating such
# is returned (as opposed to throwing an error).
#
proc proj-scope {{lvl 0}} {
  #uplevel [expr {$lvl + 1}] {lindex [info level 0] 0}
  set ilvl [info level]
  set offset [expr {$ilvl  - $lvl - 1}]
  if { $offset < 0} {
    return "invalid scope ($offset)"
  } elseif { $offset == 0} {
    return "global scope"
  } else {
    return [lindex [info level $offset] 0]
  }
}

#
# Deprecated name of [proj-scope].
#
proc proj-current-scope {{lvl 0}} {
  puts stderr \
    "Deprecated proj-current-scope called from [proj-scope 1]. Use proj-scope instead."
  proj-scope [incr lvl]
}

#
# Converts parts of tclConfig.sh to autosetup [define]s.
#
# Expects to be passed the name of a value tclConfig.sh or an empty
# string.  It converts certain parts of that file's contents to
# [define]s (see the code for the whole list). If $tclConfigSh is an
# empty string then it [define]s the various vars as empty strings.
#
proc proj-tclConfig-sh-to-autosetup {tclConfigSh} {
  set shBody {}
  set tclVars {
    TCL_INCLUDE_SPEC
    TCL_LIBS
    TCL_LIB_SPEC
    TCL_STUB_LIB_SPEC
    TCL_EXEC_PREFIX
    TCL_PREFIX
    TCL_VERSION
    TCL_MAJOR_VERSION
    TCL_MINOR_VERSION
    TCL_PACKAGE_PATH
    TCL_PATCH_LEVEL
    TCL_SHLIB_SUFFIX
  }
  # Build a small shell script which proxies the $tclVars from
  # $tclConfigSh into autosetup code...
  lappend shBody "if test x = \"x${tclConfigSh}\"; then"
  foreach v $tclVars {
    lappend shBody "$v= ;"
  }
  lappend shBody "else . \"${tclConfigSh}\"; fi"
  foreach v $tclVars {
    lappend shBody "echo define $v {\$$v} ;"
  }
  lappend shBody "exit"
  set shBody [join $shBody "\n"]
  #puts "shBody=$shBody\n"; exit
  eval [exec echo $shBody | sh]
}

#
# @proj-tweak-default-env-dirs
#
# This function is not useful before [use system] is called to set up
# --prefix and friends. It should be called as soon after [use system]
# as feasible.
#
# For certain target environments, if --prefix is _not_ passed in by
# the user, set the prefix to an environment-specific default. For
# such environments its does [define prefix ...]  and [proj-opt-set
# prefix ...], but it does not process vars derived from the prefix,
# e.g. exec-prefix. To do so it is generally necessary to also call
# proj-remap-autoconf-dir-vars late in the config process (immediately
# before ".in" files are filtered).
#
# Similar modifications may be made for --mandir.
#
# Returns >0 if it modifies the environment, else 0.
#
proc proj-tweak-default-env-dirs {} {
  set rc 0
  switch -glob -- [get-define host] {
    *-haiku {
      if {![proj-opt-was-provided prefix]} {
        set hdir /boot/home/config/non-packaged
        proj-opt-set prefix $hdir
        define prefix $hdir
        incr rc
      }
      if {![proj-opt-was-provided mandir]} {
        set hdir /boot/system/documentation/man
        proj-opt-set mandir $hdir
        define mandir $hdir
        incr rc
      }
    }
  }
  return $rc
}

#
# @proj-dot-ins-append file ?fileOut ?postProcessScript??
#
# Queues up an autosetup [make-template]-style file to be processed
# at a later time using [proj-dot-ins-process].
#
# $file is the input file. If $fileOut is empty then this function
# derives $fileOut from $file, stripping both its directory and
# extension parts. i.e. it defaults to writing the output to the
# current directory (typically $::autosetup(builddir)).
#
# If $postProcessScript is not empty then, during
# [proj-dot-ins-process], it will be eval'd immediately after
# processing the file. In the context of that script, the vars
# $dotInsIn and $dotInsOut will be set to the input and output file
# names.  This can be used, for example, to make the output file
# executable or perform validation on its contents:
#
##  proj-dot-ins-append my.sh.in my.sh {
##    catch {exec chmod u+x $dotInsOut}
##  }
#
# See [proj-dot-ins-process], [proj-dot-ins-list]
#
proc proj-dot-ins-append {fileIn args} {
  set srcdir $::autosetup(srcdir)
  switch -exact -- [llength $args] {
    0 {
      lappend fileIn [file rootname [file tail $fileIn]] ""
    }
    1 {
      lappend fileIn [join $args] ""
    }
    2 {
      lappend fileIn {*}$args
    }
    default {
      proj-fatal "Too many arguments: $fileIn $args"
    }
  }
  #puts "******* [proj-scope]: adding [llength $fileIn]-length item: $fileIn"
  lappend ::proj__Config(dot-in-files) $fileIn
}

#
# @proj-dot-ins-list
#
# Returns the current list of [proj-dot-ins-append]'d files, noting
# that each entry is a 3-element list of (inputFileName,
# outputFileName, postProcessScript).
#
proc proj-dot-ins-list {} {
  return $::proj__Config(dot-in-files)
}

#
# @proj-dot-ins-process ?-touch? ?-validate? ?-clear?
#
# Each file which has previously been passed to [proj-dot-ins-append]
# is processed, with its passing its in-file out-file names to
# [proj-make-from-dot-in].
#
# The intent is that a project accumulate any number of files to
# filter and delay their actual filtering until the last stage of the
# configure script, calling this function at that time.
#
# Optional flags:
#
# -touch: gets passed on to [proj-make-from-dot-in]
#
# -validate: after processing each file, before running the file's
#  associated script, if any, it runs the file through
#  proj-validate-no-unresolved-ats, erroring out if that does.
#
# -clear: after processing, empty the dot-ins list. This effectively
#  makes proj-dot-ins-append available for re-use.
#
proc proj-dot-ins-process {args} {
  proj-parse-flags args flags {
    -touch   "" {return "-touch"}
    -clear    0 {expr 1}
    -validate 0 {expr 1}
  }
  #puts "args=$args"; parray flags
  if {[llength $args] > 0} {
    error "Invalid argument to [proj-scope]: $args"
  }
  foreach f $::proj__Config(dot-in-files) {
    proj-assert {3==[llength $f]} \
      "Expecting proj-dot-ins-list to be stored in 3-entry lists. Got: $f"
    lassign $f fIn fOut fScript
    #puts "DOING $fIn  ==> $fOut"
    proj-make-from-dot-in {*}$flags(-touch) $fIn $fOut
    if {$flags(-validate)} {
      proj-validate-no-unresolved-ats $fOut
    }
    if {"" ne $fScript} {
      uplevel 1 [join [list set dotInsIn $fIn \; \
                         set dotInsOut $fOut \; \
                         eval \{${fScript}\} \; \
                         unset dotInsIn dotInsOut]]
    }
  }
  if {$flags(-clear)} {
    set ::proj__Config(dot-in-files) [list]
  }
}

#
# @proj-validate-no-unresolved-ats filenames...
#
# For each filename given to it, it validates that the file has no
# unresolved @VAR@ references. If it finds any, it produces an error
# with location information.
#
# Exception: if a filename matches the pattern {*[Mm]ake*} AND a given
# line begins with a # (not including leading whitespace) then that
# line is ignored for purposes of this validation. The intent is that
# @VAR@ inside of makefile comments should not (necessarily) cause
# validation to fail, as it's sometimes convenient to comment out
# sections during development of a configure script and its
# corresponding makefile(s).
#
proc proj-validate-no-unresolved-ats {args} {
  foreach f $args {
    set lnno 1
    set isMake [string match {*[Mm]ake*} $f]
    foreach line [proj-file-content-list $f] {
      if {!$isMake || ![string match "#*" [string trimleft $line]]} {
        if {[regexp {(@[A-Za-z0-9_\.]+@)} $line match]} {
          error "Unresolved reference to $match at line $lnno of $f"
        }
      }
      incr lnno
    }
  }
}

#
# @proj-first-file-found tgtVar fileList
#
# Searches $fileList for an existing file. If one is found, its name
# is assigned to tgtVar and 1 is returned, else tgtVar is set to ""
# and 0 is returned.
#
proc proj-first-file-found {tgtVar fileList} {
  upvar $tgtVar tgt
  foreach f $fileList {
    if {[file exists $f]} {
      set tgt $f
      return 1
    }
  }
  set tgt ""
  return 0
}

#
# Defines $defName to contain makefile recipe commands for re-running
# the configure script with its current set of $::argv flags.  This
# can be used to automatically reconfigure.
#
proc proj-setup-autoreconfig {defName} {
  define $defName \
    [join [list \
             cd \"$::autosetup(builddir)\" \
             && [get-define AUTOREMAKE "error - missing @AUTOREMAKE@"]]]
}

#
# @prop-append-to defineName args...
#
# A proxy for Autosetup's [define-append]. Appends all non-empty $args
# to [define-append $defineName].
#
proc proj-define-append {defineName args} {
  foreach a $args {
    if {"" ne $a} {
      define-append $defineName {*}$a
    }
  }
}

#
# @prod-define-amend ?-p|-prepend? ?-d|-define? defineName args...
#
# A proxy for Autosetup's [define-append].
#
# Appends all non-empty $args to the define named by $defineName.  If
# one of (-p | -prepend) are used it instead prepends them, in their
# given order, to $defineName.
#
# If -define is used then each argument is assumed to be a [define]'d
# flag and [get-define X ""] is used to fetch it.
#
# Re. linker flags: typically, -lXYZ flags need to be in "reverse"
# order, with each -lY resolving symbols for -lX's to its left. This
# order is largely historical, and not relevant on all environments,
# but it is technically correct and still relevant on some
# environments.
#
# See: proj-append-to
#
proc proj-define-amend {args} {
  set defName ""
  set prepend 0
  set isdefs 0
  set xargs [list]
  foreach arg $args {
    switch -exact -- $arg {
      "" {}
      -p - -prepend { incr prepend }
      -d - -define  { incr isdefs }
      default {
        if {"" eq $defName} {
          set defName $arg
        } else {
          lappend xargs $arg
        }
      }
    }
  }
  if {"" eq $defName} {
    proj-error "Missing defineName argument in call from [proj-scope 1]"
  }
  if {$isdefs} {
    set args $xargs
    set xargs [list]
    foreach arg $args {
      lappend xargs [get-define $arg ""]
    }
    set args $xargs
  }
#  puts "**** args=$args"
#  puts "**** xargs=$xargs"

  set args $xargs
  if {$prepend} {
    lappend args {*}[get-define $defName ""]
    define $defName [join $args]; # join to eliminate {} entries
  } else {
    proj-define-append $defName {*}$args
  }
}

#
# @proj-define-to-cflag ?-list? ?-quote? ?-zero-undef? defineName...
#
# Treat each argument as the name of a [define] and renders it like a
# CFLAGS value in one of the following forms:
#
#  -D$name
#  -D$name=integer   (strict integer matches only)
#  '-D$name=value'   (without -quote)
#  '-D$name="value"' (with -quote)
#
# It treats integers as numbers and everything else as a quoted
# string, noting that it does not handle strings which themselves
# contain quotes.
#
# The -zero-undef flag causes no -D to be emitted for integer values
# of 0.
#
# By default it returns the result as string of all -D... flags,
# but if passed the -list flag it will return a list of the
# individual CFLAGS.
#
proc proj-define-to-cflag {args} {
  set rv {}
  proj-parse-flags args flags {
    -list       0 {expr 1}
    -quote      0 {expr 1}
    -zero-undef 0 {expr 1}
  }
  foreach d $args {
    set v [get-define $d ""]
    set li {}
    if {"" eq $d} {
      set v "-D${d}"
    } elseif {[string is integer -strict $v]} {
      if {!$flags(-zero-undef) || $v ne "0"} {
        set v "-D${d}=$v"
      }
    } elseif {$flags(-quote)} {
      set v "'-D${d}=\"$v\"'"
    } else {
      set v "'-D${d}=$v'"
    }
    lappend rv $v
  }
  expr {$flags(-list) ? $rv : [join $rv]}
}


if {0} {
  # Turns out that autosetup's [options-add] essentially does exactly
  # this...

  # A list of lists of Autosetup [options]-format --flags definitions.
  # Append to this using [proj-options-add] and use
  # [proj-options-combine] to merge them into a single list for passing
  # to [options].
  #
  set ::proj__Config(extra-options) {}

  # @proj-options-add list
  #
  # Adds a list of options to the pending --flag processing.  It must be
  # in the format used by Autosetup's [options] function.
  #
  # This will have no useful effect if called from after [options]
  # is called.
  #
  # Use [proj-options-combine] to get a combined list of all added
  # options.
  #
  # PS: when writing this i wasn't aware of autosetup's [options-add],
  # works quite similarly. Only the timing is different.
  proc proj-options-add {list} {
    lappend ::proj__Config(extra-options) $list
  }

  # @proj-options-combine list1 ?...listN?
  #
  # Expects each argument to be a list of options compatible with
  # autosetup's [options] function. This function concatenates the
  # contents of each list into a new top-level list, stripping the outer
  # list part of each argument, and returning that list
  #
  # If passed no arguments, it uses the list generated by calls to
  # [proj-options-add].
  proc proj-options-combine {args} {
    set rv [list]
    if {0 == [llength $args]} {
      set args $::proj__Config(extra-options)
    }
    foreach e $args {
      lappend rv {*}$e
    }
    return $rv
  }
}; # proj-options-*

# Internal cache for use via proj-cache-*.
array set proj__Cache {}

#
# @proj-cache-key arg {addLevel 0}
#
# Helper to generate cache keys for [proj-cache-*].
#
# $addLevel should almost always be 0.
#
# Returns a cache key for the given argument:
#
#   integer: relative call stack levels to get the scope name of for
#   use as a key. [proj-scope [expr {1 + $arg + addLevel}]] is
#   then used to generate the key. i.e. the default of 0 uses the
#   calling scope's name as the key.
#
#   Anything else: returned as-is
#
proc proj-cache-key {arg {addLevel 0}} {
  if {[string is integer -strict $arg]} {
    return [proj-scope [expr {$arg + $addLevel + 1}]]
  }
  return $arg
}

#
# @proj-cache-set ?-key KEY? ?-level 0? value
#
# Sets a feature-check cache entry with the given key.
#
# See proj-cache-key for -key's and -level's semantics, noting that
# this function adds one to -level for purposes of that call.
proc proj-cache-set {args} {
  proj-parse-flags args flags {
    -key => 0
    -level => 0
  }
  lassign $args val
  set key [proj-cache-key $flags(-key) [expr {1 + $flags(-level)}]]
  #puts "** fcheck set $key = $val"
  set ::proj__Cache($key) $val
}

#
# @proj-cache-remove ?key? ?addLevel?
#
# Removes an entry from the proj-cache.
proc proj-cache-remove {{key 0} {addLevel 0}} {
  set key [proj-cache-key $key [expr {1 + $addLevel}]]
  set rv ""
  if {[info exists ::proj__Cache($key)]} {
    set rv $::proj__Cache($key)
    unset ::proj__Cache($key)
  }
  return $rv;
}

#
# @proj-cache-check ?-key KEY? ?-level LEVEL? tgtVarName
#
# Checks for a feature-check cache entry with the given key.
#
# If the feature-check cache has a matching entry then this function
# assigns its value to tgtVar and returns 1, else it assigns tgtVar to
# "" and returns 0.
#
# See proj-cache-key for $key's and $addLevel's semantics, noting that
# this function adds one to $addLevel for purposes of that call.
proc proj-cache-check {args} {
  proj-parse-flags args flags {
    -key => 0
    -level => 0
  }
  lassign $args tgtVar
  upvar $tgtVar tgt
  set rc 0
  set key [proj-cache-key $flags(-key) [expr {1 + $flags(-level)}]]
  #puts "** fcheck get key=$key"
  if {[info exists ::proj__Cache($key)]} {
    set tgt $::proj__Cache($key)
    incr rc
  } else {
    set tgt ""
  }
  return $rc
}

#
# @proj-coalesce ...args
#
# Returns the first argument which is not empty (eq ""), or an empty
# string on no match.
proc proj-coalesce {args} {
  foreach arg $args {
    if {"" ne $arg} {
      return $arg
    }
  }
  return ""
}

#
# @proj-parse-flags argvListName targetArrayName {prototype}
#
# A helper to parse flags from proc argument lists.
#
# The first argument is the name of a var holding the args to
# parse. It will be overwritten, possibly with a smaller list.
#
# The second argument is the name of an array variable to create in
# the caller's scope.
#
# The third argument, $prototype, is a description of how to handle
# the flags. Each entry in that list must be in one of the
# following forms:
#
#   -flag  defaultValue ?-literal|-call|-apply?
#                       script|number|incr|proc-name|{apply $aLambda}
#
#   -flag* ...as above...
#
#   -flag  => defaultValue ?-call proc-name-and-args|-apply lambdaExpr?
#
#   -flag* => ...as above...
#
#   :PRAGMA
#
# The first two forms represents a basic flag with no associated
# following argument. The third and fourth forms, called arg-consuming
# flags, extract the value from the following argument in $argvName
# (pneumonic: => points to the next argument.). The :PRAGMA form
# offers a way to configure certain aspects of this call.
#
# If $argv contains any given flag from $prototype, its default value
# is overridden depending on several factors:
#
#  - If the -literal flag is used, or the flag's script is a number,
#    value is used verbatim.
#
#  - Else if the -call flag is used, the argument must be a proc name
#    and any leading arguments, e.g. {apply $myLambda}.  The proc is passed
#    the (flag, value) as arguments (non-consuming flags will get
#    passed the flag's current/starting value and consuming flags will
#    get the next argument).  Its result becomes the result of the
#    flag.
#
#  - Else if -apply X is used, it's effectively shorthand for -call
#    {apply X}. Its argument may either be a $lambaRef or a {{f v}
#    {body}} construct.
#
#  - Else if $script is one of the following values, it is treated as
#    the result of...
#
#    - incr: increments the current value of the flag.
#
#  - Else $script is eval'd to get its result value. That result
#    becomes the new flag value for $tgtArrayName(-flag). This
#    function intercepts [return $val] from eval'ing $script.  Any
#    empty script will result in the flag having "" assigned to it.
#
# Unless the -flag has a trailing asterisk, e.g. -flag*, this function
# assumes that each flag is unique, and using a flag more than once
# causes an error to be triggered. the -flag* forms works similarly
# except that may appear in $argv any number of times:
#
#  - For non-arg-consuming flags, each invocation of -flag causes the
#    result of $script to overwrite the previous value. e.g. so
#    {-flag* {x} {incr foo}} has a default value of x, but passing in
#    -flag twice would change it to the result of incrementing foo
#    twice. This form can be used to implement, e.g., increasing
#    verbosity levels by passing -verbose multiple times.
#
#  - For arg-consuming flags, the given flag starts with value X, but
#    if the flag is provided in $argv, the default is cleared, then
#    each instance of -flag causes its value to be appended to the
#    result, so {-flag* => {a b c}} defaults to {a b c}, but passing
#    in -flag y -flag z would change it to {y z}, not {a b c y z}..
#
# By default, the args list is only inspected until the first argument
# which is not described by $prototype. i.e. the first "non-flag" (not
# counting values consumed for flags defined like -flag => default).
# The :all-flags pragma (see below) can modify this behavior.
#
# If a "--" flag is encountered, no more arguments are inspected as
# flags unless the :all-flags pragma (see below) is in effect. The
# first instance of "--" is removed from the target result list but
# all remaining instances of "--" are are passed through.
#
# Any argvName entries not described in $prototype are considered to
# be "non-flags" for purposes of this function, even if they
# ostensibly look like flags.
#
# Returns the number of flags it processed in $argvName, not counting
# "--".
#
# Example:
#
## set args [list -foo -bar {blah} -z 8 9 10 -theEnd]
## proj-parse-flags args flags {
##   -foo    0  {expr 1}
##   -bar    => 0
##   -no-baz 1  {return 0}
##   -z 0 2
## }
#
# After that $flags would contain {-foo 1 -bar {blah} -no-baz 1 -z 2}
# and $args would be {8 9 10 -theEnd}.
#
# Pragmas:
#
# Passing :PRAGMAS to this function may modify how it works. The
# following pragmas are supported (note the leading ":"):
#
#   :all-flags indicates that the whole input list should be scanned,
#   not stopping at the first non-flag or "--".
#
proc proj-parse-flags {argvName tgtArrayName prototype} {
  upvar $argvName argv
  upvar $tgtArrayName outFlags
  array set flags {}; # staging area
  array set blob {}; # holds markers for various per-key state and options
  set incrSkip 1; # 1 if we stop at the first non-flag, else 0
  # Parse $prototype for flag definitions...
  set n [llength $prototype]
  set checkProtoFlag {
    #puts "**** checkProtoFlag #$i of $n k=$k fv=$fv"
    switch -exact -- $fv {
      -literal {
        proj-assert {![info exists blob(${k}.consumes)]}
        set blob(${k}.script) [list expr [lindex $prototype [incr i]]]
      }
      -apply {
        set fv [lindex $prototype [incr i]]
        if {2 == [llength $fv]} {
          # Treat this as a lambda literal
          set fv [list $fv]
        }
        lappend blob(${k}.call) "apply $fv"
      }
      -call {
        # arg is either a proc name or {apply $aLambda}
        set fv [lindex $prototype [incr i]]
        lappend blob(${k}.call) $fv
      }
      default {
        proj-assert {![info exists blob(${k}.consumes)]}
        set blob(${k}.script) $fv
      }
    }
    if {$i >= $n} {
      proj-error -up "[proj-scope]: Missing argument for $k flag"
    }
  }
  for {set i 0} {$i < $n} {incr i} {
    set k [lindex $prototype $i]
    #puts "**** #$i of $n k=$k"

    # Check for :PRAGMA...
    switch -exact -- $k {
      :all-flags {
        set incrSkip 0
        continue
      }
    }

    proj-assert {[string match -* $k]} \
      "Invalid argument: $k"

    if {[string match {*\*} $k]} {
      # Re-map -foo* to -foo and flag -foo as a repeatable flag
      set k [string map {* ""} $k]
      incr blob(${k}.multi)
    }

    if {[info exists flags($k)]} {
      proj-error -up "[proj-scope]: Duplicated prototype for flag $k"
    }

    switch -exact -- [lindex $prototype [expr {$i + 1}]] {
      => {
        # -flag => DFLT ?-subflag arg?
        incr i 2
        if {$i >= $n} {
          proj-error -up "[proj-scope]: Missing argument for $k => flag"
        }
        incr blob(${k}.consumes)
        set vi [lindex $prototype $i]
        if {$vi in {-apply -call}} {
          proj-error -up "[proj-scope]: Missing default value for $k flag"
        } else {
          set fv [lindex $prototype [expr {$i + 1}]]
          if {$fv in {-apply -call}} {
            incr i
            eval $checkProtoFlag
          }
        }
      }
      default {
        # -flag VALUE ?flag? SCRIPT
        set vi [lindex $prototype [incr i]]
        set fv [lindex $prototype [incr i]]
        eval $checkProtoFlag
      }
    }
    #puts "**** #$i of $n k=$k vi=$vi"
    set flags($k) $vi
  }
  #puts "-- flags"; parray flags
  #puts "-- blob"; parray blob
  set rc 0
  set rv {}; # staging area for the target argv value
  set skipMode 0
  set n [llength $argv]
  # Now look for those flags in $argv...
  for {set i 0} {$i < $n} {incr i} {
    set arg [lindex $argv $i]
    #puts "-- [proj-scope] arg=$arg"
    if {$skipMode} {
      lappend rv $arg
    } elseif {"--" eq $arg} {
      # "--" is the conventional way to end processing of args
      if {[incr blob(--)] > 1} {
        # Elide only the first one
        lappend rv $arg
      }
      incr skipMode $incrSkip
    } elseif {[info exists flags($arg)]} {
      # A known flag...
      set isMulti [info exists blob(${arg}.multi)]
      incr blob(${arg}.seen)
      if {1 < $blob(${arg}.seen) && !$isMulti} {
        proj-error -up [proj-scope] "$arg flag was used multiple times"
      }
      set vMode 0; # 0=as-is, 1=eval, 2=call
      set isConsuming [info exists blob(${arg}.consumes)]
      if {$isConsuming} {
        incr i
        if {$i >= $n} {
          proj-error -up [proj-scope] "is missing argument for $arg flag"
        }
        set vv [lindex $argv $i]
      } elseif {[info exists blob(${arg}.script)]} {
        set vMode 1
        set vv $blob(${arg}.script)
      } else {
        set vv $flags($arg)
      }

      if {[info exists blob(${arg}.call)]} {
        set vMode 2
        set vv [concat {*}$blob(${arg}.call) $arg $vv]
      } elseif {$isConsuming} {
        proj-assert {!$vMode}
        # fall through
      } elseif {"" eq $vv || [string is double -strict $vv]} {
        set vMode 0
      } elseif {$vv in {incr}} {
        set vMode 0
        switch -exact $vv {
          incr {
            set xx $flags($k); incr xx; set vv $xx; unset xx
          }
          default {
            proj-error "Unhandled \$vv value $vv"
          }
        }
      } else {
        set vv [list eval $vv]
        set vMode 1
      }
      if {$vMode} {
        set code [catch [list uplevel 1 $vv] vv xopt]
        if {$code ni {0 2}} {
          return {*}$xopt $vv
        }
      }
      if {$isConsuming && $isMulti} {
        if {1 == $blob(${arg}.seen)} {
          # On the first hit, overwrite the default with a new list.
          set flags($arg) [list $vv]
        } else {
          # On subsequent hits, append to the list.
          lappend flags($arg) $vv
        }
      } else {
        set flags($arg) $vv
      }
      incr rc
    } else {
      # Non-flag
      incr skipMode $incrSkip
      lappend rv $arg
    }
  }
  set argv $rv
  array set outFlags [array get flags]
  #puts "-- rv=$rv argv=$argv flags="; parray flags
  return $rc
}; # proj-parse-flags

#
# Older (deprecated) name of proj-parse-flags.
#
proc proj-parse-simple-flags {args} {
  tailcall proj-parse-flags {*}$args
}

if {$::proj__Config(self-tests)} {
  set __ova $::proj__Config(verbose-assert);
  set ::proj__Config(verbose-assert) 1
  puts "Running [info script] self-tests..."
  # proj-cache...
  apply {{} {
    #proj-warn "Test code for proj-cache"
    proj-assert {![proj-cache-check -key here check]}
    proj-assert {"here" eq [proj-cache-key here]}
    proj-assert {"" eq $check}
    proj-cache-set -key here thevalue
    proj-assert {[proj-cache-check -key here check]}
    proj-assert {"thevalue" eq $check}

    proj-assert {![proj-cache-check check]}
    #puts "*** key = ([proj-cache-key 0])"
    proj-assert {"" eq $check}
    proj-cache-set abc
    proj-assert {[proj-cache-check check]}
    proj-assert {"abc" eq $check}

    #parray ::proj__Cache;
    proj-assert {"" ne [proj-cache-remove]}
    proj-assert {![proj-cache-check check]}
    proj-assert {"" eq [proj-cache-remove]}
    proj-assert {"" eq $check}
  }}

  # proj-parse-flags ...
  apply {{} {
    set foo 3
    set argv {-a "hi - world" -b -b -b -- -a {bye bye} -- -d -D c -a "" --}
    proj-parse-flags argv flags {
      :all-flags
      -a* => "gets overwritten"
      -b* 7 {incr foo}
      -d 1 0
      -D 0 1
    }

    #puts "-- argv = $argv"; parray flags;
    proj-assert {"-- c --" eq $argv}
    proj-assert {$flags(-a) eq "{hi - world} {bye bye} {}"}
    proj-assert {$foo == 6}
    proj-assert {$flags(-b) eq $foo}
    proj-assert {$flags(-d) == 0}
    proj-assert {$flags(-D) == 1}
    set foo 0
    foreach x $flags(-a) {
      proj-assert {$x in {{hi - world} {bye bye} {}}}
      incr foo
    }
    proj-assert {3 == $foo}

    set argv {-a {hi world} -b -maybe -- -a {bye bye} -- -b c --}
    set foo 0
    proj-parse-flags argv flags {
      -a => "aaa"
      -b 0 {incr foo}
      -maybe no -literal yes
    }
    #parray flags; puts "--- argv = $argv"
    proj-assert {"-a {bye bye} -- -b c --" eq $argv}
    proj-assert {$flags(-a) eq "hi world"}
    proj-assert {1 == $flags(-b)}
    proj-assert {"yes" eq $flags(-maybe)}

    set argv {-f -g -a aaa -M -M -M -L -H -A AAA a b c}
    set foo 0
    set myLambda {{flag val} {
      proj-assert {$flag in {-f -g -M}}
      #puts "myLambda flag=$flag val=$val"
      incr val
    }}
    proc myNonLambda {flag val} {
      proj-assert {$flag in {-A -a}}
      #puts "myNonLambda flag=$flag val=$val"
      concat $val $val
    }
    proj-parse-flags argv flags {
      -f 0 -call {apply $myLambda}
      -g 2 -apply $myLambda
      -h 3 -apply $myLambda
      -H 30 33
      -a => aAAAa -apply {{f v} {
        set v
      }}
      -A => AaaaA -call myNonLambda
      -B => 17 -call myNonLambda
      -M* 0 -apply $myLambda
      -L "" -literal $myLambda
    }
    rename myNonLambda ""
    #puts "--- argv = $argv"; parray flags
    proj-assert {$flags(-f) == 1}
    proj-assert {$flags(-g) == 3}
    proj-assert {$flags(-h) == 3}
    proj-assert {$flags(-H) == 33}
    proj-assert {$flags(-a) == {aaa}}
    proj-assert {$flags(-A) eq "AAA AAA"}
    proj-assert {$flags(-B) == 17}
    proj-assert {$flags(-M) == 3}
    proj-assert {$flags(-L) eq $myLambda}

    set argv {-touch -validate}
    proj-parse-flags argv flags {
      -touch "" {return "-touch"}
      -validate 0 1
    }
    #puts "----- argv = $argv"; parray flags
    proj-assert {$flags(-touch) eq "-touch"}
    proj-assert {$flags(-validate) == 1}
    proj-assert {$argv eq {}}

    set argv {-i -i -i}
    proj-parse-flags argv flags {
      -i* 0 incr
    }
    proj-assert {3 == $flags(-i)}
  }}
  set ::proj__Config(verbose-assert) $__ova
  unset __ova
  puts "Done running [info script] self-tests."
}; # proj- API self-tests
