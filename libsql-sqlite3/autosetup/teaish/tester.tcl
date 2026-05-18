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
#
# Helper routines for running tests on teaish extensions
#
########################################################################
# ----- @module teaish/tester.tcl -----
#
# @section TEA-ish Testing APIs.
#
# Though these are part of the autosup dir hierarchy, they are not
# intended to be run from autosetup code. Rather, they're for use
# with/via teaish.tester.tcl and target canonical Tcl only, not JimTcl
# (which the autosetup pieces do target).

#
# @test-current-scope ?lvl?
#
# Returns the name of the _calling_ proc from ($lvl + 1) levels up the
# call stack (where the caller's level will be 1 up from _this_
# call). If $lvl would resolve to global scope "global scope" is
# returned and if it would be negative then a string indicating such
# is returned (as opposed to throwing an error).
#
proc test-current-scope {{lvl 0}} {
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

# @test-msg
#
# Emits all arugments to stdout.
#
proc test-msg {args} {
  puts "$args"
}

# @test-warn
#
# Emits all arugments to stderr.
#
proc test-warn {args} {
  puts stderr "WARNING: $args"
}

#
# @test-error msg
#
# Triggers a test-failed error with a string describing the calling
# scope and the provided message.
#
proc test-fail {args} {
  #puts stderr "ERROR: \[[test-current-scope 1]]: $msg"
  #exit 1
  error "FAIL: \[[test-current-scope 1]]: $args"
}

array set ::test__Counters {}
array set ::test__Config {
  verbose-assert 0 verbose-affirm 0
}

# Internal impl for affirm and assert.
#
# $args = ?-v? script {msg-on-fail ""}
proc test__affert {failMode args} {
  if {$failMode} {
    set what assert
  } else {
    set what affirm
  }
  set verbose $::test__Config(verbose-$what)
  if {"-v" eq [lindex $args 0]} {
    lassign $args - script msg
    if {1 == [llength $args]} {
      # If -v is the only arg, toggle default verbose mode
      set ::test__Config(verbose-$what) [expr {!$::test__Config(verbose-$what)}]
      return
    }
    incr verbose
  } else {
    lassign $args script msg
  }
  incr ::test__Counters($what)
  if {![uplevel 1 expr [list $script]]} {
    if {"" eq $msg} {
      set msg $script
    }
    set txt [join [list $what # $::test__Counters($what) "failed:" $msg]]
    if {$failMode} {
      puts stderr $txt
      exit 1
    } else {
      error $txt
    }
  } elseif {$verbose} {
    puts stderr [join [list $what # $::test__Counters($what) "passed:" $script]]
  }
}

#
# @affirm ?-v? script ?msg?
#
# Works like a conventional assert method does, but reports failures
# using [error] instead of [exit]. If -v is used, it reports passing
# assertions to stderr. $script is evaluated in the caller's scope as
# an argument to [expr].
#
proc affirm {args} {
  tailcall test__affert 0 {*}$args
}

#
# @assert ?-v? script ?msg?
#
# Works like [affirm] but exits on error.
#
proc assert {args} {
  tailcall test__affert 1 {*}$args
}

#
# @assert-matches ?-e? pattern ?-e? rhs ?msg?
#
# Equivalent to assert {[string match $pattern $rhs]} except that
# if either of those are prefixed with an -e flag, they are eval'd
# and their results are used.
#
proc assert-matches {args} {
  set evalLhs 0
  set evalRhs 0
  if {"-e" eq [lindex $args 0]} {
    incr evalLhs
    set args [lassign $args -]
  }
  set args [lassign $args pattern]
  if {"-e" eq [lindex $args 0]} {
    incr evalRhs
    set args [lassign $args -]
  }
  set args [lassign $args rhs msg]

  if {$evalLhs} {
    set pattern [uplevel 1 $pattern]
  }
  if {$evalRhs} {
    set rhs [uplevel 1 $rhs]
  }
  #puts "***pattern=$pattern\n***rhs=$rhs"
  tailcall test__affert 1 \
    [join [list \[ string match [list $pattern] [list $rhs] \]]] $msg
  # why does this not work? [list \[ string match [list $pattern] [list $rhs] \]] $msg
  # "\[string match [list $pattern] [list $rhs]\]"
}

#
# @test-assert testId script ?msg?
#
# Works like [assert] but emits $testId to stdout first.
#
proc test-assert {testId script {msg ""}} {
  puts "test $testId"
  tailcall test__affert 1 $script $msg
}

#
# @test-expect testId script result
#
# Runs $script in the calling scope and compares its result to
# $result, minus any leading or trailing whitespace.  If they differ,
# it triggers an [assert].
#
proc test-expect {testId script result} {
  puts "test $testId"
  set x [string trim [uplevel 1 $script]]
  set result [string trim $result]
  tailcall test__affert 0 [list "{$x}" eq "{$result}"] \
    "\nEXPECTED: <<$result>>\nGOT:      <<$x>>"
}

#
# @test-catch cmd ?...args?
#
# Runs [cmd ...args], repressing any exception except to possibly log
# the failure. Returns 1 if it caught anything, 0 if it didn't.
#
proc test-catch {cmd args} {
  if {[catch {
    uplevel 1 $cmd {*}$args
  } rc xopts]} {
    puts "[test-current-scope] ignoring failure of: $cmd [lindex $args 0]: $rc"
    return 1
  }
  return 0
}

#
# @test-catch-matching pattern (script|cmd args...)
#
# Works like test-catch, but it expects its argument(s) to to throw an
# error matching the given string (checked with [string match]).  If
# they do not throw, or the error does not match $pattern, this
# function throws, else it returns 1.
#
# If there is no second argument, the $cmd is assumed to be a script,
# and will be eval'd in the caller's scope.
#
# TODO: add -glob and -regex flags to control matching flavor.
#
proc test-catch-matching {pattern cmd args} {
  if {[catch {
    #puts "**** catch-matching cmd=$cmd args=$args"
    if {0 == [llength $args]} {
      uplevel 1 $cmd {*}$args
    } else {
      $cmd {*}$args
    }
  } rc xopts]} {
    if {[string match $pattern $rc]} {
      return 1
    } else {
      error "[test-current-scope] exception does not match {$pattern}: {$rc}"
    }
  }
  error "[test-current-scope] expecting to see an error matching {$pattern}"
}

if {![array exists ::teaish__BuildFlags]} {
  array set ::teaish__BuildFlags {}
}

#
# @teaish-build-flag3 flag tgtVar ?dflt?
#
# If the current build has the configure-time flag named $flag set
# then tgtVar is assigned its value and 1 is returned, else tgtVal is
# assigned $dflt and 0 is returned.
#
# Caveat #1: only valid when called in the context of teaish's default
# "make test" recipe, e.g. from teaish.test.tcl. It is not valid from
# a teaish.tcl configure script because (A) the state it relies on
# doesn't fully exist at that point and (B) that level of the API has
# more direct access to the build state. This function requires that
# an external script have populated its internal state, which is
# normally handled via teaish.tester.tcl.in.
#
# Caveat #2: defines in the style of HAVE_FEATURENAME with a value of
# 0 are, by long-standing configure script conventions, treated as
# _undefined_ here.
#
proc teaish-build-flag3 {flag tgtVar {dflt ""}} {
  upvar $tgtVar tgt
  if {[info exists ::teaish__BuildFlags($flag)]} {
    set tgt $::teaish__BuildFlags($flag)
    return 1;
  } elseif {0==[array size ::teaish__BuildFlags]} {
    test-warn \
      "\[[test-current-scope]] was called from " \
      "[test-current-scope 1] without the build flags imported."
  }
  set tgt $dflt
  return 0
}

#
# @teaish-build-flag flag ?dflt?
#
# Convenience form of teaish-build-flag3 which returns the
# configure-time-defined value of $flag or "" if it's not defined (or
# if it's an empty string).
#
proc teaish-build-flag {flag {dflt ""}} {
  set tgt ""
  teaish-build-flag3 $flag tgt $dflt
  return $tgt
}
