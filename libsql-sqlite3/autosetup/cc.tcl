# Copyright (c) 2010 WorkWare Systems http://www.workware.net.au/
# All rights reserved

# @synopsis:
#
# The 'cc' module supports checking various 'features' of the C or C++
# compiler/linker environment. Common commands are 'cc-check-includes',
# 'cc-check-types', 'cc-check-functions', 'cc-with' and 'make-config-header'
#
# The following environment variables are used if set:
#
## CC       - C compiler
## CXX      - C++ compiler
## CPP      - C preprocessor
## CCACHE   - Set to "none" to disable automatic use of ccache
## CPPFLAGS  - Additional C preprocessor compiler flags (C and C++), before CFLAGS, CXXFLAGS
## CFLAGS   - Additional C compiler flags
## CXXFLAGS - Additional C++ compiler flags
## LDFLAGS  - Additional compiler flags during linking
## LINKFLAGS - ?How is this different from LDFLAGS?
## LIBS     - Additional libraries to use (for all tests)
## CROSS    - Tool prefix for cross compilation
#
# The following variables are defined from the corresponding
# environment variables if set.
#
## CC_FOR_BUILD
## LD

use system

options {}

# Checks for the existence of the given function by linking
#
proc cctest_function {function} {
	cctest -link 1 -declare "extern void $function\(void);" -code "$function\();"
}

# Checks for the existence of the given type by compiling
proc cctest_type {type} {
	cctest -code "$type _x;"
}

# Checks for the existence of the given type/structure member.
# e.g. "struct stat.st_mtime"
proc cctest_member {struct_member} {
	# split at the first dot
	regexp {^([^.]+)[.](.*)$} $struct_member -> struct member
	cctest -code "static $struct _s; return sizeof(_s.$member);"
}

# Checks for the existence of the given define by compiling
#
proc cctest_define {name} {
	cctest -code "#ifndef $name\n#error not defined\n#endif"
}

# Checks for the existence of the given name either as
# a macro (#define) or an rvalue (such as an enum)
#
proc cctest_decl {name} {
	cctest -code "#ifndef $name\n(void)$name;\n#endif"
}

# @cc-check-sizeof type ...
#
# Checks the size of the given types (between 1 and 32, inclusive).
# Defines a variable with the size determined, or 'unknown' otherwise.
# e.g. for type 'long long', defines 'SIZEOF_LONG_LONG'.
# Returns the size of the last type.
#
proc cc-check-sizeof {args} {
	foreach type $args {
		msg-checking "Checking for sizeof $type..."
		set size unknown
		# Try the most common sizes first
		foreach i {4 8 1 2 16 32} {
			if {[cctest -code "static int _x\[sizeof($type) == $i ? 1 : -1\] = { 1 };"]} {
				set size $i
				break
			}
		}
		msg-result $size
		set define [feature-define-name $type SIZEOF_]
		define $define $size
	}
	# Return the last result
	get-define $define
}

# Checks for each feature in $list by using the given script.
#
# When the script is evaluated, $each is set to the feature
# being checked, and $extra is set to any additional cctest args.
#
# Returns 1 if all features were found, or 0 otherwise.
proc cc-check-some-feature {list script} {
	set ret 1
	foreach each $list {
		if {![check-feature $each $script]} {
			set ret 0
		}
	}
	return $ret
}

# @cc-check-includes includes ...
#
# Checks that the given include files can be used.
proc cc-check-includes {args} {
	cc-check-some-feature $args {
		set with {}
		if {[dict exists $::autosetup(cc-include-deps) $each]} {
			set deps [dict keys [dict get $::autosetup(cc-include-deps) $each]]
			msg-quiet cc-check-includes {*}$deps
			foreach i $deps {
				if {[have-feature $i]} {
					lappend with $i
				}
			}
		}
		if {[llength $with]} {
			cc-with [list -includes $with] {
				cctest -includes $each
			}
		} else {
			cctest -includes $each
		}
	}
}

# @cc-include-needs include required ...
#
# Ensures that when checking for '$include', a check is first
# made for each '$required' file, and if found, it is included with '#include'.
proc cc-include-needs {file args} {
	foreach depfile $args {
		dict set ::autosetup(cc-include-deps) $file $depfile 1
	}
}

# @cc-check-types type ...
#
# Checks that the types exist.
proc cc-check-types {args} {
	cc-check-some-feature $args {
		cctest_type $each
	}
}

# @cc-check-defines define ...
#
# Checks that the given preprocessor symbols are defined.
proc cc-check-defines {args} {
	cc-check-some-feature $args {
		cctest_define $each
	}
}

# @cc-check-decls name ...
#
# Checks that each given name is either a preprocessor symbol or rvalue
# such as an enum. Note that the define used is 'HAVE_DECL_xxx'
# rather than 'HAVE_xxx'.
proc cc-check-decls {args} {
	set ret 1
	foreach name $args {
		msg-checking "Checking for $name..."
		set r [cctest_decl $name]
		define-feature "decl $name" $r
		if {$r} {
			msg-result "ok"
		} else {
			msg-result "not found"
			set ret 0
		}
	}
	return $ret
}

# @cc-check-functions function ...
#
# Checks that the given functions exist (can be linked).
proc cc-check-functions {args} {
	cc-check-some-feature $args {
		cctest_function $each
	}
}

# @cc-check-members type.member ...
#
# Checks that the given type/structure members exist.
# A structure member is of the form 'struct stat.st_mtime'.
proc cc-check-members {args} {
	cc-check-some-feature $args {
		cctest_member $each
	}
}

# @cc-check-function-in-lib function libs ?otherlibs?
#
# Checks that the given function can be found in one of the libs.
#
# First checks for no library required, then checks each of the libraries
# in turn.
#
# If the function is found, the feature is defined and 'lib_$function' is defined
# to '-l$lib' where the function was found, or "" if no library required.
# In addition, '-l$lib' is prepended to the 'LIBS' define.
#
# If additional libraries may be needed for linking, they should be specified
# with '$extralibs' as '-lotherlib1 -lotherlib2'.
# These libraries are not automatically added to 'LIBS'.
#
# Returns 1 if found or 0 if not.
#
proc cc-check-function-in-lib {function libs {otherlibs {}}} {
	msg-checking "Checking libs for $function..."
	set found 0
	cc-with [list -libs $otherlibs] {
		if {[cctest_function $function]} {
			msg-result "none needed"
			define lib_$function ""
			incr found
		} else {
			foreach lib $libs {
				cc-with [list -libs -l$lib] {
					if {[cctest_function $function]} {
						msg-result -l$lib
						define lib_$function -l$lib
						# prepend to LIBS
						define LIBS "-l$lib [get-define LIBS]"
						incr found
						break
					}
				}
			}
		}
	}
	define-feature $function $found
	if {!$found} {
		msg-result "no"
	}
	return $found
}

# @cc-check-tools tool ...
#
# Checks for existence of the given compiler tools, taking
# into account any cross compilation prefix.
#
# For example, when checking for 'ar', first 'AR' is checked on the command
# line and then in the environment. If not found, '${host}-ar' or
# simply 'ar' is assumed depending upon whether cross compiling.
# The path is searched for this executable, and if found 'AR' is defined
# to the executable name.
# Note that even when cross compiling, the simple 'ar' is used as a fallback,
# but a warning is generated. This is necessary for some toolchains.
#
# It is an error if the executable is not found.
#
proc cc-check-tools {args} {
	foreach tool $args {
		set TOOL [string toupper $tool]
		set exe [get-env $TOOL [get-define cross]$tool]
		if {[find-executable $exe]} {
			define $TOOL $exe
			continue
		}
		if {[find-executable $tool]} {
			msg-result "Warning: Failed to find $exe, falling back to $tool which may be incorrect"
			define $TOOL $tool
			continue
		}
		user-error "Failed to find $exe"
	}
}

# @cc-check-progs prog ...
#
# Checks for existence of the given executables on the path.
#
# For example, when checking for 'grep', the path is searched for
# the executable, 'grep', and if found 'GREP' is defined as 'grep'.
#
# If the executable is not found, the variable is defined as 'false'.
# Returns 1 if all programs were found, or 0 otherwise.
#
proc cc-check-progs {args} {
	set failed 0
	foreach prog $args {
		set PROG [string toupper $prog]
		msg-checking "Checking for $prog..."
		if {![find-executable $prog]} {
			msg-result no
			define $PROG false
			incr failed
		} else {
			msg-result ok
			define $PROG $prog
		}
	}
	expr {!$failed}
}

# @cc-path-progs prog ...
#
# Like cc-check-progs, but sets the define to the full path rather
# than just the program name.
#
proc cc-path-progs {args} {
	set failed 0
	foreach prog $args {
		set PROG [string toupper $prog]
		msg-checking "Checking for $prog..."
		set path [find-executable-path $prog]
		if {$path eq ""} {
			msg-result no
			define $PROG false
			incr failed
		} else {
			msg-result $path
			define $PROG $path
		}
	}
	expr {!$failed}
}

# Adds the given settings to $::autosetup(ccsettings) and
# returns the old settings.
#
proc cc-add-settings {settings} {
	if {[llength $settings] % 2} {
		autosetup-error "settings list is missing a value: $settings"
	}

	set prev [cc-get-settings]
	# workaround a bug in some versions of jimsh by forcing
	# conversion of $prev to a list
	llength $prev

	array set new $prev

	foreach {name value} $settings {
		switch -exact -- $name {
			-cflags - -includes {
				# These are given as lists
				lappend new($name) {*}[list-non-empty $value]
			}
			-declare {
				lappend new($name) $value
			}
			-libs {
				# Note that new libraries are added before previous libraries
				set new($name) [list {*}[list-non-empty $value] {*}$new($name)]
			}
			-link - -lang - -nooutput {
				set new($name) $value
			}
			-source - -sourcefile - -code {
				# XXX: These probably are only valid directly from cctest
				set new($name) $value
			}
			default {
				autosetup-error "unknown cctest setting: $name"
			}
		}
	}

	cc-store-settings [array get new]

	return $prev
}

proc cc-store-settings {new} {
	set ::autosetup(ccsettings) $new
}

proc cc-get-settings {} {
	return $::autosetup(ccsettings)
}

# Similar to cc-add-settings, but each given setting
# simply replaces the existing value.
#
# Returns the previous settings
proc cc-update-settings {args} {
	set prev [cc-get-settings]
	cc-store-settings [dict merge $prev $args]
	return $prev
}

# @cc-with settings ?{ script }?
#
# Sets the given 'cctest' settings and then runs the tests in '$script'.
# Note that settings such as '-lang' replace the current setting, while
# those such as '-includes' are appended to the existing setting.
#
# If no script is given, the settings become the default for the remainder
# of the 'auto.def' file.
#
## cc-with {-lang c++} {
##   # This will check with the C++ compiler
##   cc-check-types bool
##   cc-with {-includes signal.h} {
##     # This will check with the C++ compiler, signal.h and any existing includes.
##     ...
##   }
##   # back to just the C++ compiler
## }
#
# The '-libs' setting is special in that newer values are added *before* earlier ones.
#
## cc-with {-libs {-lc -lm}} {
##   cc-with {-libs -ldl} {
##     cctest -libs -lsocket ...
##     # libs will be in this order: -lsocket -ldl -lc -lm
##   }
## }
#
# If you wish to invoke something like cc-check-flags but not have -cflags updated,
# use the following idiom:
#
## cc-with {} {
##   cc-check-flags ...
## }
proc cc-with {settings args} {
	if {[llength $args] == 0} {
		cc-add-settings $settings
	} elseif {[llength $args] > 1} {
		autosetup-error "usage: cc-with settings ?script?"
	} else {
		set save [cc-add-settings $settings]
		set rc [catch {uplevel 1 [lindex $args 0]} result info]
		cc-store-settings $save
		if {$rc != 0} {
			return -code [dict get $info -code] $result
		}
		return $result
	}
}

# @cctest ?settings?
#
# Low level C/C++ compiler checker. Compiles and or links a small C program
# according to the arguments and returns 1 if OK, or 0 if not.
#
# Supported settings are:
#
## -cflags cflags      A list of flags to pass to the compiler
## -includes list      A list of includes, e.g. {stdlib.h stdio.h}
## -declare code       Code to declare before main()
## -link 1             Don't just compile, link too
## -lang c|c++         Use the C (default) or C++ compiler
## -libs liblist       List of libraries to link, e.g. {-ldl -lm}
## -code code          Code to compile in the body of main()
## -source code        Compile a complete program. Ignore -includes, -declare and -code
## -sourcefile file    Shorthand for -source [readfile [get-define srcdir]/$file]
## -nooutput 1         Treat any compiler output (e.g. a warning) as an error
#
# Unless '-source' or '-sourcefile' is specified, the C program looks like:
#
## #include <firstinclude>   /* same for remaining includes in the list */
## declare-code              /* any code in -declare, verbatim */
## int main(void) {
##   code                    /* any code in -code, verbatim */
##   return 0;
## }
#
# And the command line looks like:
#
## CC -cflags CFLAGS CPPFLAGS conftest.c -o conftest.o
## CXX -cflags CXXFLAGS CPPFLAGS conftest.cpp -o conftest.o
#
# And if linking:
#
## CC LDFLAGS -cflags CFLAGS conftest.c -o conftest -libs LIBS
## CXX LDFLAGS -cflags CXXFLAGS conftest.c -o conftest -libs LIBS
#
# Any failures are recorded in 'config.log'
#
proc cctest {args} {
	set tmp conftest__

	# Easiest way to merge in the settings
	cc-with $args {
		array set opts [cc-get-settings]
	}

	if {[info exists opts(-sourcefile)]} {
		set opts(-source) [readfile [get-define srcdir]/$opts(-sourcefile) "#error can't find $opts(-sourcefile)"]
	}
	if {[info exists opts(-source)]} {
		set lines $opts(-source)
	} else {
		foreach i $opts(-includes) {
			if {$opts(-code) ne "" && ![feature-checked $i]} {
				# Compiling real code with an unchecked header file
				# Quickly (and silently) check for it now

				# Remove all -includes from settings before checking
				set saveopts [cc-update-settings -includes {}]
				msg-quiet cc-check-includes $i
				cc-store-settings $saveopts
			}
			if {$opts(-code) eq "" || [have-feature $i]} {
				lappend source "#include <$i>"
			}
		}
		lappend source {*}$opts(-declare)
		lappend source "int main(void) {"
		lappend source $opts(-code)
		lappend source "return 0;"
		lappend source "}"

		set lines [join $source \n]
	}

	# Build the command line
	set cmdline {}
	lappend cmdline {*}[get-define CCACHE]
	switch -exact -- $opts(-lang) {
		c++ {
			set src conftest__.cpp
			lappend cmdline {*}[get-define CXX]
			set cflags [get-define CXXFLAGS]
		}
		c {
			set src conftest__.c
			lappend cmdline {*}[get-define CC]
			set cflags [get-define CFLAGS]
		}
		default {
			autosetup-error "cctest called with unknown language: $opts(-lang)"
		}
	}

	if {$opts(-link)} {
		lappend cmdline {*}[get-define LDFLAGS]
	} else {
		lappend cflags {*}[get-define CPPFLAGS]
		set tmp conftest__.o
		lappend cmdline -c
	}
	lappend cmdline {*}$opts(-cflags) {*}[get-define cc-default-debug ""] {*}$cflags
	lappend cmdline $src -o $tmp
	if {$opts(-link)} {
		lappend cmdline {*}$opts(-libs) {*}[get-define LIBS]
	}

	# At this point we have the complete command line and the
	# complete source to be compiled. Get the result from cache if
	# we can
	if {[info exists ::cc_cache($cmdline,$lines)]} {
		msg-checking "(cached) "
		set ok $::cc_cache($cmdline,$lines)
		if {$::autosetup(debug)} {
			configlog "From cache (ok=$ok): [join $cmdline]"
			configlog "============"
			configlog $lines
			configlog "============"
		}
		return $ok
	}

	writefile $src $lines\n

	set ok 1
	set err [catch {exec-with-stderr {*}$cmdline} result errinfo]
	if {$err || ($opts(-nooutput) && [string length $result])} {
		configlog "Failed: [join $cmdline]"
		configlog $result
		configlog "============"
		configlog "The failed code was:"
		configlog $lines
		configlog "============"
		set ok 0
	} elseif {$::autosetup(debug)} {
		configlog "Compiled OK: [join $cmdline]"
		configlog "============"
		configlog $lines
		configlog "============"
	}
	file delete $src
	file delete $tmp

	# cache it
	set ::cc_cache($cmdline,$lines) $ok

	return $ok
}

# @make-autoconf-h outfile ?auto-patterns=HAVE_*? ?bare-patterns=SIZEOF_*?
#
# Deprecated - see 'make-config-header'
proc make-autoconf-h {file {autopatterns {HAVE_*}} {barepatterns {SIZEOF_* HAVE_DECL_*}}} {
	user-notice "*** make-autoconf-h is deprecated -- use make-config-header instead"
	make-config-header $file -auto $autopatterns -bare $barepatterns
}

# @make-config-header outfile ?-auto patternlist? ?-bare patternlist? ?-none patternlist? ?-str patternlist? ...
#
# Examines all defined variables which match the given patterns
# and writes an include file, '$file', which defines each of these.
# Variables which match '-auto' are output as follows:
# - defines which have the value '0' are ignored.
# - defines which have integer values are defined as the integer value.
# - any other value is defined as a string, e.g. '"value"'
# Variables which match '-bare' are defined as-is.
# Variables which match '-str' are defined as a string, e.g. '"value"'
# Variables which match '-none' are omitted.
#
# Note that order is important. The first pattern that matches is selected.
# Default behaviour is:
#
##  -bare {SIZEOF_* HAVE_DECL_*} -auto HAVE_* -none *
#
# If the file would be unchanged, it is not written.
proc make-config-header {file args} {
	set guard _[string toupper [regsub -all {[^a-zA-Z0-9]} [file tail $file] _]]
	file mkdir [file dirname $file]
	set lines {}
	lappend lines "#ifndef $guard"
	lappend lines "#define $guard"

	# Add some defaults
	lappend args -bare {SIZEOF_* HAVE_DECL_*} -auto HAVE_*

	foreach n [lsort [dict keys [all-defines]]] {
		set value [get-define $n]
		set type [calc-define-output-type $n $args]
		switch -exact -- $type {
			-bare {
				# Just output the value unchanged
			}
			-none {
				continue
			}
			-str {
				set value \"[string map [list \\ \\\\ \" \\\"] $value]\"
			}
			-auto {
				# Automatically determine the type
				if {$value eq "0"} {
					lappend lines "/* #undef $n */"
					continue
				}
				if {![string is integer -strict $value]} {
					set value \"[string map [list \\ \\\\ \" \\\"] $value]\"
				}
			}
			"" {
				continue
			}
			default {
				autosetup-error "Unknown type in make-config-header: $type"
			}
		}
		lappend lines "#define $n $value"
	}
	lappend lines "#endif"
	set buf [join $lines \n]
	write-if-changed $file $buf {
		msg-result "Created $file"
	}
}

proc calc-define-output-type {name spec} {
	foreach {type patterns} $spec {
		foreach pattern $patterns {
			if {[string match $pattern $name]} {
				return $type
			}
		}
	}
	return ""
}

proc cc-init {} {
	global autosetup

	# Initialise some values from the environment or commandline or default settings
	foreach i {LDFLAGS LIBS CPPFLAGS LINKFLAGS CFLAGS} {
		lassign $i var default
		define $var [get-env $var $default]
	}

	if {[env-is-set CC]} {
		# Set by the user, so don't try anything else
		set try [list [get-env CC ""]]
	} else {
		# Try some reasonable options
		set try [list [get-define cross]cc [get-define cross]gcc]
	}
	define CC [find-an-executable {*}$try]
	if {[get-define CC] eq ""} {
		user-error "Could not find a C compiler. Tried: [join $try ", "]"
	}

	define CPP [get-env CPP "[get-define CC] -E"]

	# XXX: Could avoid looking for a C++ compiler until requested
	# If CXX isn't found, it is set to the empty string.
	if {[env-is-set CXX]} {
		define CXX [find-an-executable -required [get-env CXX ""]]
	} else {
		define CXX [find-an-executable [get-define cross]c++ [get-define cross]g++]
	}

	# CXXFLAGS default to CFLAGS if not specified
	define CXXFLAGS [get-env CXXFLAGS [get-define CFLAGS]]

	# May need a CC_FOR_BUILD, so look for one
	define CC_FOR_BUILD [find-an-executable [get-env CC_FOR_BUILD ""] cc gcc false]

	# These start empty and never come from the user or environment
	define AS_CFLAGS ""
	define AS_CPPFLAGS ""
	define AS_CXXFLAGS ""

	define CCACHE [find-an-executable [get-env CCACHE ccache]]

	# If any of these are set in the environment, propagate them to the AUTOREMAKE commandline
	foreach i {CC CXX CCACHE CPP CFLAGS CXXFLAGS CXXFLAGS LDFLAGS LIBS CROSS CPPFLAGS LINKFLAGS CC_FOR_BUILD LD} {
		if {[env-is-set $i]} {
			# Note: If the variable is set on the command line, get-env will return that value
			# so the command line will continue to override the environment
			define-append-argv AUTOREMAKE $i=[get-env $i ""]
		}
	}

	# Initial cctest settings
	cc-store-settings {-cflags {} -includes {} -declare {} -link 0 -lang c -libs {} -code {} -nooutput 0}
	set autosetup(cc-include-deps) {}

	msg-result "C compiler...[get-define CCACHE] [get-define CC] [get-define CFLAGS] [get-define CPPFLAGS]"
	if {[get-define CXX] ne "false"} {
		msg-result "C++ compiler...[get-define CCACHE] [get-define CXX] [get-define CXXFLAGS] [get-define CPPFLAGS]"
	}
	msg-result "Build C compiler...[get-define CC_FOR_BUILD]"

	# On Darwin, we prefer to use -g0 to avoid creating .dSYM directories
	# but some compilers may not support it, so test here.
	switch -glob -- [get-define host] {
		*-*-darwin* {
			if {[cctest -cflags {-g0}]} {
				define cc-default-debug -g0
			}
		}
	}

	if {![cc-check-includes stdlib.h]} {
		user-error "Compiler does not work. See config.log"
	}
}

cc-init
