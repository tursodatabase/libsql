# Copyright (c) 2011 WorkWare Systems http://www.workware.net.au/
# All rights reserved

# @synopsis:
#
# Provides a library of common tests on top of the 'cc' module.

use cc

# @cc-check-lfs
#
# The equivalent of the 'AC_SYS_LARGEFILE' macro.
#
# defines 'HAVE_LFS' if LFS is available,
# and defines '_FILE_OFFSET_BITS=64' if necessary
#
# Returns 1 if 'LFS' is available or 0 otherwise
#
proc cc-check-lfs {} {
	cc-check-includes sys/types.h
	msg-checking "Checking if -D_FILE_OFFSET_BITS=64 is needed..."
	set lfs 1
	if {[msg-quiet cc-with {-includes sys/types.h} {cc-check-sizeof off_t}] == 8} {
		msg-result no
	} elseif {[msg-quiet cc-with {-includes sys/types.h -cflags -D_FILE_OFFSET_BITS=64} {cc-check-sizeof off_t}] == 8} {
		define _FILE_OFFSET_BITS 64
		msg-result yes
	} else {
		set lfs 0
		msg-result none
	}
	define-feature lfs $lfs
	return $lfs
}

# @cc-check-endian
#
# The equivalent of the 'AC_C_BIGENDIAN' macro.
#
# defines 'HAVE_BIG_ENDIAN' if endian is known to be big,
# or 'HAVE_LITTLE_ENDIAN' if endian is known to be little.
#
# Returns 1 if determined, or 0 if not.
#
proc cc-check-endian {} {
	cc-check-includes sys/types.h sys/param.h
	set rc 0
	msg-checking "Checking endian..."
	cc-with {-includes {sys/types.h sys/param.h}} {
		if {[cctest -code {
			#if !defined(BIG_ENDIAN) || !defined(BYTE_ORDER)
				#error unknown
			#elif BYTE_ORDER != BIG_ENDIAN
				#error little
			#endif
		}]} {
			define-feature big-endian
			msg-result "big"
			set rc 1
		} elseif {[cctest -code {
			#if !defined(LITTLE_ENDIAN) || !defined(BYTE_ORDER)
				#error unknown
			#elif BYTE_ORDER != LITTLE_ENDIAN
				#error big
			#endif
		}]} {
			define-feature little-endian
			msg-result "little"
			set rc 1
		} else {
			msg-result "unknown"
		}
	}
	return $rc
}

# @cc-check-flags flag ?...?
#
# Checks whether the given C/C++ compiler flags can be used. Defines feature
# names prefixed with 'HAVE_CFLAG' and 'HAVE_CXXFLAG' respectively, and
# appends working flags to '-cflags' and 'AS_CFLAGS' or 'AS_CXXFLAGS'.
proc cc-check-flags {args} {
	set result 1
	array set opts [cc-get-settings]
	switch -exact -- $opts(-lang) {
		c++ {
			set lang C++
			set prefix CXXFLAG
		}
		c {
			set lang C
			set prefix CFLAG
		}
		default {
			autosetup-error "cc-check-flags failed with unknown language: $opts(-lang)"
		}
	}
	foreach flag $args {
		msg-checking "Checking whether the $lang compiler accepts $flag..."
		if {[cctest -cflags $flag]} {
			msg-result yes
			define-feature $prefix$flag
			cc-with [list -cflags [list $flag]]
			define-append AS_${prefix}S $flag
		} else {
			msg-result no
			set result 0
		}
	}
	return $result
}

# @cc-check-standards ver ?...?
#
# Checks whether the C/C++ compiler accepts one of the specified '-std=$ver'
# options, and appends the first working one to '-cflags' and 'AS_CFLAGS' or
# 'AS_CXXFLAGS'.
proc cc-check-standards {args} {
	array set opts [cc-get-settings]
	foreach std $args {
		if {[cc-check-flags -std=$std]} {
			return $std
		}
	}
	return ""
}

# Checks whether $keyword is usable as alignof
proc cctest_alignof {keyword} {
	msg-checking "Checking for $keyword..."
	if {[cctest -code "int x = ${keyword}(char), y = ${keyword}('x');"]} then {
		msg-result ok
		define-feature $keyword
	} else {
		msg-result "not found"
	}
}

# @cc-check-c11
#
# Checks for several C11/C++11 extensions and their alternatives. Currently
# checks for '_Static_assert', '_Alignof', '__alignof__', '__alignof'.
proc cc-check-c11 {} {
	msg-checking "Checking for _Static_assert..."
	if {[cctest -code {
		_Static_assert(1, "static assertions are available");
	}]} then {
		msg-result ok
		define-feature _Static_assert
	} else {
		msg-result "not found"
	}

	cctest_alignof _Alignof
	cctest_alignof __alignof__
	cctest_alignof __alignof
}

# @cc-check-alloca
#
# The equivalent of the 'AC_FUNC_ALLOCA' macro.
#
# Checks for the existence of 'alloca'
# defines 'HAVE_ALLOCA' and returns 1 if it exists.
proc cc-check-alloca {} {
	cc-check-some-feature alloca {
		cctest -includes alloca.h -code { alloca (2 * sizeof (int)); }
	}
}

# @cc-signal-return-type
#
# The equivalent of the 'AC_TYPE_SIGNAL' macro.
#
# defines 'RETSIGTYPE' to 'int' or 'void'.
proc cc-signal-return-type {} {
	msg-checking "Checking return type of signal handlers..."
	cc-with {-includes {sys/types.h signal.h}} {
		if {[cctest -code {return *(signal (0, 0)) (0) == 1;}]} {
				set type int
		} else {
				set type void
		}
		define RETSIGTYPE $type
		msg-result $type
	}
}
