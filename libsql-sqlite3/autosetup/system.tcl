# Copyright (c) 2010 WorkWare Systems http://www.workware.net.au/
# All rights reserved

# @synopsis:
#
# This module supports common system interrogation and options
# such as '--host', '--build', '--prefix', and setting 'srcdir', 'builddir', and 'EXEEXT'.
#
# It also support the "feature" naming convention, where searching
# for a feature such as 'sys/type.h' defines 'HAVE_SYS_TYPES_H'.
#
# It defines the following variables, based on '--prefix' unless overridden by the user:
#
## datadir
## sysconfdir
## sharedstatedir
## localstatedir
## infodir
## mandir
## includedir
#
# If '--prefix' is not supplied, it defaults to '/usr/local' unless 'options-defaults { prefix ... }' is used *before*
# including the 'system' module.

if {[is-defined defaultprefix]} {
	user-notice "Note: defaultprefix is deprecated. Use options-defaults to set default options"
	options-defaults [list prefix [get-define defaultprefix]]
}

options {
	host:host-alias =>		{a complete or partial cpu-vendor-opsys for the system where
							the application will run (defaults to the same value as --build)}
	build:build-alias =>	{a complete or partial cpu-vendor-opsys for the system
							where the application will be built (defaults to the
							result of running config.guess)}
	prefix:dir=/usr/local => {the target directory for the build (default: '@default@')}

	# These (hidden) options are supported for autoconf/automake compatibility
	exec-prefix:
	bindir:
	sbindir:
	includedir:
	mandir:
	infodir:
	libexecdir:
	datadir:
	libdir:
	sysconfdir:
	sharedstatedir:
	localstatedir:
	runstatedir:
	maintainer-mode=0
	dependency-tracking=0
	silent-rules=0
	program-prefix:
	program-suffix:
	program-transform-name:
	x-includes:
	x-libraries:
}

# @check-feature name { script }
#
# defines feature '$name' to the return value of '$script',
# which should be 1 if found or 0 if not found.
#
# e.g. the following will define 'HAVE_CONST' to 0 or 1.
#
## check-feature const {
##     cctest -code {const int _x = 0;}
## }
proc check-feature {name code} {
	msg-checking "Checking for $name..."
	set r [uplevel 1 $code]
	define-feature $name $r
	if {$r} {
		msg-result "ok"
	} else {
		msg-result "not found"
	}
	return $r
}

# @have-feature name ?default=0?
#
# Returns the value of feature '$name' if defined, or '$default' if not.
#
# See 'feature-define-name' for how the "feature" name
# is translated into the "define" name.
#
proc have-feature {name {default 0}} {
	get-define [feature-define-name $name] $default
}

# @define-feature name ?value=1?
#
# Sets the feature 'define' to '$value'.
#
# See 'feature-define-name' for how the "feature" name
# is translated into the "define" name.
#
proc define-feature {name {value 1}} {
	define [feature-define-name $name] $value
}

# @feature-checked name
#
# Returns 1 if feature '$name' has been checked, whether true or not.
#
proc feature-checked {name} {
	is-defined [feature-define-name $name]
}

# @feature-define-name name ?prefix=HAVE_?
#
# Converts a "feature" name to the corresponding "define",
# e.g. 'sys/stat.h' becomes 'HAVE_SYS_STAT_H'.
#
# Converts '*' to 'P' and all non-alphanumeric to underscore.
#
proc feature-define-name {name {prefix HAVE_}} {
	string toupper $prefix[regsub -all {[^a-zA-Z0-9]} [regsub -all {[*]} $name p] _]
}

# @write-if-changed filename contents ?script?
#
# If '$filename' doesn't exist, or it's contents are different to '$contents',
# the file is written and '$script' is evaluated.
#
# Otherwise a "file is unchanged" message is displayed.
proc write-if-changed {file buf {script {}}} {
	set old [readfile $file ""]
	if {$old eq $buf && [file exists $file]} {
		msg-result "$file is unchanged"
	} else {
		writefile $file $buf\n
		uplevel 1 $script
	}
}


# @include-file infile mapping
#
# The core of make-template, called recursively for each @include
# directive found within that template so that this proc's result
# is the fully-expanded template.
#
# The mapping parameter is how we expand @varname@ within the template.
# We do that inline within this step only for @include directives which
# can have variables in the filename arg.  A separate substitution pass
# happens when this recursive function returns, expanding the rest of
# the variables.
#
proc include-file {infile mapping} {
	# A stack of true/false conditions, one for each nested conditional
	# starting with "true"
	set condstack {1}
	set result {}
	set linenum 0
	foreach line [split [readfile $infile] \n] {
		incr linenum
		if {[regexp {^@(if|else|endif)(\s*)(.*)} $line -> condtype condspace condargs]} {
			if {$condtype eq "if"} {
				if {[string length $condspace] == 0} {
					autosetup-error "$infile:$linenum: Invalid expression: $line"
				}
				if {[llength $condargs] == 1} {
					# ABC => [get-define ABC] ni {0 ""}
					# !ABC => [get-define ABC] in {0 ""}
					lassign $condargs condvar
					if {[regexp {^!(.*)} $condvar -> condvar]} {
						set op in
					} else {
						set op ni
					}
					set condexpr "\[[list get-define $condvar]\] $op {0 {}}"
				} else {
					# Translate alphanumeric ABC into [get-define ABC] and leave the
					# rest of the expression untouched
					regsub -all {([A-Z][[:alnum:]_]*)} $condargs {[get-define \1]} condexpr
				}
				if {[catch [list expr $condexpr] condval]} {
					dputs $condval
					autosetup-error "$infile:$linenum: Invalid expression: $line"
				}
				dputs "@$condtype: $condexpr => $condval"
			}
			if {$condtype ne "if"} {
				if {[llength $condstack] <= 1} {
					autosetup-error "$infile:$linenum: Error: @$condtype missing @if"
				} elseif {[string length $condargs] && [string index $condargs 0] ne "#"} {
					autosetup-error "$infile:$linenum: Error: Extra arguments after @$condtype"
				}
			}
			switch -exact $condtype {
				if {
					# push condval
					lappend condstack $condval
				}
				else {
					# Toggle the last entry
					set condval [lpop condstack]
					set condval [expr {!$condval}]
					lappend condstack $condval
				}
				endif {
					if {[llength $condstack] == 0} {
						user-notice "$infile:$linenum: Error: @endif missing @if"
					}
					lpop condstack
				}
			}
			continue
		}
		# Only continue if the stack contains all "true"
		if {"0" in $condstack} {
			continue
		}
		if {[regexp {^@include\s+(.*)} $line -> filearg]} {
			set incfile [string map $mapping $filearg]
			if {[file exists $incfile]} {
				lappend ::autosetup(deps) [file-normalize $incfile]
				lappend result {*}[include-file $incfile $mapping]
			} else {
				user-error "$infile:$linenum: Include file $incfile is missing"
			}
			continue
		}
		if {[regexp {^@define\s+(\w+)\s+(.*)} $line -> var val]} {
			define $var $val
			continue
		}
		lappend result $line
	}
	return $result
}


# @make-template template ?outfile?
#
# Reads the input file '<srcdir>/$template' and writes the output file '$outfile'
# (unless unchanged).
# If '$outfile' is blank/omitted, '$template' should end with '.in' which
# is removed to create the output file name.
#
# Each pattern of the form '@define@' is replaced with the corresponding
# "define", if it exists, or left unchanged if not.
#
# The special value '@srcdir@' is substituted with the relative
# path to the source directory from the directory where the output
# file is created, while the special value '@top_srcdir@' is substituted
# with the relative path to the top level source directory.
#
# Conditional sections may be specified as follows:
## @if NAME eq "value"
## lines
## @else
## lines
## @endif
#
# Where 'NAME' is a defined variable name and '@else' is optional.
# Note that variables names *must* start with an uppercase letter.
# If the expression does not match, all lines through '@endif' are ignored.
#
# The alternative forms may also be used:
## @if NAME  (true if the variable is defined, but not empty and not "0")
## @if !NAME  (opposite of the form above)
## @if <general-tcl-expression>
#
# In the general Tcl expression, any words beginning with an uppercase letter
# are translated into [get-define NAME]
#
# Expressions may be nested
#
proc make-template {template {out {}}} {
	set infile [file join $::autosetup(srcdir) $template]

	if {![file exists $infile]} {
		user-error "Template $template is missing"
	}

	# Define this as late as possible
	define AUTODEPS $::autosetup(deps)

	if {$out eq ""} {
		if {[file ext $template] ne ".in"} {
			autosetup-error "make_template $template has no target file and can't guess"
		}
		set out [file rootname $template]
	}

	set outdir [file dirname $out]

	# Make sure the directory exists
	file mkdir $outdir

	# Set up srcdir and top_srcdir to be relative to the target dir
	define srcdir [relative-path [file join $::autosetup(srcdir) $outdir] $outdir]
	define top_srcdir [relative-path $::autosetup(srcdir) $outdir]

	# Build map from global defines to their values so they can be
	# substituted into @include file names.
	proc build-define-mapping {} {
		set mapping {}
		foreach {n v} [array get ::define] {
			lappend mapping @$n@ $v
		}
		return $mapping
	}
	set mapping [build-define-mapping]

	set result [include-file $infile $mapping]

	# Rebuild the define mapping in case we ran across @define
	# directives in the template or a file it @included, then
	# apply that mapping to the expanded template.
	set mapping [build-define-mapping]
	write-if-changed $out [string map $mapping [join $result \n]] {
		msg-result "Created [relative-path $out] from [relative-path $template]"
	}
}

proc system-init {} {
	global autosetup

	# build/host tuples and cross-compilation prefix
	opt-str build build ""
	define build_alias $build
	if {$build eq ""} {
		define build [config_guess]
	} else {
		define build [config_sub $build]
	}

	opt-str host host ""
	define host_alias $host
	if {$host eq ""} {
		define host [get-define build]
		set cross ""
	} else {
		define host [config_sub $host]
		set cross $host-
	}
	define cross [get-env CROSS $cross]

	# build/host _cpu, _vendor and _os
	foreach type {build host} {
		set v [get-define $type]
		if {![regexp {^([^-]+)-([^-]+)-(.*)$} $v -> cpu vendor os]} {
			user-error "Invalid canonical $type: $v"
		}
		define ${type}_cpu $cpu
		define ${type}_vendor $vendor
		define ${type}_os $os
	}

	opt-str prefix prefix /usr/local

	# These are for compatibility with autoconf
	define target [get-define host]
	define prefix $prefix
	define builddir $autosetup(builddir)
	define srcdir $autosetup(srcdir)
	define top_srcdir $autosetup(srcdir)
	define abs_top_srcdir [file-normalize $autosetup(srcdir)]
	define abs_top_builddir [file-normalize $autosetup(builddir)]

	# autoconf supports all of these
	define exec_prefix [opt-str exec-prefix exec_prefix $prefix]
	foreach {name defpath} {
		bindir /bin
		sbindir /sbin
		libexecdir /libexec
		libdir /lib
	} {
		define $name [opt-str $name o $exec_prefix$defpath]
	}
	foreach {name defpath} {
		datadir /share
		sharedstatedir /com
		infodir /share/info
		mandir /share/man
		includedir /include
	} {
		define $name [opt-str $name o $prefix$defpath]
	}
	if {$prefix ne {/usr}} {
		opt-str sysconfdir sysconfdir $prefix/etc
	} else {
		opt-str sysconfdir sysconfdir /etc
	}
	define sysconfdir $sysconfdir

	define localstatedir [opt-str localstatedir o /var]
	define runstatedir [opt-str runstatedir o /run]

	define SHELL [get-env SHELL [find-an-executable sh bash ksh]]

	# These could be used to generate Makefiles following some automake conventions
	define AM_SILENT_RULES [opt-bool silent-rules]
	define AM_MAINTAINER_MODE [opt-bool maintainer-mode]
	define AM_DEPENDENCY_TRACKING [opt-bool dependency-tracking]

	# Windows vs. non-Windows
	switch -glob -- [get-define host] {
		*-*-ming* - *-*-cygwin - *-*-msys {
			define-feature windows
			define EXEEXT .exe
		}
		default {
			define EXEEXT ""
		}
	}

	# Display
	msg-result "Host System...[get-define host]"
	msg-result "Build System...[get-define build]"
}

system-init
