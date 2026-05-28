# Copyright (c) 2010 WorkWare Systems http://www.workware.net.au/
# All rights reserved

# @synopsis:
#
# The 'cc-shared' module provides support for shared libraries and shared objects.
# It defines the following variables:
#
## SH_CFLAGS         Flags to use compiling sources destined for a shared library
## SH_LDFLAGS        Flags to use linking (creating) a shared library
## SH_SOPREFIX       Prefix to use to set the soname when creating a shared library
## SH_SOFULLPATH     Set to 1 if the shared library soname should include the full install path
## SH_SOEXT          Extension for shared libs
## SH_SOEXTVER       Format for versioned shared libs - %s = version
## SHOBJ_CFLAGS      Flags to use compiling sources destined for a shared object
## SHOBJ_LDFLAGS     Flags to use linking a shared object, undefined symbols allowed
## SHOBJ_LDFLAGS_R   - as above, but all symbols must be resolved
## SH_LINKRPATH      Format for setting the rpath when linking an executable, %s = path
## SH_LINKFLAGS      Flags to use linking an executable which will load shared objects
## LD_LIBRARY_PATH   Environment variable which specifies path to shared libraries
## STRIPLIBFLAGS     Arguments to strip a dynamic library

options {}

# Defaults: gcc on unix
define SHOBJ_CFLAGS -fPIC
define SHOBJ_LDFLAGS -shared
define SH_CFLAGS -fPIC
define SH_LDFLAGS -shared
define SH_LINKFLAGS -rdynamic
define SH_LINKRPATH "-Wl,-rpath -Wl,%s"
define SH_SOEXT .so
define SH_SOEXTVER .so.%s
define SH_SOPREFIX -Wl,-soname,
define LD_LIBRARY_PATH LD_LIBRARY_PATH
define STRIPLIBFLAGS --strip-unneeded

# Note: This is a helpful reference for identifying the toolchain
#       http://sourceforge.net/apps/mediawiki/predef/index.php?title=Compilers

switch -glob -- [get-define host] {
	*-*-darwin* {
		define SHOBJ_CFLAGS "-dynamic -fno-common"
		define SHOBJ_LDFLAGS "-bundle -undefined dynamic_lookup"
		define SHOBJ_LDFLAGS_R -bundle
		define SH_CFLAGS -dynamic
		define SH_LDFLAGS -dynamiclib
		define SH_LINKFLAGS ""
		define SH_SOEXT .dylib
		define SH_SOEXTVER .%s.dylib
		define SH_SOPREFIX -Wl,-install_name,
		define SH_SOFULLPATH
		define LD_LIBRARY_PATH DYLD_LIBRARY_PATH
		define STRIPLIBFLAGS -x
	}
	*-*-ming* - *-*-cygwin - *-*-msys {
		define SHOBJ_CFLAGS ""
		define SHOBJ_LDFLAGS -shared
		define SH_CFLAGS ""
		define SH_LDFLAGS -shared
		define SH_LINKRPATH ""
		define SH_LINKFLAGS ""
		define SH_SOEXT .dll
		define SH_SOEXTVER .dll
		define SH_SOPREFIX ""
		define LD_LIBRARY_PATH PATH
	}
	sparc* {
		if {[msg-quiet cc-check-decls __SUNPRO_C]} {
			msg-result "Found sun stdio compiler"
			# sun stdio compiler
			# XXX: These haven't been fully tested.
			define SHOBJ_CFLAGS -KPIC
			define SHOBJ_LDFLAGS "-G"
			define SH_CFLAGS -KPIC
			define SH_LINKFLAGS -Wl,-export-dynamic
			define SH_SOPREFIX -Wl,-h,
		}
	}
	*-*-solaris* {
		if {[msg-quiet cc-check-decls __SUNPRO_C]} {
			msg-result "Found sun stdio compiler"
			# sun stdio compiler
			# XXX: These haven't been fully tested.
			define SHOBJ_CFLAGS -KPIC
			define SHOBJ_LDFLAGS "-G"
			define SH_CFLAGS -KPIC
			define SH_LINKFLAGS -Wl,-export-dynamic
			define SH_SOPREFIX -Wl,-h,
		}
	}
	*-*-hpux {
		# XXX: These haven't been tested
		define SHOBJ_CFLAGS "+O3 +z"
		define SHOBJ_LDFLAGS -b
		define SH_CFLAGS +z
		define SH_LINKFLAGS -Wl,+s
		define LD_LIBRARY_PATH SHLIB_PATH
	}
	*-*-haiku {
		define SHOBJ_CFLAGS ""
		define SHOBJ_LDFLAGS -shared
		define SH_CFLAGS ""
		define SH_LDFLAGS -shared
		define SH_LINKFLAGS ""
		define SH_SOPREFIX ""
		define LD_LIBRARY_PATH LIBRARY_PATH
	}
}

if {![is-defined SHOBJ_LDFLAGS_R]} {
	define SHOBJ_LDFLAGS_R [get-define SHOBJ_LDFLAGS]
}
