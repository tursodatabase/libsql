#!/usr/bin/tclsh
#
set help \
{Run this TCL script to build and install the TCL interface library for
SQLite.  Run the script with the specific "tclsh" for which the installation
should occur.

There must be a valid "tclsqlite3.c" file in the working directory prior
to running this script.  Use "make tclsqlite3.c" to generate that file.

Options:

   --build-only         Only build the extension, don't install it
   --cc COMPILER        Build using this compiler
   --info               Show info on existing SQLite TCL extension installs
   --install-only       Install an extension previously build
   --uninstall          Uninstall the extension
   --version-check      Check extension version against this source tree
   --destdir DIR        Installation root (used by "make install DESTDIR=...")
   --tclConfig.sh FILE  Use this tclConfig.sh instead of looking for one

Other options are retained and passed through into the compiler.}


set build 1
set install 1
set uninstall 0
set infoonly 0
set versioncheck 0
set CC {}
set OPTS {}
set DESTDIR ""; # --destdir "$(DESTDIR)"
set tclConfigSh ""; # --tclConfig.sh FILE
for {set ii 0} {$ii<[llength $argv]} {incr ii} {
  set a0 [lindex $argv $ii]
  if {$a0=="--install-only"} {
    set build 0
  } elseif {$a0=="--build-only"} {
    set install 0
  } elseif {$a0=="--uninstall"} {
    set build 0
    set install 0
    set versioncheck 0
    set uninstall 1
  } elseif {$a0=="--info"} {
    set build 0
    set install 0
    set versioncheck 0
    set infoonly 1
  } elseif {$a0=="--version-check"} {
    set build 0
    set install 0
    set infoonly 0
    set versioncheck 1
  } elseif {$a0=="--cc" && $ii+1<[llength $argv]} {
    incr ii
    set CC [lindex $argv $ii]
  } elseif {$a0=="--destdir" && $ii+1<[llength $argv]} {
    incr ii
    set DESTDIR [lindex $argv $ii]
  } elseif {$a0=="--tclConfig.sh" && $ii+1<[llength $argv]} {
    incr ii
    set tclConfigSh [lindex $argv $ii]
  } elseif {[string match -* $a0]} {
    append OPTS " $a0"
  } else {
    puts stderr "Unknown option: \"$a0\"\n"
    puts stderr $help
    exit 1
  }
}

# Find the root of the SQLite source tree
#
set srcdir [file normalize [file dir $argv0]/..]

# Get the SQLite version number into $VERSION
#
set fd [open $srcdir/VERSION]
set VERSION [string trim [read $fd]]
close $fd

if {$tcl_platform(platform) eq "windows"} {
  # We are only able to install, uninstall, and list on Windows.
  # The build process is handled by the Makefile.msc, specifically
  # using "nmake /f Makefile.msc pkgIndex.tcl tclsqlite3.dll"
  #
  if {$build} {
    puts "Unable to build on Windows using the builttclext.tcl script."
    puts "To build, run\n"
    puts "   \"nmake /f Makefile.msc pkgIndex.tcl tclsqlite3.dll"
    exit 1
  }
  set OUT tclsqlite3.dll
} else {
  # Read the tclConfig.sh file into the $tclConfig variable
  #
  if {"" eq $tclConfigSh} {
    # Figure out the location of the tclConfig.sh file used by the
    # tclsh that is executing this script.
    #
    if {[catch {
      set LIBDIR [tcl::pkgconfig get libdir,install]
    }]} {
      puts stderr "$argv0: tclsh does not support tcl::pkgconfig."
      exit 1
    }
    if {![file exists $LIBDIR]} {
      puts stderr "$argv0: cannot find the tclConfig.sh file."
      puts stderr "$argv0: tclsh reported library directory \"$LIBDIR\"\
                 does not exist."
      exit 1
    }
    if {![file exists $LIBDIR/tclConfig.sh]
        || [file size $LIBDIR/tclConfig.sh]<5000} {
      set n1 $LIBDIR/tcl$::tcl_version
      if {[file exists $n1/tclConfig.sh]
          && [file size $n1/tclConfig.sh]>5000} {
        set LIBDIR $n1
      } else {
        puts stderr "$argv0: cannot find tclConfig.sh in either $LIBDIR or $n1"
        exit 1
      }
    }
    #puts "using $LIBDIR/tclConfig.sh"
    set fd [open $LIBDIR/tclConfig.sh rb]
    set tclConfig [read $fd]
    close $fd
  } else {
    # User-provided tclConfig.sh
    #
    set fd [open $tclConfigSh rb]
    set tclConfig [read $fd]
    close $fd
  }

  # Extract parameter we will need from the tclConfig.sh file
  #
  set TCLMAJOR 8
  regexp {TCL_MAJOR_VERSION='(\d)'} $tclConfig all TCLMAJOR
  set SUFFIX so
  regexp {TCL_SHLIB_SUFFIX='\.([^']+)'} $tclConfig all SUFFIX
  if {$CC==""} {
    set cc {}
    regexp {TCL_CC='([^']+)'} $tclConfig all cc
    if {$cc!=""} {
      set CC $cc
    }
  }
  if {$CC==""} {
    set CC gcc
  }
  set CFLAGS -fPIC
  regexp {TCL_SHLIB_CFLAGS='([^']+)'} $tclConfig all CFLAGS
  set LIBS {}
  regexp {TCL_STUB_LIB_SPEC='([^']+)'} $tclConfig all LIBS
  set INC "-I$srcdir/src"
  set inc {}
  regexp {TCL_INCLUDE_SPEC='([^']+)'} $tclConfig all inc
  if {$inc!=""} {
    append INC " $inc"
  }
  set cmd {${CC} ${CFLAGS} ${LDFLAGS} -shared}
  regexp {TCL_SHLIB_LD='([^']+)(-Wl,--out-implib.*)?'} $tclConfig all cmd
  set LDFLAGS "$INC -DUSE_TCL_STUBS"
  if {[string length $OPTS]>1} {
    append LDFLAGS $OPTS
  }
  if {$tcl_platform(os) eq "Windows NT"} {
    set OUT cyg
  } else {
    set OUT lib
  }
  if {$TCLMAJOR>8} {
    set OUT ${OUT}tcl9sqlite$VERSION.$SUFFIX
  } else {
    set OUT ${OUT}sqlite$VERSION.$SUFFIX
  }
  set @ $OUT; # Workaround for https://sqlite.org/forum/forumpost/0683a49cb02f31a1
              # in which Gentoo edits their tclConfig.sh to include an soname
              # linker flag which includes ${@} (the target file's name).
  set CMD [subst $cmd]
}

# Check the SQLite TCL extension that is loaded by default by this running
# TCL interpreter to see if it has the same SQLITE_SOURCE_ID as the source
# code in the directory holding this script.
#
if {$versioncheck} {
  if {[catch {package require sqlite3} msg]} {
    puts stderr "No SQLite TCL extension available: $msg"
    exit 1
  }
  sqlite3 db :memory:
  set extvers [db one {SELECT sqlite_source_id()}]
  db close
  set fd [open sqlite3.h rb]
  set sqlite3h [read $fd]
  close $fd
  regexp {#define SQLITE_SOURCE_ID +"([^"]+)"} $sqlite3h all srcvers
  set srcvers [string range $srcvers 0 78]
  set extvers [string range $extvers 0 78]
  if {$srcvers==$extvers} {
    puts "source code and extension versions aligned:\n$extvers"
    exit 0
  }
  puts stderr "source code and extension versions differ"
  puts stderr "source: $srcvers\nextension: $extvers"
  exit 1
}

# Show information about prior installs
#
if {$infoonly} {
  set cnt 0
  foreach dir $auto_path {
    foreach subdir [glob -nocomplain -types d $dir/sqlite3*] {
      if {[file exists $subdir/pkgIndex.tcl]} {
        puts $subdir
        incr cnt
      }
    }
  }
  if {$cnt==0} {
    puts "no current installations of the SQLite TCL extension"
  }
  exit
}

# Uninstall the extension
#
if {$uninstall} {
  set cnt 0
  foreach dir $auto_path {
    if {[file isdirectory $dir/sqlite$VERSION]} {
      incr cnt
      if {![file writable $dir] || ![file writable $dir/sqlite$VERSION]} {
        puts "cannot uninstall $dir/sqlite$VERSION - permission denied"
      } else {
        puts "uninstalling $dir/sqlite$VERSION..."
        file delete -force $dir/sqlite$VERSION
      }
    }
  }
  if {$cnt==0} {
    puts "nothing to uninstall"
  }
  exit
}

if {$install} {
  # Figure out where the extension will be installed.  Put the extension
  # in the first writable directory on $auto_path.
  #
  set DEST {}
  foreach dir $auto_path {
    if {[string match //*:* $dir]} {
      # We can't install to //zipfs: paths
      continue
    } elseif {"" ne $DESTDIR && ![file writable $DESTDIR]} {
      # In the common case, ${DESTDIR}${dir} will not exist when we
      # get to this point of the installation, and the "is writable?"
      # check just below this will fail for that case.
      #
      # Assumption made for simplification's sake: if ${DESTDIR} is
      # not writable, no part of the remaining path will
      # be. ${DESTDIR} is typically used by OS package maintainers,
      # not normal installations, and it "shouldn't" ever happen that
      # the DESTDIR is read-only while the target ${DESTDIR}${prefix}
      # is not, as it's typical for such installations to create
      # ${prefix} on-demand under ${DESTDIR}.
      break
    }
    set dir ${DESTDIR}$dir
    if {[file writable $dir] || "" ne $DESTDIR} {
      # the dir will be created later ^^^^^^^^
      set DEST $dir
      break
    } elseif {[glob -nocomplain $dir/sqlite3*/pkgIndex.tcl]!=""} {
      set conflict [lindex [glob $dir/sqlite3*/pkgIndex.tcl] 0]
      puts "Unable to install. There is already a conflicting version"
      puts "of the SQLite TCL Extension that cannot be overwritten at\n"
      puts "   [file dirname $conflict]\n"
      puts "Consider running using sudo to work around this problem."
      exit 1
    }
  }
  if {$DEST==""} {
    puts "None of the directories on \$auto_path are writable by this process,"
    puts "so the installation cannot take place.  Consider running using sudo"
    puts "to work around this problem.\n"
    puts "These are the (unwritable) \$auto_path directories:\n"
    foreach dir $auto_path {
      puts "  *  ${DESTDIR}$dir"
    }
    exit 1
  }
}

if {$build} {
  # Generate the pkgIndex.tcl file
  #
  puts "generating pkgIndex.tcl..."
  set fd [open pkgIndex.tcl w]
  puts $fd [subst -nocommands {# -*- tcl -*-
# Tcl package index file, version ???
#
package ifneeded sqlite3 $VERSION \\
    [list load [file join \$dir $OUT] Sqlite3]
}]
  close $fd

  # Generate and execute the command with which to do the compilation.
  #
  set cmd "$CMD -DUSE_TCL_STUBS tclsqlite3.c -o $OUT $LIBS"
  puts $cmd
  file delete -force $OUT
  catch {exec {*}$cmd} errmsg
  if {$errmsg!="" && ![file exists $OUT]} {
    puts $errmsg
    exit 1
  }
}


if {$install} {
  # Install the extension
  set DEST2 $DEST/sqlite$VERSION
  file mkdir $DEST2
  puts "installing $DEST2/pkgIndex.tcl"
  file copy -force pkgIndex.tcl $DEST2
  puts "installing $DEST2/$OUT"
  file copy -force $OUT $DEST2
}
