# This script attempts to install SQLite3 so that it can be used
# by TCL.  Invoke this script with single argument which is the
# version number of SQLite.  Example:
#
#    tclsh tclinstaller.tcl 3.0
#
set VERSION [lindex $argv 0]
set LIBFILE .libs/libtclsqlite3[info sharedlibextension]
if { ![info exists env(DESTDIR)] } { set env(DESTDIR) "" }
set LIBDIR $env(DESTDIR)[lindex $auto_path 0]
set LIBNAME [file tail $LIBFILE]
set LIB $LIBDIR/sqlite3/$LIBNAME

file delete -force $LIBDIR/sqlite3
file mkdir $LIBDIR/sqlite3
set fd [open $LIBDIR/sqlite3/pkgIndex.tcl w]
puts $fd "package ifneeded sqlite3 $VERSION \[list load $LIB sqlite3\]"
close $fd

# We cannot use [file copy] because that will just make a copy of
# a symbolic link.  We have to open and copy the file for ourselves.
#
set in [open $LIBFILE]
fconfigure $in -translation binary
set out [open $LIB w]
fconfigure $out -translation binary
puts -nonewline $out [read $in]
close $in
close $out
