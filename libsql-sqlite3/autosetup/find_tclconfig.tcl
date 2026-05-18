#
# Run this TCL script to find and print the pathname for the tclConfig.sh
# file.  Used by ../configure
#
if {[catch {
  set libdir [tcl::pkgconfig get libdir,install]
}]} {
  puts stderr "tclsh too old:  does not support tcl::pkgconfig"
  exit 1
}
if {![file exists $libdir]} {
  puts stderr "tclsh reported library directory \"$libdir\" does not exist"
  exit 1
}
if {![file exists $libdir/tclConfig.sh]} {
  set n1 $libdir/tcl$::tcl_version
  if {[file exists $n1/tclConfig.sh]} {
    set libdir $n1
  } else {
    puts stderr "cannot find tclConfig.sh in either $libdir or $n1"
    exit 1
  }
}
puts $libdir
