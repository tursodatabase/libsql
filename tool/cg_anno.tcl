#!/usr/bin/tclsh
#
# A wrapper around cg_annotate that sets appropriate command-line options
# and rearranges the output so that annotated files occur in a consistent
# sorted order.  Used by the run-speed-test.tcl script.
#

set in [open "|cg_annotate --show=Ir --auto=yes --context=40 $argv" r]
set dest !
set out(!) {}
while {![eof $in]} {
  set line [string map {\t {        }} [gets $in]]
  if {[regexp {^-- Auto-annotated source: (.*)} $line all name]} {
    set dest $name
  } elseif {[regexp {^-- line \d+ ------} $line]} {
    set line [lreplace $line 2 2 {#}]
  } elseif {[regexp {^The following files chosen for } $line]} {
    set dest !
  }
  append out($dest) $line\n
}
foreach x [lsort [array names out]] {
  puts $out($x)
}
