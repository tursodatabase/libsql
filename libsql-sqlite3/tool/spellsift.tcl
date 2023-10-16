#!/usr/bin/tclsh

set usage {
  Usage: spellsift.tcl <source_filenames>
  The named .c and .h source files comment blocks are spell-checked.
}

if {[llength $argv] == 0} {
  puts stderr $usage
  exit 0
}

# Want a Tcl version with 3-argument close.
package require Tcl 8.6

set ::spellchk "aspell --extra-dicts ./custom.rws list"

# Run text through aspell with custom dictionary, return finds.
proc misspelled {text} {
  set spellerr [open "|$::spellchk" r+]
  puts $spellerr $text
  flush $spellerr
  close $spellerr write
  set huhq [regsub {\s*$} [read $spellerr] {}]
  close $spellerr read
  return [split $huhq "\n"]
}

# Eliminate some common patterns that need not be well spelled.
proc decruft {text} {
  set nopp [regsub -all "\n *#\[^\n\]*\n" $text "\n\n" ]
  set noticket [regsub -all {Ticket \[?[0-9a-f]+\]?} $nopp "" ]
  return $noticket
}

# Sift out common variable spellings not in normal dictionaries.
proc varsift {words} {
  set rv [list]
  foreach w $words {
    set n [string length $w]
    set cr [string range $w 1 end]
    if {[string tolower $cr] ne $cr} continue
    lappend rv $w;
  }
  return $rv
}

foreach fname $argv {
  set ich [open $fname r]
  set dtext [decruft [read $ich]]
  close $ich
  set cbounds [regexp -indices -inline -all {(/\*)|(\*/)} $dtext]
  set ccb -1
  set cblocks [list]
  foreach {ap cb ce} $cbounds {
    set cib [lindex $cb 1]
    set cie [lindex $ce 0]
    if {$cie != -1} {
      if {$ccb != -1} {
        set cce [expr $cie - 1]
        set destar [string map [list * " "] [string range $dtext $ccb $cce]]
        lappend cblocks $destar
        set ccb -1
      } else continue
    } elseif {$cib != -1} {
      set ccb [expr $cib + 1]
    }
  }
  set oddspells [varsift [misspelled [join $cblocks "\n"]]]
  if {[llength $oddspells] > 0} {
    puts "!? Misspellings from $fname:"
    puts [join [lsort -nocase -unique $oddspells] "\n"]
  }
}
