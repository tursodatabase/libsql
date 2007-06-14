#!/usr/bin/tclsh
#
# Run this script redirecting the sqlite3.h file as standard
# inputs and this script will generate API documentation.
#
set rcsid {$Id: mkapidoc.tcl,v 1.1 2007/06/14 20:57:19 drh Exp $}
source common.tcl
header {C/C++ Interface For SQLite Version 3}
puts {
<h2 class=pdf_section>C/C++ Interface For SQLite Version 3</h2>
}

# Scan standard input to extract the information we need
# to build the documentation.
#
set title {}
set type {}
set body {}
set code {}
set phase 0
set content {}
while {![eof stdin]} {
  set line [gets stdin]
  if {$phase==0} {
    # Looking for the CAPI3REF: keyword
    if {[regexp {^\*\* CAPI3REF: +(.*)} $line all tx]} {
      set title $tx
      set phase 1
    }
  } elseif {$phase==1} {
    if {[string range $line 0 1]=="**"} {
      set lx [string trim [string range $line 3 end]]
      if {[regexp {^CATEGORY: +([a-z]*)} $lx all cx]} {
        set type $cx
      } elseif {[regexp {^KEYWORDS: +(.*)} $lx all kx]} {
        foreach k $kx {
          set keyword($k) 1
        }
      } else {
        append body $lx\n
      }
    } elseif {[string range $line 0 1]=="*/"} {
      set phase 2
    }
  } elseif {$phase==2} {
    if {$line==""} {
      set kwlist [lsort [array names keyword]]
      unset -nocomplain keyword
      set key $type:$kwlist
      lappend content [list $key $title $type $kwlist $body $code]
      set title {}
      set keywords {}
      set type {}
      set body {}
      set code {}
      set phase 0
    } else {
      if {[regexp {^#define (SQLITE_[A-Z0-9_]+)} $line all kx]} {
        set type constant
        set keyword($kx) 1
      } elseif {[regexp {^typedef .* (sqlite[0-9a-z_]+);} $line all kx]} {
        set type datatype
        set keyword($kx) 1
      } elseif {[regexp {^[a-z].*[ *](sqlite3_[a-z0-9_]+)\(} $line all kx]} {
        set type function
        set keyword($kx) 1
      }
      append code $line\n
    }
  }
}

# Output HTML that displays the given list in N columns
#
proc output_list {N lx} {
  puts {<table width="100%" cellpadding="5"><tr>}
  set len [llength $lx]
  set n [expr {($len + $N - 1)/$N}]
  for {set i 0} {$i<$N} {incr i} {
    set start [expr {$i*$n}]
    set end [expr {($i+1)*$n}]
    puts {<td valign="top"><ul>}
    for {set j $start} {$j<$end} {incr j} {
      set entry [lindex $lx $j]
      if {$entry!=""} {
        foreach {link label} $entry break
        puts "<li><a href=\"#$link\">$label</a></li>"
      }
    }
    puts {</ul></td>}
  }
  puts {</tr></table>}
}

# Do a table of contents for objects
#
set objlist {}
foreach c $content {
  foreach {key title type keywords body code} $c break
  if {$type!="datatype"} continue
  set keywords [lsort $keywords]
  set k [lindex $keywords 0]
  foreach kw $keywords {
    lappend objlist [list $k $kw]
  }
}
puts {<h2>Datatypes:</h2>}
output_list 3 $objlist
puts {<hr>}

# Do a table of contents for constants
#
set clist {}
foreach c $content {
  foreach {key title type keywords body code} $c break
  if {$type!="constant"} continue
  set keywords [lsort $keywords]
  set k [lindex $keywords 0]
  foreach kw $keywords {
    lappend clist [list $k $kw]
  }
}
puts {<h2>Constants:</h2>}
set clist [lsort -index 1 $clist]
output_list 3 $clist
puts {<hr>}


# Do a table of contents for functions
#
set funclist {}
foreach c $content {
  foreach {key title type keywords body code} $c break
  if {$type!="function"} continue
  set keywords [lsort $keywords]
  set k [lindex $keywords 0]
  foreach kw $keywords {
    lappend funclist [list $k $kw]
  }
}
puts {<h2>Functions:</h2>}
set funclist [lsort -index 1 $funclist]
output_list 3 $funclist
puts {<hr>}

# Resolve links
#
proc resolve_links {args} {
  set tag [lindex $args 0]
  regsub -all {[^a-zA-Z0-9_]} $tag {} tag
  set x "<a href=\"#$tag\">"
  if {[llength $args]>2} {
    append x [lrange $args 2 end]</a>
  } else {
    append x [lindex $args 0]</a>
  }
  return $x
}

# Output all the records
#
foreach c [lsort $content] {
  foreach {key title type keywords body code} $c break
  foreach k $keywords {
    puts "<a name=\"$k\">"
  }
  puts "<h2>$title</h2>"
  puts "<blockquote><pre>"
  puts "$code"
  puts "</pre></blockquote>"
  regsub -all "\n\n+" $body {</p>\1<p>} body
  regsub -all {\[} <p>$body</p> {[resolve_links } body
  set body [subst -novar -noback $body]
  puts "$body"
  puts "<hr>"
}
