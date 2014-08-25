#
# 2014 August 24
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#--------------------------------------------------------------------------
#
# This script extracts the documentation for the API used by fts5 auxiliary 
# functions from header file fts5.h. It outputs html text on stdout that
# is included in the documentation on the web.
# 

set input_file [file join [file dir [info script]] fts5.h]
set fd [open $input_file]
set data [read $fd]
close $fd


# Argument $data is the entire text of the fts5.h file. This function 
# extracts the definition of the Fts5ExtensionApi structure from it and
# returns a key/value list of structure member names and definitions. i.e.
#
#   iVersion {int iVersion} xUserData {void *(*xUserData)(Fts5Context*)} ...
#
proc get_struct_members {data} {

  # Extract the structure definition from the fts5.h file.
  regexp "struct Fts5ExtensionApi {(.*)};" $data -> defn

  # Remove all comments from the structure definition
  regsub -all {/[*].*?[*]/} $defn {} defn2

  set res [list]
  foreach member [split $defn2 {;}] {

    set member [string trim $member]
    if {$member!=""} { 
      catch { set name [lindex $member end] }
      regexp {.*?[(][*]([^)]*)[)]} $member -> name
      lappend res $name $member
    }
  }

  set res
}

proc get_struct_docs {data names} {
  # Extract the structure definition from the fts5.h file.
  regexp {EXTENSION API FUNCTIONS(.*?)[*]/} $data -> docs

  set current_doc    ""
  set current_header ""

  foreach line [split $docs "\n"] {
    regsub {[*]*} $line {} line
    if {[regexp {^  } $line]} {
      append current_doc "$line\n"
    } elseif {[string trim $line]==""} {
      if {$current_header!=""} { append current_doc "\n" }
    } else {
      if {$current_doc != ""} {
        lappend res $current_header $current_doc
        set current_doc ""
      }
      set subject n/a
      regexp {^ *([[:alpha:]]*)} $line -> subject
      if {[lsearch $names $subject]>=0} {
        set current_header $subject
      } else {
        set current_header [string trim $line]
      }
    }
  }

  if {$current_doc != ""} {
    lappend res $current_header $current_doc
  }

  set res
}

# Initialize global array M as a map from Fts5StructureApi member name
# to member definition. i.e.
#
#   iVersion  -> {int iVersion}
#   xUserData -> {void *(*xUserData)(Fts5Context*)}
#   ...
#
array set M [get_struct_members $data]

# Initialize global list D as a map from section name to documentation
# text. Most (all?) section names are structure member names.
#
set D [get_struct_docs $data [array names M]]

foreach {hdr docs} $D {
  if {[info exists M($hdr)]} {
    set hdr $M($hdr)
  }
  puts "<h3><pre>  $hdr</pre></h3>"

  set mode ""
  set bEmpty 1
  foreach line [split [string trim $docs] "\n"] {
    if {[string trim $line]==""} {
      if {$mode != ""} {puts "</$mode>"}
      set mode ""
    } elseif {$mode == ""} {
      if {[regexp {^     } $line]} {
        set mode code
      } else {
        set mode p
      }
      puts "<$mode>"
    }
    puts $line
  }
  if {$mode != ""} {puts "</$mode>"}
}







