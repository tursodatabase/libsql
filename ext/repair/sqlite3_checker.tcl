# Read and run TCL commands from standard input.  Used to implement
# the --tclsh option.
# This TCL script is the main driver script for the sqlite3_checker utility
# program.
#

proc tclsh {} {
  set line {}
  while {![eof stdin]} {
    if {$line!=""} {
      puts -nonewline "> "
    } else {
      puts -nonewline "% "
    }
    flush stdout
    append line [gets stdin]
    if {[info complete $line]} {
      if {[catch {uplevel #0 $line} result]} {
        puts stderr "Error: $result"
      } elseif {$result!=""} {
        puts $result
      }
      set line {}
    } else {
      append line \n
    }
  }
}

proc usage {} {
  set argv0 [file rootname [file tail [info nameofexecutable]]]
  puts stderr "Usage: $argv0 OPTIONS database-filename"
  puts stderr {
Do sanity checking on a live SQLite3 database file specified by the
"database-filename" argument.

Options:

   --tclsh      Run the built-in TCL interpreter interactively (for debugging)

   --version    Show the version number of SQLite
}
  exit 1
}

set file_to_analyze {}
append argv {}
foreach arg $argv {
  if {[regexp {^-+tclsh$} $arg]} {
    tclsh
    exit 0
  }
  if {[regexp {^-+version$} $arg]} {
    sqlite3 mem :memory:
    puts [mem one {SELECT sqlite_version()||' '||sqlite_source_id()}]
    mem close
    exit 0
  }
  if {[regexp {^-} $arg]} {
    puts stderr "Unknown option: $arg"
    usage
  }
  if {$file_to_analyze!=""} {
    usage
  } else {
    set file_to_analyze $arg
  }
}
if {$file_to_analyze==""} usage

# If a TCL script is specified on the command-line, then run that
# script.
#
if {[file extension $file_to_analyze]==".tcl"} {
  source $file_to_analyze
  exit 0
}

set root_filename $file_to_analyze
regexp {^file:(//)?([^?]*)} $file_to_analyze all x1 root_filename
if {![file exists $root_filename]} {
  puts stderr "No such file: $root_filename"
  exit 1
}
if {![file readable $root_filename]} {
  puts stderr "File is not readable: $root_filename"
  exit 1
}
