#!/usr/bin/tclsh
#
# To build a single huge source file holding all of SQLite (or at
# least the core components - the test harness, shell, and TCL 
# interface are omitted.) first do
#
#      make target_source
#
# Then run this script
#
#      tclsh mkonebigsourcefile.tcl
#
# The combined SQLite source code will be written into sqlite3.c
#

# Open the output file and write a header comment at the beginning
# of the file.
#
set out [open sqlite3.c w]
puts $out \
"/******************************************************************************
** This file is a amalgamation of many separate source files from SQLite.  By
** pulling all the source files into this single unified source file, the
** entire code can be compiled as a single translation unit, which allows the
** compiler to do a better job of optimizing.
*/"

# These are the header files used by SQLite.  The first time any of these 
# files are seen in a #include statement in the C code, include the complete
# text of the file in-line.  The file only needs to be included once.
#
foreach hdr {
   btree.h
   hash.h
   keywordhash.h
   opcodes.h
   os_common.h
   os.h
   os_os2.h
   pager.h
   parse.h
   sqlite3ext.h
   sqlite3.h
   sqliteInt.h
   vdbe.h
   vdbeInt.h
} {
  set available_hdr($hdr) 1
}

# 78 stars used for comment formatting.
set s78 \
{*****************************************************************************}

# Insert a comment into the code
#
proc section_comment {text} {
  global out s78
  set n [string length $text]
  set nstar [expr {60 - $n}]
  set stars [string range $s78 0 $nstar]
  puts $out "/************** $text $stars/"
}

# Read the source file named $filename and write it into the
# sqlite3.c output file.  If any #include statements are seen,
# process them approprately.
#
proc copy_file {filename} {
  global seen_hdr available_hdr out
  set tail [file tail $filename]
  section_comment "Begin file $tail"
  set in [open $filename r]
  while {![eof $in]} {
    set line [gets $in]
    if {[regexp {^#\s*include\s+["<]([^">]+)[">]} $line all hdr]} {
      if {[info exists available_hdr($hdr)]} {
        if {$available_hdr($hdr)} {
          if {$hdr!="os_common.h"} {
            set available_hdr($hdr) 0
          }
          section_comment "Include $hdr in the middle of $tail"
          copy_file tsrc/$hdr
          section_comment "Continuing where we left off in $tail"
        }
      } elseif {![info exists seen_hdr($hdr)]} {
        set seen_hdr($hdr) 1
        puts $out $line
      }
    } elseif {[regexp {^#ifdef __cplusplus} $line]} {
      puts $out "#if 0"
    } elseif {[regexp {^#line} $line]} {
      # Skip #line directives.
    } else {
      puts $out $line
    }
  }
  close $in
  section_comment "End of $tail"
}


# Process the source files.  Process files containing commonly
# used subroutines first in order to help the compiler find
# inlining opportunities.
#
foreach file {
   os.c

   printf.c
   random.c
   utf.c
   util.c
   hash.c
   opcodes.c

   os_os2.c
   os_unix.c
   os_win.c

   pager.c
   
   btree.c

   vdbefifo.c
   vdbemem.c
   vdbeaux.c
   vdbeapi.c
   vdbe.c

   expr.c
   alter.c
   analyze.c
   attach.c
   auth.c
   build.c
   callback.c
   complete.c
   date.c
   delete.c
   func.c
   insert.c
   legacy.c
   loadext.c
   pragma.c
   prepare.c
   select.c
   table.c
   trigger.c
   update.c
   vacuum.c
   vtab.c
   where.c

   parse.c

   tokenize.c

   main.c
} {
  copy_file tsrc/$file
}

if 0 {
puts $out "#ifdef SQLITE_TEST"
foreach file {
   test1.c
   test2.c
   test3.c
   test4.c
   test5.c
   test6.c
   test7.c
   test8.c
   test_async.c
   test_autoext.c
   test_loadext.c
   test_md5.c
   test_schema.c
   test_server.c
   test_tclvar.c
} {
  copy_file ../sqlite/src/$file
}
puts $out "#endif /* SQLITE_TEST */"
puts $out "#ifdef SQLITE_TCL"
copy_file ../sqlite/src/tclsqlite.c
puts $out "#endif /* SQLITE_TCL */"
}

close $out
