#!/usr/bin/tcl
#
# Run this script to test to see that the latest trunk changes can be
# merged into LTS branches without breaking anything.
#
# To Use:
#
#   *  Copy this script into a directory above the sqlite checkout
#   *  Run "fossil update trunk" and "fossil revert"
#   *  Run "tclsh ../merge-test.tcl"  (in other words run this script)
#
# Operation:
#
# This script changes to each LTS branch to be tested, merges the latest
# trunk changes into the branch (without committing them) and then
# runs "make test".  Any errors are stored in local files.
#
# Limitations:
#
# Some LTS branches are not synced directly from trunk but rather from
# other LTS branches.  These other branches cannot be tested because
# there is no good way to generate the intermediate merges.
#
###############################################################################

# Run a shell command contained in arguments.  Put the return code in
# global variable ::res and the output string in global variable ::result
#
proc safeexec {args} {
  global res result
  set res [catch "exec $args" result]
}

# Run the shell command contained in arguments.  Print an error and exit
# if anything goes wrong.
#
proc mustbeok {args} {
  global res result
  set res [catch "exec $args" result]
  if {$res} {
    puts "FAILED: $args"
    puts $result
    exit 1
  }
}

# Write $content into a file named $filename.  The file is overwritten if it
# already exist.  The file is create if it does not already exist.
#
proc writefile {filename content} {
  set fd [open $filename wb]
  puts $fd $content
  close $fd
}

# Run the merge-test
#
foreach {branch configopts} {
  begin-concurrent         {--enable-json1}
  begin-concurrent-pnu     {--enable-json1}
  wal2                     {--enable-all}
  reuse-schema             {--enable-all}
} {
  puts $branch
  set errorfile ${branch}-error.txt
  mustbeok fossil revert
  mustbeok fossil up $branch
  safeexec fossil merge trunk
  if {$res} {
    puts "   merge failed - see $errorfile"
    writefile $errorfile $result
  } else {
    puts "   merge ok"
    safeexec  ./configure --enable-debug {*}$configopts
    if {$res} {
      puts "   configure failed - see $errorfile"
      writefile $errorfile $result
    } else {
      puts "   configure ok"
      safeexec make fuzzcheck sqlite3 testfixture
      if {$res} {
        puts "   build failed - see $errorfile"
        writefile $errorfile $result
      } else {
        puts "   build ok"
        safeexec make test
        if {$res} {
          puts "   test failed - see $errorfile"
          writefile $errorfile $result
        } else {
          puts "   test ok"
        }
      }
    }
  }
}
mustbeok fossil revert
mustbeok fossil up trunk
puts "reset back to trunk"
