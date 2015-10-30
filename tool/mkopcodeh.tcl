#!/usr/bin/tclsh
#
# Generate the file opcodes.h.
#
# This TCL script scans a concatenation of the parse.h output file from the
# parser and the vdbe.c source file in order to generate the opcodes numbers
# for all opcodes.  
#
# The lines of the vdbe.c that we are interested in are of the form:
#
#       case OP_aaaa:      /* same as TK_bbbbb */
#
# The TK_ comment is optional.  If it is present, then the value assigned to
# the OP_ is the same as the TK_ value.  If missing, the OP_ value is assigned
# a small integer that is different from every other OP_ value.
#
# We go to the trouble of making some OP_ values the same as TK_ values
# as an optimization.  During parsing, things like expression operators
# are coded with TK_ values such as TK_ADD, TK_DIVIDE, and so forth.  Later
# during code generation, we need to generate corresponding opcodes like
# OP_Add and OP_Divide.  By making TK_ADD==OP_Add and TK_DIVIDE==OP_Divide,
# code to translate from one to the other is avoided.  This makes the
# code generator run (infinitesimally) faster and more importantly it makes
# the library footprint smaller.
#
# This script also scans for lines of the form:
#
#       case OP_aaaa:       /* jump, in1, in2, in3, out2-prerelease, out3 */
#
# When such comments are found on an opcode, it means that certain
# properties apply to that opcode.  Set corresponding flags using the
# OPFLG_INITIALIZER macro.
#

set in stdin
set currentOp {}
set nOp 0
while {![eof $in]} {
  set line [gets $in]

  # Remember the TK_ values from the parse.h file. 
  # NB:  The "TK_" prefix stands for "ToKen", not the graphical Tk toolkit
  # commonly associated with TCL.
  #
  if {[regexp {^#define TK_} $line]} {
    set tk([lindex $line 1]) [lindex $line 2]
    continue
  }

  # Find "/* Opcode: " lines in the vdbe.c file.  Each one introduces
  # a new opcode.  Remember which parameters are used.
  #
  if {[regexp {^.. Opcode: } $line]} {
    set currentOp OP_[lindex $line 2]
    set m 0
    foreach term $line {
      switch $term {
        P1 {incr m 1}
        P2 {incr m 2}
        P3 {incr m 4}
        P4 {incr m 8}
        P5 {incr m 16}
      }
    }
    set paramused($currentOp) $m
  }

  # Find "** Synopsis: " lines that follow Opcode:
  #
  if {[regexp {^.. Synopsis: (.*)} $line all x] && $currentOp!=""} {
    set synopsis($currentOp) [string trim $x]
  }

  # Scan for "case OP_aaaa:" lines in the vdbe.c file
  #
  if {[regexp {^case OP_} $line]} {
    set line [split $line]
    set name [string trim [lindex $line 1] :]
    set op($name) -1
    set jump($name) 0
    set in1($name) 0
    set in2($name) 0
    set in3($name) 0
    set out1($name) 0
    set out2($name) 0
    for {set i 3} {$i<[llength $line]-1} {incr i} {
       switch [string trim [lindex $line $i] ,] {
         same {
           incr i
           if {[lindex $line $i]=="as"} {
             incr i
             set sym [string trim [lindex $line $i] ,]
             set val $tk($sym)
             set op($name) $val
             set used($val) 1
             set sameas($val) $sym
             set def($val) $name
           }
         }
         jump {set jump($name) 1}
         in1  {set in1($name) 1}
         in2  {set in2($name) 1}
         in3  {set in3($name) 1}
         out2 {set out2($name) 1}
         out3 {set out3($name) 1}
       }
    }
    set order($nOp) $name
    incr nOp
  }
}

# Assign numbers to all opcodes and output the result.
#
set cnt 0
set max 0
puts "/* Automatically generated.  Do not edit */"
puts "/* See the tool/mkopcodeh.tcl script for details */"
set op(OP_Noop) -1
set order($nOp) OP_Noop
incr nOp
set op(OP_Explain) -1
set order($nOp) OP_Explain
incr nOp

# The following are the opcodes that are processed by resolveP2Values()
#
set rp2v_ops {
  OP_Transaction
  OP_AutoCommit
  OP_Savepoint
  OP_Checkpoint
  OP_Vacuum
  OP_JournalMode
  OP_VUpdate
  OP_VFilter
  OP_Next
  OP_NextIfOpen
  OP_SorterNext
  OP_Prev
  OP_PrevIfOpen
}

# Assign small values to opcodes that are processed by resolveP2Values()
# to make code generation for the switch() statement smaller and faster.
#
set cnt 0
for {set i 0} {$i<$nOp} {incr i} {
  set name $order($i)
  if {[lsearch $rp2v_ops $name]>=0} {
    incr cnt
    while {[info exists used($cnt)]} {incr cnt}
    set op($name) $cnt
    set used($cnt) 1
    set def($cnt) $name
  }
}

# Generate the numeric values for remaining opcodes
#
for {set i 0} {$i<$nOp} {incr i} {
  set name $order($i)
  if {$op($name)<0} {
    incr cnt
    while {[info exists used($cnt)]} {incr cnt}
    set op($name) $cnt
    set used($cnt) 1
    set def($cnt) $name
  }
}
set max $cnt
for {set i 1} {$i<=$nOp} {incr i} {
  if {![info exists used($i)]} {
    set def($i) "OP_NotUsed_$i"
  }
  set name $def($i)
  puts -nonewline [format {#define %-16s %3d} $name $i]
  set com {}
  if {[info exists sameas($i)]} {
    set com "same as $sameas($i)"
  }
  if {[info exists synopsis($name)]} {
    set x $synopsis($name)
    if {$com==""} {
      set com "synopsis: $x"
    } else {
      append com ", synopsis: $x"
    }
  }
  if {$com!=""} {
    puts -nonewline [format " /* %-42s */" $com]
  }
  puts ""
}

# Generate the bitvectors:
#
set bv(0) 0
for {set i 1} {$i<=$max} {incr i} {
  set name $def($i)
  if {[info exists jump($name)] && $jump($name)} {set a0 1}  {set a0 0}
  if {[info exists in1($name)] && $in1($name)}   {set a1 2}  {set a1 0}
  if {[info exists in2($name)] && $in2($name)}   {set a2 4}  {set a2 0}
  if {[info exists in3($name)] && $in3($name)}   {set a3 8}  {set a3 0}
  if {[info exists out2($name)] && $out2($name)} {set a4 16} {set a4 0}
  if {[info exists out3($name)] && $out3($name)} {set a5 32} {set a5 0}
  set bv($i) [expr {$a0+$a1+$a2+$a3+$a4+$a5}]
}
puts ""
puts "/* Properties such as \"out2\" or \"jump\" that are specified in"
puts "** comments following the \"case\" for each opcode in the vdbe.c"
puts "** are encoded into bitvectors as follows:"
puts "*/"
puts "#define OPFLG_JUMP            0x0001  /* jump:  P2 holds jmp target */"
puts "#define OPFLG_IN1             0x0002  /* in1:   P1 is an input */"
puts "#define OPFLG_IN2             0x0004  /* in2:   P2 is an input */"
puts "#define OPFLG_IN3             0x0008  /* in3:   P3 is an input */"
puts "#define OPFLG_OUT2            0x0010  /* out2:  P2 is an output */"
puts "#define OPFLG_OUT3            0x0020  /* out3:  P3 is an output */"
puts "#define OPFLG_INITIALIZER \173\\"
for {set i 0} {$i<=$max} {incr i} {
  if {$i%8==0} {
    puts -nonewline [format "/* %3d */" $i]
  }
  puts -nonewline [format " 0x%02x," $bv($i)]
  if {$i%8==7} {
    puts "\\"
  }
}
puts "\175"
