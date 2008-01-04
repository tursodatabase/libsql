#!/usr/bin/awk -f
#
# Generate the file opcodes.h.
#
# This AWK script scans a concatenation of the parse.h output file from the
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
#       case OP_aaaa:       /* no-push */
#
# When the no-push comment is found on an opcode, it means that that
# opcode does not leave a result on the stack.  By identifying which
# opcodes leave results on the stack it is possible to determine a
# much smaller upper bound on the size of the stack.  This allows
# a smaller stack to be allocated, which is important to embedded
# systems with limited memory space.  This script generates a series
# of "NOPUSH_MASK" defines that contain bitmaps of opcodes that leave
# results on the stack.  The NOPUSH_MASK defines are used in vdbeaux.c
# to help determine the maximum stack size.
#


# Remember the TK_ values from the parse.h file
/^#define TK_/ {
  tk[$2] = 0+$3
}

# Scan for "case OP_aaaa:" lines in the vdbe.c file
/^case OP_/ {
  name = $2
  sub(/:/,"",name)
  sub("\r","",name)
  op[name] = -1
  out1[name] = 0
  out2[name] = 0
  out3[name] = 0
  jump[name] = 0
  in1[name] = 0
  in2[name] = 0
  in3[name] = 0
  for(i=3; i<NF; i++){
    if($i=="same" && $(i+1)=="as"){
      sym = $(i+2)
      sub(/,/,"",sym)
      op[name] = tk[sym]
      used[op[name]] = 1
      sameas[op[name]] = sym
    }
    sub(",","",$i)
    if($i=="no-push"){
      nopush[name] = 1
    }else if($i=="out1"){
      out1[name] = 1
    }else if($i=="out2"){
      out2[name] = 2
    }else if($i=="out3"){
      out3[name] = 3
    }else if($i=="in1"){
      in1[name] = 1
    }else if($i=="in2"){
      in2[name] = 1
    }else if($i=="in3"){
      in3[name] = 1
    }else if($i=="jump"){
      jump[name] = 1
    }
  }
}

# Assign numbers to all opcodes and output the result.
END {
  cnt = 0
  max = 0
  print "/* Automatically generated.  Do not edit */"
  print "/* See the mkopcodeh.awk script for details */"
  for(name in op){
    if( op[name]<0 ){
      cnt++
      while( used[cnt] ) cnt++
      op[name] = cnt
    }
    used[op[name]] = 1;
    if( op[name]>max ) max = op[name]
    printf "#define %-25s %15d", name, op[name]
    if( sameas[op[name]] ) {
      printf "   /* same as %-12s*/", sameas[op[name]]
    } 
    printf "\n"

  }
  seenUnused = 0;
  for(i=1; i<max; i++){
    if( !used[i] ){
      if( !seenUnused ){
        printf "\n/* The following opcode values are never used */\n"
        seenUnused = 1
      }
      printf "#define %-25s %15d\n", sprintf( "OP_NotUsed_%-3d", i ), i
    }
  }

  # Generate the bitvectors:
  #
  #  bit 0:     jump
  #  bit 1:     output on P1
  #  bit 2:     output on P2
  #  bit 3:     output on P3
  #  bit 4:     input on P1
  #  bit 5:     input on P2
  #  bit 6:     input on P3
  #  bit 7:     pushes a result onto stack
  #
  for(i=0; i<=max; i++) bv[i] = 0;
  for(name in op){
    x = op[name]
    if( jump[name] ) bv[x] += 0x01;
    if( out1[name] ) bv[x] += 0x02;
    if( out2[name] ) bv[x] += 0x04;
    if( out3[name] ) bv[x] += 0x08;
    if( in1[name] ) bv[x] += 0x10;
    if( in2[name] ) bv[x] += 0x20;
    if( in3[name] ) bv[x] += 0x40;
    if( !nopush[name] ) bv[x] += 0x80;
  }
  print "\n"
  print "/* Properties such as \"out2\" or \"jump\" that are specified in"
  print "** comments following the "case" for each opcode in the vdbe.c"
  print "** are encoded into bitvectors as follows:"
  print "*/"
  print "#define OPFLG_JUMP     0x01    /* jump:  P2 holds a jump target */"
  print "#define OPFLG_OUT1     0x02    /* out1:  P1 specifies output reg */"
  print "#define OPFLG_OUT2     0x04    /* out2:  P2 specifies output reg */"
  print "#define OPFLG_OUT3     0x08    /* out3:  P3 specifies output reg */"
  print "#define OPFLG_IN1      0x10    /* in1:   P1 is an input reg */"
  print "#define OPFLG_IN2      0x20    /* in2:   P2 is an input reg */"
  print "#define OPFLG_IN3      0x40    /* in3:   P3 is an input reg */"
  print "#define OPFLG_PUSH     0x80    /* omits no-push:  Does not push */"
  print "#define OPFLG_INITIALIZER {\\"
  for(i=0; i<=max; i++){
    printf " 0x%02x,", bv[i]
    if( i%10==9 ) printf("\\\n");
  }
  print "}"
}
