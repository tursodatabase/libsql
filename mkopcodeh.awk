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
#       case OP_aaaa:       /* jump, in1, in2, in3, out2-prerelease, out3 */
#
# When such comments are found on an opcode, it means that certain
# properties apply to that opcode.  Set corresponding flags using the
# OPFLG_INITIALIZER macro.
#


# Remember the TK_ values from the parse.h file
/^#define TK_/ {
  tk[$2] = 0+$3    # tk[x] holds the numeric value for TK symbol X
}

# Find "/* Opcode: " lines in the vdbe.c file.  Each one introduces
# a new opcode.  Remember which parameters are used.
/^.. Opcode: / {
  currentOp = "OP_" $3
  m = 0
  for(i=4; i<=NF; i++){
    x = $i
    if( x=="P1" ) m += 1
    if( x=="P2" ) m += 2
    if( x=="P3" ) m += 4
    if( x=="P4" ) m += 8
    if( x=="P5" ) m += 16
  }
  paramused[currentOp] = m
}

# Find "** Synopsis: " lines that follow Opcode:
/^.. Synopsis: / {
  if( currentOp ){
    x = $3
    for(i=4; i<=NF; i++){
      x = x " " $i
    }
    synopsis[currentOp] = x
  }
}

# Scan for "case OP_aaaa:" lines in the vdbe.c file
/^case OP_/ {
  name = $2
  sub(/:/,"",name)
  sub("\r","",name)
  op[name] = -1       # op[x] holds the numeric value for OP symbol x
  jump[name] = 0
  in1[name] = 0
  in2[name] = 0
  in3[name] = 0
  out2[name] = 0
  out3[name] = 0
  for(i=3; i<NF; i++){
    if($i=="same" && $(i+1)=="as"){
      sym = $(i+2)
      sub(/,/,"",sym)
      val = tk[sym]
      op[name] = val
      used[val] = 1
      sameas[val] = sym
      def[val] = name
    }
    x = $i
    sub(",","",x)
    if(x=="jump"){
      jump[name] = 1
    }else if(x=="in1"){
      in1[name] = 1
    }else if(x=="in2"){
      in2[name] = 1
    }else if(x=="in3"){
      in3[name] = 1
    }else if(x=="out2"){
      out2[name] = 1
    }else if(x=="out3"){
      out3[name] = 1
    }
  }
  order[n_op++] = name;
}

# Assign numbers to all opcodes and output the result.
END {
  cnt = 0
  max = 0
  print "/* Automatically generated.  Do not edit */"
  print "/* See the mkopcodeh.awk script for details */"
  op["OP_Noop"] = -1;
  order[n_op++] = "OP_Noop";
  op["OP_Explain"] = -1;
  order[n_op++] = "OP_Explain";

  # Assign small values to opcodes that are processed by resolveP2Values()
  # to make code generation for the switch() statement smaller and faster.
  for(i=0; i<n_op; i++){
    name = order[i];
    if( op[name]>=0 ) continue;
    if( name=="OP_Transaction"   \
     || name=="OP_AutoCommit"    \
     || name=="OP_Savepoint"     \
     || name=="OP_Checkpoint"    \
     || name=="OP_Vacuum"        \
     || name=="OP_JournalMode"   \
     || name=="OP_VUpdate"       \
     || name=="OP_VFilter"       \
     || name=="OP_Next"          \
     || name=="OP_NextIfOpen"    \
     || name=="OP_SorterNext"    \
     || name=="OP_Prev"          \
     || name=="OP_PrevIfOpen"    \
    ){
      cnt++
      while( used[cnt] ) cnt++
      op[name] = cnt
      used[cnt] = 1
      def[cnt] = name
    }
  }

  # Generate the numeric values for opcodes
  for(i=0; i<n_op; i++){
    name = order[i];
    if( op[name]<0 ){
      cnt++
      while( used[cnt] ) cnt++
      op[name] = cnt
      used[cnt] = 1
      def[cnt] = name
    }
  }
  max = cnt
  for(i=1; i<=max; i++){
    if( !used[i] ){
      def[i] = "OP_NotUsed_" i 
    }
    printf "#define %-16s %3d", def[i], i
    com = ""
    if( sameas[i] ){
      com = "same as " sameas[i]
    }
    x = synopsis[def[i]]
    if( x ){
      if( com=="" ){
        com = "synopsis: " x
      } else {
        com = com ", synopsis: " x
      }
    }
    if( com!="" ){
      printf " /* %-42s */", com
    }
    printf "\n"
  }

  # Generate the bitvectors:
  #
  #  bit 0:     jump
  #  bit 1:     pushes a result onto stack
  #  bit 2:     output to p1.  release p1 before opcode runs
  #
  for(i=0; i<=max; i++){
    name = def[i]
    a0 = a1 = a2 = a3 = a4 = a5 = a6 = a7 = 0
    if( jump[name] ) a0 = 1;
    if( in1[name] ) a2 = 2;
    if( in2[name] ) a3 = 4;
    if( in3[name] ) a4 = 8;
    if( out2[name] ) a5 = 16;
    if( out3[name] ) a6 = 32;
    bv[i] = a0+a1+a2+a3+a4+a5+a6;
  }
  print "\n"
  print "/* Properties such as \"out2\" or \"jump\" that are specified in"
  print "** comments following the \"case\" for each opcode in the vdbe.c"
  print "** are encoded into bitvectors as follows:"
  print "*/"
  print "#define OPFLG_JUMP            0x0001  /* jump:  P2 holds jmp target */"
  print "#define OPFLG_IN1             0x0002  /* in1:   P1 is an input */"
  print "#define OPFLG_IN2             0x0004  /* in2:   P2 is an input */"
  print "#define OPFLG_IN3             0x0008  /* in3:   P3 is an input */"
  print "#define OPFLG_OUT2            0x0010  /* out2:  P2 is an output */"
  print "#define OPFLG_OUT3            0x0020  /* out3:  P3 is an output */"
  print "#define OPFLG_INITIALIZER {\\"
  for(i=0; i<=max; i++){
    if( i%8==0 ) printf("/* %3d */",i)
    printf " 0x%02x,", bv[i]
    if( i%8==7 ) printf("\\\n");
  }
  print "}"
  if( 0 ){
    print "\n/* Bitmask to indicate which fields (P1..P5) of each opcode are"
    print "** actually used.\n*/"
    print "#define OP_PARAM_USED_INITIALIZER {\\"
    for(i=0; i<=max; i++){
      if( i%8==0 ) printf("/* %3d */",i)
      printf " 0x%02x,", paramused[def[i]]
      if( i%8==7 ) printf("\\\n");
    }
    print "}"
  }
}
