

# Parameter $zName must be a path to the file UnicodeData.txt. This command
# reads the file and returns a list of codepoints (integers). The list
# contains all codepoints in the UnicodeData.txt assigned to any "General
# Category" that is not a "Letter" or "Number".
#
proc an_load_unicodedata_text {zName} {
  set fd [open $zName]
  set lField {
    code
    character_name
    general_category
    canonical_combining_classes
    bidirectional_category
    character_decomposition_mapping
    decimal_digit_value
    digit_value
    numeric_value
    mirrored
    unicode_1_name
    iso10646_comment_field
    uppercase_mapping
    lowercase_mapping
    titlecase_mapping
  }
  set lRet [list]

  while { ![eof $fd] } {
    set line [gets $fd]
    if {$line == ""} continue

    set fields [split $line ";"]
    if {[llength $fields] != [llength $lField]} { error "parse error: $line" }
    foreach $lField $fields {}

    set iCode [expr "0x$code"]
    set bAlnum [expr {[lsearch {L N} [string range $general_category 0 0]]>=0}]

    if { !$bAlnum } { lappend lRet $iCode }
  }

  close $fd
  set lRet
}

proc an_load_separator_ranges {} {
  global unicodedata.txt
  set lSep [an_load_unicodedata_text ${unicodedata.txt}]
  unset -nocomplain iFirst 
  unset -nocomplain nRange 
  set lRange [list]
  foreach sep $lSep {
    if {0==[info exists iFirst]} {
      set iFirst $sep
      set nRange 1
    } elseif { $sep == ($iFirst+$nRange) } {
      incr nRange
    } else {
      lappend lRange [list $iFirst $nRange]
      set iFirst $sep
      set nRange 1
    }
  } 
  lappend lRange [list $iFirst $nRange]
  set lRange
}

proc an_print_range_array {lRange} {
  set iFirstMax 0
  set nRangeMax 0
  foreach range $lRange {
    foreach {iFirst nRange} $range {}
    if {$iFirst > $iFirstMax} {set iFirstMax $iFirst}
    if {$nRange > $nRangeMax} {set nRangeMax $nRange}
  }
  if {$iFirstMax >= (1<<22)} {error "first-max is too large for format"}
  if {$nRangeMax >= (1<<10)} {error "range-max is too large for format"}

  puts -nonewline "  "
  puts [string trim {
  /* Each unsigned integer in the following array corresponds to a contiguous
  ** range of unicode codepoints that are not either letters or numbers (i.e.
  ** codepoints for which this function should return 0).
  **
  ** The most significant 22 bits in each 32-bit value contain the first 
  ** codepoint in the range. The least significant 10 bits are used to store
  ** the size of the range (always at least 1). In other words, the value 
  ** ((C<<22) + N) represents a range of N codepoints starting with codepoint 
  ** C. It is not possible to represent a range larger than 1023 codepoints 
  ** using this format.
  */
  }]
  puts -nonewline "  const static unsigned int aEntry\[\] = \{"
  set i 0
  foreach range $lRange {
    foreach {iFirst nRange} $range {}
    set u32 [format "0x%08X" [expr ($iFirst<<10) + $nRange]]

    if {($i % 5)==0} {puts "" ; puts -nonewline "   "}
    puts -nonewline " $u32,"
    incr i
  }
  puts ""
  puts "  \};"
}

proc print_isalnum {zFunc lRange} {
  puts "/*"
  puts "** Return true if the argument corresponds to a unicode codepoint"
  puts "** classified as either a letter or a number. Otherwise false."
  puts "**"
  puts "** The results are undefined if the value passed to this function"
  puts "** is less than zero."
  puts "*/"
  puts "int ${zFunc}\(int c)\{"
  an_print_range_array $lRange
  puts {
  if( c<(1<<22) ){
    unsigned int key = (((unsigned int)c)<<10) | 0x000003FF;
    int iRes;
    int iHi = sizeof(aEntry)/sizeof(aEntry[0]) - 1;
    int iLo = 0;
    while( iHi>=iLo ){
      int iTest = (iHi + iLo) / 2;
      if( key >= aEntry[iTest] ){
        iRes = iTest;
        iLo = iTest+1;
      }else{
        iHi = iTest-1;
      }
    }
    assert( aEntry[0]<key );
    assert( key>=aEntry[iRes] );
    return (c >= ((aEntry[iRes]>>10) + (aEntry[iRes]&0x3FF)));
  }
  return 1;}
  puts "\}"
}

proc print_test_isalnum {zFunc lRange} {
  foreach range $lRange {
    foreach {iFirst nRange} $range {}
    for {set i $iFirst} {$i < ($iFirst+$nRange)} {incr i} { set a($i) 1 }
  }

  puts "static int isalnum_test(int *piCode)\{"
  puts -nonewline "  unsigned char aAlnum\[\] = \{"
  for {set i 0} {$i < 70000} {incr i} {
    if {($i % 32)==0} { puts "" ; puts -nonewline "    " }
    set bFlag [expr ![info exists a($i)]]
    puts -nonewline "${bFlag},"
  }
  puts ""
  puts "  \};"

  puts -nonewline "  int aLargeSep\[\] = \{"
  set i 0
  foreach iSep [lsort -integer [array names a]] {
    if {$iSep<70000} continue
    if {($i % 8)==0} { puts "" ; puts -nonewline "   " }
    puts -nonewline " $iSep,"
    incr i
  }
  puts ""
  puts "  \};"
  puts -nonewline "  int aLargeOther\[\] = \{"
  set i 0
  foreach iSep [lsort -integer [array names a]] {
    if {$iSep<70000} continue
    if {[info exists a([expr $iSep-1])]==0} {
      if {($i % 8)==0} { puts "" ; puts -nonewline "   " }
      puts -nonewline " [expr $iSep-1],"
      incr i
    }
    if {[info exists a([expr $iSep+1])]==0} {
      if {($i % 8)==0} { puts "" ; puts -nonewline "   " }
      puts -nonewline " [expr $iSep+1],"
      incr i
    }
  }
  puts ""
  puts "  \};"

  puts [subst -nocommands {
  int i;
  for(i=0; i<sizeof(aAlnum)/sizeof(aAlnum[0]); i++){
    if( ${zFunc}(i)!=aAlnum[i] ){
      *piCode = i;
      return 1;
    }
  }
  for(i=0; i<sizeof(aLargeSep)/sizeof(aLargeSep[0]); i++){
    if( ${zFunc}(aLargeSep[i])!=0 ){
      *piCode = aLargeSep[i];
      return 1;
    }
  }
  for(i=0; i<sizeof(aLargeOther)/sizeof(aLargeOther[0]); i++){
    if( ${zFunc}(aLargeOther[i])!=1 ){
      *piCode = aLargeOther[i];
      return 1;
    }
  }
  }]
  puts "  return 0;"
  puts "\}"
}

#-------------------------------------------------------------------------

proc tl_load_casefolding_txt {zName} {
  global tl_lookup_table

  set fd [open $zName]
  while { ![eof $fd] } {
    set line [gets $fd]
    if {[string range $line 0 0] == "#"} continue
    if {$line == ""} continue

    foreach x {a b c d} {unset -nocomplain $x}
    foreach {a b c d} [split $line ";"] {}

    set a2 [list]
    set c2 [list]
    foreach elem $a { lappend a2 [expr "0x[string trim $elem]"] }
    foreach elem $c { lappend c2 [expr "0x[string trim $elem]"] }
    set b [string trim $b]
    set d [string trim $d]

    if {$b=="C" || $b=="S"} { set tl_lookup_table($a2) $c2 }
  }
}

proc tl_create_records {} {
  global tl_lookup_table

  set iFirst ""
  set nOff 0
  set nRange 0
  set nIncr 0

  set lRecord [list]
  foreach code [lsort -integer [array names tl_lookup_table]] {
    set mapping $tl_lookup_table($code)
    if {$iFirst == ""} {
      set iFirst $code
      set nOff   [expr $mapping - $code]
      set nRange 1
      set nIncr 1
    } else {
      set diff [expr $code - ($iFirst + ($nIncr * ($nRange - 1)))]
      if { $nRange==1 && ($diff==1 || $diff==2) } {
        set nIncr $diff
      }

      if {$diff != $nIncr || ($mapping - $code)!=$nOff} {
        if { $nRange==1 } {set nIncr 1}
        lappend lRecord [list $iFirst $nIncr $nRange $nOff]
        set iFirst $code
        set nOff   [expr $mapping - $code]
        set nRange 1
        set nIncr 1
      } else {
        incr nRange
      }
    }
  }

  lappend lRecord [list $iFirst $nIncr $nRange $nOff]

  set lRecord
}

proc tl_print_table_header {} {
  puts -nonewline "  "
  puts [string trim {
  /* Each entry in the following array defines a rule for folding a range
  ** of codepoints to lower case. The rule applies to a range of nRange
  ** codepoints starting at codepoint iCode.
  **
  ** If bFlag is clear, then all the codepoints in the range are upper
  ** case and require folding. Or, if bFlag is set, then only every second
  ** codepoint in the range, starting with iCode, requires folding. If a
  ** specific codepoint C does require folding, then the lower-case version
  ** is ((C + iOff)&0xFFFF).
  **
  ** The contents of this array are generated by parsing the CaseFolding.txt
  ** file distributed as part of the "Unicode Character Database". See
  ** http://www.unicode.org for details.
  */
  }]
  puts "  static const struct TableEntry \{"
  puts "    unsigned short iCode;"
  puts "    unsigned char bFlag;"
  puts "    unsigned char nRange;"
  puts "    unsigned short iOff;"
  puts "  \} aEntry\[\] = \{"
}

proc tl_print_table_entry {togglevar entry} {
  upvar $togglevar t
  foreach {iFirst nIncr nRange nOff} $entry {}

  if {$iFirst > (1<<16)} { return 1 }

  if {[info exists t]==0} {set t 0}
  if {$t==0} { puts -nonewline "    " }

  set flags 0
  if {$nIncr==2} { set flags 1 ; set nRange [expr $nRange * 2]}
  if {$nOff<0}   { incr nOff [expr (1<<16)] }

  set txt "{$iFirst, $flags, $nRange, $nOff},"
  if {$t==2} {
    puts $txt
  } else {
    puts -nonewline [format "% -23s" $txt]
  }
  set t [expr ($t+1)%3]

  return 0
}

proc tl_print_table_footer {togglevar} {
  upvar $togglevar t
  if {$t!=0} {puts ""}
  puts "  \};"
}

proc tl_print_if_entry {entry} {
  foreach {iFirst nIncr nRange nOff} $entry {}
  if {$nIncr==2} {error "tl_print_if_entry needs improvement!"}

  puts "  else if( c>=$iFirst && c<[expr $iFirst+$nRange] )\{"
  puts "    ret = c + $nOff;"
  puts "  \}"
}

proc print_tolower {zFunc} {

  set lRecord [tl_create_records]

  set lHigh [list]
  puts "/*"
  puts "** Interpret the argument as a unicode codepoint. If the codepoint"
  puts "** is an upper case character that has a lower case equivalent,"
  puts "** return the codepoint corresponding to the lower case version."
  puts "** Otherwise, return a copy of the argument."
  puts "**"
  puts "** The results are undefined if the value passed to this function"
  puts "** is less than zero."
  puts "*/"
  puts "int ${zFunc}\(int c)\{"
  tl_print_table_header
  foreach entry $lRecord { 
    if {[tl_print_table_entry toggle $entry]} { 
      lappend lHigh $entry 
    } 
  }
  tl_print_table_footer toggle
  puts {
  int ret = c;

  assert( c>=0 );
  assert( sizeof(unsigned short)==2 && sizeof(unsigned char)==1 );

  if( c<65536 ){
    int iHi = sizeof(aEntry)/sizeof(aEntry[0]) - 1;
    int iLo = 0;
    int iRes = -1;

    while( iHi>=iLo ){
      int iTest = (iHi + iLo) / 2;
      int cmp = (c - aEntry[iTest].iCode);
      if( cmp>=0 ){
        iRes = iTest;
        iLo = iTest+1;
      }else{
        iHi = iTest-1;
      }
    }
    assert( iRes<0 || c>=aEntry[iRes].iCode );

    if( iRes>=0 ){
      const struct TableEntry *p = &aEntry[iRes];
      if( c<(p->iCode + p->nRange) && 0==(p->bFlag & (p->iCode ^ c)) ){
        ret = (c + p->iOff) & 0x0000FFFF;
        assert( ret>0 );
      }
    }
  }
  }

  foreach entry $lHigh {
    tl_print_if_entry $entry
  }

  puts ""
  puts "  return ret;"
  puts "\}"
}

proc print_tolower_test {zFunc} {
  global tl_lookup_table

  puts "static int tolower_test(int *piCode)\{"
  puts -nonewline "  static int aLookup\[\] = \{"
  for {set i 0} {$i < 70000} {incr i} {
    set expected $i
    catch { set expected $tl_lookup_table($i) }
    if {($i % 8)==0}  { puts "" ; puts -nonewline "    " }
    puts -nonewline "$expected, "
  }
  puts "  \};"
  puts "  int i;"
  puts "  for(i=0; i<sizeof(aLookup)/sizeof(aLookup\[0\]); i++)\{"
  puts "    if( ${zFunc}\(i)!=aLookup\[i\] )\{"
  puts "      *piCode = i;"
  puts "      return 1;"
  puts "    \}"
  puts "  \}"
  puts "  return 0;"
  puts "\}"
}


proc print_fileheader {} {
  puts [string trim {
/*
** 2012 May 25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
*/

/*
** DO NOT EDIT THIS MACHINE GENERATED FILE.
*/
  }]
  puts ""
  puts "#include <assert.h>"
  puts ""
}

proc print_test_main {} {
  puts ""
  puts "#include <stdio.h>"
  puts ""
  puts "int main(int argc, char **argv)\{"
  puts "  int r1, r2;"
  puts "  int code;"
  puts "  r1 = isalnum_test(&code);"
  puts "  if( r1 ) printf(\"isalnum(): Problem with code %d\\n\",code);"
  puts "  else printf(\"isalnum(): test passed\\n\");"
  puts "  r2 = tolower_test(&code);"
  puts "  if( r2 ) printf(\"tolower(): Problem with code %d\\n\",code);"
  puts "  else printf(\"tolower(): test passed\\n\");"
  puts "  return (r1 || r2);"
  puts "\}"
}

# Proces the command line arguments. Exit early if they are not to
# our liking.
#
proc usage {} {
  puts -nonewline stderr "Usage: $::argv0 ?-test? i"
  puts            stderr "<CaseFolding.txt file> <UnicodeData.txt file>"
  exit 1
}
if {[llength $argv]!=2 && [llength $argv]!=3} usage
if {[llength $argv]==3 && [lindex $argv 0]!="-test"} usage
set unicodedata.txt [lindex $argv end]
set casefolding.txt [lindex $argv end-1]
set generate_test_code [expr {[llength $argv]==3}]

# Print the isalnum() function to stdout.
#
print_fileheader
set lRange [an_load_separator_ranges]
print_isalnum sqlite3FtsUnicodeIsalnum $lRange

# Leave a gap between the two generated C functions.
#
puts ""
puts ""

# Print the tolower() function to stdout.
#
tl_load_casefolding_txt ${casefolding.txt}
print_tolower sqlite3FtsUnicodeTolower

# Print the test routines and main() function to stdout, if -test 
# was specified.
#
if {$::generate_test_code} {
  print_test_isalnum sqlite3FtsUnicodeIsalnum $lRange
  print_tolower_test sqlite3FtsUnicodeTolower 
  print_test_main 
}

