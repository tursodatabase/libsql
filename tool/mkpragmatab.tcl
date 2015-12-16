#!/usr/bin/tclsh
#
# Run this script to generate the pragma name lookup table C code.
#
# To add new pragmas, first add the name and other relevant attributes
# of the pragma to the "pragma_def" object below.  Then run this script
# to generate the ../src/pragma.h header file that contains macros and
# the lookup table needed for pragma name lookup in the pragma.c module.
# Then add the extra "case PragTyp_XXXXX:" and subsequent code for the
# new pragma in ../src/pragma.c.
#

set pragma_def {
  NAME: full_column_names
  TYPE: FLAG
  ARG:  SQLITE_FullColNames
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: short_column_names
  TYPE: FLAG
  ARG:  SQLITE_ShortColNames
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: count_changes
  TYPE: FLAG
  ARG:  SQLITE_CountRows
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: empty_result_callbacks
  TYPE: FLAG
  ARG:  SQLITE_NullCallback
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: legacy_file_format
  TYPE: FLAG
  ARG:  SQLITE_LegacyFileFmt
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: fullfsync
  TYPE: FLAG
  ARG:  SQLITE_FullFSync
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: checkpoint_fullfsync
  TYPE: FLAG
  ARG:  SQLITE_CkptFullFSync
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: cache_spill
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: reverse_unordered_selects
  TYPE: FLAG
  ARG:  SQLITE_ReverseOrder
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: query_only
  TYPE: FLAG
  ARG:  SQLITE_QueryOnly
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: automatic_index
  TYPE: FLAG
  ARG:  SQLITE_AutoIndex
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   !defined(SQLITE_OMIT_AUTOMATIC_INDEX)

  NAME: sql_trace
  TYPE: FLAG
  ARG:  SQLITE_SqlTrace
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_listing
  TYPE: FLAG
  ARG:  SQLITE_VdbeListing
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_trace
  TYPE: FLAG
  ARG:  SQLITE_VdbeTrace
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_addoptrace
  TYPE: FLAG
  ARG:  SQLITE_VdbeAddopTrace
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_debug
  TYPE: FLAG
  ARG:  SQLITE_SqlTrace|SQLITE_VdbeListing|SQLITE_VdbeTrace
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_eqp
  TYPE: FLAG
  ARG:  SQLITE_VdbeEQP
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   defined(SQLITE_DEBUG)

  NAME: ignore_check_constraints
  TYPE: FLAG
  ARG:  SQLITE_IgnoreChecks
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   !defined(SQLITE_OMIT_CHECK)

  NAME: writable_schema
  TYPE: FLAG
  ARG:  SQLITE_WriteSchema|SQLITE_RecoveryMode
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: read_uncommitted
  TYPE: FLAG
  ARG:  SQLITE_ReadUncommitted
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: recursive_triggers
  TYPE: FLAG
  ARG:  SQLITE_RecTriggers
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)

  NAME: foreign_keys
  TYPE: FLAG
  ARG:  SQLITE_ForeignKeys
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)

  NAME: defer_foreign_keys
  TYPE: FLAG
  ARG:  SQLITE_DeferFKs
  IF:   !defined(SQLITE_OMIT_FLAG_PRAGMAS)
  IF:   !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)

  NAME: cell_size_check
  TYPE: FLAG
  ARG:  SQLITE_CellSizeCk

  NAME: default_cache_size
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS) && !defined(SQLITE_OMIT_DEPRECATED)

  NAME: page_size
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: secure_delete
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: page_count
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: max_page_count
  TYPE: PAGE_COUNT
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: locking_mode
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: journal_mode
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: journal_size_limit
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: cache_size
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: mmap_size
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: auto_vacuum
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_AUTOVACUUM)

  NAME: incremental_vacuum
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_AUTOVACUUM)

  NAME: temp_store
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: temp_store_directory
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: data_store_directory
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS) && SQLITE_OS_WIN

  NAME: lock_proxy_file
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS) && SQLITE_ENABLE_LOCKING_STYLE

  NAME: synchronous
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: table_info
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: stats
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: index_info
  TYPE: INDEX_INFO
  ARG:  0
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: index_xinfo
  TYPE: INDEX_INFO
  ARG:  1
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: index_list
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: database_list
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: collation_list
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: foreign_key_list
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_FOREIGN_KEY)

  NAME: foreign_key_check
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)

  NAME: parser_trace
  IF:   defined(SQLITE_DEBUG) && !defined(SQLITE_OMIT_PARSER_TRACE)

  NAME: case_sensitive_like

  NAME: integrity_check
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_INTEGRITY_CHECK)

  NAME: quick_check
  TYPE: INTEGRITY_CHECK
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_INTEGRITY_CHECK)

  NAME: encoding
  IF:   !defined(SQLITE_OMIT_UTF16)

  NAME: schema_version
  TYPE: HEADER_VALUE
  ARG:  BTREE_SCHEMA_VERSION
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: user_version
  TYPE: HEADER_VALUE
  ARG:  BTREE_USER_VERSION
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: data_version
  TYPE: HEADER_VALUE
  ARG:  BTREE_DATA_VERSION
  FLAG: ReadOnly
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: freelist_count
  TYPE: HEADER_VALUE
  ARG:  BTREE_FREE_PAGE_COUNT
  FLAG: ReadOnly
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: application_id
  TYPE: HEADER_VALUE
  ARG:  BTREE_APPLICATION_ID
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: compile_options
  IF:   !defined(SQLITE_OMIT_COMPILEOPTION_DIAGS)

  NAME: wal_checkpoint
  FLAG: NeedSchema
  IF:   !defined(SQLITE_OMIT_WAL)

  NAME: wal_autocheckpoint
  IF:   !defined(SQLITE_OMIT_WAL)

  NAME: shrink_memory

  NAME: busy_timeout

  NAME: lock_status
  IF:   defined(SQLITE_DEBUG) || defined(SQLITE_TEST)

  NAME: key
  IF:   defined(SQLITE_HAS_CODEC)

  NAME: rekey
  IF:   defined(SQLITE_HAS_CODEC)

  NAME: hexkey
  IF:   defined(SQLITE_HAS_CODEC)

  NAME: hexrekey
  TYPE: HEXKEY
  IF:   defined(SQLITE_HAS_CODEC)

  NAME: activate_extensions
  IF:   defined(SQLITE_HAS_CODEC) || defined(SQLITE_ENABLE_CEROD)

  NAME: soft_heap_limit

  NAME: threads
}

# Open the output file
#
set destfile "[file dir [file dir [file normal $argv0]]]/src/pragma.h"
puts "Overwriting $destfile with new pragma table..."
set fd [open $destfile wb]
puts $fd {/* DO NOT EDIT!
** This file is automatically generated by the script at
** ../tool/mkpragmatab.tcl.  To update the set of pragmas, edit
** that script and rerun it.
*/}

# Parse the PRAGMA table above.
#
set name {}
set type {}
set if {}
set flags {}
set arg 0
proc record_one {} {
  global name type if arg allbyname typebyif flags
  if {$name==""} return
  set allbyname($name) [list $type $arg $if $flags]
  set name {}
  set type {}
  set if {}
  set flags {}
  set arg 0
}
foreach line [split $pragma_def \n] {
  set line [string trim $line]
  if {$line==""} continue
  foreach {id val} [split $line :] break
  set val [string trim $val]
  if {$id=="NAME"} {
    record_one    
    set name $val
    set type [string toupper $val]
  } elseif {$id=="TYPE"} {
    set type $val
  } elseif {$id=="ARG"} {
    set arg $val
  } elseif {$id=="IF"} {
    lappend if $val
  } elseif {$id=="FLAG"} {
    foreach term [split $val] {
      lappend flags $term
      set allflags($term) 1
    }
  } else {
    error "bad pragma_def line: $line"
  }
}
record_one
set allnames [lsort [array names allbyname]]

# Generate #defines for all pragma type names.  Group the pragmas that are
# omit in default builds (defined(SQLITE_DEBUG) and defined(SQLITE_HAS_CODEC))
# at the end.
#
set pnum 0
foreach name $allnames {
  set type [lindex $allbyname($name) 0]
  if {[info exists seentype($type)]} continue
  set if [lindex $allbyname($name) 2]
  if {[regexp SQLITE_DEBUG $if] || [regexp SQLITE_HAS_CODEC $if]} continue
  set seentype($type) 1
  puts $fd [format {#define %-35s %4d} PragTyp_$type $pnum]
  incr pnum
}
foreach name $allnames {
  set type [lindex $allbyname($name) 0]
  if {[info exists seentype($type)]} continue
  set if [lindex $allbyname($name) 2]
  if {[regexp SQLITE_DEBUG $if]} continue
  set seentype($type) 1
  puts $fd [format {#define %-35s %4d} PragTyp_$type $pnum]
  incr pnum
}
foreach name $allnames {
  set type [lindex $allbyname($name) 0]
  if {[info exists seentype($type)]} continue
  set seentype($type) 1
  puts $fd [format {#define %-35s %4d} PragTyp_$type $pnum]
  incr pnum
}

# Generate #defines for flags
#
set fv 1
foreach f [lsort [array names allflags]] {
  puts $fd [format {#define PragFlag_%-20s 0x%02x} $f $fv]
  set fv [expr {$fv*2}]
}

# Generate the lookup table
#
puts $fd "static const struct sPragmaNames \173"
puts $fd "  const char *const zName;  /* Name of pragma */"
puts $fd "  u8 ePragTyp;              /* PragTyp_XXX value */"
puts $fd "  u8 mPragFlag;             /* Zero or more PragFlag_XXX values */"
puts $fd "  u32 iArg;                 /* Extra argument */"
puts $fd "\175 aPragmaNames\[\] = \173"

set current_if {}
set spacer [format {    %26s } {}]
foreach name $allnames {
  foreach {type arg if flag} $allbyname($name) break
  if {$if!=$current_if} {
    if {$current_if!=""} {
      foreach this_if $current_if {
        puts $fd "#endif"
      }
    }
    set current_if $if
    if {$current_if!=""} {
      foreach this_if $current_if {
        puts $fd "#if $this_if"
      }
    }
  }
  set typex [format PragTyp_%-23s $type,]
  if {$flag==""} {
    set flagx "0"
  } else {
    set flagx PragFlag_[join $flag {|PragFlag_}]
  }
  puts $fd "  \173 /* zName:     */ \"$name\","
  puts $fd "    /* ePragTyp:  */ PragTyp_$type,"
  puts $fd "    /* ePragFlag: */ $flagx,"
  puts $fd "    /* iArg:      */ $arg \175,"
}
if {$current_if!=""} {
  foreach this_if $current_if {
    puts $fd "#endif"
  }
}
puts $fd "\175;"

# count the number of pragmas, for information purposes
#
set allcnt 0
set dfltcnt 0
foreach name $allnames {
  incr allcnt
  set if [lindex $allbyname($name) 2]
  if {[regexp {^defined} $if] || [regexp {[^!]defined} $if]} continue
  incr dfltcnt
}
puts $fd "/* Number of pragmas: $dfltcnt on by default, $allcnt total. */"
