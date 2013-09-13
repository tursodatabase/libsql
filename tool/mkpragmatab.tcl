#!/usr/bin/tclsh
#
# Run this script to generate the pragma name lookup table C code.
#
# To add new pragmas, first add the name and other relevant attributes
# of the pragma to the "pragma_def" object below.  Then run this script
# to generate the C-code for the lookup table and copy/paste the output
# of this script into the appropriate spot in the pragma.c source file.
# Then add the extra "case PragTyp_XXXXX:" and subsequent code for the
# new pragma.
#

set pragma_def {
  NAME: full_column_names
  TYPE: FLAG
  ARG:  SQLITE_FullColNames

  NAME: short_column_names
  TYPE: FLAG
  ARG:  SQLITE_ShortColNames

  NAME: count_changes
  TYPE: FLAG
  ARG:  SQLITE_CountRows

  NAME: empty_result_callbacks
  TYPE: FLAG
  ARG:  SQLITE_NullCallback

  NAME: legacy_file_format
  TYPE: FLAG
  ARG:  SQLITE_LegacyFileFmt

  NAME: fullfsync
  TYPE: FLAG
  ARG:  SQLITE_FullFSync

  NAME: checkpoint_fullfsync
  TYPE: FLAG
  ARG:  SQLITE_CkptFullFSync

  NAME: cache_spill
  TYPE: FLAG
  ARG:  SQLITE_CacheSpill

  NAME: reverse_unordered_selects
  TYPE: FLAG
  ARG:  SQLITE_ReverseOrder

  NAME: query_only
  TYPE: FLAG
  ARG:  SQLITE_QueryOnly

  NAME: automatic_index
  TYPE: FLAG
  ARG:  SQLITE_AutoIndex
  IF:   !defined(SQLITE_OMIT_AUTOMATIC_INDEX)

  NAME: sql_trace
  TYPE: FLAG
  ARG:  SQLITE_SqlTrace
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_listing
  TYPE: FLAG
  ARG:  SQLITE_VdbeListing
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_trace
  TYPE: FLAG
  ARG:  SQLITE_VdbeTrace
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_addoptrace
  TYPE: FLAG
  ARG:  SQLITE_VdbeAddopTrace
  IF:   defined(SQLITE_DEBUG)

  NAME: vdbe_debug
  TYPE: FLAG
  ARG:  SQLITE_SqlTrace|SQLITE_VdbeListing|SQLITE_VdbeTrace
  IF:   defined(SQLITE_DEBUG)

  NAME: ignore_check_constraints
  TYPE: FLAG
  ARG:  SQLITE_IgnoreChecks
  IF:   !defined(SQLITE_OMIT_CHECK)

  NAME: writable_schema
  TYPE: FLAG
  ARG:  SQLITE_WriteSchema|SQLITE_RecoveryMode

  NAME: read_uncommitted
  TYPE: FLAG
  ARG:  SQLITE_ReadUncommitted

  NAME: recursive_triggers
  TYPE: FLAG
  ARG:  SQLITE_RecTriggers

  NAME: foreign_keys
  TYPE: FLAG
  ARG:  SQLITE_ForeignKeys
  IF:   !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)

  NAME: defer_foreign_keys
  TYPE: FLAG
  ARG:  SQLITE_DeferFKs
  IF:   !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)

  NAME: default_cache_size
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS) && !defined(SQLITE_OMIT_DEPRECATED)

  NAME: page_size
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: secure_delete
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: page_count
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: max_page_count
  TYPE: PAGE_COUNT
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: locking_mode
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: journal_mode
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: journal_size_limit
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: cache_size
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: mmap_size
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: auto_vacuum
  IF:   !defined(SQLITE_OMIT_AUTOVACUUM)

  NAME: incremental_vacuum
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
  IF:   !defined(SQLITE_OMIT_PAGER_PRAGMAS)

  NAME: table_info
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: index_info
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: index_list
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: database_list
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: collation_list
  IF:   !defined(SQLITE_OMIT_SCHEMA_PRAGMAS)

  NAME: foreign_key_list
  IF:   !defined(SQLITE_OMIT_FOREIGN_KEY)

  NAME: foreign_key_check
  IF:   !defined(SQLITE_OMIT_FOREIGN_KEY) && !defined(SQLITE_OMIT_TRIGGER)

  NAME: parser_trace
  IF:   defined(SQLITE_DEBUG)

  NAME: case_sensitive_like

  NAME: integrity_check
  IF:   !defined(SQLITE_OMIT_INTEGRITY_CHECK)

  NAME: quick_check
  TYPE: INTEGRITY_CHECK
  IF:   !defined(SQLITE_OMIT_INTEGRITY_CHECK)

  NAME: encoding
  IF:   !defined(SQLITE_OMIT_UTF16)

  NAME: schema_version
  TYPE: HEADER_VALUE
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: user_version
  TYPE: HEADER_VALUE
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: freelist_count
  TYPE: HEADER_VALUE
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: application_id
  TYPE: HEADER_VALUE
  IF:   !defined(SQLITE_OMIT_SCHEMA_VERSION_PRAGMAS)

  NAME: compile_options
  IF:   !defined(SQLITE_OMIT_COMPILEOPTION_DIAGS)

  NAME: wal_checkpoint
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

  NAME: activate_extensions
  IF:   defined(SQLITE_HAS_CODEC) || defined(SQLITE_ENABLE_CEROD)

  NAME: soft_heap_limit
}
set name {}
set type {}
set if {}
set arg 0
proc record_one {} {
  global name type if arg allbyname typebyif
  if {$name==""} return
  set allbyname($name) [list $type $arg $if]
  set name {}
  set type {}
  set if {}
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
    set if $val
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
  puts [format {#define %-35s %4d} PragTyp_$type $pnum]
  incr pnum
}
foreach name $allnames {
  set type [lindex $allbyname($name) 0]
  if {[info exists seentype($type)]} continue
  set if [lindex $allbyname($name) 2]
  if {[regexp SQLITE_DEBUG $if]} continue
  set seentype($type) 1
  puts [format {#define %-35s %4d} PragTyp_$type $pnum]
  incr pnum
}
foreach name $allnames {
  set type [lindex $allbyname($name) 0]
  if {[info exists seentype($type)]} continue
  set seentype($type) 1
  puts [format {#define %-35s %4d} PragTyp_$type $pnum]
  incr pnum
}

# Generate the lookup table
#
puts "static const struct sPragmaNames \173"
puts "  const char *const zName;  /* Name of pragma */"
puts "  u8 ePragTyp;              /* PragTyp_XXX value */"
puts "  u32 iArg;                 /* Extra argument */"
puts "\175 aPragmaNames\[\] = \173"

set current_if {}
set spacer [format {    %26s } {}]
foreach name $allnames {
  foreach {type arg if} $allbyname($name) break
  if {$if!=$current_if} {
    if {$current_if!=""} {puts "#endif"}
    set current_if $if
    if {$current_if!=""} {puts "#if $current_if"}
  }
  set namex [format %-26s \"$name\",]
  set typex [format PragTyp_%-23s $type,]
  if {[string length $arg]>10} {
    puts "  \173 $namex $typex\n$spacer$arg \175,"
  } else {
    puts "  \173 $namex $typex $arg \175,"
  }
}
if {$current_if!=""} {puts "#endif"}
puts "\175;"

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
puts "/* Number of pragmas: $dfltcnt on by default, $allcnt total. */"
