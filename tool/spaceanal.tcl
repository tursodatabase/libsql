# Run this TCL script using "testfixture" in order get a report that shows
# how much disk space is used by a particular data to actually store data
# versus how much space is unused.
#

# Get the name of the database to analyze
#
#set argv $argv0
if {[llength $argv]!=1} {
  puts stderr "Usage: $argv0 database-name"
  exit 1
}
set file_to_analyze [lindex $argv 0]
if {![file exists $file_to_analyze]} {
  puts stderr "No such file: $file_to_analyze"
  exit 1
}
if {![file readable $file_to_analyze]} {
  puts stderr "File is not readable: $file_to_analyze"
  exit 1
}
if {[file size $file_to_analyze]<512} {
  puts stderr "Empty or malformed database: $file_to_analyze"
  exit 1
}

# Open the database
#
sqlite3 db [lindex $argv 0]
set DB [btree_open [lindex $argv 0] 1000 0]

# In-memory database for collecting statistics
#
sqlite3 mem :memory:
set tabledef\
{CREATE TABLE space_used(
   name clob,        -- Name of a table or index in the database file
   tblname clob,     -- Name of associated table
   is_index boolean, -- TRUE if it is an index, false for a table
   nentry int,       -- Number of entries in the BTree
   leaf_entries int, -- Number of leaf entries
   payload int,      -- Total amount of data stored in this table or index
   ovfl_payload int, -- Total amount of data stored on overflow pages
   ovfl_cnt int,     -- Number of entries that use overflow
   mx_payload int,   -- Maximum payload size
   int_pages int,    -- Number of interior pages used
   leaf_pages int,   -- Number of leaf pages used
   ovfl_pages int,   -- Number of overflow pages used
   int_unused int,   -- Number of unused bytes on interior pages
   leaf_unused int,  -- Number of unused bytes on primary pages
   ovfl_unused int   -- Number of unused bytes on overflow pages
);}
mem eval $tabledef

# This query will be used to find the root page number for every table
# in the database.
#
set sql {
  SELECT name, rootpage 
    FROM sqlite_master WHERE type='table'
  UNION ALL
  SELECT 'sqlite_master', 1
  ORDER BY 1
}

# Quote a string for SQL
#
proc quote txt {
  regsub -all ' $txt '' q
  return '$q'
}

# Analyze every table in the database, one at a time.
#
set pageSize [db eval {PRAGMA page_size}]
foreach {name rootpage} [db eval $sql] {
  puts stderr "Analyzing table $name..."
  set cursor [btree_cursor $DB $rootpage 0]
  set go [btree_first $cursor]
  catch {unset seen}
  set total_payload 0        ;# Payload space used by all entries
  set total_ovfl 0           ;# Payload space on overflow pages
  set unused_int 0           ;# Unused space on interior nodes
  set unused_leaf 0          ;# Unused space on leaf nodes
  set unused_ovfl 0          ;# Unused space on overflow pages
  set cnt_ovfl 0             ;# Number of entries that use overflows
  set cnt_leaf_entry 0       ;# Number of leaf entries
  set cnt_int_entry 0        ;# Number of interor entries
  set mx_payload 0           ;# Maximum payload size
  set ovfl_pages 0           ;# Number of overflow pages used
  set leaf_pages 0           ;# Number of leaf pages
  set int_pages 0            ;# Number of interior pages
  while {$go==0} {
    incr cnt_leaf_entry
    set stat [btree_cursor_info $cursor]
    set payload [lindex $stat 6]
    if {$payload>$mx_payload} {set mx_payload $payload}
    incr total_payload $payload
    set local [lindex $stat 8]    
    set ovfl [expr {$payload-$local}]
    if {$ovfl} {
      incr cnt_ovfl
      incr total_ovfl $ovfl
      set n [expr {int(ceil($ovfl/($pageSize-4.0)))}]
      incr ovfl_pages $n
      incr unused_ovfl [expr {$n*($pageSize-4) - $ovfl}]
    }
    set pgno [lindex $stat 0]
    if {![info exists seen($pgno)]} {
      set seen($pgno) 1
      incr leaf_pages
      incr unused_leaf [lindex $stat 4]
      set parent [lindex $stat 9]
      set up 0
      while {$parent!=0 && ![info exists seen($parent)]} {
        incr up
        set stat [btree_cursor_info $cursor $up]
        set seen($parent) 1
        incr int_pages
        incr cnt_int_entry [lindex $stat 2]
        incr unused_int [lindex $stat 4]
        set parent [lindex $stat 9]
      }
    }
    set go [btree_next $cursor]
  }
  btree_close_cursor $cursor
  if {[llength [array names seen]]==0} {
    set leaf_pages 1
    set unused_leaf [expr {$pageSize-8}]
  } elseif {$rootpage==1 && ![info exists seen(1)]} {
    incr int_pages
    incr unused_int [expr {$pageSize-112}]
  }
  set sql "INSERT INTO space_used VALUES("
  append sql [quote $name]
  append sql ",[quote $name]"
  append sql ",0"
  append sql ",[expr {$cnt_leaf_entry+$cnt_int_entry}]"
  append sql ",$cnt_leaf_entry"
  append sql ",$total_payload"
  append sql ",$total_ovfl"
  append sql ",$cnt_ovfl"
  append sql ",$mx_payload"
  append sql ",$int_pages"
  append sql ",$leaf_pages"
  append sql ",$ovfl_pages"
  append sql ",$unused_int"
  append sql ",$unused_leaf"
  append sql ",$unused_ovfl"
  append sql );
  mem eval $sql
}

# This query will be used to find the root page number for every index
# in the database.
#
set sql {
  SELECT name, tbl_name, rootpage 
    FROM sqlite_master WHERE type='index'
  ORDER BY 2, 1
}

# Analyze every index in the database, one at a time.
#
set pageSize [db eval {PRAGMA page_size}]
foreach {name tbl_name rootpage} [db eval $sql] {
  puts stderr "Analyzing index $name of table $tbl_name..."
  set cursor [btree_cursor $DB $rootpage 0]
  set go [btree_first $cursor]
  catch {unset seen}
  set total_payload 0        ;# Payload space used by all entries
  set total_ovfl 0           ;# Payload space on overflow pages
  set unused_leaf 0          ;# Unused space on leaf nodes
  set unused_ovfl 0          ;# Unused space on overflow pages
  set cnt_ovfl 0             ;# Number of entries that use overflows
  set cnt_leaf_entry 0       ;# Number of leaf entries
  set mx_payload 0           ;# Maximum payload size
  set ovfl_pages 0           ;# Number of overflow pages used
  set leaf_pages 0           ;# Number of leaf pages
  while {$go==0} {
    incr cnt_leaf_entry
    set stat [btree_cursor_info $cursor]
    set payload [btree_keysize $cursor]
    if {$payload>$mx_payload} {set mx_payload $payload}
    incr total_payload $payload
    set local [lindex $stat 8]    
    set ovfl [expr {$payload-$local}]
    if {$ovfl} {
      incr cnt_ovfl
      incr total_ovfl $ovfl
      set n [expr {int(ceil($ovfl/($pageSize-4.0)))}]
      incr ovfl_pages $n
      incr unused_ovfl [expr {$n*($pageSize-4) - $ovfl}]
    }
    set pgno [lindex $stat 0]
    if {![info exists seen($pgno)]} {
      set seen($pgno) 1
      incr leaf_pages
      incr unused_leaf [lindex $stat 4]
    }
    set go [btree_next $cursor]
  }
  btree_close_cursor $cursor
  if {[llength [array names seen]]==0} {
    set leaf_pages 1
    set unused_leaf [expr {$pageSize-8}]
  }
  set sql "INSERT INTO space_used VALUES("
  append sql [quote $name]
  append sql ",[quote $tbl_name]"
  append sql ",1"
  append sql ",$cnt_leaf_entry"
  append sql ",$cnt_leaf_entry"
  append sql ",$total_payload"
  append sql ",$total_ovfl"
  append sql ",$cnt_ovfl"
  append sql ",$mx_payload"
  append sql ",0"
  append sql ",$leaf_pages"
  append sql ",$ovfl_pages"
  append sql ",0"
  append sql ",$unused_leaf"
  append sql ",$unused_ovfl"
  append sql );
  mem eval $sql
}

# Generate a single line of output in the statistics section of the
# report.
#
proc statline {title value {extra {}}} {
  set len [string length $title]
  set dots [string range {......................................} $len end]
  set len [string length $value]
  set sp2 [string range {          } $len end]
  if {$extra ne ""} {
    set extra " $extra"
  }
  puts "$title$dots $value$sp2$extra"
}

# Generate a formatted percentage value for $num/$denom
#
proc percent {num denom {of {}}} {
  if {$denom==0.0} {return ""}
  set v [expr {$num*100.0/$denom}]
  set of {}
  if {$v==1.0 || $v==0.0 || ($v>1.0 && $v<99.0)} {
    return [format {%5.1f%% %s} $v $of]
  } elseif {$v<0.1 || $v>99.9} {
    return [format {%7.3f%% %s} $v $of]
  } else {
    return [format {%6.2f%% %s} $v $of]
  }
}

# Generate a subreport that covers some subset of the database.
# the $where clause determines which subset to analyze.
#
proc subreport {title where} {
  global pageSize
  set hit 0
  mem eval "
    SELECT
      sum(nentry) AS nentry,
      sum(leaf_entries) AS nleaf,
      sum(payload) AS payload,
      sum(ovfl_payload) AS ovfl_payload,
      max(mx_payload) AS mx_payload,
      sum(ovfl_cnt) as ovfl_cnt,
      sum(leaf_pages) AS leaf_pages,
      sum(int_pages) AS int_pages,
      sum(ovfl_pages) AS ovfl_pages,
      sum(leaf_unused) AS leaf_unused,
      sum(int_unused) AS int_unused,
      sum(ovfl_unused) AS ovfl_unused
    FROM space_used WHERE $where" {} {set hit 1}
  if {!$hit} {return 0}
  puts ""
  set len [string length $title]
  incr len 5
  set stars "***********************************"
  append stars $stars
  set stars [string range $stars $len end]
  puts "*** $title $stars"
  puts ""
  set total_pages [expr {$leaf_pages+$int_pages+$ovfl_pages}]
  statline "Percentage of total database" [percent $total_pages $::file_pgcnt]
  statline "Number of entries" $nleaf
  set total_unused [expr {$ovfl_unused+$int_unused+$leaf_unused}]
  set storage [expr {$total_pages*$pageSize}]
  statline "Bytes of storage consumed" $storage
  statline "Bytes of payload" $payload \
       [percent $payload $storage {of storage consumed}]
  statline "Average payload per entry" [expr {$nleaf>0?$payload/$nleaf:0}]
  set avgunused [expr {$nleaf>0?$total_unused/$nleaf:0}]
  statline "Average unused bytes per entry" $avgunused
  set nint [expr {$nentry-$nleaf}]
  if {$int_pages>0} {
    statline "Average fanout" [format %.2f [expr {($nint+0.0)/$int_pages}]]
  }
  statline "Maximum payload per entry" $mx_payload
  statline "Entries that use overflow" $ovfl_cnt \
        [percent $ovfl_cnt $nleaf {of all entries}]
  if {$int_pages>0} {
    statline "Index pages used" $int_pages
  }
  statline "Primary pages used" $leaf_pages
  statline "Overflow pages used" $ovfl_pages
  statline "Total pages used" $total_pages
  if {$int_unused>0} {
    statline "Unused bytes on index pages" $int_unused \
         [percent $int_unused [expr {$int_pages*$pageSize}] {of index space}]
  }
  statline "Unused bytes on primary pages" $leaf_unused \
     [percent $leaf_unused [expr {$leaf_pages*$pageSize}] {of primary space}]
  statline "Unused bytes on overflow pages" $ovfl_unused \
     [percent $ovfl_unused [expr {$ovfl_pages*$pageSize}] {of overflow space}]
  statline "Unused bytes on all pages" $total_unused \
               [percent $total_unused $storage {of all space}]
  return 1
}

# Calculate the overhead in pages caused by auto-vacuum. 
#
# This procedure calculates and returns the number of pages used by the 
# auto-vacuum 'pointer-map'. If the database does not support auto-vacuum,
# then 0 is returned. The two arguments are the size of the database file in
# bytes and the page size used by the database (also in bytes).
proc autovacuum_overhead {filePages pageSize} {

  # Read the value of meta 4. If non-zero, then the database supports
  # auto-vacuum. It would be possible to use "PRAGMA auto_vacuum" instead,
  # but that would not work if the SQLITE_OMIT_PRAGMA macro was defined
  # when the library was built.
  set meta4 [lindex [btree_get_meta $::DB] 4]

  # If the database is not an auto-vacuum database or the file consists
  # of one page only then there is no overhead for auto-vacuum. Return zero.
  if {0==$meta4 || $filePages==1} {
    return 0
  }

  # The number of entries on each pointer map page. The layout of the
  # database file is one pointer-map page, followed by $ptrsPerPage other
  # pages, followed by a pointer-map page etc. The first pointer-map page
  # is the second page of the file overall.
  set ptrsPerPage [expr double($pageSize/5)]

  # Return the number of pointer map pages in the database.
  return [expr int(ceil( ($filePages-1.0)/($ptrsPerPage+1.0) ))]
}

# Output summary statistics:
#
puts "/** Disk-Space Utilization Report For $file_to_analyze"
puts "*** As of [clock format [clock seconds] -format {%Y-%b-%d %H:%M:%S}]"
puts ""

# Variables:
#
# pageSize:      Size of each page in bytes.
# file_bytes:    File size in bytes.
# file_pgcnt:    Number of pages in the file.
# file_pgcnt2:   Number of pages in the file (calculated).
# av_pgcnt:      Pages consumed by the auto-vacuum pointer-map.
# av_percent:    Percentage of the file consumed by auto-vacuum pointer-map.
# inuse_pgcnt:   Data pages in the file.
# inuse_percent: Percentage of pages used to store data.
# free_pgcnt:    Free pages calculated as (<total pages> - <in-use pages>)
# free_pgcnt2:   Free pages in the file according to the file header.
# free_percent:  Percentage of file consumed by free pages (calculated).
# free_percent2: Percentage of file consumed by free pages (header).
# ntable:        Number of tables in the db.
# nindex:        Number of indices in the db.
# nautoindex:    Number of indices created automatically.
# nmanindex:     Number of indices created manually.
# user_payload:

set file_bytes  [file size $file_to_analyze]
set file_pgcnt  [expr {$file_bytes/$pageSize}]

set av_pgcnt    [autovacuum_overhead $file_pgcnt $pageSize]
set av_percent  [percent $av_pgcnt $file_pgcnt]

set q {SELECT sum(leaf_pages+int_pages+ovfl_pages) FROM space_used}
set inuse_pgcnt   [expr [mem eval $q]]
set inuse_percent [percent $inuse_pgcnt $file_pgcnt]

set free_pgcnt    [expr $file_pgcnt-$inuse_pgcnt-$av_pgcnt]
set free_percent  [percent $free_pgcnt $file_pgcnt]
set free_pgcnt2   [lindex [btree_get_meta $DB] 0]
set free_percent2 [percent $free_pgcnt2 $file_pgcnt]

set file_pgcnt2 [expr {$inuse_pgcnt+$free_pgcnt2+$av_pgcnt}]

set ntable [db eval {SELECT count(*)+1 FROM sqlite_master WHERE type='table'}]
set nindex [db eval {SELECT count(*) FROM sqlite_master WHERE type='index'}]
set q {SELECT count(*) FROM sqlite_master WHERE name LIKE 'sqlite_autoindex%'}
set nautoindex [db eval $q]
set nmanindex [expr {$nindex-$nautoindex}]

# set total_payload [mem eval "SELECT sum(payload) FROM space_used"]
set user_payload [mem one {SELECT sum(payload) FROM space_used
     WHERE NOT is_index AND name NOT LIKE 'sqlite_master'}]
set user_payload_percent [percent $user_payload $file_bytes]

statline {Page size in bytes} $pageSize
statline {Pages in the whole file (measured)} $file_pgcnt
statline {Pages in the whole file (calculated)} $file_pgcnt2
statline {Pages that store data} $inuse_pgcnt $inuse_percent
statline {Pages on the freelist (per header)} $free_pgcnt2 $free_percent2
statline {Pages on the freelist (calculated)} $free_pgcnt $free_percent
statline {Pages of auto-vacuum overhead} $av_pgcnt $av_percent
statline {Number of tables in the database} $ntable
statline {Number of indices} $nindex
statline {Number of named indices} $nmanindex
statline {Automatically generated indices} $nautoindex
statline {Size of the file in bytes} $file_bytes
statline {Bytes of user payload stored} $user_payload $user_payload_percent

# Output table rankings
#
puts ""
puts "*** Page counts for all tables with their indices ********************"
puts ""
mem eval {SELECT tblname, count(*) AS cnt, 
              sum(int_pages+leaf_pages+ovfl_pages) AS size
          FROM space_used GROUP BY tblname ORDER BY size DESC, tblname} {} {
  statline [string toupper $tblname] $size [percent $size $file_pgcnt]
}

# Output subreports
#
if {$nindex>0} {
  subreport {All tables and indices} 1
}
subreport {All tables} {NOT is_index}
if {$nindex>0} {
  subreport {All indices} {is_index}
}
foreach tbl [mem eval {SELECT name FROM space_used WHERE NOT is_index
                       ORDER BY name}] {
  regsub ' $tbl '' qn
  set name [string toupper $tbl]
  set n [mem eval "SELECT count(*) FROM space_used WHERE tblname='$qn'"]
  if {$n>1} {
    subreport "Table $name and all its indices" "tblname='$qn'"
    subreport "Table $name w/o any indices" "name='$qn'"
    subreport "Indices of table $name" "tblname='$qn' AND is_index"
  } else {
    subreport "Table $name" "name='$qn'"
  }
}

# Output instructions on what the numbers above mean.
#
puts {
*** Definitions ******************************************************

Page size in bytes

    The number of bytes in a single page of the database file.  
    Usually 1024.

Number of pages in the whole file
}
puts \
"    The number of $pageSize-byte pages that go into forming the complete
    database"
puts \
{
Pages that store data

    The number of pages that store data, either as primary B*Tree pages or
    as overflow pages.  The number at the right is the data pages divided by
    the total number of pages in the file.

Pages on the freelist

    The number of pages that are not currently in use but are reserved for
    future use.  The percentage at the right is the number of freelist pages
    divided by the total number of pages in the file.

Pages of auto-vacuum overhead

    The number of pages that store data used by the database to facilitate
    auto-vacuum. This is zero for databases that do not support auto-vacuum.

Number of tables in the database

    The number of tables in the database, including the SQLITE_MASTER table
    used to store schema information.

Number of indices

    The total number of indices in the database.

Number of named indices

    The number of indices created using an explicit CREATE INDEX statement.

Automatically generated indices

    The number of indices used to implement PRIMARY KEY or UNIQUE constraints
    on tables.

Size of the file in bytes

    The total amount of disk space used by the entire database files.

Bytes of user payload stored

    The total number of bytes of user payload stored in the database. The
    schema information in the SQLITE_MASTER table is not counted when
    computing this number.  The percentage at the right shows the payload
    divided by the total file size.

Percentage of total database

    The amount of the complete database file that is devoted to storing
    information described by this category.

Number of entries

    The total number of B-Tree key/value pairs stored under this category.

Bytes of storage consumed

    The total amount of disk space required to store all B-Tree entries
    under this category.  The is the total number of pages used times
    the pages size.

Bytes of payload

    The amount of payload stored under this category.  Payload is the data
    part of table entries and the key part of index entries.  The percentage
    at the right is the bytes of payload divided by the bytes of storage 
    consumed.

Average payload per entry

    The average amount of payload on each entry.  This is just the bytes of
    payload divided by the number of entries.

Average unused bytes per entry

    The average amount of free space remaining on all pages under this
    category on a per-entry basis.  This is the number of unused bytes on
    all pages divided by the number of entries.

Maximum payload per entry

    The largest payload size of any entry.

Entries that use overflow

    The number of entries that user one or more overflow pages.

Total pages used

    This is the number of pages used to hold all information in the current
    category.  This is the sum of index, primary, and overflow pages.

Index pages used

    This is the number of pages in a table B-tree that hold only key (rowid)
    information and no data.

Primary pages used

    This is the number of B-tree pages that hold both key and data.

Overflow pages used

    The total number of overflow pages used for this category.

Unused bytes on index pages

    The total number of bytes of unused space on all index pages.  The
    percentage at the right is the number of unused bytes divided by the
    total number of bytes on index pages.

Unused bytes on primary pages

    The total number of bytes of unused space on all primary pages.  The
    percentage at the right is the number of unused bytes divided by the
    total number of bytes on primary pages.

Unused bytes on overflow pages

    The total number of bytes of unused space on all overflow pages.  The
    percentage at the right is the number of unused bytes divided by the
    total number of bytes on overflow pages.

Unused bytes on all pages

    The total number of bytes of unused space on all primary and overflow 
    pages.  The percentage at the right is the number of unused bytes 
    divided by the total number of bytes.
}

# Output the database
#
puts "**********************************************************************"
puts "The entire text of this report can be sourced into any SQL database"
puts "engine for further analysis.  All of the text above is an SQL comment."
puts "The data used to generate this report follows:"
puts "*/"
puts "BEGIN;"
puts $tabledef
unset -nocomplain x
mem eval {SELECT * FROM space_used} x {
  puts -nonewline "INSERT INTO space_used VALUES"
  set sep (
  foreach col $x(*) {
    set v $x($col)
    if {$v=="" || ![string is double $v]} {set v [quote $v]}
    puts -nonewline $sep$v
    set sep ,
  }
  puts ");"
}
puts "COMMIT;"
