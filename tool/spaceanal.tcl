# Run this TCL script using "testfixture" in order get a report that shows
# how much disk space is used by a particular data to actually store data
# versus how much space is unused.
#

# Get the name of the database to analyze
#
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
if {[file size $file_to_analyze]<2048} {
  puts stderr "Empty or malformed database: $file_to_analyze"
  exit 1
}

# Open the database
#
sqlite db [lindex $argv 0]
set DB [btree_open [lindex $argv 0]]

# In-memory database for collecting statistics
#
sqlite mem :memory:
set tabledef\
{CREATE TABLE space_used(
   name clob,        -- Name of a table or index in the database file
   tblname clob,     -- Name of associated table
   is_index boolean, -- TRUE if it is an index, false for a table
   nentry int,       -- Number of entries in the BTree
   payload int,      -- Total amount of data stored in this table or index
   mx_payload int,   -- Maximum payload size
   n_ovfl int,       -- Number of entries that overflow
   pri_pages int,    -- Number of primary pages used
   ovfl_pages int,   -- Number of overflow pages used
   pri_unused int,   -- Number of unused bytes on primary pages
   ovfl_unused int   -- Number of unused bytes on overflow pages
);}
mem eval $tabledef

# This query will be used to find the root page number for every index and
# table in the database.
#
set sql {
  SELECT name, tbl_name, type, rootpage 
    FROM sqlite_master WHERE type IN ('table','index')
  UNION ALL
  SELECT 'sqlite_master', 'sqlite_master', 'table', 2
  ORDER BY 1
}

# Analyze every table in the database, one at a time.
#
foreach {name tblname type rootpage} [db eval $sql] {
  puts stderr "Analyzing $name..."
  set cursor [btree_cursor $DB $rootpage 0]
  set go [btree_first $cursor]
  set size 0
  catch {unset pg_used}
  set unused_ovfl 0
  set n_overflow 0
  set cnt_ovfl 0
  set n_entry 0
  set mx_size 0
  set pg_used($rootpage) 1016
  while {$go==0} {
    incr n_entry
    set payload [btree_payload_size $cursor]
    incr size $payload
    set stat [btree_cursor_dump $cursor]
    set pgno [lindex $stat 0]
    set freebytes [lindex $stat 4]
    set pg_used($pgno) $freebytes
    if {$payload>236} {
      # if {[lindex $stat 8]==0} {error "overflow is empty with $payload"}
      set n [expr {($payload-236+1019)/1020}]
      incr n_overflow $n
      incr cnt_ovfl
      incr unused_ovfl [expr {$n*1020+236-$payload}]
    } else {
      # if {[lindex $stat 8]!=0} {error "overflow not empty with $payload"}
    }
    if {$payload>$mx_size} {set mx_size $payload}
    set go [btree_next $cursor]
  }
  btree_close_cursor $cursor
  set n_primary [llength [array names pg_used]]
  set unused_primary 0
  foreach x [array names pg_used] {incr unused_primary $pg_used($x)}
  regsub -all ' $name '' name
  set sql "INSERT INTO space_used VALUES('$name'"
  regsub -all ' $tblname '' tblname
  append sql ",'$tblname',[expr {$type=="index"}],$n_entry"
  append sql ",$size,$mx_size,$cnt_ovfl,"
  append sql "$n_primary,$n_overflow,$unused_primary,$unused_ovfl);"
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
proc percent {num denom} {
  if {$denom==0.0} {return ""}
  set v [expr {$num*100.0/$denom}]
  if {$v>1.0 && $v<99.0} {
    return [format %4.1f%% $v]
  } elseif {$v<0.1 || $v>99.9} {
    return [format %6.3f%% $v]
  } else {
    return [format %5.2f%% $v]
  }
}

# Generate a subreport that covers some subset of the database.
# the $where clause determines which subset to analyze.
#
proc subreport {title where} {
  set hit 0
  mem eval "SELECT sum(nentry) AS nentry, \
                   sum(payload) AS payload, \
                   sum(CASE is_index WHEN 1 THEN 0 ELSE payload-4*nentry END) \
                       AS data, \
                   max(mx_payload) AS mx_payload, \
                   sum(n_ovfl) as n_ovfl, \
                   sum(pri_pages) AS pri_pages, \
                   sum(ovfl_pages) AS ovfl_pages, \
                   sum(pri_unused) AS pri_unused, \
                   sum(ovfl_unused) AS ovfl_unused \
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
  statline "Percentage of total database" \
     [percent [expr {$pri_pages+$ovfl_pages}] $::file_pgcnt]
  statline "Number of entries" $nentry
  set storage [expr {($pri_pages+$ovfl_pages)*1024}]
  statline "Bytes of storage consumed" $storage
  statline "Bytes of payload" $payload [percent $payload $storage]
  statline "Bytes of data" $data [percent $data $storage]
  set key [expr {$payload-$data}]
  statline "Bytes of key" $key [percent $key $storage]
  set avgpay [expr {$nentry>0?$payload/$nentry:0}]
  statline "Average payload per entry" $avgpay
  set avgunused [expr {$nentry>0?($pri_unused+$ovfl_unused)/$nentry:0}]
  statline "Average unused bytes per entry" $avgunused
  statline "Average fanout" \
     [format %.2f [expr {$pri_pages==0?0:($nentry+0.0)/$pri_pages}]]
  statline "Maximum payload per entry" $mx_payload
  statline "Entries that use overflow" $n_ovfl [percent $n_ovfl $nentry]
  statline "Total pages used" [set allpgs [expr {$pri_pages+$ovfl_pages}]]
  statline "Primary pages used" $pri_pages ;# [percent $pri_pages $allpgs]
  statline "Overflow pages used" $ovfl_pages ;# [percent $ovfl_pages $allpgs]
  statline "Unused bytes on primary pages" $pri_unused \
               [percent $pri_unused [expr {$pri_pages*1024}]]
  statline "Unused bytes on overflow pages" $ovfl_unused \
               [percent $ovfl_unused [expr {$ovfl_pages*1024}]]
  set allunused [expr {$ovfl_unused+$pri_unused}]
  statline "Unused bytes on all pages" $allunused \
               [percent $allunused [expr {$allpgs*1024}]]
  return 1
}

# Output summary statistics:
#
puts "/** Disk-Space Utilization Report For $file_to_analyze"
puts "*** As of [clock format [clock seconds] -format {%Y-%b-%d %H:%M:%S}]"
puts ""
set fsize [file size [lindex $argv 0]]
set file_pgcnt [expr {$fsize/1024}]
set usedcnt [mem eval {SELECT sum(pri_pages+ovfl_pages) FROM space_used}]
set freecnt [expr {$file_pgcnt-$usedcnt-1}]
set freecnt2 [lindex [btree_get_meta $DB] 0]
statline {Pages in the whole file (measured)} $file_pgcnt
set file_pgcnt2 [expr {$usedcnt+$freecnt2+1}]
statline {Pages in the whole file (calculated)} $file_pgcnt2
statline {Pages that store data} $usedcnt [percent $usedcnt $file_pgcnt]
statline {Pages on the freelist (per header)}\
   $freecnt2 [percent $freecnt2 $file_pgcnt]
statline {Pages on the freelist (calculated)}\
   $freecnt [percent $freecnt $file_pgcnt]
statline {Header pages} 1 [percent 1 $file_pgcnt]

set ntable [db eval {SELECT count(*)+1 FROM sqlite_master WHERE type='table'}]
statline {Number of tables in the database} $ntable
set nindex [db eval {SELECT count(*) FROM sqlite_master WHERE type='index'}]
set autoindex [db eval {SELECT count(*) FROM sqlite_master
                        WHERE type='index' AND name LIKE '(% autoindex %)'}]
set manindex [expr {$nindex-$autoindex}]
statline {Number of indices} $nindex
statline {Number of named indices} $manindex [percent $manindex $nindex]
statline {Automatically generated indices} $autoindex \
     [percent $autoindex $nindex]

set bytes_data [mem eval "SELECT sum(payload-4*nentry) FROM space_used
                          WHERE NOT is_index AND name!='sqlite_master'"]
set total_payload [mem eval "SELECT sum(payload) FROM space_used"]
statline "Size of the file in bytes" $fsize
statline "Bytes of payload stored" $total_payload \
    [percent $total_payload $fsize]
statline "Bytes of user data stored" $bytes_data \
    [percent $bytes_data $fsize]

# Output table rankings
#
puts ""
puts "*** Page counts for all tables with their indices ********************"
puts ""
mem eval {SELECT tblname, count(*) AS cnt, sum(pri_pages+ovfl_pages) AS size
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

Number of pages in the whole file

    The number of 1024-byte pages that go into forming the complete database

Pages that store data

    The number of pages that store data, either as primary B*Tree pages or
    as overflow pages.  The number at the right is the data pages divided by
    the total number of pages in the file.

Pages on the freelist

    The number of pages that are not currently in use but are reserved for
    future use.  The percentage at the right is the number of freelist pages
    divided by the total number of pages in the file.

Header pages

    The number of pages of header overhead in the database.  This value is
    always 1.  The percentage at the right is the number of header pages
    divided by the total number of pages in the file.

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

Bytes of payload stored

    The total number of bytes of payload stored in the database.  Payload
    includes both key and data.  The content of the SQLITE_MASTER table is
    counted when computing this number.  The percentage at the right shows
    the payload divided by the total file size.

Bytes of user data stored

    The total number of bytes of data stored in the database, not counting
    the database schema information stored in the SQLITE_MASTER table.  The
    percentage at the right is the user data size divided by the total file
    size.

Percentage of total database

    The amount of the complete database file that is devoted to storing
    information described by this category.

Number of entries

    The total number of B*Tree key/value pairs stored under this category.

Bytes of storage consumed

    The total amount of disk space required to store all B*Tree entries
    under this category.  The is the total number of pages used times
    the pages size (1024).

Bytes of payload

    The amount of payload stored under this category.  Payload is the sum
    of keys and data.  Each table entry has 4 bytes of key and an arbitrary
    amount of data.  Each index entry has 4 or more bytes of key and no
    data.  The percentage at the right is the bytes of payload divided by
    the bytes of storage consumed.

Bytes of data

    The amount of data stored under this category.  The data space reported
    includes formatting information such as nul-terminators and field-lengths
    that are stored with the data.  The percentage at the right is the bytes
    of data divided by bytes of storage consumed.

Bytes of key

    The sum of the sizes of all keys under this category.  The percentage at
    the right is the bytes of key divided by the bytes of storage consumed.

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

    Up to 236 bytes of payload for each entry are stored directly in the
    primary B*Tree page.  Any additional payload is stored on a linked list
    of overflow pages.  This is the number of entries that exceed 236 bytes
    in size.  The value to the right is the number of entries that overflow
    divided by the total number of entries.

Total pages used

    This is the number of 1024 byte pages used to hold all information in
    the current category.  This is the sum of primary and overflow pages.

Primary pages used

    This is the number of primary B*Tree pages used.

Overflow pages used

    The total number of overflow pages used for this category.

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
  puts -nonewline "INSERT INTO space_used VALUES("
  regsub ' $x(name) '' qn
  regsub ' $x(tblname) '' qtn
  puts -nonewline "'$qn','$qtn',"
  puts -nonewline "$x(is_index),$x(nentry),$x(payload),$x(mx_payload),"
  puts -nonewline "$x(n_ovfl),$x(pri_pages),$x(ovfl_pages),$x(pri_unused),"
  puts "$x(ovfl_unused));"
}
puts "COMMIT;"
