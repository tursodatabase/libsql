#
# Run this TCL script to generate HTML for the goals.html file.
#
set rcsid {$Id: optimizer.tcl,v 1.1 2005/08/30 22:44:06 drh Exp $}
source common.tcl
header {The SQLite Query Optimizer}

proc CODE {text} {
  puts "<blockquote><pre>"
  puts $text
  puts "</pre></blockquote>"
}
proc IMAGE {name {caption {}}} {
  puts "<center><img src=\"$name\">"
  if {$caption!=""} {
    puts "<br>$caption"
  }
  puts "</center>"
}
proc PARAGRAPH {text} {
  puts "<p>$text</p>\n"
}
proc HEADING {level name} {
  puts "<h$level>$name</h$level>"
}

HEADING 1 {The SQLite Query Optimizer}

PARAGRAPH {
  This article describes how the SQLite query optimizer works.
  This is not something you have to know in order to use SQLite - many
  programmers use SQLite successfully without the slightest hint of what
  goes on in the inside.
  But a basic understanding of what SQLite is doing
  behind the scenes will help you to write more efficient SQL.  And the
  knowledge gained by studying the SQLite query optimizer has broad
  application since most other relational database engines operate 
  similarly.
  A solid understanding of how the query optimizer works is also
  required before making meaningful changes or additions to the SQLite, so 
  this article should be read closely by anyone aspiring
  to hack the source code.
}

HEADING 2 Background

PARAGRAPH {
  It is important to understand that SQL is a programming language.
  SQL is a perculiar programming language in that it
  describes <u>what</u> the programmer wants to compute not <u>how</u>
  to compute it as most other programming languages do.
  But perculiar or not, SQL is still just a programming language.
}

PARAGRAPH {
  It is very helpful to think of each SQL statement as a separate
  program.
  An important job of the SQL database engine is to translate each
  SQL statement from its descriptive form that specifies what the
  information is desired (the <u>what</u>) 
  into a procedural form that specifies how to go
  about acquiring the desired information (the <u>how</u>).
  The task of translating the <u>what</u> into a 
  <u>how</u> is assigned to the query optimizer.
}

PARAGRAPH {
  The beauty of SQL comes from the fact that the optimizer frees the programmer
  from having to worry over the details of <u>how</u>.  The programmer
  only has to specify the <u>what</u> and then leave the optimizer
  to deal with all of the minutae of implementing the
  <u>how</u>.  Thus the programmer is able to think and work at a
  much higher level and leave the optimizer to stress over the low-level
  work.
}

HEADING 2 {Database Layout}

PARAGRAPH {
  An SQLite database consists of one or more "b-trees".
  Each b-tree contains zero or more "rows". 
  A single row contains a "key" and some "data".
  In general, both the key and the data are arbitrary binary
  data of any length.
  The keys must all be unique within a single b-tree.
  Rows are stored in order of increasing key values - each
  b-tree has a comparision functions for keys that determines
  this order.
}

PARAGRAPH {
  In SQLite, each SQL table is stored as a b-tree where the
  key is a 64-bit integer and the data is the content of the
  table row.  The 64-bit integer key is the ROWID.  And, of course,
  if the table has an INTEGER PRIMARY KEY, then that integer is just
  an alias for the ROWID.
}

PARAGRAPH {
  Consider the following block of SQL code:
}

CODE {
  CREATE TABLE ex1(
     id INTEGER PRIMARY KEY,
     x  VARCHAR(30),
     y  INTEGER
  );
  INSERT INTO ex1 VALUES(NULL,'abc',12345);
  INSERT INTO ex1 VALUES(NULL,456,'def');
  INSERT INTO ex1 VALUES(100,'hello','world');
  INSERT INTO ex1 VALUES(-5,'abc','xyz');
  INSERT INTO ex1 VALUES(54321,NULL,987);
}

PARAGRAPH {
  This code generates a new b-tree (named "ex1") containing 5 rows.
  This table can be visualized as follows:
}
IMAGE table-ex1b2.gif

PARAGRAPH {
  Note that the key for each row if the b-tree is the INTEGER PRIMARY KEY
  for that row.  (Remember that the INTEGER PRIMARY KEY is just an alias
  for the ROWID.)  The other fields of the table form the data for each
  entry in the b-tree.  Note also that the b-tree entries are in ROWID order
  which is different from the order that they were originally inserted.
}

PARAGRAPH {
  Now consider the following SQL query:
}
CODE {
  SELECT y FROM ex1 WHERE x=456;
}

PARAGRAPH {
  When the SQLite parser and query optimizer are handed this query, they
  have to translate it into a procedure that will find the desired result.
  In this case, they do what is call a "full table scan".  They start
  at the beginning of the b-tree that contains the table and visit each
  row.  Within each row, the value of the "x" column is tested and when it
  is found to match 456, the value of the "y" column is output.
  We can represent this procedure graphically as follows:
}
IMAGE fullscanb.gif

PARAGRAPH {
  A full table scan is the access method of last resort.  It will always
  work.  But if the table contains millions of rows and you are only looking
  a single one, it might take a very long time to find the particular row
  you are interested in.
  In particular, the time needed to access a single row of the table is
  proportional to the total number of rows in the table.
  So a big part of the job of the optimizer is to try to find ways to 
  satisfy the query without doing a full table scan.
}
PARAGRAPH {
  The usual way to avoid doing a full table scan is use a binary search
  to find the particular row or rows of interest in the table.
  Consider the next query which searches on rowid instead of x:
}
CODE {
  SELECT y FROM ex1 WHERE rowid=2;
}

PARAGRAPH {
  In the previous query, we could not use a binary search for x because
  the values of x were not ordered.  But the rowid values are ordered.
  So instead of having to visit every row of the b-tree looking for one
  that has a rowid value of 2, we can do a binary search for that particular
  row and output its corresponding y value.  We show this graphically
  as follows:
}
IMAGE direct1b.gif

PARAGRAPH {
  When doing a binary search, we only have to look at a number of
  rows with is proportional to the logorithm of the number of entries
  in the table.  For a table with just 5 entires as in the example above,
  the difference between a full table scan and a binary search is
  negligible.  In fact, the full table scan might be faster.  But in
  a database that has 5 million rows, a binary search will be able to
  find the desired row in only about 23 tries, whereas the full table
  scan will need to look at all 5 million rows.  So the binary search
  is about 200,000 times faster in that case.
}
PARAGRAPH {
  A 200,000-fold speed improvement is huge.  So we always want to do
  a binary search rather than a full table scan when we can.
}
PARAGRAPH {
  The problem with a binary search is that the it only works if the
  fields you are search for are in sorted order.  So we can do a binary
  search when looking up the rowid because the rows of the table are
  sorted by rowid.  But we cannot use a binary search when looking up
  x because the values in the x column are in no particular order.
}
PARAGRAPH {
  The way to work around this problem and to permit binary searching on
  fields like x is to provide an index.
  An index is another b-tree.
  But in the index b-tree the key is not the rowid but rather the field
  or fields being indexed followed by the rowid.
  The data in an index b-tree is empty - it is not needed or used.
  The following diagram shows an index on the x field of our example table:
}
IMAGE index-ex1-x-b.gif

PARAGRAPH {
  An important point to note in the index are that they keys of the
  b-tree are in sorted order.  (Recall that NULL values in SQLite sort
  first, followed by numeric values in numerical order, then strings, and
  finally BLOBs.)  This is the property that will allow use to do a
  binary search for the field x.  The rowid is also included in every
  key for two reasons.  First, by including the rowid we guarantee that
  every key will be unique.  And second, the rowid will be used to look
  up the actual table entry after doing the binary search.  Finally, note
  that the data portion of the index b-tree serves no purpose and is thus
  kept empty to save space in the disk file.
}
PARAGRAPH {
  Remember what the original query example looked like:
}
CODE {
  SELECT y FROM ex1 WHERE x=456;
}

PARAGRAPH {
  The first time this query was encountered we had to do a full table
  scan.  But now that we have an index on x, we can do a binary search
  on that index for the entry where x==456.  Then from that entry we
  can find the rowid value and use the rowid to look up the corresponding
  entry in the original table.  From the entry in the original table,
  we can find the value y and return it as our result.  The following
  diagram shows this process graphically:
}
IMAGE indirect1b1.gif

PARAGRAPH {
  With the index, we are able to look up an entry based on the value of
  x after visiting only a logorithmic number of b-tree entries.  Unlike
  the case where we were searching using rowid, we have to do two binary
  searches for each output row.  But for a 5-million row table, that is
  still only 46 searches instead of 5 million for a 100,000-fold speedup.
}

HEADING 3 {Parsing The WHERE Clause}



# parsing the where clause
# rowid lookup
# index lookup
# index lookup without the table
# how an index is chosen
# joins
# join reordering
# order by using an index
# group by using an index
# OR -> IN optimization
# Bitmap indices
# LIKE and GLOB optimization
# subquery flattening
# MIN and MAX optimizations
