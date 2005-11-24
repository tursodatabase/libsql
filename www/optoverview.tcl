#
# Run this TCL script to generate HTML for the goals.html file.
#
set rcsid {$Id: optoverview.tcl,v 1.5 2005/11/24 13:15:34 drh Exp $}
source common.tcl
header {The SQLite Query Optimizer Overview}

proc CODE {text} {
  puts "<blockquote><pre>"
  puts $text
  puts "</pre></blockquote>"
}
proc SYNTAX {text} {
  puts "<blockquote><pre>"
  set t2 [string map {& &amp; < &lt; > &gt;} $text]
  regsub -all "/(\[^\n/\]+)/" $t2 {</b><i>\1</i><b>} t3
  puts "<b>$t3</b>"
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
  # regsub -all "/(\[a-zA-Z0-9\]+)/" $text {<i>\1</i>} t2
  regsub -all "\\*(\[^\n*\]+)\\*" $text {<tt><b><big>\1</big></b></tt>} t3
  puts "<p>$t3</p>\n"
}
set level(0) 0
set level(1) 0
proc HEADING {n name {tag {}}} {
  if {$tag!=""} {
    puts "<a name=\"$tag\">"
  }
  global level
  incr level($n)
  for {set i [expr {$n+1}]} {$i<10} {incr i} {
    set level($i) 0
  }
  if {$n==0} {
    set num {}
  } elseif {$n==1} {
    set num $level(1).0
  } else {
    set num $level(1)
    for {set i 2} {$i<=$n} {incr i} {
      append num .$level($i)
    }
  }
  incr n 1
  puts "<h$n>$num $name</h$n>"
}

HEADING 0 {The SQLite Query Optimizer Overview}

PARAGRAPH {
  This document provides a terse overview of how the query optimizer
  for SQLite works.  This is not a tutorial.  The reader is likely to
  need some prior knowledge of how database engines operate 
  in order to fully understand this text.
}

HEADING 1 {WHERE clause analysis} where_clause

PARAGRAPH {
  The WHERE clause on a query is broken up into "terms" where each term
  is separated from the others by an AND operator.
}
PARAGRAPH {
  All terms of the WHERE clause are analyzed to see if they can be
  satisfied using indices.
  Terms that cannot be satisfied through the use of indices become
  tests that are evaluated against each row of the relevant input
  tables.  No tests are done for terms that are completely satisfied by
  indices.  Sometimes
  one or more terms will provide hints to indices but still must be
  evaluated against each row of the input tables.
}

PARAGRAPH {
  The analysis of a term might cause new "virtual" terms to
  be added to the WHERE clause.  Virtual terms can be used with
  indices to restrict a search.  But virtual terms never generate code
  that is tested against input rows.
}

PARAGRAPH {
  To be usable by an index a term must be of one of the following
  forms:
}
SYNTAX {
  /column/ = /expression/
  /column/ > /expression/
  /column/ >= /expression/
  /column/ < /expression/
  /column/ <= /expression/
  /expression/ = /column/
  /expression/ > /column/
  /expression/ >= /column/
  /expression/ < /column/
  /expression/ <= /column/
  /column/ IN (/expression-list/)
  /column/ IN (/subquery/)
}
PARAGRAPH {
  If an index is created using a statement like this:
}
CODE {
  CREATE INDEX idx_ex1 ON ex1(a,b,c,d,e,...,y,z);
}
PARAGRAPH {
  Then the index might be used if the initial columns of the index
  (columns a, b, and so forth) appear in WHERE clause terms.
  All index columns must be used with
  the *=* or *IN* operators except for
  the right-most column which can use inequalities.  For the right-most
  column of an index that is used, there can be up to two inequalities
  that must sandwich the allowed values of the column between two extremes.
}
PARAGRAPH {
  It is not necessary for every column of an index to appear in a
  WHERE clause term in order for that index to be used. 
  But there can not be gaps in the columns of the index that are used.
  Thus for the example index above, if there is no WHERE clause term
  that constraints column c, then terms that constraint columns a and b can
  be used with the index but not terms that constraint columns d through z.
  Similarly, no index column will be used (for indexing purposes)
  that is to the right of a 
  column that is constrained only by inequalities.
  For the index above and WHERE clause like this:
}
CODE {
  ... WHERE a=5 AND b IN (1,2,3) AND c>12 AND d='hello'
}
PARAGRAPH {
  Only columns a, b, and c of the index would be usable.  The d column
  would not be usable because it occurs to the right of c and c is
  constrained only by inequalities.
}

HEADING 1 {The BETWEEN optimization} between_opt

PARAGRAPH {
  If a term of the WHERE clause is of the following form:
}
SYNTAX {
  /expr1/ BETWEEN /expr2/ AND /expr3/
}
PARAGRAPH {
  Then two virtual terms are added as follows:
}
SYNTAX {
  /expr1/ >= /expr2/ AND /expr1/ <= /expr3/
}
PARAGRAPH {
  If both virtual terms end up being used as constraints on an index,
  then the original BETWEEN term is omitted and the corresponding test
  is not performed on input rows.
  Thus if the BETWEEN term ends up being used as an index constraint
  no tests are ever performed on that term.
  On the other hand, the
  virtual terms themselves never causes tests to be performed on
  input rows.
  Thus if the BETWEEN term is not used as an index constraint and
  instead must be used to test input rows, the <i>expr1</i> expression is
  only evaluated once.
}

HEADING 1 {The OR optimization} or_opt

PARAGRAPH {
  If a term consists of multiple subterms containing a common column
  name and separated by OR, like this:
}
SYNTAX {
  /column/ = /expr1/ OR /column/ = /expr2/ OR /column/ = /expr3/ OR ...
}
PARAGRAPH {
  Then the term is rewritten as follows:
}
SYNTAX {
  /column/ IN (/expr1/,/expr2/,/expr3/,/expr4/,...)
}
PARAGRAPH {
  The rewritten term then might go on to constraint an index using the
  normal rules for *IN* operators.
  Note that <i>column</i> must be the same column in every OR-connected subterm,
  although the column can occur on either the left or the right side of
  the *=* operator.
}

HEADING 1 {The LIKE optimization} like_opt

PARAGRAPH {
  Terms that are composed of the LIKE or GLOB operator
  can sometimes be used to constrain indices.
  There are many conditions on this use:
}
PARAGRAPH {
  <ol>
  <li>The left-hand side of the LIKE or GLOB operator must be the name
      of an indexed column.</li>
  <li>The right-hand side of the LIKE or GLOB must be a string literal
      that does not begin with a wildcard character.</li>
  <li>The ESCAPE clause cannot appear on the LIKE operator.</li>
  <li>The build-in functions used to implement LIKE and GLOB must not
      have been overloaded using the sqlite3_create_function() API.</li>
  <li>For the GLOB operator, the column must use the default BINARY
      collating sequence.</li>
  <li>For the LIKE operator, if case_sensitive_like mode is enabled then
      the column must use the default BINARY collating sequence, or if
      case_sensitive_like mode is disabled then the column must use the
      built-in NOCASE collating sequence.</li>
  </ol>
}
PARAGRAPH {
  The LIKE operator has two modes that can be set by a pragma.  The
  default mode is for LIKE comparisons to be insensitive to differences
  of case for latin1 characters.  Thus, by default, the following
  expression is true:
}
CODE {
  'a' LIKE 'A'
}
PARAGRAPH {
  By turned on the case_sensitive_like pragma as follows:
}
CODE {
  PRAGMA case_sensitive_like=ON;
}
PARAGRAPH {
  Then the LIKE operator pays attention to case and the example above would
  evaluate to false.  Note that case insensitivity only applies to
  latin1 characters - basically the upper and lower case letters of English
  in the lower 127 byte codes of ASCII.  International character sets
  are case sensitive in SQLite unless a user-supplied collating
  sequence is used.  But if you employ a user-supplied collating sequence,
  the LIKE optimization describe here will never be taken.
}
PARAGRAPH {
  The LIKE operator is case insensitive by default because this is what
  the SQL standard requires.  You can change the default behavior at
  compile time by using the -DSQLITE_CASE_SENSITIVE_LIKE command-line option
  to the compiler.
}
PARAGRAPH {
  The LIKE optimization might occur if the column named on the left of the
  operator uses the BINARY collating sequence (which is the default) and
  case_sensitive_like is turned on.  Or the optimization might occur if
  the column uses the built-in NOCASE collating sequence and the 
  case_sensitive_like mode is off.  These are the only two combinations
  under which LIKE operators will be optimized.  If the column on the
  right-hand side of the LIKE operator uses any collating sequence other
  than the built-in BINARY and NOCASE collating sequences, then no optimizations
  will ever be attempted on the LIKE operator.
}
PARAGRAPH {
  The GLOB operator is always case sensitive.  The column on the left side
  of the GLOB operator must always use the built-in BINARY collating sequence
  or no attempt will be made to optimize that operator with indices.
}
PARAGRAPH {
  The right-hand side of the GLOB or LIKE operator must be a literal string
  value that does not begin with a wildcard.  If the right-hand side is a
  parameter that is bound to a string, then no optimization is attempted.
  If the right-hand side begins with a wildcard character then no 
  optimization is attempted.
}
PARAGRAPH {
  Suppose the initial sequence of non-wildcard characters on the right-hand
  side of the LIKE or GLOB operator is <i>x</i>.  We are using a single 
  character to denote this non-wildcard prefix but the reader should
  understand that the prefix can consist of more than 1 character.
  Let <i>y</i> the smallest string that is the same length as /x/ but which
  compares greater than <i>x</i>.  For example, if <i>x</i> is *hello* then
  <i>y</i> would be *hellp*.
  The LIKE and GLOB optimizations consist of adding two virtual terms
  like this:
}
SYNTAX {
  /column/ >= /x/ AND /column/ < /y/
}
PARAGRAPH {
  Under most circumstances, the original LIKE or GLOB operator is still
  tested against each input row even if the virtual terms are used to
  constrain an index.  This is because we do not know what additional
  constraints may be imposed by characters to the right
  of the <i>x</i> prefix.  However, if there is only a single global wildcard
  to the right of <i>x</i>, then the original LIKE or GLOB test is disabled.
  In other words, if the pattern is like this:
}
SYNTAX {
  /column/ LIKE /x/%
  /column/ GLOB /x/*
}
PARAGRAPH {
  Then the original LIKE or GLOB tests are disabled when the virtual
  terms constrain an index because in that case we know that all of the
  rows selected by the index will pass the LIKE or GLOB test.
}

HEADING 1 {Joins} joins

PARAGRAPH {
  The current implementation of 
  SQLite uses only loop joins.  That is to say, joins are implemented as
  nested loops.
}
PARAGRAPH {
  The default order of the nested loops in a join is for the left-most
  table in the FROM clause to form the outer loop and the right-most
  table to form the inner loop.
  However, SQLite will nest the loops in a different order if doing so
  will help it to select better indices.
}
PARAGRAPH {
  Inner joins can be freely reordered.  However a left outer join is
  neither commutative nor associative and hence will not be reordered.
  Inner joins to the left and right of the outer join might be reordered
  if the optimizer thinks that is advantageous but the outer joins are
  always evaluated in the order in which they occur.
}
PARAGRAPH {
  When selecting the order of tables in a join, SQLite uses a greedy
  algorithm that runs in polynomial time.
}
PARAGRAPH {
  The ON and USING clauses of a join are converted into additional
  terms of the WHERE clause prior to WHERE clause analysis described
  above in paragraph 1.0.  Thus
  with SQLite, there is no advantage to use the newer SQL92 join syntax
  over the older SQL89 comma-join syntax.  They both end up accomplishing
  exactly the same thing.
}
PARAGRAPH {
  Join reordering is automatic and usually works well enough that
  programmer do not have to think about it.  But occasionally some
  hints from the programmer are needed.  For a description of when
  hints might be necessary and how to provide those hints, see the
  <a href="http://www.sqlite.org/cvstrac/wiki?p=QueryPlans">QueryPlans</a>
  page in the Wiki.
}

HEADING 1 {Choosing between multiple indices} multi_index

PARAGRAPH {
  Each table in the FROM clause of a query can use at most one index,
  and SQLite strives to use at least one index on each table.  Sometimes,
  two or more indices might be candidates for use on a single table.
  For example:
}
CODE {
  CREATE TABLE ex2(x,y,z);
  CREATE INDEX ex2i1 ON ex2(x);
  CREATE INDEX ex2i2 ON ex2(y);
  SELECT z FROM ex2 WHERE x=5 AND y=6;
}
PARAGRAPH {
  For the SELECT statement above, the optimizer can use the ex2i1 index
  to lookup rows of ex2 that contain x=5 and then test each row against
  the y=6 term.  Or it can use the ex2i2 index to lookup rows
  of ex2 that contain y=6 then test each of those rows against the
  x=5 term.
}
PARAGRAPH {
  When faced with a choice of two or more indices, SQLite tries to estimate
  the total amount of work needed to perform the query using each option.
  It then selects the option that gives the least estimated work.
}
PARAGRAPH {
  To help the optimizer get a more accurate estimate of the work involved
  in using various indices, the user may optional run the ANALYZE command.
  The ANALYZE command scans all indices of database where there might
  be a choice between two or more indices and gathers statistics on the
  selectiveness of those indices.  The results of this scan are stored
  in the sqlite_stat1 table.
  The contents of the sqlite_stat1 table are not updated as the database
  changes so after making significant changes it might be prudent to
  rerun ANALYZE.
  The results of an ANALYZE command are only available to database connections
  that are opened after the ANALYZE command completes.
}
PARAGRAPH {
  Once created, the sqlite_stat1 table cannot be dropped.  But its
  content can be viewed, modified, or erased.  Erasing the entire content
  of the sqlite_stat1 table has the effect of undoing the ANALYZE command.
  Changing the content of the sqlite_stat1 table can get the optimizer
  deeply confused and cause it to make silly index choices.  Making
  updates to the sqlite_stat1 table (except by running ANALYZE) is
  not recommended.
}
PARAGRAPH {
  Terms of the WHERE clause can be manually disqualified for use with
  indices by prepending a unary *+* operator to the column name.  The
  unary *+* is a no-op and will not slow down the evaluation of the test
  specified by the term.
  But it will prevent the term from constraining an index.
  So, in the example above, if the query were rewritten as:
}
CODE {
  SELECT z FROM ex2 WHERE +x=5 AND y=6;
}
PARAGRAPH {
  The *+* operator on the *x* column would prevent that term from 
  constraining an index.  This would force the use of the ex2i2 index.
}

HEADING 1 {Avoidance of table lookups} index_only

PARAGRAPH {
  When doing an indexed lookup of a row, the usual procedure is to
  do a binary search on the index to find the index entry, then extract
  the rowid from the index and use that rowid to do a binary search on
  the original table.  Thus a typical indexed lookup involves two
  binary searches.
  If, however, all columns that were to be fetched from the table are
  already available in the index itself, SQLite will use the values
  contained in the index and will never look up the original table
  row.  This saves one binary search for each row and can make many
  queries run twice as fast.
}

HEADING 1 {ORDER BY optimizations} order_by

PARAGRAPH {
  SQLite attempts to use an index to satisfy the ORDER BY clause of a
  query when possible.
  When faced with the choice of using an index to satisfy WHERE clause
  constraints or satisfying an ORDER BY clause, SQLite does the same
  work analysis described in section 6.0
  and chooses the index that it believes will result in the fastest answer.

}

HEADING 1 {Subquery flattening} flattening

PARAGRAPH {
  When a subquery occurs in the FROM clause of a SELECT, the default
  behavior is to evaluate the subquery into a transient table, then run
  the outer SELECT against the transient table. 
  This is problematic since the transient table will not have any indices
  and the outer query (which is likely a join) will be forced to do a
  full table scan on the transient table.
}
PARAGRAPH {
  To overcome this problem, SQLite attempts to flatten subqueries in
  the FROM clause of a SELECT.
  This involves inserting the FROM clause of the subquery into the
  FROM clause of the outer query and rewriting expressions in
  the outer query that refer to the result set of the subquery.
  For example:
}
CODE {
  SELECT a FROM (SELECT x+y AS a FROM t1 WHERE z<100) WHERE a>5
}
PARAGRAPH {
  Would be rewritten using query flattening as:
}
CODE {
  SELECT x+y AS a FROM t1 WHERE z<100 AND a>5
}
PARAGRAPH {
  There is a long list of conditions that must all be met in order for
  query flattening to occur.
}
PARAGRAPH {
  <ol>
  <li> The subquery and the outer query do not both use aggregates.</li>
  <li> The subquery is not an aggregate or the outer query is not a join. </li>
  <li> The subquery is not the right operand of a left outer join, or
          the subquery is not itself a join. </li>
  <li>  The subquery is not DISTINCT or the outer query is not a join. </li>
  <li>  The subquery is not DISTINCT or the outer query does not use
          aggregates. </li>
  <li>  The subquery does not use aggregates or the outer query is not
          DISTINCT. </li>
  <li>  The subquery has a FROM clause. </li>
  <li>  The subquery does not use LIMIT or the outer query is not a join. </li>
  <li>  The subquery does not use LIMIT or the outer query does not use
         aggregates. </li>
  <li>  The subquery does not use aggregates or the outer query does not
         use LIMIT. </li>
  <li>  The subquery and the outer query do not both have ORDER BY clauses.</li>
  <li>  The subquery is not the right term of a LEFT OUTER JOIN or the
         subquery has no WHERE clause.  </li>
  </ol>
}
PARAGRAPH {
  The proof that query flattening may safely occur if all of the the
  above conditions are met is left as an exercise to the reader.
}
PARAGRAPH {
  Query flattening is an important optimization when views are used as
  each use of a view is translated into a subquery.
}

HEADING 1 {The MIN/MAX optimization} minmax

PARAGRAPH {
  Queries of the following forms will be optimized to run in logarithmic
  time assuming appropriate indices exist:
}
CODE {
  SELECT MIN(x) FROM table;
  SELECT MAX(x) FROM table;
}
PARAGRAPH {
  In order for these optimizations to occur, they must appear in exactly
  the form shown above - changing only the name of the table and column.
  It is not permissible to add a WHERE clause or do any arithmetic on the
  result.  The result set must contain a single column.
  The column in the MIN or MAX function must be an indexed column.
}
