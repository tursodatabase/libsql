#
# Run this Tcl script to generate the vdbe.html file.
#
set rcsid {$Id: vdbe.tcl,v 1.1 2000/06/23 17:02:18 drh Exp $}

puts {<html>
<head>
  <title>The Virtual Database Engine of SQLite</title>
</head>
<body bgcolor=white>
<h1 align=center>
The Virtual Database Engine of SQLite
</h1>}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] GMT)
</p>"
puts {
<blockquote><font color="red"><b>This document is
currently under development.  It is incomplete and contains
errors.  Use it accordingly.</b></font></blockquote>
}

puts {
<p>If you want to know how the SQLite library works internally,
you need to begin with a solid understanding of the Virtual Database
Engine or VDBE.  The VDBE occurs right in the middle of the
processing stream (see the <a href="arch.html">architecture diagram</a>)
and so it seems to touch most as parts of the library.  Even
parts of the code that do not directly interact with the VDBE
are usually in a supporting role.  The VDBE really is the heart of
SQLite.</p>

<p>This article is a brief tutorial introduction to how the VDBE
works and in particular how the various VDBE instructions
(documented <a href="opcode.html">here</a>) work together
to do useful things with the database.  The style is tutorial,
beginning with simple tasks and working toward solving more
complex problems.  Along the way we will touch briefly on most
aspects of the SQLite library.  After completeing this tutorial,
you should have a pretty good understanding of how SQLite works
and will be ready to begin studying the actual source code.</p>

<h2>Preliminaries</h2>

<p>The VDBE implements a virtual computer that runs a program in
its virtual machine language.  The goal of each program is to 
interagate or change the database.  Toward this end, the machine
language that the VDBE implements is specifically designed to
work with databases.</p>

<p>Each instruction of the VDBE language contains an opcode and
three operands labeled P1, P2, and P3.  Operand P1 is an arbitrary
integer.   P2 is a non-negative integer.  P3 is a null-terminated
string, or possibly just a null pointer.  Only a few VDBE
instructions use all three operands.  Many instructions use only
one or two operands.  A significant number of instructions use
no operands at all, taking there data and storing their results
on the execution stack.  The details of what each instruction
does and which operands it uses are described in the separate
<a href="opcode.html">opcode description</a> document.</p>

<p>A VDBE program begins
execution on instruction 0 and continues with successive instructions
until it either (1) encounters a fatal error, (2) executes a
Halt instruction, or (3) advances the program counter past the
last instruction of the program.  When the VDBE completes execution,
all open database cursors are closed, all memory is freed, and 
everything is popped from the stack.
So there are never any worries about memory leaks or 
undeallocated resources.</p>

<p>If you have done any assembly language programming or have
worked with any kind of abstract machine before, all of these
details should be familiar to you.  So let's jump right in and
start looking as some code.</p>

<a name="insert1">
<h2>Inserting Records Into The Database</h2>

<p>We begin with a problem that can be solved using a VDBE program
that is only a few instructions long.  Suppose we have an SQL
table that was created like this:</p>

<blockquote><pre>
CREATE TABLE ex1(col1 text);
</pre></blockquote>

<p>In words, we have a database table named "ex1" that has a single
column of data named "col1".  Now suppose we want to insert a single
record into this table.  Like this:</p>

<blockquote><pre>
INSERT INTO ex1 VALUES('Hello, World!');
</pre></blockquote>

<p>We can see the VDBE program that SQLite uses to implement this
INSERT using the <b>sqlite</b> command-line utility.  First start
up <b>sqlite</b> on a new, empty database, then create the table.
Finally, enter the INSERT statement shown above, but precede the
INSERT with the special keyword "EXPLAIN".  The EXPLAIN keyword
will cause <b>sqlite</b> to print the VDBE program rather than 
execute it.  We have:</p>
}

proc Code {body} {
  puts {<blockquote><pre>}
  regsub -all {&} [string trim $body] {\&amp;} body
  regsub -all {>} $body {\&gt;} body
  regsub -all {<} $body {\&lt;} body
  regsub -all {\(\(\(} $body {<font color="#00671f"><u>} body
  regsub -all {\)\)\)} $body {</u></font>} body
  puts $body
  puts {</pre></blockquote>}
}

Code {
$ (((sqlite test_database_1)))
sqlite> (((CREATE TABLE ex1(col1 test);)))
sqlite> (((.explain)))
sqlite> (((EXPLAIN INSERT INTO ex1 VALUES('Hello, World!');)))
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     Open          0      1      ex1                                     
1     New           0      0                                              
2     String        0      0      Hello, World!                           
3     MakeRecord    1      0                                              
4     Put           0      0                                              
}

puts {<p>As you can see above, our simple insert statement is
implemented in just 5 instructions.  There are no jumps, so the
program executes once through from top to bottom.  Let's now
look at each instruction in detail.</p>

<p>The first instruction opens a cursor that points into the
"ex1" table.   The P1 operand is a handle for the cursor: zero
in this case.  Cursor handles can be any non-negative integer.
But the VDBE allocates cursors in an array with the size of the
array being one more than the largest cursor.  So to conserve
memory, it is best to use handles beginning with zero and
working upward consecutively.</p>

<p>The P2 operand to the open instruction is 1 which means
that the cursor is opened for writing.  0 would have been used
for P2 if we wanted to open the cursor for reading only.
It is acceptable to open more than one cursor to the same
database file at the same time.  But all simultaneously
opened cursors must be opened with the same P2 value.  It is
not allowed to have one cursor open for reading a file and
another cursor open for writing that same file.</p>

<p>The second instruction, New, generates an integer key that
has not been previously used in the file "ex1".  The New instruction
uses its P1 operand as the handle of a cursor for the file
for which the new key will be generated.  The new key is
pushed onto the stack.  The P2 and P3 operands are not used
by the New instruction.</p>

<p>The third instruction, String, simply pushes its P3
operand onto the stack.  After the string instruction executes,
the stack will contain two elements, as follows:</p>
}

proc stack args {
  puts "<blockquote><table border=2>"
  foreach elem $args {
    puts "<tr><td align=center>$elem</td></tr>"
  }
  puts "</table></blockquote>"
}

stack {The string "Hello, World!"} {A random integer key}

puts {<p>The 4th instructionn, MakeRecord, pops the top P1
elements off the stack (1 element in this case) and converts them
all into the binary format used for storing records in a
database file.  (See the <a href="fileformat.html">file format</a>
description for details.)  The record format consists of
a header with one integer for each column giving the offset
into the record for the beginning of data for that column.
Following the header is the data for each column,  Each column
is stored as a null-terminated ASCII text string.  The new
record generated by the MakeRecord instruction is pushed back
onto the stack, so that after the 4th instruction executes,
the stack looks like this:</p>
}

stack {A one-column record containing "Hello, World!"} \
  {A random integer key}

puts {<p>The last instruction pops top elements from the stack
and uses them as data and key to make a new entry in database
database file pointed to by cursor P1.  This instruction is where
the insert actually occurs.</p>

<p>After the last instruction executes, the program counter
advances to one past the last instruction, which causes the
VDBE to halt.  When the VDBE halts, it automatically closes
all open cursors, frees any elements left on the stack,
and releases any other resources we may have allocated.
In this case, the only cleanup necessary is to close the
open cursor to the "ex1" file.</p>

<a name="trace">
<h2>Tracing VDBE Program Execution</h2>

<p>If the SQLite library is compiled without the NDEBUG 
preprocessor macro being defined, then
there is a special SQL comment that will cause the 
the VDBE to traces the execution of programs.
Though this features was originally intended for testing
and debugging, it might also be useful in learning about
how the VDBE operates.
Use the "<tt>--vdbe-trace-on--</tt>" comment to
turn tracing on and "<tt>--vdbe-trace-off--</tt>" to turn tracing
back off.  Like this:</p>
}

Code {
sqlite> (((--vdbe-trace-on--)))
   ...> (((INSERT INTO ex1 VALUES('Hello, World!');)))
   0 Open            0    1 ex1
   1 New             0    0 
Stack: i:179007474
   2 String          0    0 Hello, World!
Stack: s:[Hello, Worl] i:179007474
   3 MakeRecord      1    0 
Stack: z:[] i:179007474
   4 Put             0    0 
}

puts {
<p>With tracing mode on, the VDBE prints each instruction prior
to executing it.  After the instruction is executed, the top few
entries in the stack are displayed.  The stack display is omitted
if the stack is empty.</p>

<p>On the stack display, most entries are show with a prefix
that tells the datatype of that stack entry.  Integers begin
with "<tt>i:</tt>".  Floating point values begin with "<tt>r:</tt>".
(The "r" stands for "real-number".)  Strings begin with either
"<tt>s:</tt>" or "<tt>z:</tt>".  The difference between s: and
z: strings is that z: strings are stored in memory obtained
from <b>malloc()</b>.  This doesn't make any difference to you,
the observer, but it is vitally important to the VDBE since the
z: strings need to be passed to <b>free()</b> when they are
popped to avoid a memory leak.  Note that only the first 10
characters of string values are displayed and that binary
values (such as the result of the MakeRecord instruction) are
treated as strings.  The only other datatype that can be stored
on the VDBE stack is a NULL, which is display without prefix
as simply "<tt>NULL</tt>".

<a name="query1">
<h2>Simple Queries</h2>

<p>At this point, you should understand the basics of how the VDBE
writes to a database.  Now let's look at how it does queries.
We will use the follow simple SELECT statement as our example:</p>

<blockquote><pre>
SELECT col1 FROM ex1;
</pre></blockquote>

<p>The VDBE program generated for this SQL statement is as follows:</p>
}

Code {
sqlite> (((EXPLAIN SELECT * FROM ex1;)))
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ColumnCount   1      0                                              
1     ColumnName    0      0      col1                                    
2     Open          0      0      ex1                                     
3     Next          0      7                                              
4     Field         0      0                                              
5     Callback      1      0                                              
6     Goto          0      3                                              
7     Close         0      0                                              
}

puts {
<p>Before we begin looking at this problem, let's briefly review
how queries work in SQLite so that we will know what we are trying
to accomplish.  For each row in the result of a query,
SQLite will invoke a callback function with the following
prototype:</p>

<blockquote><pre>
int Callback(void *pUserData, int nColumn, char *azData[], char *azColumnName[]);
</pre></blockquote>

<p>The SQLite library supplies the VDBE with a pointer to the callback function
itself, and the <b>pUserData</b> pointer.  The job of the VDBE is to
come up with values for <b>nColumn</b>, <b>azData[]</b>, 
and <b>azColumnName[]</b>.
<b>nColumn</b> is the number of columns in the results, of course.
<b>azColumnName[]</b> is an array of strings where each string is the name
of one of the result column.  <b>azData[]</b> is an array of strings holding
the actual data.</p>

<p>The first two instructions in the VDBE program for our query are
considered with setting up values for <b>azColumn</b>.
The ColumnCount instruction tells the VDBE how much space to allocate
for the <b>azColumnName[]</b> array.  The ColumnName instructions
tell the VDBE what value to fill in for each element of the 
<b>azColumnName[]</b> array.  Every query will begin with once
ColumnCount instruction and once ColumnName instruction for each
column in the result.</p>

<p>The third instruction opens a cursor into the database file
that is to be queried.  This works the same as the Open instruction
in the INSERT example <a href="#insert1">above</a> except that the
cursor is opened for reading this time instead of for writing.</p>

<p>The instructions at address 3 and 6 form a loop that will execute
once for each record in the database file.  This is a key concept that
you should pay close attention to.  The Next instruction at
address 3 tell the VDBE to advance the cursor (identified by P1)
to the next record.  The first time Next instruction is executed, 
the cursor is set to the first record of the file.  If there are
no more records in the database file when Next is executed, then 
the VDBE makes an immediate jump over the body of the loop to
instruction 7 (specified by operand P2).  The body of the loop
is formed by instructions at addresses 4 and 5.  After the loop
body is an unconditional jump at instruction 6 which takes us
back to the Next instruction at the beginning of the loop.
</p>

<p>The body of the loop consists of instructions at addresses 4 and
5.  The Field instruction at address 4 takes the P2-th column from
the P1-th cursor and pushes it onto the stack.
(The "Field" instruction probably should be renamed as the "Column"
instruction.)  In this example, the Field instruction is pushing the
value for the "col1" data column onto the stack.</p>

<p>The Callback instruction at address 5 invokes the callback function.
The P1 operand to callback becomes the value for <b>nColumn</b>.
The Callback instruction also pops P1 values from the stack and
uses them to form the <b>azData[]</b> for the callback.</p>

<p>The Close instruction at the end of the program closes the
cursor that points into the database file.  It is not really necessary
to call close here since all cursors will be automatically closed
by the VDBE when the program halts.  But we needed an instruction
for the Next to jump to so we might as well go ahead and have that
instruction do something useful.</p>

<a name="query2">
<h2>A Slightly More Complex Query</h2>

<p>The key points of the previous example where the use of the Callback
instruction to invoke the callback function, and the use of the Next
instruction to implement a loop over all records of the database file.
This example attempts to drive home those ideas by demonstrating a
slightly more complex query that involves multiple columns of
output, some of which are computed values, and a WHERE clause that
limits which records actually make it to the callback function.
Consider this query:</p>

<blockquote><pre>
SELECT col1 AS 'Name', '**' || col1 || '**' AS 'With Stars'
FROM ex1
WHERE col1 LIKE '%ll%'
</pre></blockquote>

<p>This query is perhaps a bit contrived, but it does serve to
illustrate our points.  The result will have two column with
names "Name" and "With Stars".  The first column is just the
sole column in our simple example table.  The second column
of the result is the same as the first column except that
asterisks have been prepended and appended.  Finally, the
WHERE clause says that we will only chose rows for the 
results that contain two "l" characters in a row.  Here is
what the VDBE program looks like for this query:</p>
}

Code {
sqlite> (((EXPLAIN SELECT col1 AS 'Name', '**' || col1 || '**' AS 'With Stars')))
   ...> (((FROM ex1)))
   ...> (((WHERE col1 LIKE '%ll%';)))
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ColumnCount   2      0                                              
1     ColumnName    0      0      Name                                    
2     ColumnName    1      0      With Stars                              
3     Open          0      0      ex1                                     
4     Next          0      16                                             
5     Field         0      0                                              
6     String        0      0      %ll%                                    
7     Like          1      4                                              
8     Field         0      0                                              
9     String        0      0      **                                      
10    Field         0      0                                              
11    Concat        2      0                                              
12    String        0      0      **                                      
13    Concat        2      0                                              
14    Callback      2      0                                              
15    Goto          0      4                                              
16    Close         0      0                                              
}

puts {
<p>Except for the WHERE clause, the structure of the program for
this example is very much like the prior example, just with an
extra column.  The ColumnCount is 2 now, instead of 1 as before,
and there are two ColumnName instructions.
A cursor is opened using the Open instruction, just like in the
prior example.  The Next instruction at address 4 and the
Goto at address 15 form a loop over all records of the database
file.  The Close instruction at the end is there to give the
Next instruction something to jump to when it is done.  All of
this is just like in the first query demonstration.</p>

<p>The Callback instruction in this example has to generate
data for two result columns instead of one, but is otherwise
the same as in the first query.  When the Callback instruction
is invoked, the left-most column of the result should be
the lowest in the stack and the right-most result column should
be the top of the stack.  We can see the stack being set up 
this way at addresses 8 through 13.  The Field instruction at
8 pushes the value of the "col1" column of table "ex1" onto the 
stack, and that is all that has to be done for the left column
of the result.  Instructions at 9 through 13 evaluate the
expression used for the second result column and leave it
on the stack as well.</p>

<p>The only thing that is really new about the current example
is the WHERE clause which is implemented by instructions at
addresses 5, 6, and 7.  Instructions at address 5 and 6 push
onto the stack the value of the "col1" column and the literal
string "%ll%".  The Like instruction at address 7 pops these
two values from the stack and causes an
immediate jump back to the Next instruction if the "col1" value
is <em>not</em> like the literal string "%ll%".  Taking this
jump effectively skips the callback, which is the whole point
of the WHERE clause.  If the result
of the comparison is true, the jump is not taken and control
falls through to the Callback instruction below.</p>

<p>Notice how the Like instruction works.  It uses the top of
the stack as its pattern and the next on stack as the data
to compare.  Because P1 is 1, a jump is made to P2 if the
comparison fails.  So with P1 equal to one, a more precise
name for this instruction might be "Jump If NOS Is Not Like TOS".
The sense of the test in inverted if P1 is 0.  So when P1
is zero, the instruction is more like "Jump If NOS Is Like TOS".
</p>

<a name="pattern1">
<h2>A Template For SELECT Programs</h2>

<p>The first two query examples illustrate a kind of template that
every SELECT program will follow.  Basically, we have:</p>

<p>
<ol>
<li>Initialize the <b>azColumnName[]</b> array for the callback.</li>
<li>Open a cursor into the table to be queried.</li>
<li>For each record in the table, do:
    <ol type="a">
    <li>If the WHERE clause evaluates to FALSE, then skip the steps that
        follow and continue to the next record.</li>
    <li>Compute all columns for the current row of the result.</li>
    <li>Invoke the callback function for the current row of the result.</li>
    </ol>
<li>Close the cursor.</li>
</ol>
</p>

<p>This template will be expanded considerably as we consider
additional complications such as joins, compound selects, using
indices to speed the search, sorting, and aggregate functions
with and without GROUP BY and HAVING clauses.
But the same basic ideas will continue to apply.</p>

<h2>UPDATE And DELETE Statements</h2>

<p>The UPDATE and DELETE statements are coded using a template
that is very similar to the SELECT statement template.  The main
difference, of course, is that the end action is to modify the
database rather than invoke a callback function. Let's begin
by looking at a DELETE statement:</p>

<blockquote><pre>
DELETE FROM ex1 WHERE col1 NOT LIKE '%H%'
</pre></blockquote>

<p>This DELETE statement will remove every record from the "ex1"
table that does not contain an "H" characters in the "col1"
column.  The code generated to do this is as follows:</p>
}

Code {
sqlite> (((EXPLAIN DELETE FROM ex1 WHERE col1 NOT LIKE '%H%';)))
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ListOpen      0      0                                              
1     Open          0      0      ex1                                     
2     Next          0      9                                              
3     Field         0      0                                              
4     String        0      0      %H%                                    
5     Like          0      2                                              
6     Key           0      0                                              
7     ListWrite     0      0                                              
8     Goto          0      2                                              
9     Close         0      0                                              
10    ListRewind    0      0                                              
11    Open          0      1      ex1                                     
12    ListRead      0      15                                             
13    Delete        0      0                                              
14    Goto          0      12                                             
15    ListClose     0      0                                              
}

puts {
<p>Here is what the program must do.  First it has to locate all of
the records in the "ex1" database that are to be deleted.  This is
done using a loop very much like the loop used in the SELECT examples
above.  Once all records have been located, then we can go back through
an delete them one by one.  Note that we cannot delete each record
as soon as we find it.  We have to locate all records first, then
go back and delete them.  This is because with the GDBM database
backend (as with most other backends based on hashing) when you
delete a record it might change the scan order.  And if the scan
order changes in the middle of the scan, some records might be
tested more than once, and some records might not be tested at all.</p>

<p>So the implemention of DELETE is really in two loops.  The
first loop (instructions 3 through 8 in the example) locates the records that
are to be deleted and the second loop (instructions 12 through 14)
do the actual deleting.</p>

<p>The very first instruction in the program, the ListOpen instruction,
creates a new List object in which we can store the keys of the records
that are to be deleted.  The P1 operand serves as a handle to the
list.  As with cursors, you can open as many lists as you like
(though in practice we never need more than one at a time.)  Each list
has a handle specified by P1 which is a non-negative integer.  The
VDBE allocates an array of handles, so it is best to use only small
handles.  As currently implemented, SQLite never uses more than one
list at a time and so it always uses the handle of 0 for every list.</p>

<p>Each list is really a file descriptor for a temporary file that
is created for holding the list.  What's going to happen is this: the
first loop of the program is going to locate records that need to
be deleted and write their keys onto the list.  Then the second
loop is going to playback the list and delete the records one by one.</p>

<p>The second instruction opens a cursor to the database file "ex1".
Notice that the cursor is opened for reading, not writing.  At this
stage of the program we are going to be scanning the file not changing
it.  We will reopen the same file for writing it later, at instruction 11.
</p>

<p>Following the Open, there is a loop composed of the Next instruction
at address 2 and continuing down to the Goto at 8.  This loop works
the same way as the query loops worked in the prior examples.  But
instead of invoking a callback at the end of each loop iteration, this
program calls ListWrite at instruction 7.  The ListWrite instruction
pops an integer from the stack and appends it to the List identified
by P1.  The integer is a key to a record that should be deleted and
was placed on the stack by the preceding Key instruction.
The WHERE clause is implemented by instructions 3, 4, and 5.
The implementation of the WHERE clause is exactly the same as in the
previous SELECT statement, except that the P1 operand to the Like
instruction is 0 instead of one because the DELETE statement uses
the NOT LIKE operator instead of the LIKE operator.  If the WHERE
clause evaluates to false (if col2 is like "%ll%") then the ListWrite
instruction gets skipped and the key to that record is never written
to the list.  Hence, the record is not deleted.</p>

<p>At the end of the first loop, the cursor is closed at instruction 9,
and the list is rewound back to the beginning at instruction 10.
Next, instruction 11 reopens the same database file, but for
writing this time.  The loop that does the actual deleting of records
is on instructions 12, 13, and 14.</p>

<p>The ListRead instruction as 12 reads a single integer key from
the list and pushes that key onto the stack.  If there are no
more keys, nothing gets pushed onto the stack but instead a jump
is made to instruction 15.  Notice the similarity of operation
between the ListRead and Next instructions.  Both operations work
something like this:</p>

<blockquote>
Push the next "thing" onto the stack and fall through.
Or if there is no next "thing" to push, jump immediately to P2.
</blockquote>

<p>The only difference between Next and ListRead is the definition
of "next thing".  The "things" for the Next instruction are records
in a database file.  "Things" for ListRead are integer keys in a list.
Later on,
we will see other looping instructions (NextIdx and SortNext) that
operating using the same principle.</p>

<p>The Delete instruction at address 13 pops an integer key from
the stack (the key was put there by the preceding ListRead
instruction) and deletes the record of cursor P1 that has that key.
If there is not record in the database with the given key, then
Delete is a no-op.</p>

<p>There is a Goto instruction at 14 to complete the second loop.
Then at instruction 15 is as ListClose operation.  The ListClose
closes the list and deletes the temporary that held the ist.
Calling ListClose is optional.  The VDBE will automatically close
the list when it halts.  But we need an instruction for the
ListRead to jump to when it reaches the end of the list and
ListClose seemed like a natural candidate.</p>

<p>UPDATE statements work very much like DELETE statements except
that instead of deleting the record they replace it with a new one.
Consider this example:
</p>

<blockquote><pre>
UPDATE ex1 SET col1='H' || col1 WHERE col1 NOT LIKE '%H%'
</pre></blockquote>

<p>Instead of deleting records that lack an "H" in column "col1",
this statement changes the column by prepending an "H".
The VDBE program to implement this statement follows:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ListOpen      0      0                                              
1     Open          0      0      ex1                                     
2     Next          0      9                                              
3     Field         0      0                                              
4     String        0      0      %H%                                    
5     Like          0      2                                              
6     Key           0      0                                              
7     ListWrite     0      0                                              
8     Goto          0      2                                              
9     Close         0      0                                              
10    ListRewind    0      0                                              
11    Open          0      1      ex1                                     
12    ListRead      0      21                                             
13    Dup           0      0                                              
14    Fetch         0      0                                              
15    String        0      0      H                                       
16    Field         0      0                                              
17    Concat        2      0                                              
18    MakeRecord    1      0                                              
19    Put           0      0                                              
20    Goto          0      12                                             
21    ListClose     0      0                                              
}

puts {
<p>This program is exactly the same as the DELETE program
except that the single Delete instruction in the second loop
has been replace by a sequence of instructions (at addresses
13 through 19) that update the record rather than delete it.
Most of this instruction sequence you already be familiar to
you, but there are a couple of minor twists so we will go
over it briefly.</p>

<p>As we enter the interior of the second loop (at instruction 13)
the stack contains a single integer which is the key of the
record we want to modify.  We are going to need to use this
key twice: once to fetch the old value of column "col1" and
a second time to write back the new value.  So the first instruction
is a Dup to make a duplicate of the top of the stack.  The
VDBE Dup instruction is actually a little more general than that.
It will duplicate any element of the stack, not just the top
element.  You specify which element to duplication using the
P1 operand.  When P1 is 0, the top of the stack is duplicated.
When P1 is 1, the next element down on the stack duplication.
And so forth.</p>

<p>After duplicating the key, the next instruction is Fetch
pops the stack once and uses the value popped as a key to
load a record from the database file.  In this way, we obtain
the old column values for the record that is about to be
updated.</p>

<p>Instructions 15 through 18 construct a new database record
that will be used to replace the existing record.  This is
the same kind of code that we say <a href="#insert1">above</a>
in the description of INSERT and will not be described further.
After instruction 18 executes, the stack looks like this:</p>
}

stack {New data record} {Integer key}

puts {
<p>The Put instruction (also described <a href="#insert1">above</a>
during the discussion about INSERT) writes an entry into the
database file whose data is the top of the stack and whose key
is the next on the stack, and then pops the stack twice.  The
Put instruction will overwrite the data of an existing record
with the same key, which is what we want here.  It was not
an issue with INSERT because with INSERT the key was generated
by the Key instruction which is guaranteed to generate a key
that has not been used before.  (By the way, since keys must
all be unique and each key is a 32-bit integer, a single
SQLite database table can have no more than 2<sup>32</sup>
rows.  Actually, the Key instruction starts to become
very inefficient as you approach this upper bound, so it
is best to keep the number of entries below 2<sup>31</sup>
or so.  Surely a couple billion records will be enough for
most applications!)</p>

<p>The rest of the UPDATE program is the same as for DELETE,
and for all the same reasons.</p>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}
