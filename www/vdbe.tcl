#
# Run this Tcl script to generate the vdbe.html file.
#
set rcsid {$Id: vdbe.tcl,v 1.5 2000/07/30 20:04:43 drh Exp $}

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

# puts {
# <blockquote><font color="red"><b>This document is
# currently under development.  It is incomplete and contains
# errors.  Use it accordingly.</b></font></blockquote>
# }

puts {
<p>If you want to know how the SQLite library works internally,
you need to begin with a solid understanding of the Virtual Database
Engine or VDBE.  The VDBE occurs right in the middle of the
processing stream (see the <a href="arch.html">architecture diagram</a>)
and so it seems to touch most parts of the library.  Even
parts of the code that do not directly interact with the VDBE
are usually in a supporting role.  The VDBE really is the heart of
SQLite.</p>

<p>This article is a brief introduction to how the VDBE
works and in particular how the various VDBE instructions
(documented <a href="opcode.html">here</a>) work together
to do useful things with the database.  The style is tutorial,
beginning with simple tasks and working toward solving more
complex problems.  Along the way we will visit most
submodules in the SQLite library.  After completeing this tutorial,
you should have a pretty good understanding of how SQLite works
and will be ready to begin studying the actual source code.</p>

<h2>Preliminaries</h2>

<p>The VDBE implements a virtual computer that runs a program in
its virtual machine language.  The goal of each program is to 
interrogate or change the database.  Toward this end, the machine
language that the VDBE implements is specifically designed to
search, read, and modify databases.</p>

<p>Each instruction of the VDBE language contains an opcode and
three operands labeled P1, P2, and P3.  Operand P1 is an arbitrary
integer.   P2 is a non-negative integer.  P3 is a null-terminated
string, or possibly just a null pointer.  Only a few VDBE
instructions use all three operands.  Many instructions use only
one or two operands.  A significant number of instructions use
no operands at all but instead take their data and storing their results
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
CREATE TABLE examp(one text, two int);
</pre></blockquote>

<p>In words, we have a database table named "examp" that has two
columns of data named "one" and "two".  Now suppose we want to insert a single
record into this table.  Like this:</p>

<blockquote><pre>
INSERT INTO examp VALUES('Hello, World!',99);
</pre></blockquote>

<p>We can see the VDBE program that SQLite uses to implement this
INSERT using the <b>sqlite</b> command-line utility.  First start
up <b>sqlite</b> on a new, empty database, then create the table.
Next change the output format of <b>sqlite</b> to a form that
is designed to work with VDBE program dumps by entering the
".explain" command.
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
sqlite> (((CREATE TABLE examp(one text, two int);)))
sqlite> (((.explain)))
sqlite> (((EXPLAIN INSERT INTO examp VALUES('Hello, World!',99);)))
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     Open          0      1      examp
1     New           0      0                                              
2     String        0      0      Hello, World!                           
3     Integer       99     0                                              
4     MakeRecord    2      0                                              
5     Put           0      0                                              
}

puts {<p>As you can see above, our simple insert statement is
implemented in just 6 instructions.  There are no jumps, so the
program executes once through from top to bottom.  Let's now
look at each instruction in detail.</p>

<p>The first instruction opens a cursor that points into the
"examp" table.   The P1 operand is a handle for the cursor: zero
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
has not been previously used in the file "examp".  The New instruction
uses its P1 operand as the handle of a cursor for the file
for which the new key will be generated.  The generated key is
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

puts {<p>The 4th instruction pushes an integer value 99 onto the
stack.  After the 4th instruction executes, the stack looks like this:</p>
}

stack {Integer value 99} {The string "Hello, World!"} {A random integer key}


puts {<p>The 5th instructionn, MakeRecord, pops the top P1
elements off the stack (2 elements in this case) and converts them
all into the binary format used for storing records in a
database file.  (See the <a href="fileformat.html">file format</a>
description for details.)  The record format consists of
a header with one integer for each column giving the offset
into the record for the beginning of data for that column.
Following the header is the data for each column,  Each column
is stored as a null-terminated ASCII text string.  The new
record generated by the MakeRecord instruction is pushed back
onto the stack, so that after the 5th instruction executes,
the stack looks like this:</p>
}

stack {A data record holding "Hello, World!" and 99} \
  {A random integer key}

puts {<p>The last instruction pops the top two elements from the stack
and uses them as data and key to make a new entry in the
database file pointed to by cursor P1.  This instruction is where
the insert actually occurs.</p>

<p>After the last instruction executes, the program counter
advances to one past the last instruction, which causes the
VDBE to halt.  When the VDBE halts, it automatically closes
all open cursors, frees any elements left on the stack,
and releases any other resources we may have allocated.
In this case, the only cleanup necessary is to close the
cursor to the "examp" file.</p>

<a name="trace">
<h2>Tracing VDBE Program Execution</h2>

<p>If the SQLite library is compiled without the NDEBUG 
preprocessor macro, then
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
   ...> (((INSERT INTO examp VALUES('Hello, World!',99);)))
   0 Open            0    1 examp
   1 New             0    0 
Stack: i:1053779177
   2 String          0    0 Hello, World!
Stack: s:[Hello, Worl] i:1053779177
   3 Integer        99    0 
Stack: i:99 s:[Hello, Worl] i:1053779177
   4 MakeRecord      2    0 
Stack: z:] i:1053779177
   5 Put             0    0 
}

puts {
<p>With tracing mode on, the VDBE prints each instruction prior
to executing it.  After the instruction is executed, the top few
entries in the stack are displayed.  The stack display is omitted
if the stack is empty.</p>

<p>On the stack display, most entries are shown with a prefix
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
SELECT * FROM examp;
</pre></blockquote>

<p>The VDBE program generated for this SQL statement is as follows:</p>
}

Code {
sqlite> (((EXPLAIN SELECT * FROM examp;)))
0     ColumnCount   2      0                                              
1     ColumnName    0      0      one                                     
2     ColumnName    1      0      two                                     
3     Open          0      0      examp                                   
4     Next          0      9                                              
5     Field         0      0                                              
6     Field         0      1                                              
7     Callback      2      0                                              
8     Goto          0      4                                              
9     Close         0      0                                              
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
and the <b>pUserData</b> pointer.  (Both the callback and the user data were
originally passed in as argument to the <b>sqlite_exec()</b> API function.)
The job of the VDBE is to
come up with values for <b>nColumn</b>, <b>azData[]</b>, 
and <b>azColumnName[]</b>.
<b>nColumn</b> is the number of columns in the results, of course.
<b>azColumnName[]</b> is an array of strings where each string is the name
of one of the result column.  <b>azData[]</b> is an array of strings holding
the actual data.</p>

<p>The first three instructions in the VDBE program for our query are
concerned with setting up values for <b>azColumn</b>.
The ColumnCount instruction tells the VDBE how much space to allocate
for the <b>azColumnName[]</b> array.  The ColumnName instructions
tell the VDBE what values to fill in for each element of the 
<b>azColumnName[]</b> array.  Every query will begin with one
ColumnCount instruction and one ColumnName instruction for each
column in the result.</p>

<p>The 4th instruction opens a cursor into the database file
that is to be queried.  This works the same as the Open instruction
in the INSERT example except that the
cursor is opened for reading this time instead of for writing.</p>

<p>The instructions at address 4 and 8 form a loop that will execute
once for each record in the database file.  This is a key concept that
you should pay close attention to.  The Next instruction at
address 4 tells the VDBE to advance the cursor (identified by P1)
to the next record.  The first time this Next instruction is executed, 
the cursor is set to the first record of the file.  If there are
no more records in the database file when Next is executed, then 
the VDBE makes an immediate jump over the body of the loop to
instruction 9 (specified by operand P2).  The body of the loop
is formed by instructions at addresses 5, 6, and 7.  After the loop
body is an unconditional jump at instruction 8 which takes us
back to the Next instruction at the beginning of the loop.
</p>

<p>The body of the loop consists of instructions at addresses 5 through
7.  The Field instructions at addresses 5 and 6 each 
take the P2-th column from
the P1-th cursor and pushes it onto the stack.
(The "Field" instruction probably should be renamed as the "Column"
instruction.)  In this example, the first Field instruction is pushing the
value for the "one" data column onto the stack and the second Field
instruction is pushing the data for "two".</p>

<p>The Callback instruction at address 7 invokes the callback function.
The P1 operand to callback becomes the value for <b>nColumn</b>.
The Callback instruction also pops P1 values from the stack and
uses them to form the <b>azData[]</b> for the callback.</p>

<p>The Close instruction at the end of the program closes the
cursor that points into the database file.  It is not really necessary
to call Close here since all cursors will be automatically closed
by the VDBE when the program halts.  But we needed an instruction
for the Next to jump to so we might as well go ahead and have that
instruction do something useful.</p>

<a name="query2">
<h2>A Slightly More Complex Query</h2>

<p>The key points of the previous example where the use of the Callback
instruction to invoke the callback function, and the use of the Next
instruction to implement a loop over all records of the database file.
This example attempts to drive home those ideas by demonstrating a
slightly more complex query that involves more columns of
output, some of which are computed values, and a WHERE clause that
limits which records actually make it to the callback function.
Consider this query:</p>

<blockquote><pre>
SELECT one, two, one || two AS 'both'
FROM examp
WHERE one LIKE 'H%'
</pre></blockquote>

<p>This query is perhaps a bit contrived, but it does serve to
illustrate our points.  The result will have three column with
names "one", "two", and "both".  The first two columns are direct
copies of the two columns in the table and the third result
column is a string formed by concatenating the first and
second columns of the table.
Finally, the
WHERE clause says that we will only chose rows for the 
results where the "one" column begins with an "H".
Here is what the VDBE program looks like for this query:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ColumnCount   3      0                                              
1     ColumnName    0      0      one                                     
2     ColumnName    1      0      two                                     
3     ColumnName    2      0      both                                    
4     Open          0      0      examp                                   
5     Next          0      16                                             
6     Field         0      0                                              
7     String        0      0      H%                                      
8     Like          1      5                                              
9     Field         0      0                                              
10    Field         0      1                                              
11    Field         0      0                                              
12    Field         0      1                                              
13    Concat        2      0                                              
14    Callback      3      0                                              
15    Goto          0      5                                              
16    Close         0      0                                              
}

puts {
<p>Except for the WHERE clause, the structure of the program for
this example is very much like the prior example, just with an
extra column.  The ColumnCount is 3 now, instead of 2 as before,
and there are three ColumnName instructions.
A cursor is opened using the Open instruction, just like in the
prior example.  The Next instruction at address 5 and the
Goto at address 15 form a loop over all records of the database
file.  The Close instruction at the end is there to give the
Next instruction something to jump to when it is done.  All of
this is just like in the first query demonstration.</p>

<p>The Callback instruction in this example has to generate
data for three result columns instead of two, but is otherwise
the same as in the first query.  When the Callback instruction
is invoked, the left-most column of the result should be
the lowest in the stack and the right-most result column should
be the top of the stack.  We can see the stack being set up 
this way at addresses 9 through 13.  The Field instructions at
9 and 10 push the values for the first two columns in the result.
The two Field instructions at 11 and 12 pull in the values needed
to compute the third result column and the Concat instruction at
13 joins them together into a single entry on the stack.</p>

<p>The only thing that is really new about the current example
is the WHERE clause which is implemented by instructions at
addresses 6, 7, and 8.  Instructions at address 6 and 7 push
onto the stack the value of the "one" column from the table
and the literal string "H%".  The Like instruction at address 8 pops these
two values from the stack and causes an
immediate jump back to the Next instruction if the "one" value
is <em>not</em> like the literal string "H%".  Taking this
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
is zero, the instruction is "Jump If NOS Is Like TOS".
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
DELETE FROM examp WHERE two<50;
</pre></blockquote>

<p>This DELETE statement will remove every record from the "examp"
table where the "two" column is less than 50.
The code generated to do this is as follows:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ListOpen      0      0                                              
1     Open          0      0      examp                                     
2     Next          0      9                                              
3     Field         0      1                                              
4     Integer       50     0                                              
5     Ge            0      2                                              
6     Key           0      0                                              
7     ListWrite     0      0                                              
8     Goto          0      2                                              
9     Close         0      0                                              
10    ListRewind    0      0                                              
11    Open          0      1      examp                                     
12    ListRead      0      15                                             
13    Delete        0      0                                              
14    Goto          0      12                                             
15    ListClose     0      0                                              
}

puts {
<p>Here is what the program must do.  First it has to locate all of
the records in the "examp" database that are to be deleted.  This is
done using a loop very much like the loop used in the SELECT examples
above.  Once all records have been located, then we can go back through
and delete them one by one.  Note that we cannot delete each record
as soon as we find it.  We have to locate all records first, then
go back and delete them.  This is because the GDBM database
backend might change the scan order after a delete operation.
And if the scan
order changes in the middle of the scan, some records might be
visited more than once and other records might not be visited at all.</p>

<p>So the implemention of DELETE is really in two loops.  The
first loop (instructions 2 through 8 in the example) locates the records that
are to be deleted and the second loop (instructions 12 through 14)
does the actual deleting.</p>

<p>The very first instruction in the program, the ListOpen instruction,
creates a new List object in which we can store the keys of the records
that are to be deleted.  The P1 operand serves as a handle to the
list.  As with cursors, you can open as many lists as you like
(though in practice we never need more than one at a time.)  Each list
has a handle specified by P1 which is a non-negative integer.  The
VDBE allocates an array of handles, so it is best to use only small
handles.  As currently implemented, SQLite never uses more than one
list at a time and so it always uses the handle of 0 for every list.</p>

<p>Lists are implemented using temporary files.
The program will work like this:
the first loop will locate records that need to
be deleted and write their keys onto the list.  Then the second
loop will playback the list and delete the records one by one.</p>

<p>The second instruction opens a cursor to the database file "examp".
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
The job of the where clause is to skip the ListWrite if the WHERE
condition is false.  To this end, it jumps back to the Next instruction
if the "two" column (extracted by the Field instruction at 3) is
greater than or equal to 50.</p>

<p>At the end of the first loop, the cursor is closed at instruction 9,
and the list is rewound back to the beginning at instruction 10.
The Open instruction at 11 reopens the same database file, but for
writing this time.  The loop that does the actual deleting of records
is on instructions 12, 13, and 14.</p>

<p>The ListRead instruction at 12 reads a single integer key from
the list and pushes that key onto the stack.  If there are no
more keys, nothing gets pushed onto the stack but instead a jump
is made to instruction 15.  Notice the similarity 
between the ListRead and Next instructions.  Both operations work
according to this rule:</p>

<blockquote>
Push the next "thing" onto the stack and fall through.
Or if there is no next "thing" to push, jump immediately to P2.
</blockquote>

<p>The only difference between Next and ListRead is their idea
of a "thing". The "things" for the Next instruction are records
in a database file.  "Things" for ListRead are integer keys in a list.
Later on,
we will see other looping instructions (NextIdx and SortNext) that
operate using the same principle.</p>

<p>The Delete instruction at address 13 pops an integer key from
the stack (the key was put there by the preceding ListRead
instruction) and deletes the record of cursor P1 that has that key.
If there is no record in the database with the given key, then
Delete is a no-op.</p>

<p>There is a Goto instruction at 14 to complete the second loop.
Then at instruction 15 is as ListClose operation.  The ListClose
closes the list and deletes the temporary file that held it.
Calling ListClose is optional.  The VDBE will automatically close
the list when it halts.  But we need an instruction for the
ListRead to jump to when it reaches the end of the list and
ListClose seemed like a natural candidate.</p>

<p>UPDATE statements work very much like DELETE statements except
that instead of deleting the record they replace it with a new one.
Consider this example:
</p>

<blockquote><pre>
UPDATE examp SET one= '(' || one || ')' WHERE two < 50;
</pre></blockquote>

<p>Instead of deleting records where the "two" column is less than
50, this statement just puts the "one" column in parentheses
The VDBE program to implement this statement follows:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ListOpen      0      0                                              
1     Open          0      0      examp                                   
2     Next          0      9                                              
3     Field         0      1                                              
4     Integer       50     0                                              
5     Ge            0      2                                              
6     Key           0      0                                              
7     ListWrite     0      0                                              
8     Goto          0      2                                              
9     Close         0      0                                              
10    ListRewind    0      0                                              
11    Open          0      1      examp                                   
12    ListRead      0      24                                             
13    Dup           0      0                                              
14    Fetch         0      0                                              
15    String        0      0      (                                       
16    Field         0      0                                              
17    Concat        2      0                                              
18    String        0      0      )                                       
19    Concat        2      0                                              
20    Field         0      1                                              
21    MakeRecord    2      0                                              
22    Put           0      0                                              
23    Goto          0      12                                             
24    ListClose     0      0                                              
}

puts {
<p>This program is exactly the same as the DELETE program
except that the single Delete instruction in the second loop
has been replace by a sequence of instructions (at addresses
13 through 22) that update the record rather than delete it.
Most of this instruction sequence should already be familiar to
you, but there are a couple of minor twists so we will go
over it briefly.</p>

<p>As we enter the interior of the second loop (at instruction 13)
the stack contains a single integer which is the key of the
record we want to modify.  We are going to need to use this
key twice: once to fetch the old value of the record and
a second time to write back the revised record.  So the first instruction
is a Dup to make a duplicate of the key on the top of the stack.  The
Dup instruction will duplicate any element of the stack, not just the top
element.  You specify which element to duplication using the
P1 operand.  When P1 is 0, the top of the stack is duplicated.
When P1 is 1, the next element down on the stack duplication.
And so forth.</p>

<p>After duplicating the key, the next instruction, Fetch, 
pops the stack once and uses the value popped as a key to
load a record from the database file.  In this way, we obtain
the old column values for the record that is about to be
updated.</p>

<p>Instructions 15 through 21 construct a new database record
that will be used to replace the existing record.  This is
the same kind of code that we saw 
in the description of INSERT and will not be described further.
After instruction 21 executes, the stack looks like this:</p>
}

stack {New data record} {Integer key}

puts {
<p>The Put instruction (also described
during the discussion about INSERT) writes an entry into the
database file whose data is the top of the stack and whose key
is the next on the stack, and then pops the stack twice.  The
Put instruction will overwrite the data of an existing record
with the same key, which is what we want here.  Overwriting was not
an issue with INSERT because with INSERT the key was generated
by the Key instruction which is guaranteed to provide a key
that has not been used before.</p>
}

if 0 {(By the way, since keys must
all be unique and each key is a 32-bit integer, a single
SQLite database table can have no more than 2<sup>32</sup>
rows.  Actually, the Key instruction starts to become
very inefficient as you approach this upper bound, so it
is best to keep the number of entries below 2<sup>31</sup>
or so.  Surely a couple billion records will be enough for
most applications!)</p>
}

puts {
<h2>CREATE and DROP</h2>

<p>Using CREATE or DROP to create or destroy a table or index is
really the same as doing an INSERT or DELETE from the special
"sqlite_master" table, at least from the point of view of the VDBE.
The sqlite_master table is a special table that is automatically
created for every SQLite database.  It looks like this:</p>

<blockquote><pre>
CREATE TABLE sqlite_master (
  type      TEXT,    -- either "table" or "index"
  name      TEXT,    -- name of this table or index
  tbl_name  TEXT,    -- for indices: name of associated table
  sql       TEXT     -- SQL text of the original CREATE statement
)
</pre></blockquote>

<p>Every table (except the "sqlite_master" table itself)
and every named index in an SQLite database has an entry
in the sqlite_master table.  You can query this table using
a SELECT statement just like any other table.  But you are
not allowed to directly change the table using UPDATE, INSERT,
or DELETE.  Changes to sqlite_master have to occur using
the CREATE and DROP commands because SQLite also has to update
some of its internal data structures when tables and indices
are added or destroyed.</p>

<p>But from the point of view of the VDBE, a CREATE works
pretty much like an INSERT and a DROP works like a DELETE.
When the SQLite library opens to an existing database,
the first thing it does is a SELECT to read the "sql"
columns from all entries of the sqlite_master table.
The "sql" column contains the complete SQL text of the
CREATE statement that originally generated the index or
table.  This text is fed back into the SQLite parser
and used to reconstruct the
internal data structures describing the index or table.</p>

<h2>Using Indexes To Speed Searching</h2>

<p>In the example queries above, every row of the table being
queried must be loaded off of the disk and examined, even if only
a small percentage of the rows end up in the result.  This can
take a long time on a big table.  To speed things up, SQLite
can use an index.</p>

<p>An GDBM file associates a key with some data.  For a SQLite
table, the GDBM file is set up so that the key is a integer
and the data is the information for one row of the table.
Indices in SQLite reverse this arrangement.  The GDBM key
is (some of) the information being stored and the GDBM data 
is an integer.
To access a table row that has some particular
content, we first look up the content in the GDBM index file to find
its integer index, then we use that integer to look up the
complete record in the GDBM table file.</p>

<p>Note that because GDBM uses hashing instead of b-trees, indices
are only helpful when the WHERE clause of the SELECT statement
contains tests for equality.  Inequalities will not work since there
is no way to ask GDBM to fetch records that do not match a key.
So, in other words, queries like the following will use an index
if it is available:</p>

<blockquote><pre>
SELECT * FROM examp WHERE two==50;
</pre></blockquote>

<p>If there exists an index that maps the "two" column of the "examp"
table into integers, then SQLite will use that index to find the integer
keys of all rows in examp that have a value of 50 for column two.
But the following query will not use an index:</p>

<blockquote><pre>
SELECT * FROM examp WHERE two<50;
</pre></blockquote>

<p>GDBM does not have the ability to select records based on
a magnitude comparison, and so there is no way to use an index
to speed the search in this case.</p>

<p>To understand better how indices work, lets first look at how
they are created.  Let's go ahead and put an index on the two
column of the examp table.  We have:</p>

<blockquote><pre>
CREATE INDEX examp_idx1 ON examp(two);
</pre></blockquote>

<p>The VDBE code generated by the above statement looks like the
following:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     Open          0      0      examp                                   
1     Open          1      1      examp_idx1                              
2     Open          2      1      sqlite_master                           
3     New           2      0                                              
4     String        0      0      index                                   
5     String        0      0      examp_idx1                              
6     String        0      0      examp                                   
7     String        0      0      CREATE INDEX examp_idx1 ON examp(two)   
8     MakeRecord    4      0                                              
9     Put           2      0                                              
10    Close         2      0                                              
11    Next          0      17                                             
12    Key           0      0                                              
13    Field         0      1                                              
14    MakeKey       1      0                                              
15    PutIdx        1      0                                              
16    Goto          0      11                                             
17    Noop          0      0                                              
18    Close         1      0                                              
19    Close         0      0                                              
}

puts {
<p>Remember that every table (except sqlite_master) and every named
index has an entry in the sqlite_master table.  Since we are creating
a new index, we have to add a new entry to sqlite_master.  This is
handled by instructions 2 through 10.  Adding an entry to sqlite_master
works just like any other INSERT statement so we will not say anymore
about it here.  In this example, we want to focus on populating the
new index with valid data, which happens on instructions 0 and 1 and
on instructions 11 through 19.</p>

<p>The first thing that happens is that we open the table being
indexed for reading.  In order to construct an index for a table,
we have to know what is in that table.  The second instruction
opens the index file for writing.</p>

<p>Instructions 11 through 16 implement a loop over every row
of the table being indexed.  For each table row, we first extract
the integer key for that row in instruction 12, then get the
value of the two column in instruction 13.  The MakeKey instruction
at 14 converts data from the two column (which is on the top of
the stack) into a valid index key.  For an index on a single column,
this is basically a no-op.  But if the P1 operand to MakeKey had
been greater than one multiple entries would have been popped from
the stack and converted into a single index key.  The PutIdx
instruction at 15 is what actually creates the index entry.  PutIdx
pops two elements from the stack.  The top of the stack is used as
a key to fetch an entry from the GDBM index file.  Then the integer
which was second on stack is added to the set of integers for that
index and the new record is written back to the GDBM file.  Note
that the same index entry can store multiple integers if there
are two or more table entries with the same value for the two
column.
</p>

<p>Now let's look at how this index will be used.  Consider the
following query:</p>

<blockquote><pre>
SELECT * FROM examp WHERE two==50;
</pre></blockquote>

<p>SQLite generates the following VDBE code to handle this query:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ColumnCount   2      0                                              
1     ColumnName    0      0      one                                     
2     ColumnName    1      0      two                                     
3     Open          0      0      examp                                   
4     Open          1      0      examp_idx1                              
5     Integer       50     0                                              
6     MakeKey       1      0                                              
7     Fetch         1      0                                              
8     NextIdx       1      14                                             
9     Fetch         0      0                                              
10    Field         0      0                                              
11    Field         0      1                                              
12    Callback      2      0                                              
13    Goto          0      8                                              
14    Close         0      0                                              
15    Close         1      0                                              
}

puts {
<p>The SELECT begins in a familiar fashion.  First the column
names are initialized and the table being queried is opened.
Things become different beginning with instruction 4 where
the index file is also opened.  Instructions 5 and 6 make
a key with the value of 50 and instruction 7 fetches the
record of the GDBM index file that has this key.  This will
be the only fetch from the index file.</p>

<p>Instructions 8 through 13 implement a loop over all
integers in the payload of the index record that was fetched
by instruction 7.  The NextIdx operation works much like
the Next and ListRead operations that are discussed above.
Each NextIdx instruction reads a single integer from the
payload of the index record and falls through, except that
if there are no more records it jumps immediately to 14.</p>

<p>The Fetch instruction at 9 loads a single record from
the GDBM file that holds the table.  Then there are two
Field instructions to construct the result and the callback
is invoked.  All this is the same as we have seen before.
The only difference is that the loop is now constructed using
NextIdx instead of Next.</p>

<p>Since the index is used to look up values in the table,
it is important that the index and table be kept consistent.
Now that there is an index on the examp table, we will have
to update that index whenever data is inserted, deleted, or
changed in the examp table.  Remember the first example above
how we were able to insert a new row into the examp table using
only 6 VDBE instructions.  Now that this table is indexed, 10
instructions are required.  The SQL statement is this:</p>

<blockquote><pre>
INSERT INTO examp VALUES('Hello, World!',99);
</pre></blockquote>

<p>And the generated code looks like this:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     Open          0      1      examp                                   
1     Open          1      1      examp_idx1                              
2     New           0      0                                              
3     Dup           0      0                                              
4     String        0      0      Hello, World!                           
5     Integer       99     0                                              
6     MakeRecord    2      0                                              
7     Put           0      0                                              
8     Integer       99     0                                              
9     MakeKey       1      0                                              
10    PutIdx        1      0                                              
}

puts {
<p>At this point, you should understand the VDBE well enough to
figure out on your own how the above program works.  So we will
not discuss it further in this text.</p>

<h2>Joins</h2>

<p>In a join, two or more tables are combined to generate a single
result.  The result table consists of every possible combination
of rows from the tables being joined.  The easiest and most natural
way to implement this is with nested loops.</p>

<p>Recall the query template discussed above where there was a
single loop that searched through every record of the table.
In a join we have basically the same thing except that there
are nested loops.  For example, to join two tables, the query
template might look something like this:</p>

<p>
<ol>
<li>Initialize the <b>azColumnName[]</b> array for the callback.</li>
<li>Open two cursors, one to each of the two tables being queried.</li>
<li>For each record in the first table, do:
    <ol type="a">
    <li>For each record in the second table do:
      <ol type="i">
      <li>If the WHERE clause evaluates to FALSE, then skip the steps that
          follow and continue to the next record.</li>
      <li>Compute all columns for the current row of the result.</li>
      <li>Invoke the callback function for the current row of the result.</li>
      </ol></li>
    </ol>
<li>Close both cursors.</li>
</ol>
</p>

<p>This template will work, but it is likely to be slow since we
are now dealing with an O(N<sup>2</sup>) loop.  But it often works
out that the WHERE clause can be factored into terms and that one or
more of those terms will involve only columns in the first table.
When this happens, we can factor part of the WHERE clause test out of
the inner loop and gain a lot of efficiency.  So a better template
would be something like this:</p>

<p>
<ol>
<li>Initialize the <b>azColumnName[]</b> array for the callback.</li>
<li>Open two cursors, one to each of the two tables being queried.</li>
<li>For each record in the first table, do:
    <ol type="a">
    <li>Evaluate terms of the WHERE clause that only involve columns from
        the first table.  If any term is false (meaning that the whole
        WHERE clause must be false) then skip the rest of this loop and
        continue to the next record.</li>
    <li>For each record in the second table do:
      <ol type="i">
      <li>If the WHERE clause evaluates to FALSE, then skip the steps that
          follow and continue to the next record.</li>
      <li>Compute all columns for the current row of the result.</li>
      <li>Invoke the callback function for the current row of the result.</li>
      </ol></li>
    </ol>
<li>Close both cursors.</li>
</ol>
</p>

<p>Additional speed-up can occur if an index can be used to speed
the search of either or the two loops.</p>

<p>SQLite always constructs the loops in the same order as the
tables appear in the FROM clause of the SELECT statement.  The
left-most table becomes the outer loop and the right-most table
becomes the inner loop.  It is possible, in theory, to reorder
the loops in some circumstances to speed the evaluation of the
join.  But SQLite does not attempt this optimization.</p>

<p>You can see how SQLite constructs nested loops in the following
example:</p>

<blockquote><pre>
CREATE TABLE examp2(three int, four int);
SELECT * FROM examp, examp2 WHERE two<50 AND four==two;
</pre></blockquote>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ColumnCount   4      0                                              
1     ColumnName    0      0      examp.one                               
2     ColumnName    1      0      examp.two                               
3     ColumnName    2      0      examp2.three                            
4     ColumnName    3      0      examp2.four                             
5     Open          0      0      examp                                   
6     Open          1      0      examp2                                  
7     Next          0      21                                             
8     Field         0      1                                              
9     Integer       50     0                                              
10    Ge            0      7                                              
11    Next          1      7                                              
12    Field         1      1                                              
13    Field         0      1                                              
14    Ne            0      11                                             
15    Field         0      0                                              
16    Field         0      1                                              
17    Field         1      0                                              
18    Field         1      1                                              
19    Callback      4      0                                              
20    Goto          0      11                                             
21    Close         0      0                                              
22    Close         1      0                                              
}

puts {
<p>The outer loop over table examp is implement by instructions
7 through 20.  The inner loop is instructions 11 through 20.
Notice that the "two<50" term of the WHERE expression involves
only columns from the first table and can be factored out of
the inner loop.  SQLite does this and implements the "two<50"
test in instructions 8 through 10.  The "four==two" test is
implement by instructions 12 through 14 in the inner loop.</p>

<p>SQLite does not impose any arbitrary limits on the tables in
a join.  It also allows a table to be joined with itself.</p>

<h2>The ORDER BY clause</h2>

<p>As noted previously, GDBM does not have any facility for
handling inequalities.  A consequence of this is that we cannot
sort on disk using GDBM.  All sorted must be done in memory.</p>

<p>SQLite implements the ORDER BY clause using a special
set of instruction control an object called a sorter.  In the
inner-most loop of the query, where there would normally be
a Callback instruction, instead a record is constructed that
contains both callback parameters and a key.  This record
is added to a linked list.  After the query loop finishes,
the list of records is sort and this walked.  For each record
on the list, the callback is invoked.  Finally, the sorter
is closed and memory is deallocated.</p>

<p>We can see the process in action in the following query:</p>

<blockquote><pre>
SELECT * FROM examp ORDER BY one DESC, two;
</pre></blockquote>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     SortOpen      0      0                                              
1     ColumnCount   2      0                                              
2     ColumnName    0      0      one                                     
3     ColumnName    1      0      two                                     
4     Open          0      0      examp                                   
5     Next          0      14                                             
6     Field         0      0                                              
7     Field         0      1                                              
8     SortMakeRec   2      0                                              
9     Field         0      0                                              
10    Field         0      1                                              
11    SortMakeKey   2      0      -+                                      
12    SortPut       0      0                                              
13    Goto          0      5                                              
14    Close         0      0                                              
15    Sort          0      0                                              
16    SortNext      0      19                                             
17    SortCallback  2      0                                              
18    Goto          0      16                                             
19    SortClose     0      0    
}

puts {
<p>The sorter is opened on the first instruction.  The VDBE allows
any number of sorters, but in practice no more than one is every used.</p>

<p>The query loop is built from instructions 5 through 13.  Instructions
6 through 8 build a record that contains the azData[] values for a single
invocation of the callback.  A sort key is generated by instructions
9 through 11.  Instruction 12 combines the invocation record and the
sort key into a single entry and puts that entry on the sort list.<p>

<p>The P3 argument of instruction 11 is of particular interest.  The
sort key is formed by prepending one character from P3 to each string
and concatenating all the strings.  The sort comparison function will
look at this character to determine whether the sort order is
ascending or descending.  In this example, the first column should be
sorted in descending order so its prefix is "-" and the second column
should sort in ascending order so its prefix is "+".</p>

<p>After the query loop ends, the table being queried is closed at
instruction 14.  This is done early in order to allow other processes
or threads to access that table, if desired.  The list of records
that was built up inside the query loop is sorted by the instruction
at 15.  Instructions 16 through 18 walk through the record list
(which is now in sorted order) and invoke the callback once for
each record.  Finally, the sorter is closed at instruction 19.</p>

<h2>Aggregate Functions And The GROUP BY and HAVING Clauses</h2>

<p>To compute aggregate functions, the VDBE implements a special 
data structure and instructions for controlling that data structure.
The data structure is an unordered set of buckets, where each bucket
has a key and one or more memory locations.  Within the query
loop, the GROUP BY clause is used to construct a key and the bucket
with that key is brought into focus.  A new bucket is created with
the key if one did not previously exist.  Once the bucket is in
focus, the memory locations of the bucket are used to accumulate
the values of the various aggregate functions.  After the query
loop terminates, the each bucket is visited once to generate a
single row of the results.</p>

<p>An example will help to clarify this concept.  Consider the
following query:</p>

<blockquote><pre>
SELECT three, min(three+four)+avg(four) 
FROM examp2
GROUP BY three;
</pre></blockquote>
}

puts {
<p>The VDBE code generated for this query is as follows:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ColumnCount   2      0                                              
1     ColumnName    0      0      three                                   
2     ColumnName    1      0      min(three+four)+avg(four)               
3     AggReset      0      4                                              
4     Open          0      0      examp2                                  
5     Next          0      23                                             
6     Field         0      0                                              
7     MakeKey       1      0                                              
8     AggFocus      0      11                                             
9     Field         0      0                                              
10    AggSet        0      0                                              
11    Field         0      0                                              
12    Field         0      1                                              
13    Add           0      0                                              
14    AggGet        0      1                                              
15    Min           0      0                                              
16    AggSet        0      1                                              
17    AggIncr       1      2                                              
18    Field         0      1                                              
19    AggGet        0      3                                              
20    Add           0      0                                              
21    AggSet        0      3                                              
22    Goto          0      5                                              
23    Close         0      0                                              
24    AggNext       0      33                                             
25    AggGet        0      0                                              
26    AggGet        0      1                                              
27    AggGet        0      3                                              
28    AggGet        0      2                                              
29    Divide        0      0                                              
30    Add           0      0                                              
31    Callback      2      0                                              
32    Goto          0      24                                             
33    Noop          0      0                                              
}

puts {
<p>The first instruction of interest is the AggReset at 3.
The AggReset instruction initializes the set of buckets to be the
empty set and specifies the number of memory slots available in each
bucket.  In this example, each bucket will hold four memory slots.
It is not obvious, but if you look closely at the rest of the program
you can figure out what each of these four slots is intended for.</p>

<blockquote><table border="2" cellpadding="5">
<tr><th>Memory Slot</th><th>Intended Use Of This Memory Slot</th></tr>
<tr><td>0</td><td>The "three" column -- the key to the bucket</td></tr>
<tr><td>1</td><td>The minimum "three+four" value</td></tr>
<tr><td>2</td><td>The number of records with the same key. This value
   divides the value in slot 3 to compute "avg(four)".</td></tr>
<tr><td>3</td><td>The sum of all "four" values. This is used to compute 
   "avg(four)".</td></tr>
</table></blockquote>

<p>The query loop is implement by instructions 5 through 22.
The aggregate key specified by the GROUP BY clause is computed
by instructions 6 and 7.  Instruction 8 causes the appropriate
bucket to come into focus.  If a bucket with the given key does
not already exists, a new bucket is created and control falls
through to instructions 9 and 10 which initialize the bucket.
If the bucket does already exist, then a jump is made to instruction
11.  The values of aggregate functions are updated by the instructions
between 11 and 21.  Instructions 11 through 16 update memory
slot 1 to hold the next value "min(three+four)".  The counter in
slot 2 is incremented by instruction 17.  Finally the sum of
the "four" column is updated by instructions 18 through 21.</p>

<p>After the query loop is finished, the GDBM table is closed at
instruction 23 so that its lock will be released and it can be
used by other threads or processes.  The next step is to loop
over all aggregate buckets and output one row of the result for
each bucket.  This is done by the loop at instructions 24
through 32.  The AggNext instruction at 24 brings the next bucket
into focus, or jumps to the end of the loop if all buckets have
been examined already.  The first column of the result ("three")
is computed by instruction 25.  The second result column
("min(three+four)+avg(four)") is computed by instructions
26 through 30.  Notice how the avg() function is computed
as if it where sum()/count().  Finally, the callback is invoked
at instruction 31.</p>

<p>In summary then, any query with aggregate functions is implemented
by two loops.  The first loop scans the input table and computes
aggregate information into buckets and the second loop scans through
all the buckets to compute the final result.</p>

<p>The realization that an aggregate query is really two consequtive
loops makes it much easier to understand the difference between
a WHERE clause and a HAVING clause in SQL query statement.  The
WHERE clause is a restriction on the first loop and the HAVING
clause is a restriction on the second loop.  You can see this
by adding both a WHERE and a HAVING clause to our example query:</p>


<blockquote><pre>
SELECT three, min(three+four)+avg(four) 
FROM examp2
WHERE three>four
GROUP BY three
HAVING avg(four)<10;
</pre></blockquote>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     ColumnCount   2      0                                              
1     ColumnName    0      0      three                                   
2     ColumnName    1      0      min(three+four)+avg(four)               
3     AggReset      0      4                                              
4     Open          0      0      examp2                                  
5     Next          0      26                                             
6     Field         0      0                                              
7     Field         0      1                                              
8     Le            0      5                                              
9     Field         0      0                                              
10    MakeKey       1      0                                              
11    AggFocus      0      14                                             
12    Field         0      0                                              
13    AggSet        0      0                                              
14    Field         0      0                                              
15    Field         0      1                                              
16    Add           0      0                                              
17    AggGet        0      1                                              
18    Min           0      0                                              
19    AggSet        0      1                                              
20    AggIncr       1      2                                              
21    Field         0      1                                              
22    AggGet        0      3                                              
23    Add           0      0                                              
24    AggSet        0      3                                              
25    Goto          0      5                                              
26    Close         0      0                                              
27    AggNext       0      41                                             
28    AggGet        0      3                                              
29    AggGet        0      2                                              
30    Divide        0      0                                              
31    Integer       10     0                                              
32    Ge            0      27                                             
33    AggGet        0      0                                              
34    AggGet        0      1                                              
35    AggGet        0      3                                              
36    AggGet        0      2                                              
37    Divide        0      0                                              
38    Add           0      0                                              
39    Callback      2      0                                              
40    Goto          0      27                                             
41    Noop          0      0                                              
}

puts {
<p>The code generated in this last example is the same as the
previous except for the addition of two conditional jumps used
to implement the extra WHERE and HAVING clauses.  The WHERE
clause is implemented by instructions 6 through 8 in the query
loop.  The HAVING clause is implemented by instruction 28 through
32 in the output loop.</p>

<h2>Using SELECT Statements As Terms In An Expression</h2>

<p>The very name "Structured Query Language" tells us that SQL should
support nested queries.  And, in fact, two different kinds of nesting
are supported.  Any SELECT statement that returns a single-row, single-column
result can be used as a term in an expression of another SELECT statement.
And, a SELECT statement that returns a single-column, multi-row result
can be used as the right-hand operand of the IN and NOT IN operators.
We will begin this section with an example of the first kind of nesting,
where a single-row, single-column SELECT is used as a term in an expression
of another SELECT.  Here is our example:</p>

<blockquote><pre>
SELECT * FROM examp
WHERE two!=(SELECT three FROM examp2
            WHERE four=5);
</pre></blockquote>

<p>The way SQLite deals with this is to first run the inner SELECT
(the one against examp2) and store its result in a private memory
cell.  SQLite then substitutes the value of this private memory
cell for the inner SELECT when it evaluations the outer SELECT.
The code looks like this:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     Null          0      0                                              
1     MemStore      0      0                                              
2     Open          0      0      examp2                                  
3     Next          0      11                                             
4     Field         0      1                                              
5     Integer       5      0                                              
6     Ne            0      3                                              
7     Field         0      0                                              
8     MemStore      0      0                                              
9     Goto          0      11                                             
10    Goto          0      3                                              
11    Close         0      0                                              
12    ColumnCount   2      0                                              
13    ColumnName    0      0      one                                     
14    ColumnName    1      0      two                                     
15    Open          0      0      examp                                   
16    Next          0      24                                             
17    Field         0      1                                              
18    MemLoad       0      0                                              
19    Eq            0      16                                             
20    Field         0      0                                              
21    Field         0      1                                              
22    Callback      2      0                                              
23    Goto          0      16                                             
24    Close         0      0                                              
}

puts {
<p>The private memory cell is initialized to NULL by the first
two instructions.  Instructions 2 through 11 implement the inner
SELECT statement against the examp2 table.  Notice that instead of
sending the result to a callback or storing the result on a sorter,
the result of the query is pushed into the memory cell by instruction
8 and the loop is abandoned by the jump at instruction 9.  
The jump at instruction at 10 is vestigial and
never executes.</p>

<p>The outer SELECT is implemented by instructions 12 through 24.
In particular, the WHERE clause that contains the nested select
is implemented by instructions 17 through 19.  You can see that
the result of the inner select is loaded onto the stack by instruction
18 and used by the conditional jump at 19.</p>

<p>When the result of a sub-select is a scalar, a single private memory
cell can be used, as shown in the previous
example.  But when the result of a sub-select is a vector, such
as when the sub-select is the right-hand operand of IN or NOT IN,
a different approach is needed.  In this case, 
the result of the sub-select is
stored in a temporary GDBM table and the contents of that table
are tested using the Found or NotFound operators.  Consider this
example:</p>

<blockquote><pre>
SELECT * FROM examp
WHERE two IN (SELECT three FROM examp2);
</pre></blockquote>

<p>The code generated to implement this last query is as follows:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     Open          0      1                                              
1     Open          1      0      examp2                                  
2     Next          1      7                                              
3     Field         1      0                                              
4     String        0      0                                              
5     Put           0      0                                              
6     Goto          0      2                                              
7     Close         1      0                                              
8     ColumnCount   2      0                                              
9     ColumnName    0      0      one                                     
10    ColumnName    1      0      two                                     
11    Open          1      0      examp                                   
12    Next          1      19                                             
13    Field         1      1                                              
14    NotFound      0      12                                             
15    Field         1      0                                              
16    Field         1      1                                              
17    Callback      2      0                                              
18    Goto          0      12                                             
19    Close         1      0                                              
}

puts {
<p>The temporary table in which the results of the inner SELECT are
stored is created by instruction 0.  Notice that the P3 field of
this Open instruction is empty.  An empty P3 field on an Open
instruction tells the VDBE to create a temporary table.  This temporary
table will be automatically deleted from the disk when the
VDBE halts.</p>

<p>The inner SELECT statement is implemented by instructions 1 through 7.
All this code does is make an entry in the temporary table for each
row of the examp2 table.  The key for each temporary table entry
is the "three" column of examp2 and the data 
entries is an empty string since it is never used.</p>

<p>The outer SELECT is implemented by instructions 8 through 19.  In
particular, the WHERE clause containing the IN operator is implemented
by two instructions at 13 and 14.  Instruction 13 pushes the value of
the "two" column for the current row onto the stack and instruction 14
tests to see if top of the stack matches any key in the temporary table.
All the rest of the code is the same as what has been shown before.</p>

<h2>Compound SELECT Statements</h2>

<p>SQLite also allows two or more SELECT statements to be joined as
peers using operators UNION, UNION ALL, INTERSECT, and EXCEPT.  These
compound select statements are implemented using temporary tables.
The implementation is slightly different for each operator, but the
basic ideas are the same.  For an example we will use the EXCEPT
operator.</p>

<blockquote><pre>
SELECT two FROM examp
EXCEPT
SELECT four FROM examp2;
</pre></blockquote>

<p>The result of this last example should be every unique value
of the two column in the examp table except any value that is
in the four column of examp2 is removed.  The code to implement
this query is as follows:</p>
}

Code {
addr  opcode        p1     p2     p3                                      
----  ------------  -----  -----  ----------------------------------------
0     Open          0      1                                              
1     KeyAsData     0      1                                              
2     Open          1      0      examp                                   
3     Next          1      9                                              
4     Field         1      1                                              
5     MakeRecord    1      0                                              
6     String        0      0                                              
7     Put           0      0                                              
8     Goto          0      3                                              
9     Close         1      0                                              
10    Open          1      0      examp2                                  
11    Next          1      16                                             
12    Field         1      1                                              
13    MakeRecord    1      0                                              
14    Delete        0      0                                              
15    Goto          0      11                                             
16    Close         1      0                                              
17    ColumnCount   1      0                                              
18    ColumnName    0      0      four                                    
19    Next          0      23                                             
20    Field         0      0                                              
21    Callback      1      0                                              
22    Goto          0      19                                             
23    Close         0      0                                              
}

puts {
<p>The temporary table in which the result is built is created by
instruction 0.  Three loops then follow.  The loop at instructions
3 through 8 implements the first SELECT statement.  The second
SELECT statement is implemented by the loop at instructions 11 through
15.  Finally, a loop at instructions 19 through 22 reads the temporary
table and invokes the callback once for each row in the result.</p>

<p>Instruction 1 is of particular importance in this example.  Normally,
the Field opcode extracts the value of a column from a larger
record in the data of a GDBM file entry.  Instructions 1 sets a flag on
the temporary table so that Field will instead treat the key of the
GDBM file entry as if it were data and extract column information from
the key.</p>

<p>Here is what is going to happen:  The first SELECT statement
will construct rows of the result and save each row as the key of
an entry in the temporary table.  The data for each entry in the
temporary table is a never used so we fill it in with an empty string.
The second SELECT statement also constructs rows, but the rows
constructed by the second SELECT are removed from the temporary table.
That is why we want the rows to be stored in the key of the GDBM file
instead of in the data -- so they can be easily located and deleted.</p>

<p>Let's look more closely at what is happening here.  The first
SELECT is implemented by the loop at instructions 3 through 8.
Instruction 4 extracts the value of the "two" column from "examp"
and instruction 5 converts this into a row.  Instruction 6 pushes
an empty string onto the stack.  Finally, instruction 7 writes the
row into the temporary table.  But remember, the Put opcode uses
the top of the stack as the GDBM data and the next on stack as the
GDBM key.  For an INSERT statement, the row generated by the
MakeRecord opcode is the GDBM data and the GDBM key is an integer
created by the New opcode.  But here the roles are reversed and
the row created by MakeRecord is the GDBM key and the GDBM data is
just an empty string.</p>

<p>The second SELECT is implemented by instructions 11 through 15.
A new result row is created from the "four" column of table "examp2"
by instructions 12 and 13.  But instead of using Put to write this
new row into the temporary table, we instead call Delete to remove
it from the temporary table if it exists.</p>

<p>The result of the compound select is sent to the callback routine
by the loop at instructions 19 through 22.  There is nothing new
or remarkable about this loop, except for the fact that the Field 
instruction at 20 will be extracting a column out of the GDBM key
rather than the GDBM data.</p>

<h2>Summary</h2>

<p>This article has reviewed all of the major techniques used by
SQLite's VDBE to implement SQL statements.  What has not been shown
is that most of these techniques can be used in combination to
generate code for an appropriately complex query statement.  For
example, we have shown how sorting is accomplished on a simple query
and we have shown how to implement a compound query.  But we did
not give an example of sorting in a compound query.  This is because
sorting a compound query does not introduce any new concepts: it
merely combines two previous ideas (sorting and compounding)
in the same VDBE program.</p>

<p>For additional information on how the SQLite library
functions, the reader is directed to look at the SQLite source
code directly.  If you understand the material in this article,
you should not have much difficulty in following the sources.
Serious students of the internals of SQLite will probably
also what to make a careful study of the VDBE opcodes
as documented <a href="opcode.html">here</a>.  Most of the
opcode documentation is extracted from comments in the source
code using a script so you can also get information about the
various opcodes directly from the <b>vdbe.c</b> source file.
If you have successfully read this far, you should have little
difficulty understanding the rest.</p>

<p>If you find errors in either the documentation or the code,
feel free to fix them and/or contact the author at
<a href="drh@hwaci.com">drh@hwaci.com</a>.  Your bug fixes or
suggestions are always welcomed.</p>
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}
