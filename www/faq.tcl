#
# Run this script to generated a faq.html output file
#
puts {<html>
<head>
  <title>SQLite Frequently Asked Questions</title>
</head>
<body bgcolor="white">
<h1 align="center">Frequently Asked Questions</h1>
}

set cnt 1
proc faq {question answer} {
  set ::faq($::cnt) [list [string trim $question] [string trim $answer]]
  incr ::cnt
}

#############
# Enter questions and answers here.

faq {
  How do I create an AUTOINCREMENT field.
} {
  SQLite does not support AUTOINCREMENT.  If you need a unique key for
  a new entry in a table, you can create an auxiliary table
  with a single entry that holds the next available value for that key.
  Like this:
<blockquote><pre>
CREATE TABLE counter(cnt);
INSERT INTO counter VALUES(1);
</pre></blockquote>
  Once you have a counter set up, you can generate a unique key as follows:
<blockquote><pre>
BEGIN TRANSACTION;
SELECT cnt FROM counter;
UPDATE counter SET cnt=cnt+1;
COMMIT;
</pre></blockquote>
  There are other ways of simulating the effect of AUTOINCREMENT but
  this approach seems to be the easiest and most efficient.
}

faq {
  SQLite lets me insert a string into a database column of type integer!
} {
  <p>This is a feature, not a bug.  SQLite is typeless.  Any data can be
  inserted into any column.  You can put arbitrary length strings into
  integer columns, floating point numbers in boolean columns, or dates
  in character columns.  The datatype you assign to a column in the
  CREATE TABLE command is (mostly) ignored.  Every column is able to hold
  an arbitrary length string.</p>

  <p>Because SQLite ignores data types, you can omit the data type definition
  from columns in CREATE TABLE statements.  For example, instead of saying
<blockquote><pre>
CREATE TABLE t1(
  f1 int,
  f2 varchar(10),
  f3 boolean
);
</pre></blockquote>
  You can save yourself a lot of typing and formatting by omitting the
  data type declarations, like this:
<blockquote><pre>
CREATE TABLE t1(f1,f2,f3);
</pre></blockquote>
  </p>
}

faq {
  Why does SQLite think that the expression '0'=='00' is TRUE?
} {
  <p>This is a consequence of SQLite being typeless.  All data is stored
  internally as a null-terminated string.  There is no concept of
  separate data types for strings and numbers.</p>

  <p>When doing a comparison, SQLite looks at the string on both sides of
  the comparison operator.  If both strings look like pure numeric
  values (with no extra punctuation or spacing) then the strings are
  converted to floating point numbers using <b>atof()</b> and the results
  are compared.  The results of <b>atof("0")</b> and <b>atof("00")</b>
  are both 0.0, so those two strings are considered to be equal.</p>

  <p>If only one string in a comparison is a pure numeric, then that string
  is assumed to be less than the other.  Of neither string is a pure numeric,
  then <b>strcmp()</b> is used for the comparison.</p>
}

faq {
  The second INSERT in the following sequence of commands returns with 
  constraint error.
  <blockquote>
     CREATE TABLE t(s varchar(10) primary key);<br>
     INSERT INTO t VALUES('0');<br>
     INSERT INTO t VALUES('0.0');<br>
  </blockquote>
  Why is this?
} {
  <p>Because column <b>s</b> is a primary key, all values of <b>s</b> must
  be unique.  But SQLite thinks that <b>'0'</b> and <b>'0.0'</b> are the
  same value because they compare equal to one another numerically.
  (See the previous question.)  Hence the values are not unique and the
  constraint fails.</p>

  <p>You can work around this issue in several ways:</p>
  <ol>
  <li><p>Remove the <b>primary key</b> clause from the CREATE TABLE so that
         <b>s</b> can contain more than one entry with the same value. 
         If you need an index on the <b>s</b> column then create it separately.
         </p></li>
  <li><p>Prepend a space to the beginning of every <b>s</b> value.  The initial
         space will mean that the entries are not pure numerics and hence
         will be compared as strings using <b>strcmp()</b>.</p></li>
  </ol>
}
        
faq {
  My linux box is not able to read an SQLite database that was created
  on my SparcStation.
} {
  <p>The x86 processor on your windows box is little-endian (meaning that
  the least signification byte of integers comes first) but the Sparc is
  big-endian (the most significant bytes comes first).  SQLite databases
  created on a little-endian architecture cannot be used on a big-endian
  machine and vice versa.</p>

  <p>If you need to move the database from one machine to another, you'll
  have to do an ASCII dump of the database on the source machine and then
  reconstruct the database at the destination machine.  The following is
  a typical command for transferring an SQLite databases between two
  machines:
<blockquote><pre>
echo .dump | sqlite from.db | ssh sparc 'sqlite to.db'
</pre></blockquote>
  The command above assumes the name of the destination machine is
  <b>sparc</b> and that you have SSH running on both the source and
  destination.  An alternative approach is to save the output of the first
  <b>sqlite</b> command in a temporary file, move the temporary file
  to the destination machine, then run the second <b>sqlite</b> command
  while redirecting input from the temporary file.</p>
}

# End of questions and answers.
#############

puts {<DL COMPACT>}
for {set i 1} {$i<$cnt} {incr i} {
  puts "  <DT><A HREF=\"#q$i\">($i)</A></DT>"
  puts "  <DD>[lindex $faq($i) 0]</DD>"
}
puts {</DL><HR />}

for {set i 1} {$i<$cnt} {incr i} {
  puts "<A NAME=\"q$i\">"
  puts "<P><B>($i) [lindex $faq($i) 0]</B></P>\n"
  puts "<BLOCKQUOTE>[lindex $faq($i) 1]</BLOCKQUOTE>\n"
}

puts {
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}
