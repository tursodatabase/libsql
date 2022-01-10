# JSON Functions Enhancements (2022)

This document summaries enhancements to the SQLite JSON support added in
early 2022.

## 1.0 Change summary:

  1.  New **->** and **->>** operators that work like MySQL and PostgreSQL (PG).
  2.  JSON functions are built-in rather than being an extension.  They
      are included by default, but can be omitted using the
      -DSQLITE_OMIT_JSON compile-time option.


## 2.0 New operators **->** and **->>**

The SQLite language adds two new binary operators **->** and **->>**.
Both operators are similar to json_extract().  The left operand is
JSON and the right operand is a JSON path expression (possibly abbreviated
for compatibility with PG - see below).  So they are similar to a
two-argument call to json_extract().

The difference between -> and ->> (and json_extract()) is as follows:

  *  The -> operator always returns JSON.

  *  The ->> operator converts the answer into a primitive SQL datatype
     such as TEXT, INTEGER, REAL, or NULL.  If a JSON object or array
     is selected, that object or array is rendered as text.  If a JSON
     value is selected, that value is converted into its corresponding
     SQL type

  *  The json_extract() interface returns JSON when a JSON object or
     array is selected, or a primitive SQL datatype when a JSON value
     is selected.  This is different from MySQL, in which json_extract()
     always returns JSON, but the difference is retained because it has
     worked that way for 6 years and changing it now would likely break
     a lot of legacy code.

In MySQL and PG, the ->> operator always returns TEXT (or NULL) and never
INTEGER or REAL.  This is due to limitations in the type handling capabilities
of those systems.  In MySQL and PG, the result type a function or operator
may only depend on the type of its arguments, never the value of its arguments.
But the underlying JSON type depends on the value of the JSON path
expression, not the type of the JSON path expression (which is always TEXT).
Hence, the result type of ->> in MySQL and PG is unable to vary according
to the type of the JSON value being extracted.

The type system in SQLite is more general.  Functions in SQLite are able
to return different datatypes depending on the value of their arguments.
So the ->> operator in SQLite is able to return TEXT, INTEGER, REAL, or NULL
depending on the JSON type of the value being extracted.  This means that
the behavior of the ->> is slightly different in SQLite versus MySQL and PG
in that it will sometimes return INTEGER and REAL values, depending on its
inputs.  It is possible to implement the ->> operator in SQLite so that it
always operates exactly like MySQL and PG and always returns TEXT or NULL,
but I have been unable to think of any situations where returning the
actual JSON value this would cause problems, so I'm including the enhanced
functionality in SQLite.

The table below attempts to summarize the differences between the
-> and ->> operators and the json_extract() function, for SQLite, MySQL,
and PG.  JSON values are shown using their SQL text representation but
in a bold font.


<table border=1 cellpadding=5 cellspacing=0>
<tr><th>JSON<th>PATH<th>-&gt; operator<br>(all)<th>-&gt;&gt; operator<br>(MySQL/PG)
    <th>-&gt;&gt; operator<br>(SQLite)<th>json_extract()<br>(SQLite)
<tr><td> **'{"a":123}'**     <td>'$.a'<td> **'123'**     <td> '123'          <td> 123           <td> 123
<tr><td> **'{"a":4.5}'**     <td>'$.a'<td> **'4.5'**     <td> '4.5'          <td> 4.5           <td> 4.5
<tr><td> **'{"a":"xyz"}'**   <td>'$.a'<td> **'"xyz"'**   <td> 'xyz'          <td> 'xyz'         <td> 'xyz'
<tr><td> **'{"a":null}'**    <td>'$.a'<td> **'null'**    <td> NULL           <td> NULL          <td> NULL
<tr><td> **'{"a":[6,7,8]}'** <td>'$.a'<td> **'[6,7,8]'** <td> '[6,7,8]'      <td> '[6,7,8]'     <td> **'[6,7,8]'**
<tr><td> **'{"a":{"x":9}}'** <td>'$.a'<td> **'{"x":9}'** <td> '{"x":9}'      <td> '{"x":9}'     <td> **'{"x":9}'**
<tr><td> **'{"b":999}'**     <td>'$.a'<td> NULL          <td> NULL           <td> NULL          <td> NULL
</table>

Important points about the table above:

  *  The -> operator always returns either JSON or NULL.

  *  The ->> operator never returns JSON.  It always returns TEXT or NULL, or in the
     case of SQLite, INTEGER or REAL.

  *  The MySQL json_extract() function works exactly the same
     as the MySQL -> operator.

  *  The SQLite json_extract() operator works like -> for JSON objects and
     arrays, and like ->> for JSON values.

  *  The -> operator works the same for all systems.

  *  The only difference in ->> between SQLite and other systems is that
     when the JSON value is numeric, SQLite returns a numeric SQL value,
     whereas the other systems return a text representation of the numeric
     value.

### 2.1 Abbreviated JSON path expressions for PG compatibility

The table above always shows the full JSON path expression: '$.a'.  But
PG does not accept this syntax.  PG only allows a single JSON object label
name or a single integer array index.  In order to provide compatibility
with PG, The -> and ->> operators in SQLite are extended to also support
a JSON object label or an integer array index for the right-hand side
operand, in addition to a full JSON path expression.

Thus, a -> or ->> operator that works on MySQL will work in
SQLite.  And a -> or ->> operator that works in PG will work in SQLite.
But because SQLite supports the union of the disjoint capabilities of
MySQL and PG, there will always be -> and ->> operators that work in
SQLite that do not work in one of MySQL and PG.  This is an unavoidable
consequence of the different syntax for -> and ->> in MySQL and PG.

In the following table, assume that "value1" is a JSON object and
"value2" is a JSON array.

<table border=1 cellpadding=5 cellspacing=0>
<tr><th>SQL expression     <th>Works in MySQL?<th>Works in PG?<th>Works in SQLite
<tr><td>value1-&gt;'$.a'   <td> yes           <td>  no        <td> yes
<tr><td>value1-&gt;'a'     <td> no            <td>  yes       <td> yes
<tr><td>value2-&gt;'$[2]'  <td> yes           <td>  no        <td> yes
<tr><td>value2-&gt;2       <td> no            <td>  yes       <td> yes
</table>

The abbreviated JSON path expressions only work for the -> and ->> operators
in SQLite.  The json_extract() function, and all other built-in SQLite
JSON functions, continue to require complete JSON path expressions for their
PATH arguments.

## 3.0 JSON moved into the core

The JSON interface is now moved into the SQLite core.

When originally written in 2015, the JSON functions were an extension
that could be optionally included at compile-time, or loaded at run-time.
The implementation was in a source file named ext/misc/json1.c in the
source tree.  JSON functions were only compiled in if the
-DSQLITE_ENABLE_JSON1 compile-time option was used.

After these enhancements, the JSON functions are now built-ins.
The source file that implements the JSON functions is moved to src/json.c.
No special compile-time options are needed to load JSON into the build.
Instead, there is a new -DSQLITE_OMIT_JSON compile-time option to leave
them out.
