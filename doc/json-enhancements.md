# Proposed Enhancements To JSON Functions

## 1.0 New function json_nextract()

The new function json_nextract() works the same as json_extract() except
when the input JSON is not well-formed.  This is what the routines do
when the input JSON in the first argument is not well-formed:

  1.  **json_extract()** &rarr; raises an error and aborts the query.

  2.  **json_nextract()** with 2 arguments the second argument is
      exactly `'$'` &rarr; work like json_quote() and return the first
      argument as a JSON quoted string.

  3.  **json_nextract()** otherwise &rarr; return NULL.

If the input is known to be JSON, then json_extract() should work just
fine for all your needs.  But sometimes a table might have a column that
sometimes holds JSON and sometimes holds some other content.  Suppose,
for example, an application started out holding a single phone number for
each user, but later was enhanced so that the same database file could
hold a JSON array of phone numbers.  The USER table might have some entries
that are JSON arrays and some entries which are just text strings containing
phone numbers.  The application can use json_nextract() to be robust in
extracting values from that column.

The feature (2) above is envisioned to be useful for sanitizing table
content.  Suppose a table is populated from dirty CSV, and some of the
JSON is mis-formatted.  You could convert all entries in a table to use
well-formed JSON using something like this:

> ~~~
UPDATE data SET jsonData = json_nextract(jsonData,'$');
~~~

In the query above, well-formed JSON would be unchanged, and mis-formatted
JSON would be converted into a well-formatted JSON string.

## 2.0 Add the `->` and '->>` operators as aliases for json_extract().

Two new binary operators "`->`" and "`->>`" operators are the same
as json_nextract() and json_extract(), respectively.

> ~~~
SELECT '{"a":5,"b":17}' -> '$.a',  '[4,1,-6]' ->> '$[0]';
~~~

Is equivalent to (and generates the same bytecode as):

> ~~~
SELECT json_nextract('{"a":5,"b":17}','$.a'), json_extract('[4,1,-6]','$[0]');
~~~

The ->> operator works the same as the ->> operator in MySQL
and mostly compatible with PostgreSQL (hereafter "PG").  Addition enhancements
in section 3.0 below are required to bring ->> into compatibility with PG.

The -> operator is mostly compatible with MySQL and PG too.  The main
difference is that in MySQL and PG, the result from -> is not a primitive
SQL datatype type but rather more JSON.  It is unclear how this would ever
be useful for anything, and so I am unsure why they do this.  But that is
the way it is done in those system.

SQLite strives to be compatible with MySQL and PG with the ->> operator,
but not with the -> operator.

## 3.0 Abbreviated JSON path specifications for use with -> and ->>

The "->" and "->>" and operators allow abbreviated
forms of JSON path specs that omit unnecessary $-prefix text.  For
example, the following queries are equivalent:

> ~~~
SELECT '{"a":17, "b":4.5}' ->> '$.a';
SELECT '{"a":17, "b":4.5}' ->> 'a';
~~~

Similarly, these queries mean exactly the same thing:

> ~~~
SELECT '[17,4.5,"hello",0]' ->> '$[1]';
SELECT '[17,4.5,"hello",0]' ->> 1;
~~~

The abbreviated JSON path specs are allowed with the -> and ->> operators
only.  The json_extract() and json_nextract() functions, and all the other
JSON functions, still use the full path spec and will raise an error if
the full path spec is not provided.

This enhancement provides compatibility with PG.
PG does not support JSON path specs on its ->> operator.  With PG, the
right-hand side of ->> must be either an integer (if the left-hand side
is a JSON array) or a text string which is interpreted as a field name
(if the left-hand side is a JSON object).  So the ->> operator in PG is
rather limited.  With this enhancement, the ->> operator in SQLite
covers all the functionality of PG, plus a lot more.

MySQL also supports the ->> operator, but it requires a full JSON path
spec on the right-hand side.  SQLite also supports this, so SQLite is
compatibility with MySQL as well.  Note, however, that MySQL and PG
are incompatible with each other.  You can (in theory) write SQL that
uses the ->> operator that is compatible between SQLite and MySQL,
or that is compatible between SQLite and PG, but not that is compatible
with all three.

## 4.0 New json_ntype() SQL function

A new function "json_ntype(JSON)" works like the existing one-argument
version of the "json_type(JSON)" function, except that json_ntype(JSON)
returns NULL if the argument is not well-formed JSON, whereas the
existing json_type() function raises an error in that case.

In other words, "`json_ntype($json)`" is equivalent to
"`CASE WHEN json_valid($json) THEN json_type($json) END`".

This function is seen as useful for figuring out which rows of a table
have a JSON type in a column and which do not.  For example, to find
all rows in a table in which the value of the the "phonenumber" column
contains a JSON array, you could write:

> ~~~
SELECT * FROM users WHERE json_ntype(phonenumber) IS 'array';
~~~
