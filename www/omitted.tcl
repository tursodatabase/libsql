#
# Run this script to generated a omitted.html output file
#
set rcsid {$Id: omitted.tcl,v 1.6 2004/05/31 15:06:30 drh Exp $}
source common.tcl
header {SQL Features That SQLite Does Not Implement}
puts {
<h2>SQL Features That SQLite Does Not Implement</h2>

<p>
Rather than try to list all the features of SQL92 that SQLite does
support, it is much easier to list those that it does not.
Unsupported features of SQL92 are shown below.</p>

<p>
The order of this list gives some hint as to when a feature might
be added to SQLite.  Those features near the top of the list are
likely to be added in the near future.  There are no immediate
plans to add features near the bottom of the list.
</p>

<table cellpadding="10">
}

proc feature {name desc} {
  puts "<tr><td valign=\"top\"><b><nobr>$name</nobr></b></td>"
  puts "<td width=\"10\">&nbsp;</th>"
  puts "<td valign=\"top\">$desc</td></tr>"
}

feature {CHECK constraints} {
  CHECK constraints are parsed but they are not enforced.
  NOT NULL and UNIQUE constraints are enforced, however.
}

feature {Variable subqueries} {
  Subqueries must be static.  They are evaluated only once.  They may not,
  therefore, refer to variables in the main query.
}

feature {FOREIGN KEY constraints} {
  FOREIGN KEY constraints are parsed but are not enforced.
}

feature {Complete trigger support} {
  There is some support for triggers but it is not complete.  Missing
  subfeatures include FOR EACH STATEMENT triggers (currently all triggers
  must be FOR EACH ROW), INSTEAD OF triggers on tables (currently 
  INSTEAD OF triggers are only allowed on views), and recursive
  triggers - triggers that trigger themselves.
}

feature {ALTER TABLE} {
  To change a table you have to delete it (saving its contents to a temporary
  table) and recreate it from scratch.
}

feature {Nested transactions} {
  The current implementation only allows a single active transaction.
}

feature {The COUNT(DISTINCT X) function} {
  You can accomplish the same thing using a subquery, like this:<br />
  &nbsp;&nbsp;SELECT count(x) FROM (SELECT DISTINCT x FROM tbl);
}

feature {RIGHT and FULL OUTER JOIN} {
  LEFT OUTER JOIN is implemented, but not RIGHT OUTER JOIN or
  FULL OUTER JOIN.
}

feature {Writing to VIEWs} {
  VIEWs in SQLite are read-only.  You may not execute a DELETE, INSERT, or
  UPDATE statement on a view. But you can create a trigger
  that fires on an attempt to DELETE, INSERT, or UPDATE a view and do
  what you need in the body of the trigger.
}

feature {GRANT and REVOKE} {
  Since SQLite reads and writes an ordinary disk file, the
  only access permissions that can be applied are the normal
  file access permissions of the underlying operating system.
  The GRANT and REVOKE commands commonly found on client/server
  RDBMSes are not implemented because they would be meaningless
  for an embedded database engine.
}

puts {
</table>

<p>
If you find other SQL92 features that SQLite does not support, please
add them to the Wiki page at 
<a href="http://www.sqlite.org/cvstrac/wiki?p=UnsupportedSql">
http://www.sqlite.org/cvstrac/wiki?p=Unsupported</a>
</p>
}
footer $rcsid
