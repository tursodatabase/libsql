#
# Run this script to generated a omitted.html output file
#
set rcsid {$Id: omitted.tcl,v 1.1 2002/08/14 00:08:14 drh Exp $}

puts {<html>
<head>
  <title>SQL Features That SQLite Does Not Implement</title>
</head>
<body bgcolor="white">
<h1 align="center">
SQL Features That SQLite Does Not Implement
</h1>
}
puts "<p align=center>
(This page was last modified on [lrange $rcsid 3 4] UTC)
</p>"

puts {
<p>
Rather than try to list all the features of SQL92 that SQLite does
support, it is much easier to list those that it does not.
The following are features of of SQL92 that SQLite does not implement.
</p>

<table cellpadding="10">
}

proc feature {name desc} {
  puts "<tr><td valign=\"top\"><b><nobr>$name</nobr></b></td>"
  puts "<td valign=\"top\">$desc</td></tr>"
}

feature {RIGHT and FULL OUTER JOIN} {
  LEFT OUTER JOIN is implemented, but not RIGHT OUTER JOIN or
  FULL OUTER JOIN.
}

feature {CHECK constraints} {
  CHECK constraints are parsed but they are not enforced.
  NOT NULL and UNIQUE constraints are enforced, however.
}

feature {FOREIGN KEY constraints} {
  FOREIGN KEY constraints are parsed but are ignored.
}

feature {GRANT and REVOKE} {
  Since SQLite reads and writes an ordinary disk file, the
  only access permissions that can be applied are the normal
  file access permissions of the underlying operating system.
  The GRANT and REVOKE commands commonly found on client/server
  RDBMSes are not implemented because they would be meaningless
  for an embedded database engine.
}

feature {DELETE, INSERT, and UPDATE on VIEWs} {
  VIEWs in SQLite are read-only.  But you can create a trigger
  that fires on an attempt to DELETE, INSERT, or UPDATE a view and do
  what you need in the body of the trigger.
}

feature {ALTER TABLE} {
  To change a table you have to delete it (saving its contents to a temporary
  table) and recreate it from scratch.
}

feature {The COUNT(DISTINCT X) function} {
  You can accomplish the same thing using a subquery, like this:<br />
  &nbsp;&nbsp;SELECT count(x) FROM (SELECT DISTINCT x FROM tbl);
}

feature {Variable subqueries} {
  Subqueries must be static.  They are evaluated only once.  They must not,
  therefore, refer to variables in the containing query.
}

puts {
</table>

<p>
If you find other SQL92 features that SQLite does not support, please
send e-mail to <a href="mailto:drh@hwaci.com">drh@hwaci.com</a> so they
can be added to this list.
</p>
<p><hr /></p>
<p><a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite Home Page</a>
</p>

</body></html>}
