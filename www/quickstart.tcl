#
# Run this TCL script to generate HTML for the quickstart.html file.
#
set rcsid {$Id: quickstart.tcl,v 1.6 2004/10/19 01:31:20 drh Exp $}
source common.tcl
header {SQLite In 5 Minutes Or Less}
puts {
<p>Here is what you do to start experimenting with SQLite without having
to do a lot of tedious reading and configuration:</p>

<h2>Download The Code</h2>

<ul>
<li><p>Get a copy of the prebuilt binaries for your machine, or get a copy
of the sources and compile them yourself.  Visit
the <a href="download.html">download</a> page for more information.</p></li>
</ul>

<h2>Create A New Database</h2>

<ul>
<li><p>At a shell or DOS prompt, enter: "<b>sqlite3 test.db</b>".  This will
create a new database named "test.db".  (You can use a different name if
you like.)</p></li>
<li><p>Enter SQL commands at the prompt to create and populate the
new database.</p></li>
</ul>

<h2>Write Programs That Use SQLite</h2>

<ul>
<li><p>Below is a simple TCL program that demonstrates how to use
the TCL interface to SQLite.  The program executes the SQL statements
given as the second argument on the database defined by the first
argument.  The commands to watch for are the <b>sqlite3</b> command
on line 7 which opens an SQLite database and creates
a new TCL command named "<b>db</b>" to access that database, the
invocation of the <b>db</b> command on line 8 to execute
SQL commands against the database, and the closing of the database connection
on the last line of the script.</p>

<blockquote><pre>
#!/usr/bin/tclsh
if {$argc!=2} {
  puts stderr "Usage: %s DATABASE SQL-STATEMENT"
  exit 1
}
load /usr/lib/tclsqlite3.so Sqlite3
<b>sqlite3</b> db [lindex $argv 0]
<b>db</b> eval [lindex $argv 1] x {
  foreach v $x(*) {
    puts "$v = $x($v)"
  }
  puts ""
}
<b>db</b> close
</pre></blockquote>
</li>

<li><p>Below is a simple C program that demonstrates how to use
the C/C++ interface to SQLite.  The name of a database is given by
the first argument and the second argument is one or more SQL statements
to execute against the database.  The function calls to pay attention
to here are the call to <b>sqlite3_open()</b> on line 22 which opens
the database, <b>sqlite3_exec()</b> on line 27 that executes SQL
commands against the database, and <b>sqlite3_close()</b> on line 31
that closes the database connection.</p>

<blockquote><pre>
#include &lt;stdio.h&gt;
#include &lt;sqlite3.h&gt;

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i&lt;argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

int main(int argc, char **argv){
  sqlite3 *db;
  char *zErrMsg = 0;
  int rc;

  if( argc!=3 ){
    fprintf(stderr, "Usage: %s DATABASE SQL-STATEMENT\n", argv[0]);
    exit(1);
  }
  rc = <b>sqlite3_open</b>(argv[1], &db);
  if( rc ){
    fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
    sqlite3_close(db);
    exit(1);
  }
  rc = <b>sqlite3_exec</b>(db, argv[2], callback, 0, &zErrMsg);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
  }
  <b>sqlite3_close</b>(db);
  return 0;
}
</pre></blockquote>
</li>
</ul>
}
footer {$Id: quickstart.tcl,v 1.6 2004/10/19 01:31:20 drh Exp $}
