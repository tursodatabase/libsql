#
# Run this TCL script to generate HTML for the quickstart.html file.
#
set rcsid {$Id: quickstart.tcl,v 1.1 2002/08/15 13:45:17 drh Exp $}

puts {<html>
<head><title>SQLite In 5 Minutes Or Less</title></head>
<body bgcolor=white>
<h1 align=center>SQLite In 5 Minutes Or Less</h1>}

puts {
<p>Here is what you do to start experimenting with SQLite without having
to do a lot of tedious reading and configuration:</p>

<h2>Download The Code</h2>

<ul>
<li><p>Get a copy of the prebuild binaries for your machine, or get a copy
of the sources and compile them yourself.  Visit
the <a href="download.html">download</a> page for more information.</p></li>
</ul>

<h2>Create A New Database</h2>

<ul>
<li><p>At a shell or DOS prompt, enter: "<b>sqlite test.db</b>".  This will
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
argument.</p>

<blockquote><pre>
#!/usr/bin/tclsh
if {$argc!=2} {
  puts stderr "Usage: %s DATABASE SQL-STATEMENT"
  exit 1
}
load /usr/lib/tclsqlite.so Sqlite
sqlite db [lindex $argv 0]
db eval [lindex $argv 1] x {
  foreach v $x(*) {
    puts "$v = $x($v)"
  }
  puts ""
}
db close
</pre></blockquote>
</li>

<li><p>Below is a simple C program that demonstrates how to use
the C/C++ interface to SQLite.  The name of a database is given by
the first argument and the second argument is one or more SQL statements
to execute against the database.</p>

<blockquote><pre>
#include &lt;stdio.h&gt;
#include &lt;sqlite.h&gt;

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i&lt;argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("\n");
  return 0;
}

int main(int argc, char **argv){
  sqlite *db;
  char *zErrMsg = 0;
  int rc;

  if( argc!=3 ){
    fprintf(stderr, "Usage: %s DATABASE SQL-STATEMENT\n", argv[0]);
    exit(1);
  }
  db = sqlite_open(argv[1], 0, &zErrMsg);
  if( db==0 ){
    fprintf(stderr, "Can't open database: %s\n", &zErrMsg);
    exit(1);
  }
  rc = sqlite_exec(db, argv[2], callback, 0, &zErrMsg);
  if( rc!=SQLITE_OK ){
    fprintf(stderr, "SQL error: %s\n", zErrMsg);
  }
  sqlite_close(db);
  return 0;
}
</pre></blockquote>
</li>
</ul>
}

puts {
<p><hr /></p>
<p>
<a href="index.html"><img src="/goback.jpg" border=0 />
Back to the SQLite home page</a>
</p>

</body></html>}
