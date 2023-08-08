# Specifications For A Rudimentary SQLite Test Script Interpreter

## Overview

The purpose of the Test Script Interpreter is to read and interpret
script files that contain SQL commands and desired results.  The
interpreter will check results and report an discrepencies found.

The test script files are ASCII text files.  The filename always ends with
".test".  Each script is evaluated independently; context does not carry
forward from one script to the next.  So, for example, the --null command
run in one test script does not cause any changes in the behavior of
subsequent test scripts.  All open database connections are closed
at the end of each test script.  All database files created by a test
script are deleted when the script finishes.

## Parsing Rules:

  1.   Ignore the entire script if the script does not contain the
       string "SCRIPT_MODULE_NAME:".

  2.   Ignore any script that contains the character sequence "\\n\|"
       (0x0a, 0x7c).  In other words, ignore scripts that contain the
       pipe character at the beginning of a line.  Such lines represent
       test database file content in the "dbtotxt" format.  We might add
       support for this later, but omit it for the first version.

  3.   Ignore individual lines that begin with '#'  (C-preprocessor lines).

  4.   If a line begins with exactly two minus signs followed by a
       lowercase letter, that is a command.  Process commands as described
       below.

  5.   All other lines should be accumulated into the "input buffer".
       The various commands will have access to this input buffer.
       Some commands will reset the buffer.

## Commands:

Each command looks like an SQL comment.  The command begins at the left
margin (no leading space) and starts with exactly 2 minus signs ("-").
The command name consists of lowercase letters and maybe a "-" or two.
Some commands have arguments.
The arguments are separated from the command name by one or more spaces.

Commands have access to the input buffer and might reset the input buffer.
The command can also optionally read (and consume) additional text from
script that comes after the command.

Unknown or unrecognized commands should cause an error message to be
printed and execution to stop.

The initial implemention will only recognize a few commands.  Other
commands may be added later.  The following is the initial set of
commands:

### The --testcase command

Every test case starts with a --testcase command.  The --testcase command
resets both the "input buffer" and the "result buffer".
The argument to the --testcase command is the
name of the test case.  That test case name is used for logging and debugging
and when printing errors.

### The --result command

The --result command tries to execute the text in the input buffer as SQL.
For each row of result coming out of this SQL, the text of that result is
appended to the "result buffer".  If a result row contains multiple columns,
the columns are processed from left to right.  For each column, text is
appended to the result buffer according to the following rules:

  *   If the result buffer already contains some text, append a space.
      (In this way, all column values and all row values are separated from
      each other by a single space.)

  *   If the sqlite3_column_text() returns NULL, then append "nil" - or
      some other text that is specified by the --null command - and skip
      all subsequent rules.

  *   If sqlite3_column_text() does not contain any special characters,
      append it to the result buffer without any formatting and skip all
      subsequent rules.

  *   If sqlite3_column_text() does not contains curly braces, then put
      the text inside of `{...}` and append it and skip all subsequent rules.

  *   Append the text within double-quotes (`"..."`) and within the text
      escape '"' and '\\' by prepending a single '\\' and escape any
      control characters (characters less than 0x20) using octal notation:
      '\\NNN'.

If an error is encountered while running the SQL, then append the
symbolic C-preprocessor name for the error
code (ex: "SQLITE_CONSTRAINT") as if it were a column value.  Then append
the error message text as if it where a column value.  Then stop processing.

After the SQL text has been run, compare the content of the result buffer
against the argument to the --result command and report a testing error if
there are any differences.

The --result command resets the input buffer, but it does not reset
the result buffer.  This distinction does not matter for the --result
command itself, but it is important for related commands like --glob
and --notglob.  Sometimes test cases will contains a bunch of SQL
followed by multiple --glob and/or --notglob statements.  All of the
globs should be evaluted agains the result buffer correct, but the SQL
should only be run once.  This is accomplished by resetting the input
buffer but not the result buffer.

### The --glob command

The --glob command works just like --result except that the argument to
--glob is interpreted as a TEST-GLOB pattern and the results are compared
using that glob pattern rather than using strcmp().  Other than that,
the two operate the same.

The TEST-GLOB pattern is slightly different for a standard GLOB:

   *    The '*' character matches zero or more characters.

   *    The '?' character matches any single character

   *    The '[...]' character sequence machines a single character
        in between the brackets.

   *    The '#' character matches one or more digits  (This is the main
        difference between standard unix-glob and TEST-GLOB.  unix-glob
        does not have this feature.  It was added to because it comes
        up a lot during SQLite testing.)

### The --notglob command

The --notglob command works just like --glob except that it reports an
error if the GLOB does match, rather than if the GLOB does not matches.

### The --oom command

This command is to be used for out-of-memory testing.  It means that
OOM errors should be simulated to ensure that SQLite is able to deal with
them.  This command can be silently ignored for now.  We might add support
for this later.

### The --tableresult command

The --tableresult command works like --glob except that the GLOB pattern
to be matched is taken from subsequent lines of the input script up to
the next --end.  Every span of one or more whitespace characters in this
pattern text is collapsed into a single space (0x20).
Leading and trailing whitespace are removed from the pattern.
The --end that ends the GLOB pattern is not part of the GLOB pattern, but
the --end is consumed from the script input.

### The --new and --open commands

The --new and --open commands cause a database file to be opened.
The name of the file is the argument to the command.  The --new command
opens an initially empty database (it deletes the file before opening it)
whereas the --open command opens an existing database if it already
exists.

### The --db command

The script interpreter can have up to 7 different SQLite database
connections open at a time.  The --db command is used to switch between
them.  The argument to --db is an integer between 0 and 6 that selects
which database connection to use moving forward.

### The --close command

The --close command causes an existing database connetion to close.
This command is a no-op if the database connection is not currently
open.  There can be up to 7 different database connections, numbered 0
through 6.  The number of the database connection to close is an
argument to the --close command, which will fail if an out-of-range
value is provided.  Or if the argument to --close is "all" then all
open database connections are closed. If passed no argument, the
currently-active database is assumed.

### The --null command

The NULL command changes the text that is used to represent SQL NULL
values in the result buffer.

### The --run command

The --run command executes text in the input buffer as if it where SQL.
However, nothing is added to the result buffer.  Any output from the SQL
is silently ignored. Errors in the SQL are silently ignored.

The --run command normally executes the SQL in the current database
connection.  However, if --run has an argument that is an integer between
0 and 6 then the SQL is run in the alternative database connection specified
by that argument.

### The --json and --json-block commands

The --json and --json-block commands work like --result and --tableresult,
respectively.  The difference is that column values are appended to the
result buffer literally, without every enclosing the values in `{...}` or
`"..."` and without escaping any characters in the column value and comparison
is always an exact strcmp() not a GLOB.
