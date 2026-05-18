

# The testrunner.tcl Script

<ul type=none>
  <li> 1. <a href=#overview>Overview</a>
  <li> 2. <a href=#binary_tests>Binary Tests</a>
<ul type=none>
  <li> 2.1. <a href=#organization_tests>Organization of Tcl Tests</a>
  <li> 2.2. <a href=#run_tests>Commands to Run Tests</a>
  <li> 2.3. <a href=#binary_test_failures>Investigating Binary Test Failures</a>
</ul>
  <li> 3. <a href=#source_code_tests>Source Tests</a>
<ul type=none>
  <li> 3.1. <a href=#commands_to_run_tests>Commands to Run SQLite Tests</a>
  <li> 3.2. <a href=#zipvfs_tests>Running ZipVFS Tests</a>
  <li> 3.3. <a href=#source_code_test_failures>Investigating Source Code Test Failures</a>
  <li> 3.4. <a href=#fuzzdb>External Fuzzcheck Databases</a>
</ul>
  <li> 4. <a href=#testrunner_options>Extra testrunner.tcl Options</a>
  <li> 5. <a href=#cpu_cores>Controlling CPU Core Utilization</a>
</ul>

<a name=overview></a>
# 1. Overview

The testrunner.tcl program is a Tcl script used to run multiple SQLite
tests in parallel, thus reducing testing time on multi-core machines.
It supports the following types of tests:

  *  Tcl test scripts.

  *  Fuzzcheck tests, including using an external fuzzcheck database.

  *  Tests run with `make` commands.  Examples:
      -  `make devtest`
      -  `make releasetest`
      -  `make sdevtest`
      -  `make testrunner`

The testrunner.tcl program stores output of all tests and builds run in
log file **testrunner.log**, created in the current working directory.
Search this file to find details of errors.  Suggested search commands:

   *  `grep "^!" testrunner.log`
   *  `grep failed testrunner.log`

The testrunner.tcl program also populates SQLite database **testrunner.db**.
This database contains details of all tests run, running and to be run.
A useful query might be:

```
  SELECT * FROM script WHERE state='failed'
```

You can get a summary of errors in a prior run by invoking commands like
these:

```
  tclsh $(TESTDIR)/testrunner.tcl errors
  tclsh $(TESTDIR)/testrunner.tcl errors -v
```

Running the command:

```
  tclsh $(TESTDIR)/testrunner.tcl status
```

in the directory containing the testrunner.db database runs various queries
to produce a succinct report on the state of a running testrunner.tcl script.
A good way to keep and eye on test progress is to run either of the two
following commands:

```
  watch tclsh $(TESTDIR)/testrunner.tcl status
  tclsh $(TESTDIR)/testrunner.tcl status -d 2
```

Both of the commands above accomplish about the same thing, but the second
one has the advantage of not requiring "watch" to be installed on your
system.

Sometimes testrunner.tcl uses the `testfixture` binary that it is run with
to run tests (see "Binary Tests" below). Sometimes it builds testfixture and
other binaries in specific configurations to test (see "Source Tests").

<a name=binary_tests></a>
# 2. Binary Tests

The commands described in this section all run various combinations of the Tcl
test scripts using the `testfixture` binary used to run the testrunner.tcl
script (i.e. they do not invoke the compiler to build new binaries, or the
`make` command to run tests that are not Tcl scripts). The procedure to run
these tests is therefore:

  1. Build the "testfixture" (or "testfixture.exe" for windows) binary using
     whatever method seems convenient.

  2. Test the binary built in step 1 by running testrunner.tcl with it, 
     perhaps with various options.

The following sub-sections describe the various options that can be
passed to testrunner.tcl to test binary testfixture builds.

<a name=organization_tests></a>
## 2.1. Organization of Tcl Tests

Tcl tests are stored in files that match the pattern *\*.test*. They are
found in both the $TOP/test/ directory, and in the various sub-directories
of the $TOP/ext/ directory of the source tree. Not all *\*.test* files
contain Tcl tests - a handful are Tcl scripts designed to invoke other
*\*.test* files.

The **veryquick** set of tests is a subset of all Tcl test scripts in the
source tree. In includes most tests, but excludes some that are very slow.
Almost all fault-injection tests (those that test the response of the library
to OOM or IO errors) are excluded. It is defined in source file 
*test/permutations.test*.

The **full** set of tests includes all Tcl test scripts in the source tree.
To run a "full" test is to run all Tcl test scripts that can be found in the
source tree.

File *permutations.test* defines various test "permutations". A permutation
consists of:

  *  A subset of Tcl test scripts, and 

  *  Runtime configuration to apply before running each test script 
     (e.g. enabling auto-vacuum, or disable lookaside).

Running **all** tests is to run all tests in the full test set, plus a dozen
or so permutations. The specific permutations that are run as part of "all"
are defined in file *testrunner_data.tcl*.

<a name=run_tests></a>
## 2.2. Commands to Run Tests

To run the "veryquick" test set, use either of the following:

```
  ./testfixture $TESTDIR/testrunner.tcl
  ./testfixture $TESTDIR/testrunner.tcl veryquick
```

To run the "full" test suite:

```
  ./testfixture $TESTDIR/testrunner.tcl full
```

To run the subset of the "full" test suite for which the test file name matches
a specified pattern (e.g. all tests that start with "fts5"), either of:

```
  ./testfixture $TESTDIR/testrunner.tcl fts5%
  ./testfixture $TESTDIR/testrunner.tcl 'fts5*'
```

Strictly speaking, for a test to be run the pattern must match the script
filename, not including the directory, using the rules of Tcl's 
\[string match\] command. Except that before the matching is done, any "%"
characters specified as part of the pattern are transformed to "\*".


To run "all" tests (full + permutations):

```
  ./testfixture $TESTDIR/testrunner.tcl all
```

<a name=binary_test_failures></a>
## 2.3. Investigating Binary Test Failures

If a test fails, testrunner.tcl reports name of the Tcl test script and, if
applicable, the name of the permutation, to stdout. This information can also
be retrieved from either *testrunner.log* or *testrunner.db*.

If there is no permutation, the individual test script may be run with:

```
  ./testfixture $PATH_TO_SCRIPT
```

Or, if the failure occured as part of a permutation:

```
  ./testfixture $TESTDIR/testrunner.tcl $PERMUTATION $PATH_TO_SCRIPT
```

TODO: An example instead of "$PERMUTATION" and $PATH\_TO\_SCRIPT?

<a name=source_code_tests></a>
# 3. Source Code Tests

The commands described in this section invoke the C compiler to build 
binaries from the source tree, then use those binaries to run Tcl and
other tests. The advantages of this are that:

  *  it is possible to test multiple build configurations with a single
     command, and 

  *  it ensures that tests are always run using binaries created with the
     same set of compiler options.

The testrunner.tcl commands described in this section may be run using
either a *testfixture* (or testfixture.exe) build, or with any other Tcl
shell that supports SQLite 3.31.1 or newer via "package require sqlite3".

TODO: ./configure + Makefile.msc build systems.

<a name=commands_to_run_tests></a>
## 3.1. Commands to Run SQLite Tests

The **mdevtest** command is equivalent to running the veryquick tests and
the `make fuzztest` target once for each of two --enable-all builds - one 
with debugging enabled and one without:

```
  tclsh $TESTDIR/testrunner.tcl mdevtest
```

In other words, it is equivalent to running:

```
  $TOP/configure --enable-all --enable-debug
  make fuzztest
  make testfixture
  ./testfixture $TOP/test/testrunner.tcl veryquick

  # Then, after removing files created by the tests above:
  $TOP/configure --enable-all OPTS="-O0"
  make fuzztest
  make testfixture
  ./testfixture $TOP/test/testrunner.tcl veryquick
```

The **sdevtest** command is identical to the mdevtest command, except that the
second of the two builds is a sanitizer build. Specifically, this means that
OPTS="-fsanitize=address,undefined" is specified instead of OPTS="-O0":

```
  tclsh $TESTDIR/testrunner.tcl sdevtest
```

The **release** command runs lots of tests under lots of builds. It runs
different combinations of builds and tests depending on whether it is run
on Linux, Windows or OSX. Refer to *testrunner\_data.tcl* for the details
of the specific tests run.

```
  tclsh $TESTDIR/testrunner.tcl release
```

As with <a href=#source code tests>source code tests</a>, one or more patterns
may be appended to any of the above commands (mdevtest, sdevtest or release).
Pattern matching is used for both Tcl tests and fuzz tests.

```
  tclsh $TESTDIR/testrunner.tcl release rtree%
```

<a name=zipvfs_tests></a>
## 3.2. Running ZipVFS Tests

testrunner.tcl can build a zipvfs-enabled testfixture and use it to run
tests from the Zipvfs project with the following command:

```
  tclsh $TESTDIR/testrunner.tcl --zipvfs $PATH_TO_ZIPVFS
```

This can be combined with any of "mdevtest", "sdevtest" or "release" to
test both SQLite and Zipvfs with a single command:

```
  tclsh $TESTDIR/testrunner.tcl --zipvfs $PATH_TO_ZIPVFS mdevtest
```

<a name=source_code_test_failures></a>
## 3.3. Investigating Source Code Test Failures

Investigating a test failure that occurs during source code testing is a
two step process:

  1. Recreating the build configuration in which the test failed, and

  2. Re-running the actual test.

To recreate a build configuration, use the testrunner.tcl **script** command
to create a build script. A build script is a bash script on Linux or OSX, or
a dos \*.bat file on windows. For example:

```
  # Create a script that recreates build configuration "Device-One" on 
  # Linux or OSX:
  tclsh $TESTDIR/testrunner.tcl script Device-One > make.sh 

  # Create a script that recreates build configuration "Have-Not" on Windows:
  tclsh $TESTDIR/testrunner.tcl script Have-Not > make.bat 
```

The generated bash or \*.bat file script accepts a single argument - a makefile
target to build. This may be used either to run a `make` command test directly,
or else to build a testfixture (or testfixture.exe) binary with which to
run a Tcl test script, as <a href=#binary_test_failures>described above</a>.

<a name=fuzzdb></a>
## 3.4 External Fuzzcheck Databases

Testrunner.tcl will also run fuzzcheck against an external (out of tree)
database, for example fuzzcheck databases generated by dbsqlfuzz.  To do
this, simply add the "`--fuzzdb` *FILENAME*" command-line option or set 
the FUZZDB environment variable to the name of the external
database.  For large external databases, testrunner.tcl will automatically use
the "`--slice`" command-line option of fuzzcheck to divide the work up into
multiple jobs, to increase parallelism.

Thus, for example, to run a full releasetest including an external
dbsqlfuzz database, run a command like one of these:

```
  tclsh test/testrunner.tcl releasetest --fuzzdb ../fuzz/20250415.db
  FUZZDB=../fuzz/20250415.db make releasetest
  nmake /f Makefile.msc FUZZDB=../fuzz/20250415.db releasetest
```

The patternlist option to testrunner.tcl will match against fuzzcheck
databases.  So if you want to run *only* tests involving the external
database, you can use a command something like this:

```
  tclsh test/testrunner.tcl releasetest 20250415 --fuzzdb ../fuzz/20250415.db
```

<a name=testrunner_options></a>
# 4. Extra testrunner.tcl Options

The testrunner.tcl script options in this section may be used with both source
code and binary tests.

The **--buildonly** option instructs testrunner.tcl just to build the binaries
required by a test, not to run any actual tests. For example:

```
  # Build binaries required by release test.
  tclsh $TESTDIR/testrunner.tcl --buildonly release"
```

The **--dryrun** option prevents testrunner.tcl from building any binaries
or running any tests. Instead, it just writes the shell commands that it
would normally execute into the testrunner.log file. Example:

```
  # Log the shell commmands that make up the mdevtest test.
  tclsh $TESTDIR/testrunner.tcl --dryrun mdevtest"
```

The **--explain** option is similar to --dryrun in that it prevents
testrunner.tcl from building any binaries or running any tests.  The
difference is that --explain prints on standard output a human-readable
summary of all the builds and tests that would have been run.

```
  # Show what builds and tests would have been run
  tclsh $TESTDIR/testrunner.tcl --explain mdevtest
```

The **--status** option uses VT100 escape sequences to display the test
status full-screen.  This is similar to running 
"`watch test/testrunner status`" in a separate window, just more convenient.
Unfortunately, this option does not work correctly on Windows, due to the
sketchy implementation of VT100 escapes on the Windows console.

<a name=cpu_cores></a>
# 5. Controlling CPU Core Utilization

When running either binary or source code tests, testrunner.tcl reports the
number of jobs it intends to use to stdout. e.g.

```
  $ ./testfixture $TESTDIR/testrunner.tcl
  splitting work across 16 jobs
  ... more output ...
```

By default, testfixture.tcl attempts to set the number of jobs to the number 
of real cores on the machine. This can be overridden using the "--jobs" (or -j)
switch:

```
  $ ./testfixture $TESTDIR/testrunner.tcl --jobs 8
  splitting work across 8 jobs
  ... more output ...
```

The number of jobs may also be changed while an instance of testrunner.tcl is
running by exucuting the following command from the directory containing the
testrunner.log and testrunner.db files:

```
  $ ./testfixture $TESTDIR/testrunner.tcl njob $NEW_NUMBER_OF_JOBS
```
