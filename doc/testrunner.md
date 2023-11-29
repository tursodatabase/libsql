

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
</ul>
  <li> 4. <a href=#testrunner_options>Extra testrunner.tcl Options</a>
# 4. Extra testrunner.tcl Options
  <li> 5. <a href=#cpu_cores>Controlling CPU Core Utilization</a>
</ul>

<a name=overview></a>
# 1. Overview

testrunner.tcl is a Tcl script used to run multiple SQLite tests using 
multiple jobs. It supports the following types of tests:

  *  Tcl test scripts.

  *  Tests run with [make] commands. Specifically, at time of writing, 
     [make fuzztest], [make mptest], [make sourcetest] and [make threadtest].

testrunner.tcl pipes the output of all tests and builds run into log file
**testrunner.log**, created in the cwd directory. Searching this file for
"failed" is a good way to find the output of a failed test.

testrunner.tcl also populates SQLite database **testrunner.db**. This database
contains details of all tests run, running and to be run. A useful query
might be:

```
  SELECT * FROM script WHERE state='failed'
```

Running the command:

```
  ./testfixture $(TESTDIR)/testrunner.tcl status
```

in the directory containing the testrunner.db database runs various queries
to produce a succinct report on the state of a running testrunner.tcl script.
Running:

```
  watch ./testfixture $(TESTDIR)/testrunner.tcl status
```

in another terminal is a good way to keep an eye on a long running test.

Sometimes testrunner.tcl uses the [testfixture] binary that it is run with
to run tests (see "Binary Tests" below). Sometimes it builds testfixture and
other binaries in specific configurations to test (see "Source Tests").

<a name=binary_tests></a>
# 2. Binary Tests

The commands described in this section all run various combinations of the Tcl
test scripts using the [testfixture] binary used to run the testrunner.tcl
script (i.e. they do not invoke the compiler to build new binaries, or the
[make] command to run tests that are not Tcl scripts). The procedure to run
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
the [make fuzztest] target once for each of two --enable-all builds - one 
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
In that case only Tcl tests (no fuzz or other tests) that match the specified
pattern are run. For example, to run the just the Tcl rtree tests in all 
builds and configurations supported by "release":

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
target to build. This may be used either to run a [make] command test directly,
or else to build a testfixture (or testfixture.exe) binary with which to
run a Tcl test script, as <a href=#binary_test_failures>described above</a>.

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



