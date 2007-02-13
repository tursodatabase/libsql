#!/usr/bin/tclsh
source common.tcl
header {SQLite Older News}

proc newsitem {date title text} {
  puts "<h3>$date - $title</h3>"
  regsub -all "\n( *\n)+" $text "</p>\n\n<p>" txt
  puts "<p>$txt</p>"
  puts "<hr width=\"50%\">"
}


newsitem {2007-Jan-4} {Version 3.3.9} {
  Version 3.3.9 fixes bugs that can lead to database corruption under
  obscure and difficult to reproduce circumstances.  See
  <a href="http://www.sqlite.org/cvstrac/wiki?p=DatabaseCorruption">
  DatabaseCorruption</a> in the
  <a href="http://www.sqlite.org/cvstrac/wiki">wiki</a> for details.
  This release also adds the new
  <a href="capi3ref.html#sqlite3_prepare_v2">sqlite3_prepare_v2()</a>
  API and includes important bug fixes in the command-line
  shell and enhancements to the query optimizer.  Upgrading is
  recommended.
}

newsitem {2006-Oct-9} {Version 3.3.8} {
  Version 3.3.8 adds support for full-text search using the 
  <a href="http://www.sqlite.org/cvstrac/wiki?p=FtsOne">FTS1
  module.</a>  There are also minor bug fixes.  Upgrade only if
  you want to try out the new full-text search capabilities or if
  you are having problems with 3.3.7.
}

newsitem {2006-Aug-12} {Version 3.3.7} {
  Version 3.3.7 includes support for loadable extensions and virtual
  tables.  But both features are still considered "beta" and their
  APIs are subject to change in a future release.  This release is
  mostly to make available the minor bug fixes that have accumulated
  since 3.3.6.  Upgrading is not necessary.  Do so only if you encounter
  one of the obscure bugs that have been fixed or if you want to try
  out the new features.
}

newsitem {2006-Jun-19} {New Book About SQLite} {
  <a href="http://www.apress.com/book/bookDisplay.html?bID=10130">
  <i>The Definitive Guide to SQLite</i></a>, a new book by
  <a href="http://www.mikesclutter.com">Mike Owens</a>.
  is now available from <a href="http://www.apress.com">Apress</a>.
  The books covers the latest SQLite internals as well as
  the native C interface and bindings for PHP, Python,
  Perl, Ruby, Tcl, and Java.  Recommended.
}


newsitem {2006-Jun-6} {Version 3.3.6} {
  Changes include improved tolerance for windows virus scanners
  and faster :memory: databases.  There are also fixes for several
  obscure bugs.  Upgrade if you are having problems.
}

newsitem {2006-Apr-5} {Version 3.3.5} {
  This release fixes many minor bugs and documentation typos and
  provides some minor new features and performance enhancements.
  Upgrade only if you are having problems or need one of the new features.
}

newsitem {2006-Feb-11} {Version 3.3.4} {
  This release fixes several bugs, including a 
  a blunder that might cause a deadlock on multithreaded systems.
  Anyone using SQLite in a multithreaded environment should probably upgrade.
}

newsitem {2006-Jan-31} {Version 3.3.3 stable} {
  There have been no major problems discovered in version 3.3.2, so
  we hereby declare the new APIs and language features to be stable
  and supported.
}

newsitem {2006-Jan-24} {Version 3.3.2 beta} {
  More bug fixes and performance improvements as we move closer to
  a production-ready version 3.3.x.
}

newsitem {2006-Jan-16} {Version 3.3.1 alpha} {
  Many bugs found in last week's alpha release have now been fixed and
  the library is running much faster again.

  Database connections can now be moved between threads as long as the
  connection holds no locks at the time it is moved.  Thus the common
  paradigm of maintaining a pool of database connections and handing
  them off to transient worker threads is now supported.
  Please help test this new feature.
  See <a href="http://www.sqlite.org/cvstrac/wiki?p=MultiThreading">
  the MultiThreading wiki page</a> for additional
  information.
}

newsitem {2006-Jan-10} {Version 3.3.0 alpha} {
  Version 3.3.0 adds support for CHECK constraints, DESC indices,
  separate REAL and INTEGER column affinities, a new OS interface layer
  design, and many other changes.  The code passed a regression
  test but should still be considered alpha.  Please report any
  problems.

  The file format for version 3.3.0 has changed slightly to support
  descending indices and
  a more efficient encoding of boolean values.  SQLite 3.3.0 will read and
  write legacy databases created with any prior version of SQLite 3.  But
  databases created by version 3.3.0 will not be readable or writable
  by earlier versions of the SQLite.  The older file format can be
  specified at compile-time for those rare cases where it is needed.
}

newsitem {2005-Dec-19} {Versions 3.2.8 and 2.8.17} {
  These versions contain one-line changes to 3.2.7 and 2.8.16 to fix a bug
  that has been present since March of 2002 and version 2.4.0.
  That bug might possibly cause database corruption if a large INSERT or
  UPDATE statement within a multi-statement transaction fails due to a
  uniqueness constraint but the containing transaction commits.
}


newsitem {2005-Sep-24} {Version 3.2.7} {
  This version fixes several minor and obscure bugs.
  Upgrade only if you are having problems.
}

newsitem {2005-Sep-16} {Version 3.2.6 - Critical Bug Fix} {
  This version fixes a bug that can result in database
  corruption if a VACUUM of a 1 gibibyte or larger database fails
  (perhaps do to running out of disk space or an unexpected power loss)
  and is later rolled back.
  <p>
  Also in this release:
  The ORDER BY and GROUP BY processing was rewritten to use less memory.
  Support for COUNT(DISTINCT) was added.  The LIKE operator can now be
  used by the optimizer on columns with COLLATE NOCASE.
}

newsitem {2005-Aug-27} {Version 3.2.5} {
  This release fixes a few more lingering bugs in the new code.
  We expect that this release will be stable and ready for production use.
}

newsitem {2005-Aug-24} {Version 3.2.4} {
  This release fixes a bug in the new optimizer that can lead to segfaults
  when parsing very complex WHERE clauses.
}

newsitem {2005-Aug-21} {Version 3.2.3} {
  This release adds the <a href="lang_analyze.html">ANALYZE</a> command,
  the <a href="lang_expr.html">CAST</a> operator, and many
  very substantial improvements to the query optimizer.  See the
  <a href="changes.html#version_3_2_3">change log</a> for additional
  information.
}

newsitem {2005-Aug-2} {2005 Open Source Award for SQLite} {
  SQLite and its primary author D. Richard Hipp have been honored with
  a <a href="http://osdir.com/Article6677.phtml">2005 Open Source
  Award</a> from Google and O'Reilly.<br clear="right">
}


newsitem {2005-Jun-13} {Version 3.2.2} {
  This release includes numerous minor bug fixes, speed improvements,
  and code size reductions.  There is no reason to upgrade unless you
  are having problems or unless you just want to.
}

newsitem {2005-Mar-29} {Version 3.2.1} {
  This release fixes a memory allocation problem in the new
  <a href="lang_altertable.html">ALTER TABLE ADD COLUMN</a>
  command.
}

newsitem {2005-Mar-21} {Version 3.2.0} {
  The primary purpose for version 3.2.0 is to add support for
  <a href="lang_altertable.html">ALTER TABLE ADD COLUMN</a>.
  The new ADD COLUMN capability is made
  possible by AOL developers supporting and embracing great
  open-source software.  Thanks, AOL!

  Version 3.2.0 also fixes an obscure but serious bug that was discovered
  just prior to release.  If you have a multi-statement transaction and
  within that transaction an UPDATE or INSERT statement fails due to a
  constraint, then you try to rollback the whole transaction, the rollback
  might not work correctly.  See
  <a href="http://www.sqlite.org/cvstrac/tktview?tn=1171">Ticket #1171</a>
  for details.  Upgrading is recommended for all users.
}

newsitem {2005-Mar-16} {Version 3.1.6} {
  Version 3.1.6 fixes a critical bug that can cause database corruption
  when inserting rows into tables with around 125 columns. This bug was
  introduced in version 3.0.0.  See
  <a href="http://www.sqlite.org/cvstrac/tktview?tn=1163">Ticket #1163</a>
  for additional information.
}

newsitem {2005-Mar-11} {Versions 3.1.4 and 3.1.5 Released} {
  Version 3.1.4 fixes a critical bug that could cause database corruption
  if the autovacuum mode of version 3.1.0 is turned on (it is off by
  default) and a CREATE UNIQUE INDEX is executed within a transaction but
  fails because the indexed columns are not unique.  Anyone using the
  autovacuum feature and unique indices should upgrade.

  Version 3.1.5 adds the ability to disable
  the F_FULLFSYNC ioctl() in OS-X by setting "PRAGMA synchronous=on" instead
  of the default "PRAGMA synchronous=full".  There was an attempt to add
  this capability in 3.1.4 but it did not work due to a spelling error.
}

newsitem {2005-Feb-19} {Version 3.1.3 Released} {
  Version 3.1.3 cleans up some minor issues discovered in version 3.1.2.
}
  
newsitem {2005-Feb-15} {Versions 2.8.16 and 3.1.2 Released} {
  A critical bug in the VACUUM command that can lead to database
  corruption has been fixed in both the 2.x branch and the main
  3.x line.  This bug has existed in all prior versions of SQLite.
  Even though it is unlikely you will ever encounter this bug,
  it is suggested that all users upgrade.  See
  <a href="http://www.sqlite.org/cvstrac/tktview?tn=1116">
  ticket #1116</a>. for additional information.

  Version 3.1.2 is also the first stable release of the 3.1
  series.  SQLite 3.1 features added support for correlated
  subqueries, autovacuum, autoincrement, ALTER TABLE, and
  other enhancements.  See the 
  <a href="http://www.sqlite.org/releasenotes310.html">release notes
  for version 3.1.0</a> for a detailed description of the
  changes available in the 3.1 series.
}

newsitem {2005-Feb-01} {Version 3.1.1 (beta) Released} {
  Version 3.1.1 (beta) is now available on the
  website.  Verison 3.1.1 is fully backwards compatible with the 3.0 series
  and features many new features including Autovacuum and correlated
  subqueries.  The
  <a href="http://www.sqlite.org/releasenotes310.html">release notes</a>
  From version 3.1.0 apply equally to this release beta.  A stable release
  is expected within a couple of weeks.
}

newsitem {2005-Jan-21} {Version 3.1.0 (alpha) Released} {
  Version 3.1.0 (alpha) is now available on the
  website.  Verison 3.1.0 is fully backwards compatible with the 3.0 series
  and features many new features including Autovacuum and correlated
  subqueries.  See the
  <a href="http://www.sqlite.org/releasenotes310.html">release notes</a>
  for details.

  This is an alpha release.  A beta release is expected in about a week
  with the first stable release to follow after two more weeks.
}

newsitem {2004-Nov-09} {SQLite at the 2004 International PHP Conference} {
  There was a talk on the architecture of SQLite and how to optimize
  SQLite queries at the 2004 International PHP Conference in Frankfurt,
  Germany.
  <a href="http://www.sqlite.org/php2004/page-001.html">
  Slides</a> from that talk are available.
}

newsitem {2004-Oct-11} {Version 3.0.8} {
  Version 3.0.8 of SQLite contains several code optimizations and minor
  bug fixes and adds support for DEFERRED, IMMEDIATE, and EXCLUSIVE
  transactions.  This is an incremental release.  There is no reason
  to upgrade from version 3.0.7 if that version is working for you.
}


newsitem {2004-Oct-10} {SQLite at the 11<sup><small>th</small></sup>
Annual Tcl/Tk Conference} {
  There will be a talk on the use of SQLite in Tcl/Tk at the
  11<sup><small>th</small></sup> Tcl/Tk Conference this week in
  New Orleans.  Visit <a href="http://www.tcl.tk/community/tcl2004/">
  http://www.tcl.tk/</a> for details.
  <a href="http://www.sqlite.org/tclconf2004/page-001.html">
  Slides</a> from the talk are available.
}

newsitem {2004-Sep-18} {Version 3.0.7} {
  Version 3.0 has now been in use by multiple projects for several
  months with no major difficulties.   We consider it stable and
  ready for production use. 
}

newsitem {2004-Sep-02} {Version 3.0.6 (beta)} {
  Because of some important changes to sqlite3_step(),
  we have decided to
  do an additional beta release prior to the first "stable" release.
  If no serious problems are discovered in this version, we will
  release version 3.0 "stable" in about a week.
}


newsitem {2004-Aug-29} {Version 3.0.5 (beta)} {
  The fourth beta release of SQLite version 3.0 is now available.
  The next release is expected to be called "stable".
}


newsitem {2004-Aug-08} {Version 3.0.4 (beta)} {
  The third beta release of SQLite version 3.0 is now available.
  This new beta fixes several bugs including a database corruption
  problem that can occur when doing a DELETE while a SELECT is pending.
  Expect at least one more beta before version 3.0 goes final.
}

newsitem {2004-July-22} {Version 3.0.3 (beta)} {
  The second beta release of SQLite version 3.0 is now available.
  This new beta fixes many bugs and adds support for databases with
  varying page sizes.  The next 3.0 release will probably be called
  a final or stable release.

  Version 3.0 adds support for internationalization and a new
  more compact file format. 
  <a href="version3.html">Details.</a>
  The API and file format have been fixed since 3.0.2.  All
  regression tests pass (over 100000 tests) and the test suite
  exercises over 95% of the code.

  SQLite version 3.0 is made possible in part by AOL
  developers supporting and embracing great Open-Source Software.
}

newsitem {2004-Jly-22} {Version 2.8.15} {
  SQLite version 2.8.15 is a maintenance release for the version 2.8
  series.  Version 2.8 continues to be maintained with bug fixes, but
  no new features will be added to version 2.8.  All the changes in
  this release are minor.  If you are not having problems, there is
  there is no reason to upgrade.
}

newsitem {2004-Jun-30} {Version 3.0.2 (beta) Released} {
  The first beta release of SQLite version 3.0 is now available.
  Version 3.0 adds support for internationalization and a new
  more compact file format. 
  <a href="version3.html">Details.</a>
  As of this release, the API and file format are frozen.  All
  regression tests pass (over 100000 tests) and the test suite
  exercises over 95% of the code.

  SQLite version 3.0 is made possible in part by AOL
  developers supporting and embracing great Open-Source Software.
}
  

newsitem {2004-Jun-25} {Website hacked} {
  The www.sqlite.org website was hacked sometime around 2004-Jun-22
  because the lead SQLite developer failed to properly patch CVS.
  Evidence suggests that the attacker was unable to elevate privileges
  above user "cvs".  Nevertheless, as a precaution the entire website
  has been reconstructed from scratch on a fresh machine.  All services
  should be back to normal as of 2004-Jun-28.
}


newsitem {2004-Jun-18} {Version 3.0.0 (alpha) Released} {
  The first alpha release of SQLite version 3.0 is available for
  public review and comment.  Version 3.0 enhances internationalization support
  through the use of UTF-16 and user-defined text collating sequences.
  BLOBs can now be stored directly, without encoding.
  A new file format results in databases that are 25% smaller (depending
  on content).  The code is also a little faster.  In spite of the many
  new features, the library footprint is still less than 240KB
  (x86, gcc -O1).
  <a href="version3.html">Additional information</a>.

  Our intent is to freeze the file format and API on 2004-Jul-01.
  Users are encouraged to review and evaluate this alpha release carefully 
  and submit any feedback prior to that date.

  The 2.8 series of SQLite will continue to be supported with bug
  fixes for the foreseeable future.
}

newsitem {2004-Jun-09} {Version 2.8.14 Released} {
  SQLite version 2.8.14 is a patch release to the stable 2.8 series.
  There is no reason to upgrade if 2.8.13 is working ok for you.
  This is only a bug-fix release.  Most development effort is
  going into version 3.0.0 which is due out soon.
}

newsitem {2004-May-31} {CVS Access Temporarily Disabled} {
  Anonymous access to the CVS repository will be suspended
  for 2 weeks beginning on 2004-June-04.  Everyone will still
  be able to download
  prepackaged source bundles, create or modify trouble tickets, or view
  change logs during the CVS service interruption. Full open access to the
  CVS repository will be restored on 2004-June-18.
}

newsitem {2004-Apr-23} {Work Begins On SQLite Version 3} {
  Work has begun on version 3 of SQLite.  Version 3 is a major
  changes to both the C-language API and the underlying file format
  that will enable SQLite to better support internationalization.
  The first beta is schedule for release on 2004-July-01.

  Plans are to continue to support SQLite version 2.8 with
  bug fixes.  But all new development will occur in version 3.0.
}
footer {$Id: oldnews.tcl,v 1.19 2007/02/13 02:03:25 drh Exp $}
