#!/usr/bin/tclsh
source common.tcl
header {SQLite Older News}

proc newsitem {date title text} {
  puts "<h3>$date - $title</h3>"
  regsub -all "\n( *\n)+" $text "</p>\n\n<p>" txt
  puts "<p>$txt</p>"
  puts "<hr width=\"50%\">"
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
footer {$Id: oldnews.tcl,v 1.7 2004/10/11 22:26:00 drh Exp $}
