#!/usr/bin/tclsh
source common.tcl
header {SQLite Older News}

proc newsitem {date title text} {
  puts "<h3>$date - $title</h3>"
  regsub -all "\n( *\n)+" $text "</p>\n\n<p>" txt
  puts "<p>$txt</p>"
  puts "<hr width=\"50%\">"
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
  This is only a bug-fix release.  Most developement effort is
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
footer {$Id: oldnews.tcl,v 1.2 2004/06/30 22:35:07 drh Exp $}
