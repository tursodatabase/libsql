#!/usr/bin/tclsh
source common.tcl
header {SQLite Older News}

proc newsitem {date title text} {
  puts "<h3>$date - $title</h3>"
  regsub -all "\n( *\n)+" $text "</p>\n\n<p>" txt
  puts "<p>$txt</p>"
  puts "<hr width=\"50%\">"
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
footer {$Id: oldnews.tcl,v 1.1 2004/06/16 03:02:05 drh Exp $}
