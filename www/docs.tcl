# This script generates the "docs.html" page that describes various
# sources of documentation available for SQLite.
#
set rcsid {$Id: docs.tcl,v 1.11 2005/03/19 14:45:50 drh Exp $}
source common.tcl
header {SQLite Documentation}
puts {
<h2>Available Documentation</h2>
<table width="100%" cellpadding="5">
}

proc doc {name url desc} {
  puts {<tr><td valign="top" align="right">}
  regsub -all { +} $name {\&nbsp;} name
  puts "<a href=\"$url\">$name</a></td>"
  puts {<td width="10"></td>}
  puts {<td align="top" align="left">}
  puts $desc
  puts {</td></tr>}
}

doc {Appropriate Uses For SQLite} {whentouse.html} {
  This document describes situations where SQLite is an approriate
  database engine to use versus situations where a client/server
  database engine might be a better choice.
}

doc {Distinctive Features} {different.html} {
  This document enumerates and describes some of the features of
  SQLite that make it different from other SQL database engines.
}

doc {SQLite In 5 Minutes Or Less} {quickstart.html} {
  A very quick introduction to programming with SQLite.
}

doc {SQL Syntax} {lang.html} {
  This document describes the SQL language that is understood by
  SQLite.  
}

doc {Pragma commands} {pragma.html} {
  This document describes SQLite performance tuning options and other 
  special purpose database commands.
}

doc {Version 2 C/C++ API} {c_interface.html} {
  A description of the C/C++ interface bindings for SQLite through version 
  2.8
}
doc {SQLite Version 3} {version3.html} {
  A summary of of the changes between SQLite version 2.8 and SQLite version 3.0.
}
doc {Version 3 C/C++ API} {capi3.html} {
  A description of the C/C++ interface bindings for SQLite version 3.0.0
  and following.
}
doc {Version 3 C/C++ API<br>Reference} {capi3ref.html} {
  This document describes each API function separately.
}

doc {Tcl API} {tclsqlite.html} {
  A description of the TCL interface bindings for SQLite.
}

doc {Locking And Concurrency<br>In SQLite Version 3} {lockingv3.html} {
  A description of how the new locking code in version 3 increases
  concurrancy and decreases the problem of writer starvation.
}

doc {Version 2 DataTypes } {datatypes.html} {
  A description of how SQLite version 2 handles SQL datatypes.
  Short summary:  Everything is a string.
}
doc {Version 3 DataTypes } {datatype3.html} {
  SQLite version 3 introduces the concept of manifest typing, where the
  type of a value is associated with the value itself, not the column that
  it is stored in.
  This page describes data typing for SQLite version 3 in further detail.
}

doc {Release History} {changes.html} {
  A chronology of SQLite releases going back to version 1.0.0
}

doc {Null Handling} {nulls.html} {
  Different SQL database engines handle NULLs in different ways.  The
  SQL standards are ambiguous.  This document describes how SQLite handles
  NULLs in comparison with other SQL database engines.
}

doc {Copyright} {copyright.html} {
  SQLite is in the public domain.  This document describes what that means
  and the implications for contributors.
}

doc {Unsupported SQL} {omitted.html} {
  This page describes features of SQL that SQLite does not support.
}

doc {Speed Comparison} {speed.html} {
  The speed of version 2.7.6 of SQLite is compared against PostgreSQL and
  MySQL.
}

doc {Architecture} {arch.html} {
  An architectural overview of the SQLite library, useful for those who want
  to hack the code.
}

doc {VDBE Tutorial} {vdbe.html} {
  The VDBE is the subsystem within SQLite that does the actual work of
  executing SQL statements.  This page describes the principles of operation
  for the VDBE in SQLite version 2.7.  This is essential reading for anyone
  who want to modify the SQLite sources.
}

doc {VDBE Opcodes} {opcode.html} {
  This document is an automatically generated description of the various
  opcodes that the VDBE understands.  Programmers can use this document as
  a reference to better understand the output of EXPLAIN listings from
  SQLite.
}

doc {Compilation Options} {compile.html} {
  This document describes the compile time options that may be set to 
  modify the default behaviour of the library or omit optional features
  in order to reduce binary size.
}

doc {Backwards Compatibility} {formatchng.html} {
  This document details all of the incompatible changes to the SQLite
  file format that have occurred since version 1.0.0.
}

puts {</table>}
footer $rcsid
