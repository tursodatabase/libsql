#!/usr/bin/tcl
#
# Removes and/or replaces specially marked sections from the Makefile
# for MSVC, writing the resulting output to another (different) file.
# In addition, some other strings are also removed and/or replaced if
# they are present in the Makefile.
#
set fromFileName [lindex $argv 0]

if {![file exists $fromFileName]} then {
  error "input file \"$fromFileName\" does not exist"
}

set toFileName [lindex $argv 1]

if {[file exists $toFileName]} then {
  error "output file \"$toFileName\" already exists"
}

proc readFile { fileName } {
  set file_id [open $fileName RDONLY]
  fconfigure $file_id -encoding binary -translation binary
  set result [read $file_id]
  close $file_id
  return $result
}

proc writeFile { fileName data } {
  set file_id [open $fileName {WRONLY CREAT TRUNC}]
  fconfigure $file_id -encoding binary -translation binary
  puts -nonewline $file_id $data
  close $file_id
  return ""
}

proc escapeSubSpec { data } {
  regsub -all -- {&} $data {\\\&} data
  regsub -all -- {\\(\d+)} $data {\\\\\1} data
  return $data
}

proc substVars { data } {
  return [uplevel 1 [list subst -nocommands -nobackslashes $data]]
}

#
# NOTE: This block is used to replace the section marked <<block1>> in
#       the Makefile, if it exists.
#
set blocks(1) [string trimleft [string map [list \\\\ \\] {
_HASHCHAR=^#
!IF ![echo !IFNDEF VERSION > rcver.vc] && \\
    ![for /F "delims=" %V in ('type "$(SQLITE3H)" ^| find "$(_HASHCHAR)define SQLITE_VERSION "') do (echo VERSION = ^^%V >> rcver.vc)] && \\
    ![echo !ENDIF >> rcver.vc]
!INCLUDE rcver.vc
!ENDIF

RESOURCE_VERSION = $(VERSION:^#=)
RESOURCE_VERSION = $(RESOURCE_VERSION:define=)
RESOURCE_VERSION = $(RESOURCE_VERSION:SQLITE_VERSION=)
RESOURCE_VERSION = $(RESOURCE_VERSION:"=)
RESOURCE_VERSION = $(RESOURCE_VERSION:.=,)

$(LIBRESOBJS):	$(TOP)\sqlite3.rc rcver.vc $(SQLITE3H)
	echo #ifndef SQLITE_RESOURCE_VERSION > sqlite3rc.h
	echo #define SQLITE_RESOURCE_VERSION $(RESOURCE_VERSION) >> sqlite3rc.h
	echo #endif >> sqlite3rc.h
	$(LTRCOMPILE) -fo $(LIBRESOBJS) -DRC_VERONLY $(TOP)\sqlite3.rc
}]]

set data [readFile $fromFileName]

regsub -all -- {# <<mark>>\n.*?# <</mark>>\n} \
    $data "" data

foreach i [lsort -integer [array names blocks]] {
  regsub -all -- [substVars \
      {# <<block${i}>>\n.*?# <</block${i}>>\n}] \
      $data [escapeSubSpec $blocks($i)] data
}

set data [string map [list " -I\$(TOP)\\src" ""] $data]
set data [string map [list " /DEF:sqlite3.def" ""] $data]
set data [string map [list " sqlite3.def" ""] $data]
set data [string map [list " libtclsqlite3.lib" ""] $data]
set data [string map [list "\$(TOP)\\src\\" "\$(TOP)\\"] $data]

writeFile $toFileName $data
