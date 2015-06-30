#!/bin/sh
# restart with tclsh \
exec tclsh "$0" "$@"

set srcdir [file dirname [file dirname [info script]]]
set G(src) [string map [list %dir% $srcdir] {
  %dir%/fts5.h
  %dir%/fts5Int.h
  fts5parse.h
  %dir%/fts5_aux.c
  %dir%/fts5_buffer.c
  %dir%/fts5_config.c
  %dir%/fts5_expr.c
  %dir%/fts5_hash.c
  %dir%/fts5_index.c
  %dir%/fts5_main.c
  %dir%/fts5_storage.c
  %dir%/fts5_tokenize.c
  %dir%/fts5_unicode2.c
  %dir%/fts5_varint.c
  %dir%/fts5_vocab.c
  fts5parse.c
}]

set G(hdr) {

#if !defined(NDEBUG) && !defined(SQLITE_DEBUG) 
# define NDEBUG 1
#endif
#if defined(NDEBUG) && defined(SQLITE_DEBUG)
# undef NDEBUG
#endif

}

proc readfile {zFile} {
  set fd [open $zFile]
  set data [read $fd]
  close $fd
  return $data
}

proc fts5c_init {zOut} {
  global G
  set G(fd) stdout
  set G(fd) [open $zOut w]

  puts -nonewline $G(fd) $G(hdr)
}

proc fts5c_printfile {zIn} {
  global G
  set data [readfile $zIn]
  puts $G(fd) "#line 1 \"[file tail $zIn]\""
  foreach line [split $data "\n"] {
    if {[regexp {^#include.*fts5} $line]} continue
    if {[regexp {^(const )?[a-zA-Z][a-zA-Z0-9]* [*]?sqlite3Fts5} $line]} {
      set line "static $line"
    }
    puts $G(fd) $line
  }
}

proc fts5c_close {} {
  global G
  if {$G(fd)!="stdout"} {
    close $G(fd)
  }
}


fts5c_init fts5.c
foreach f $G(src) { fts5c_printfile $f }
fts5c_close




