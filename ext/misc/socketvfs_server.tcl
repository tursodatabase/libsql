#!/bin/sh
# \
exec tclsh "$0" ${1+"$@"}

if {[llength $argv]!=2} {
  puts stderr "Usage: $argv0 <filename> <port>"
  exit -1
}
set G(filename) [lindex $argv 0]
set G(port) [lindex $argv 1]

proc new_message {chan} {
  global G
  if {[eof $chan]} {
    close $chan
    puts "Close channel $chan"
  } else {
    set msg [read $chan 16]
    if {[string length $msg]>0} {
      binary scan $msg WW offset amt
      # puts "Request from $chan for $amt bytes at offset $offset"
      seek $G(fd) $offset
      set data [read $G(fd) $amt]
      puts -nonewline $chan $data
      flush $chan
    }
  }
}

proc new_connection {chan addr port} {
  global G

  set sz [file size $G(filename)]

  puts -nonewline "$addr:$port connects! "
  puts "Sending file size ($sz) as a 64-bit big-endian integer."
  set bin [binary format W $sz]
  puts -nonewline $chan $bin
  flush $chan

  fconfigure $chan -encoding binary
  fconfigure $chan -translation binary
  fileevent $chan readable [list new_message $chan]
}  

set G(fd) [open $G(filename) r]
fconfigure $G(fd) -encoding binary
fconfigure $G(fd) -translation binary

socket -server new_connection $G(port)
vwait forever

