# Script to build ARM64-EC binaries on Windows. 
#
# Run from the top-level of a check-out, on a Windows11 ARM machine that
# has Fossil installed, using a command like this:
#
#    tclsh tool/mmkwinarm64ec.tcl
#
# Output will be a file named:
#
#    sqlite-win-arm64ec-3MMPP00.zip
#
# Where MM is the minor version number and PP is that patch level.
#
puts "Running nmake..."
flush stdout
exec nmake /f Makefile.msc "PLATFORM=ARM64EC" "BCC=cl -nologo -arm64EC" "TCC=cl -nologo -arm64EC -I." "CFLAGS=-nologo -arm64EC -I." clean sqlite3.exe sqldiff.exe sqlite3_rsync.exe sqlite3.dll >@ stdout 2>@ stderr

proc check-type {file} {
  set in [open "| link /dump /headers $file" rb]
  set txt [read $in]
  close $in
  if {![string match {*8664 machine (x64) (ARM64X)*} $txt]} {
    puts "$file is not an ARM64-EC binary"
    puts "OUTPUT:\n$txt"
    flush stdout
    exit 1
  }
  puts "Confirmed: $file is an ARM64-EC binary"
}
check-type sqlite3.exe
check-type sqldiff.exe
check-type sqlite3_rsync.exe
check-type sqlite3.dll

set in [open VERSION rb]
set VERSION [read $in]
close $in
regexp {3.(\d+).(\d+)} $VERSION all minor patch
set filename [format sqlite-win-arm64ec-3%02d%02d00.zip $minor $patch]

puts "Constructing $filename..."
file delete -force $filename
exec fossil test-filezip $filename sqlite3.def sqlite3.dll sqlite3.exe sqldiff.exe sqlite3_rsync.exe >@ stdout 2>@ stderr
puts "$filename: [file size $filename] bytes"
exec fossil test-filezip -l $filename >@ stdout 2>@ stderr
