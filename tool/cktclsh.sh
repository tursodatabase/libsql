# Fail with an error if the TCLSH named in $1 is not tclsh 8.5 or later.
#
echo 'if {$tcl_version<"8.5"} {exit 1}' >cktclsh.tcl
if ! $1 cktclsh.tcl
then
   echo 'ERROR: This makefile target requires tclsh 8.5 or later.'
   exit 1
fi
