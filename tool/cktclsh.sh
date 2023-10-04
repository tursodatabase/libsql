# Fail with an error if the TCLSH named in $2 is not tclsh version $1 or later.
#
echo "set vers $1" >cktclsh$1.tcl
echo 'if {$tcl_version<$vers} {exit 1}' >>cktclsh$1.tcl
if ! $2 cktclsh$1.tcl
then
   echo "ERROR: This makefile target requires tclsh $1 or later."
   rm cktclsh$1.tcl
   exit 1
fi
rm cktclsh$1.tcl
