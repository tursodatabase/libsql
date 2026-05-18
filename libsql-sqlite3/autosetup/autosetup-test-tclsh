# A small Tcl script to verify that the chosen
# interpreter works. Sometimes we might e.g. pick up
# an interpreter for a different arch.
# Outputs the full path to the interpreter

if {[catch {info version} version] == 0} {
	# This is Jim Tcl
	if {$version >= 0.72} {
		# Ensure that regexp works
		regexp (a.*?) a
		puts [info nameofexecutable]
		exit 0
	}
} elseif {[catch {info tclversion} version] == 0} {
	if {$version >= 8.5 && ![string match 8.5a* [info patchlevel]]} {
		puts [info nameofexecutable]
		exit 0
	}
}
exit 1
