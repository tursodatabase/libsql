
set sizes {1024 2048 4096 8192 16384 32768}
set fmt { %-8s}

puts -nonewline "page size: "
foreach s $sizes {
  puts -nonewline [format $fmt $s]
}
puts ""

puts -nonewline "on leaf:   "
foreach s $sizes {
  set x [expr {$s - 18*4}]
  set p($s) $x
  puts -nonewline [format $fmt $x]
}
puts ""

puts -nonewline "direct:    "
foreach s $sizes {
  set x [expr {$p($s) + 10*$s}]
  set p($s) $x
  puts -nonewline [format $fmt $x]
}
puts ""

puts -nonewline "indirect:  "
foreach s $sizes {
  set x [expr {$p($s) + ($s/4.0)*$s}]
  set p($s) $x
  puts -nonewline [format $fmt $x]
}
puts ""

puts -nonewline "dbl indir: "
foreach s $sizes {
  set x [expr {$p($s) + ($s/4.0)*($s/4)*$s}]
  set p($s) $x
  puts -nonewline [format $fmt $x]
}
puts ""
