#
# This script looks for memory leaks by analyzing the output of "sqlite" 
# when compiled with the MEMORY_DEBUG=2 option.
#
/^malloc / {
  mem[$5] = $0
}
/^realloc / {
  mem[$7] = "";
  mem[$9] = $0
}
/^free / {
  mem[$5] = "";
}
/^string at / {
  addr = $3
  sub("string at " addr " is ","")
  str[addr] = $0
}
END {
  for(addr in mem){
    if( mem[addr]=="" ) continue
    print mem[addr], str[addr]
  }
}
