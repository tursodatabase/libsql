# Read the sqliteVdbe.h file and generate a table of opcode names.
#
BEGIN {
  printf "static char *zOpName[] = { 0,\n"
  n = 0
}
/^#define OP_MAX/ {
  next
}
/^#define OP_/ {
  name = "\"" substr($2,4) "\","
  if( n<3 ){
    printf "  %-16s", name
    n++
  } else {
    printf "  %s\n", name
    n = 0
  }
}
END {
  if( n ){ printf "\n" }
  printf "};\n"
}
