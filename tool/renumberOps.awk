# Run this script on sqliteVdbe.h to renumber the opcodes sequentially.
#
BEGIN { cnt = 1 }
/^#define OP_MAX/ {
  printf "#define %-20s %3d\n",$2, cnt-1
  next
}
/^#define OP_/ {
  printf "#define %-20s %3d\n",$2, cnt++
  next
}
{ print }
