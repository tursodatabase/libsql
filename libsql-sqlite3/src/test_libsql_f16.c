/*
 * GENERATE: python3 test_libsql_f16.py > test_libsql_f16_table.h
 * BUILD: cc test_libsql_f16.c -I ../ -L ../.libs -llibsql -lm -o test_libsql_f16
 * RUN:   LD_LIBRARY_PATH=../.libs ./test_libsql_diskann
*/

#include "assert.h"
#include "stdbool.h"
#include "stdarg.h"
#include "stddef.h"
#include "vectorfloat16.c"
#include "test_libsql_f16_table.h"

#define eprintf(...) fprintf(stderr, __VA_ARGS__)
#define ensure(condition, ...) { if (!(condition)) { eprintf(__VA_ARGS__); exit(1); } }

int main() {
  for(int i = 0; i < 65536; i++){
    u32 expected = F16ToF32[i];
    float actual = vectorF16ToFloat(i);
    u32 actual_u32 = *((u32*)&actual);
    ensure(expected == actual_u32, "conversion from %x failed: %f != %f (%x != %x)", i, *(float*)&expected, *(float*)&actual_u32, expected, actual_u32);
  }
  for(int i = 0; i < 65536; i++){
    u16 expected = F32ToF16[i];
    u16 actual = vectorF16FromFloat(*(float*)&F32[i]);
    ensure(expected == actual, "conversion from %x (%f, it=%d) failed: %x != %x", F32[i], *(float*)&F32[i], i, expected, actual);
  }
}
