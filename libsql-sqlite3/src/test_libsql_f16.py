import random
import struct
import numpy as np

u32_list = [random.randint(0, 2**32) for _ in range(65536)]

print("""
u32 F32[65536] = {
""")
for i, x in enumerate(u32_list):
    if i % 8 == 0: print("    ", end='');
    print('{:>10}u, '.format(x), end='')
    if i % 8 == 7: print()
print("};")


print("""
u16 F32ToF16[65536] = {
""")
for i, x in enumerate(u32_list):
    if i % 8 == 0: print("    ", end='');
    u32_bytes = struct.pack('<I', x)
    f32 = np.float16(struct.unpack('<f', u32_bytes)[0])
    f16_bytes = struct.pack('<e', f32)
    u16 = struct.unpack('<H', f16_bytes)[0]
    print('{:>10}, '.format(u16), end='')
    if i % 8 == 7: print()
print("};")

print("""
u32 F16ToF32[65536] = {
""")

for x in range(65536):
    if x % 8 == 0: print("    ", end='');
    u16_bytes = struct.pack('<H', x)
    f16 = struct.unpack('<e', u16_bytes)[0]
    f32_bytes = struct.pack('<f', f16)
    u32 = struct.unpack('<I', f32_bytes)[0]
    print('{:>10}u, '.format(u32), end='')
    if x % 8 == 7: print()
print("};")
