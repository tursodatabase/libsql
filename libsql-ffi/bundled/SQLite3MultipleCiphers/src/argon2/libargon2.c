#ifndef ARGON2_API
#define ARGON2_API static
#endif

#ifndef ARGON2_PRIVATE
#define ARGON2_PRIVATE static
#endif

#ifndef ARGON2_PUBLIC
#define ARGON2_PUBLIC static
#endif

#ifndef ARGON2_LOCAL
#define ARGON2_LOCAL static
#endif

#ifndef BLAKE2B_API
#define BLAKE2B_API static
#endif

#include "src/blake2/blake2b.c"
#include "src/argon2.c"
#include "src/core.c"
#include "src/encoding.c"
#include "src/ref.c"
#include "src/thread.c"
