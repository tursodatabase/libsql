#ifndef ENDIAN_H_
#define ENDIAN_H_

#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__

/* macros for big endian machines */
#ifdef PRAGMA_ENDIAN
#pragma message("Using macros for big endian machines")
#endif
#define ASCON_U64BIG(x) (x)
#define ASCON_U32BIG(x) (x)
#define ASCON_U16BIG(x) (x)

#elif defined(_MSC_VER) || \
    (defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)

/* macros for little endian machines */
#ifdef PRAGMA_ENDIAN
#pragma message("Using macros for little endian machines")
#endif
#define ASCON_U64BIG(x)                          \
  (((0x00000000000000FFULL & (x)) << 56) | \
   ((0x000000000000FF00ULL & (x)) << 40) | \
   ((0x0000000000FF0000ULL & (x)) << 24) | \
   ((0x00000000FF000000ULL & (x)) << 8) |  \
   ((0x000000FF00000000ULL & (x)) >> 8) |  \
   ((0x0000FF0000000000ULL & (x)) >> 24) | \
   ((0x00FF000000000000ULL & (x)) >> 40) | \
   ((0xFF00000000000000ULL & (x)) >> 56))
#define ASCON_U32BIG(x)                                           \
  (((0x000000FF & (x)) << 24) | ((0x0000FF00 & (x)) << 8) | \
   ((0x00FF0000 & (x)) >> 8) | ((0xFF000000 & (x)) >> 24))
#define ASCON_U16BIG(x) (((0x00FF & (x)) << 8) | ((0xFF00 & (x)) >> 8))

#else
#error "Ascon byte order macros not defined in bendian.h"
#endif

#endif /* ENDIAN_H_ */
