#ifndef FORCEINLINE_H_
#define FORCEINLINE_H_

/* define forceinline macro */
#ifdef _MSC_VER
#define forceinline __forceinline
#elif defined(__GNUC__)
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 199901L
#define forceinline inline __attribute__((__always_inline__))
#else
#define forceinline static inline
#endif
#elif defined(__CLANG__)
#if __has_attribute(__always_inline__)
#define forceinline inline __attribute__((__always_inline__))
#else
#define forceinline inline
#endif
#else
#define forceinline inline
#endif

#endif /* FORCEINLINE_H_ */
