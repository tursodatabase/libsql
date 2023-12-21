#ifndef CONFIG_H_
#define CONFIG_H_

/* inline the ascon mode */
#ifndef ASCON_INLINE_MODE
#define ASCON_INLINE_MODE 1
#endif

/* inline all permutations */
#ifndef ASCON_INLINE_PERM
#define ASCON_INLINE_PERM 1
#endif

/* unroll permutation loops */
#ifndef ASCON_UNROLL_LOOPS
#define ASCON_UNROLL_LOOPS 1
#endif

#endif /* CONFIG_H_ */
