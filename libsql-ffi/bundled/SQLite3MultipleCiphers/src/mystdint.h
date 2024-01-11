#ifndef MY_STDINT_H_
#define MY_STDINT_H_

/*
** MS Visual C++ 2008 and below do not provide the header file <stdint.h>
** That is, we need to define the necessary types ourselves
*/

#if defined(_MSC_VER) && (_MSC_VER < 1600)
typedef signed char int8_t;
typedef short int16_t;
typedef int int32_t;
typedef long long int64_t;
typedef unsigned char uint8_t;
typedef unsigned short uint16_t;
typedef unsigned int uint32_t;
typedef unsigned long long uint64_t;

#define UINT8_MAX 255
#define UINT16_MAX 65535
#define UINT32_MAX 0xffffffffU  /* 4294967295U */
#define UINT64_MAX 0xffffffffffffffffULL /* 18446744073709551615ULL */ 
#else
#include <stdint.h>
#endif

#endif /* MY_STDINT_H_ */
