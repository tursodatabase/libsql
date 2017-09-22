/*
** 2017-03-08
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
******************************************************************************
**
** This SQLite extension implements a functions that compute SHA1 hashes.
** Two SQL functions are implemented:
**
**     sha3(X,SIZE)
**     sha3_query(Y,SIZE)
**
** The sha3(X) function computes the SHA3 hash of the input X, or NULL if
** X is NULL.
**
** The sha3_query(Y) function evalutes all queries in the SQL statements of Y
** and returns a hash of their results.
**
** The SIZE argument is optional.  If omitted, the SHA3-256 hash algorithm
** is used.  If SIZE is included it must be one of the integers 224, 256,
** 384, or 512, to determine SHA3 hash variant that is computed.
*/
#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <assert.h>
#include <string.h>
#include <stdarg.h>
typedef sqlite3_uint64 u64;

/******************************************************************************
** The Hash Engine
*/
/*
** Macros to determine whether the machine is big or little endian,
** and whether or not that determination is run-time or compile-time.
**
** For best performance, an attempt is made to guess at the byte-order
** using C-preprocessor macros.  If that is unsuccessful, or if
** -DSHA3_BYTEORDER=0 is set, then byte-order is determined
** at run-time.
*/
#ifndef SHA3_BYTEORDER
# if defined(i386)     || defined(__i386__)   || defined(_M_IX86) ||    \
     defined(__x86_64) || defined(__x86_64__) || defined(_M_X64)  ||    \
     defined(_M_AMD64) || defined(_M_ARM)     || defined(__x86)   ||    \
     defined(__arm__)
#   define SHA3_BYTEORDER    1234
# elif defined(sparc)    || defined(__ppc__)
#   define SHA3_BYTEORDER    4321
# else
#   define SHA3_BYTEORDER 0
# endif
#endif


/*
** State structure for a SHA3 hash in progress
*/
typedef struct SHA3Context SHA3Context;
struct SHA3Context {
  union {
    u64 s[25];                /* Keccak state. 5x5 lines of 64 bits each */
    unsigned char x[1600];    /* ... or 1600 bytes */
  } u;
  unsigned nRate;        /* Bytes of input accepted per Keccak iteration */
  unsigned nLoaded;      /* Input bytes loaded into u.x[] so far this cycle */
  unsigned ixMask;       /* Insert next input into u.x[nLoaded^ixMask]. */
};

/*
** A single step of the Keccak mixing function for a 1600-bit state
*/
static void KeccakF1600Step(SHA3Context *p){
  int i;
  u64 B0, B1, B2, B3, B4;
  u64 C0, C1, C2, C3, C4;
  u64 D0, D1, D2, D3, D4;
  static const u64 RC[] = {
    0x0000000000000001ULL,  0x0000000000008082ULL,
    0x800000000000808aULL,  0x8000000080008000ULL,
    0x000000000000808bULL,  0x0000000080000001ULL,
    0x8000000080008081ULL,  0x8000000000008009ULL,
    0x000000000000008aULL,  0x0000000000000088ULL,
    0x0000000080008009ULL,  0x000000008000000aULL,
    0x000000008000808bULL,  0x800000000000008bULL,
    0x8000000000008089ULL,  0x8000000000008003ULL,
    0x8000000000008002ULL,  0x8000000000000080ULL,
    0x000000000000800aULL,  0x800000008000000aULL,
    0x8000000080008081ULL,  0x8000000000008080ULL,
    0x0000000080000001ULL,  0x8000000080008008ULL
  };
# define A00 (p->u.s[0])
# define A01 (p->u.s[1])
# define A02 (p->u.s[2])
# define A03 (p->u.s[3])
# define A04 (p->u.s[4])
# define A10 (p->u.s[5])
# define A11 (p->u.s[6])
# define A12 (p->u.s[7])
# define A13 (p->u.s[8])
# define A14 (p->u.s[9])
# define A20 (p->u.s[10])
# define A21 (p->u.s[11])
# define A22 (p->u.s[12])
# define A23 (p->u.s[13])
# define A24 (p->u.s[14])
# define A30 (p->u.s[15])
# define A31 (p->u.s[16])
# define A32 (p->u.s[17])
# define A33 (p->u.s[18])
# define A34 (p->u.s[19])
# define A40 (p->u.s[20])
# define A41 (p->u.s[21])
# define A42 (p->u.s[22])
# define A43 (p->u.s[23])
# define A44 (p->u.s[24])
# define ROL64(a,x) ((a<<x)|(a>>(64-x)))

  for(i=0; i<24; i+=4){
    C0 = A00^A10^A20^A30^A40;
    C1 = A01^A11^A21^A31^A41;
    C2 = A02^A12^A22^A32^A42;
    C3 = A03^A13^A23^A33^A43;
    C4 = A04^A14^A24^A34^A44;
    D0 = C4^ROL64(C1, 1);
    D1 = C0^ROL64(C2, 1);
    D2 = C1^ROL64(C3, 1);
    D3 = C2^ROL64(C4, 1);
    D4 = C3^ROL64(C0, 1);

    B0 = (A00^D0);
    B1 = ROL64((A11^D1), 44);
    B2 = ROL64((A22^D2), 43);
    B3 = ROL64((A33^D3), 21);
    B4 = ROL64((A44^D4), 14);
    A00 =   B0 ^((~B1)&  B2 );
    A00 ^= RC[i];
    A11 =   B1 ^((~B2)&  B3 );
    A22 =   B2 ^((~B3)&  B4 );
    A33 =   B3 ^((~B4)&  B0 );
    A44 =   B4 ^((~B0)&  B1 );

    B2 = ROL64((A20^D0), 3);
    B3 = ROL64((A31^D1), 45);
    B4 = ROL64((A42^D2), 61);
    B0 = ROL64((A03^D3), 28);
    B1 = ROL64((A14^D4), 20);
    A20 =   B0 ^((~B1)&  B2 );
    A31 =   B1 ^((~B2)&  B3 );
    A42 =   B2 ^((~B3)&  B4 );
    A03 =   B3 ^((~B4)&  B0 );
    A14 =   B4 ^((~B0)&  B1 );

    B4 = ROL64((A40^D0), 18);
    B0 = ROL64((A01^D1), 1);
    B1 = ROL64((A12^D2), 6);
    B2 = ROL64((A23^D3), 25);
    B3 = ROL64((A34^D4), 8);
    A40 =   B0 ^((~B1)&  B2 );
    A01 =   B1 ^((~B2)&  B3 );
    A12 =   B2 ^((~B3)&  B4 );
    A23 =   B3 ^((~B4)&  B0 );
    A34 =   B4 ^((~B0)&  B1 );

    B1 = ROL64((A10^D0), 36);
    B2 = ROL64((A21^D1), 10);
    B3 = ROL64((A32^D2), 15);
    B4 = ROL64((A43^D3), 56);
    B0 = ROL64((A04^D4), 27);
    A10 =   B0 ^((~B1)&  B2 );
    A21 =   B1 ^((~B2)&  B3 );
    A32 =   B2 ^((~B3)&  B4 );
    A43 =   B3 ^((~B4)&  B0 );
    A04 =   B4 ^((~B0)&  B1 );

    B3 = ROL64((A30^D0), 41);
    B4 = ROL64((A41^D1), 2);
    B0 = ROL64((A02^D2), 62);
    B1 = ROL64((A13^D3), 55);
    B2 = ROL64((A24^D4), 39);
    A30 =   B0 ^((~B1)&  B2 );
    A41 =   B1 ^((~B2)&  B3 );
    A02 =   B2 ^((~B3)&  B4 );
    A13 =   B3 ^((~B4)&  B0 );
    A24 =   B4 ^((~B0)&  B1 );

    C0 = A00^A20^A40^A10^A30;
    C1 = A11^A31^A01^A21^A41;
    C2 = A22^A42^A12^A32^A02;
    C3 = A33^A03^A23^A43^A13;
    C4 = A44^A14^A34^A04^A24;
    D0 = C4^ROL64(C1, 1);
    D1 = C0^ROL64(C2, 1);
    D2 = C1^ROL64(C3, 1);
    D3 = C2^ROL64(C4, 1);
    D4 = C3^ROL64(C0, 1);

    B0 = (A00^D0);
    B1 = ROL64((A31^D1), 44);
    B2 = ROL64((A12^D2), 43);
    B3 = ROL64((A43^D3), 21);
    B4 = ROL64((A24^D4), 14);
    A00 =   B0 ^((~B1)&  B2 );
    A00 ^= RC[i+1];
    A31 =   B1 ^((~B2)&  B3 );
    A12 =   B2 ^((~B3)&  B4 );
    A43 =   B3 ^((~B4)&  B0 );
    A24 =   B4 ^((~B0)&  B1 );

    B2 = ROL64((A40^D0), 3);
    B3 = ROL64((A21^D1), 45);
    B4 = ROL64((A02^D2), 61);
    B0 = ROL64((A33^D3), 28);
    B1 = ROL64((A14^D4), 20);
    A40 =   B0 ^((~B1)&  B2 );
    A21 =   B1 ^((~B2)&  B3 );
    A02 =   B2 ^((~B3)&  B4 );
    A33 =   B3 ^((~B4)&  B0 );
    A14 =   B4 ^((~B0)&  B1 );

    B4 = ROL64((A30^D0), 18);
    B0 = ROL64((A11^D1), 1);
    B1 = ROL64((A42^D2), 6);
    B2 = ROL64((A23^D3), 25);
    B3 = ROL64((A04^D4), 8);
    A30 =   B0 ^((~B1)&  B2 );
    A11 =   B1 ^((~B2)&  B3 );
    A42 =   B2 ^((~B3)&  B4 );
    A23 =   B3 ^((~B4)&  B0 );
    A04 =   B4 ^((~B0)&  B1 );

    B1 = ROL64((A20^D0), 36);
    B2 = ROL64((A01^D1), 10);
    B3 = ROL64((A32^D2), 15);
    B4 = ROL64((A13^D3), 56);
    B0 = ROL64((A44^D4), 27);
    A20 =   B0 ^((~B1)&  B2 );
    A01 =   B1 ^((~B2)&  B3 );
    A32 =   B2 ^((~B3)&  B4 );
    A13 =   B3 ^((~B4)&  B0 );
    A44 =   B4 ^((~B0)&  B1 );

    B3 = ROL64((A10^D0), 41);
    B4 = ROL64((A41^D1), 2);
    B0 = ROL64((A22^D2), 62);
    B1 = ROL64((A03^D3), 55);
    B2 = ROL64((A34^D4), 39);
    A10 =   B0 ^((~B1)&  B2 );
    A41 =   B1 ^((~B2)&  B3 );
    A22 =   B2 ^((~B3)&  B4 );
    A03 =   B3 ^((~B4)&  B0 );
    A34 =   B4 ^((~B0)&  B1 );

    C0 = A00^A40^A30^A20^A10;
    C1 = A31^A21^A11^A01^A41;
    C2 = A12^A02^A42^A32^A22;
    C3 = A43^A33^A23^A13^A03;
    C4 = A24^A14^A04^A44^A34;
    D0 = C4^ROL64(C1, 1);
    D1 = C0^ROL64(C2, 1);
    D2 = C1^ROL64(C3, 1);
    D3 = C2^ROL64(C4, 1);
    D4 = C3^ROL64(C0, 1);

    B0 = (A00^D0);
    B1 = ROL64((A21^D1), 44);
    B2 = ROL64((A42^D2), 43);
    B3 = ROL64((A13^D3), 21);
    B4 = ROL64((A34^D4), 14);
    A00 =   B0 ^((~B1)&  B2 );
    A00 ^= RC[i+2];
    A21 =   B1 ^((~B2)&  B3 );
    A42 =   B2 ^((~B3)&  B4 );
    A13 =   B3 ^((~B4)&  B0 );
    A34 =   B4 ^((~B0)&  B1 );

    B2 = ROL64((A30^D0), 3);
    B3 = ROL64((A01^D1), 45);
    B4 = ROL64((A22^D2), 61);
    B0 = ROL64((A43^D3), 28);
    B1 = ROL64((A14^D4), 20);
    A30 =   B0 ^((~B1)&  B2 );
    A01 =   B1 ^((~B2)&  B3 );
    A22 =   B2 ^((~B3)&  B4 );
    A43 =   B3 ^((~B4)&  B0 );
    A14 =   B4 ^((~B0)&  B1 );

    B4 = ROL64((A10^D0), 18);
    B0 = ROL64((A31^D1), 1);
    B1 = ROL64((A02^D2), 6);
    B2 = ROL64((A23^D3), 25);
    B3 = ROL64((A44^D4), 8);
    A10 =   B0 ^((~B1)&  B2 );
    A31 =   B1 ^((~B2)&  B3 );
    A02 =   B2 ^((~B3)&  B4 );
    A23 =   B3 ^((~B4)&  B0 );
    A44 =   B4 ^((~B0)&  B1 );

    B1 = ROL64((A40^D0), 36);
    B2 = ROL64((A11^D1), 10);
    B3 = ROL64((A32^D2), 15);
    B4 = ROL64((A03^D3), 56);
    B0 = ROL64((A24^D4), 27);
    A40 =   B0 ^((~B1)&  B2 );
    A11 =   B1 ^((~B2)&  B3 );
    A32 =   B2 ^((~B3)&  B4 );
    A03 =   B3 ^((~B4)&  B0 );
    A24 =   B4 ^((~B0)&  B1 );

    B3 = ROL64((A20^D0), 41);
    B4 = ROL64((A41^D1), 2);
    B0 = ROL64((A12^D2), 62);
    B1 = ROL64((A33^D3), 55);
    B2 = ROL64((A04^D4), 39);
    A20 =   B0 ^((~B1)&  B2 );
    A41 =   B1 ^((~B2)&  B3 );
    A12 =   B2 ^((~B3)&  B4 );
    A33 =   B3 ^((~B4)&  B0 );
    A04 =   B4 ^((~B0)&  B1 );

    C0 = A00^A30^A10^A40^A20;
    C1 = A21^A01^A31^A11^A41;
    C2 = A42^A22^A02^A32^A12;
    C3 = A13^A43^A23^A03^A33;
    C4 = A34^A14^A44^A24^A04;
    D0 = C4^ROL64(C1, 1);
    D1 = C0^ROL64(C2, 1);
    D2 = C1^ROL64(C3, 1);
    D3 = C2^ROL64(C4, 1);
    D4 = C3^ROL64(C0, 1);

    B0 = (A00^D0);
    B1 = ROL64((A01^D1), 44);
    B2 = ROL64((A02^D2), 43);
    B3 = ROL64((A03^D3), 21);
    B4 = ROL64((A04^D4), 14);
    A00 =   B0 ^((~B1)&  B2 );
    A00 ^= RC[i+3];
    A01 =   B1 ^((~B2)&  B3 );
    A02 =   B2 ^((~B3)&  B4 );
    A03 =   B3 ^((~B4)&  B0 );
    A04 =   B4 ^((~B0)&  B1 );

    B2 = ROL64((A10^D0), 3);
    B3 = ROL64((A11^D1), 45);
    B4 = ROL64((A12^D2), 61);
    B0 = ROL64((A13^D3), 28);
    B1 = ROL64((A14^D4), 20);
    A10 =   B0 ^((~B1)&  B2 );
    A11 =   B1 ^((~B2)&  B3 );
    A12 =   B2 ^((~B3)&  B4 );
    A13 =   B3 ^((~B4)&  B0 );
    A14 =   B4 ^((~B0)&  B1 );

    B4 = ROL64((A20^D0), 18);
    B0 = ROL64((A21^D1), 1);
    B1 = ROL64((A22^D2), 6);
    B2 = ROL64((A23^D3), 25);
    B3 = ROL64((A24^D4), 8);
    A20 =   B0 ^((~B1)&  B2 );
    A21 =   B1 ^((~B2)&  B3 );
    A22 =   B2 ^((~B3)&  B4 );
    A23 =   B3 ^((~B4)&  B0 );
    A24 =   B4 ^((~B0)&  B1 );

    B1 = ROL64((A30^D0), 36);
    B2 = ROL64((A31^D1), 10);
    B3 = ROL64((A32^D2), 15);
    B4 = ROL64((A33^D3), 56);
    B0 = ROL64((A34^D4), 27);
    A30 =   B0 ^((~B1)&  B2 );
    A31 =   B1 ^((~B2)&  B3 );
    A32 =   B2 ^((~B3)&  B4 );
    A33 =   B3 ^((~B4)&  B0 );
    A34 =   B4 ^((~B0)&  B1 );

    B3 = ROL64((A40^D0), 41);
    B4 = ROL64((A41^D1), 2);
    B0 = ROL64((A42^D2), 62);
    B1 = ROL64((A43^D3), 55);
    B2 = ROL64((A44^D4), 39);
    A40 =   B0 ^((~B1)&  B2 );
    A41 =   B1 ^((~B2)&  B3 );
    A42 =   B2 ^((~B3)&  B4 );
    A43 =   B3 ^((~B4)&  B0 );
    A44 =   B4 ^((~B0)&  B1 );
  }
}

/*
** Initialize a new hash.  iSize determines the size of the hash
** in bits and should be one of 224, 256, 384, or 512.  Or iSize
** can be zero to use the default hash size of 256 bits.
*/
static void SHA3Init(SHA3Context *p, int iSize){
  memset(p, 0, sizeof(*p));
  if( iSize>=128 && iSize<=512 ){
    p->nRate = (1600 - ((iSize + 31)&~31)*2)/8;
  }else{
    p->nRate = (1600 - 2*256)/8;
  }
#if SHA3_BYTEORDER==1234
  /* Known to be little-endian at compile-time. No-op */
#elif SHA3_BYTEORDER==4321
  p->ixMask = 7;  /* Big-endian */
#else
  {
    static unsigned int one = 1;
    if( 1==*(unsigned char*)&one ){
      /* Little endian.  No byte swapping. */
      p->ixMask = 0;
    }else{
      /* Big endian.  Byte swap. */
      p->ixMask = 7;
    }
  }
#endif
}

/*
** Make consecutive calls to the SHA3Update function to add new content
** to the hash
*/
static void SHA3Update(
  SHA3Context *p,
  const unsigned char *aData,
  unsigned int nData
){
  unsigned int i = 0;
#if SHA3_BYTEORDER==1234
  if( (p->nLoaded % 8)==0 && ((aData - (const unsigned char*)0)&7)==0 ){
    for(; i+7<nData; i+=8){
      p->u.s[p->nLoaded/8] ^= *(u64*)&aData[i];
      p->nLoaded += 8;
      if( p->nLoaded>=p->nRate ){
        KeccakF1600Step(p);
        p->nLoaded = 0;
      }
    }
  }
#endif
  for(; i<nData; i++){
#if SHA3_BYTEORDER==1234
    p->u.x[p->nLoaded] ^= aData[i];
#elif SHA3_BYTEORDER==4321
    p->u.x[p->nLoaded^0x07] ^= aData[i];
#else
    p->u.x[p->nLoaded^p->ixMask] ^= aData[i];
#endif
    p->nLoaded++;
    if( p->nLoaded==p->nRate ){
      KeccakF1600Step(p);
      p->nLoaded = 0;
    }
  }
}

/*
** After all content has been added, invoke SHA3Final() to compute
** the final hash.  The function returns a pointer to the binary
** hash value.
*/
static unsigned char *SHA3Final(SHA3Context *p){
  unsigned int i;
  if( p->nLoaded==p->nRate-1 ){
    const unsigned char c1 = 0x86;
    SHA3Update(p, &c1, 1);
  }else{
    const unsigned char c2 = 0x06;
    const unsigned char c3 = 0x80;
    SHA3Update(p, &c2, 1);
    p->nLoaded = p->nRate - 1;
    SHA3Update(p, &c3, 1);
  }
  for(i=0; i<p->nRate; i++){
    p->u.x[i+p->nRate] = p->u.x[i^p->ixMask];
  }
  return &p->u.x[p->nRate];
}
/* End of the hashing logic
*****************************************************************************/

/*
** Implementation of the sha3(X,SIZE) function.
**
** Return a BLOB which is the SIZE-bit SHA3 hash of X.  The default
** size is 256.  If X is a BLOB, it is hashed as is.  
** For all other non-NULL types of input, X is converted into a UTF-8 string
** and the string is hashed without the trailing 0x00 terminator.  The hash
** of a NULL value is NULL.
*/
static void sha3Func(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  SHA3Context cx;
  int eType = sqlite3_value_type(argv[0]);
  int nByte = sqlite3_value_bytes(argv[0]);
  int iSize;
  if( argc==1 ){
    iSize = 256;
  }else{
    iSize = sqlite3_value_int(argv[1]);
    if( iSize!=224 && iSize!=256 && iSize!=384 && iSize!=512 ){
      sqlite3_result_error(context, "SHA3 size should be one of: 224 256 "
                                    "384 512", -1);
      return;
    }
  }
  if( eType==SQLITE_NULL ) return;
  SHA3Init(&cx, iSize);
  if( eType==SQLITE_BLOB ){
    SHA3Update(&cx, sqlite3_value_blob(argv[0]), nByte);
  }else{
    SHA3Update(&cx, sqlite3_value_text(argv[0]), nByte);
  }
  sqlite3_result_blob(context, SHA3Final(&cx), iSize/8, SQLITE_TRANSIENT);
}

/* Compute a string using sqlite3_vsnprintf() with a maximum length
** of 50 bytes and add it to the hash.
*/
static void hash_step_vformat(
  SHA3Context *p,                 /* Add content to this context */
  const char *zFormat,
  ...
){
  va_list ap;
  int n;
  char zBuf[50];
  va_start(ap, zFormat);
  sqlite3_vsnprintf(sizeof(zBuf),zBuf,zFormat,ap);
  va_end(ap);
  n = (int)strlen(zBuf);
  SHA3Update(p, (unsigned char*)zBuf, n);
}

/*
** Implementation of the sha3_query(SQL,SIZE) function.
**
** This function compiles and runs the SQL statement(s) given in the
** argument. The results are hashed using a SIZE-bit SHA3.  The default
** size is 256.
**
** The format of the byte stream that is hashed is summarized as follows:
**
**       S<n>:<sql>
**       R
**       N
**       I<int>
**       F<ieee-float>
**       B<size>:<bytes>
**       T<size>:<text>
**
** <sql> is the original SQL text for each statement run and <n> is
** the size of that text.  The SQL text is UTF-8.  A single R character
** occurs before the start of each row.  N means a NULL value.
** I mean an 8-byte little-endian integer <int>.  F is a floating point
** number with an 8-byte little-endian IEEE floating point value <ieee-float>.
** B means blobs of <size> bytes.  T means text rendered as <size>
** bytes of UTF-8.  The <n> and <size> values are expressed as an ASCII
** text integers.
**
** For each SQL statement in the X input, there is one S segment.  Each
** S segment is followed by zero or more R segments, one for each row in the
** result set.  After each R, there are one or more N, I, F, B, or T segments,
** one for each column in the result set.  Segments are concatentated directly
** with no delimiters of any kind.
*/
static void sha3QueryFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(context);
  const char *zSql = (const char*)sqlite3_value_text(argv[0]);
  sqlite3_stmt *pStmt = 0;
  int nCol;                   /* Number of columns in the result set */
  int i;                      /* Loop counter */
  int rc;
  int n;
  const char *z;
  SHA3Context cx;
  int iSize;

  if( argc==1 ){
    iSize = 256;
  }else{
    iSize = sqlite3_value_int(argv[1]);
    if( iSize!=224 && iSize!=256 && iSize!=384 && iSize!=512 ){
      sqlite3_result_error(context, "SHA3 size should be one of: 224 256 "
                                    "384 512", -1);
      return;
    }
  }
  if( zSql==0 ) return;
  SHA3Init(&cx, iSize);
  while( zSql[0] ){
    rc = sqlite3_prepare_v2(db, zSql, -1, &pStmt, &zSql);
    if( rc ){
      char *zMsg = sqlite3_mprintf("error SQL statement [%s]: %s",
                                   zSql, sqlite3_errmsg(db));
      sqlite3_finalize(pStmt);
      sqlite3_result_error(context, zMsg, -1);
      sqlite3_free(zMsg);
      return;
    }
    if( !sqlite3_stmt_readonly(pStmt) ){
      char *zMsg = sqlite3_mprintf("non-query: [%s]", sqlite3_sql(pStmt));
      sqlite3_finalize(pStmt);
      sqlite3_result_error(context, zMsg, -1);
      sqlite3_free(zMsg);
      return;
    }
    nCol = sqlite3_column_count(pStmt);
    z = sqlite3_sql(pStmt);
    n = (int)strlen(z);
    hash_step_vformat(&cx,"S%d:",n);
    SHA3Update(&cx,(unsigned char*)z,n);

    /* Compute a hash over the result of the query */
    while( SQLITE_ROW==sqlite3_step(pStmt) ){
      SHA3Update(&cx,(const unsigned char*)"R",1);
      for(i=0; i<nCol; i++){
        switch( sqlite3_column_type(pStmt,i) ){
          case SQLITE_NULL: {
            SHA3Update(&cx, (const unsigned char*)"N",1);
            break;
          }
          case SQLITE_INTEGER: {
            sqlite3_uint64 u;
            int j;
            unsigned char x[9];
            sqlite3_int64 v = sqlite3_column_int64(pStmt,i);
            memcpy(&u, &v, 8);
            for(j=8; j>=1; j--){
              x[j] = u & 0xff;
              u >>= 8;
            }
            x[0] = 'I';
            SHA3Update(&cx, x, 9);
            break;
          }
          case SQLITE_FLOAT: {
            sqlite3_uint64 u;
            int j;
            unsigned char x[9];
            double r = sqlite3_column_double(pStmt,i);
            memcpy(&u, &r, 8);
            for(j=8; j>=1; j--){
              x[j] = u & 0xff;
              u >>= 8;
            }
            x[0] = 'F';
            SHA3Update(&cx,x,9);
            break;
          }
          case SQLITE_TEXT: {
            int n2 = sqlite3_column_bytes(pStmt, i);
            const unsigned char *z2 = sqlite3_column_text(pStmt, i);
            hash_step_vformat(&cx,"T%d:",n2);
            SHA3Update(&cx, z2, n2);
            break;
          }
          case SQLITE_BLOB: {
            int n2 = sqlite3_column_bytes(pStmt, i);
            const unsigned char *z2 = sqlite3_column_blob(pStmt, i);
            hash_step_vformat(&cx,"B%d:",n2);
            SHA3Update(&cx, z2, n2);
            break;
          }
        }
      }
    }
    sqlite3_finalize(pStmt);
  }
  sqlite3_result_blob(context, SHA3Final(&cx), iSize/8, SQLITE_TRANSIENT);
}


#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_shathree_init(
  sqlite3 *db,
  char **pzErrMsg,
  const sqlite3_api_routines *pApi
){
  int rc = SQLITE_OK;
  SQLITE_EXTENSION_INIT2(pApi);
  (void)pzErrMsg;  /* Unused parameter */
  rc = sqlite3_create_function(db, "sha3", 1, SQLITE_UTF8, 0,
                               sha3Func, 0, 0);
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "sha3", 2, SQLITE_UTF8, 0,
                                 sha3Func, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "sha3_query", 1, SQLITE_UTF8, 0,
                                 sha3QueryFunc, 0, 0);
  }
  if( rc==SQLITE_OK ){
    rc = sqlite3_create_function(db, "sha3_query", 2, SQLITE_UTF8, 0,
                                 sha3QueryFunc, 0, 0);
  }
  return rc;
}
