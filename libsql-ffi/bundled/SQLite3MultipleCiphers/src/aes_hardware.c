/*
** Name:        aes_hardware.c
** Purpose:     AES algorithms based on AES NI
** Author:      Ulrich Telle
** Created:     2020-12-01
** Copyright:   (c) 2020 Ulrich Telle
** License:     MIT
*/

/*
** Check whether the platform offers hardware support for AES
*/

#define AES_HARDWARE_NONE  0
#define AES_HARDWARE_NI    1
#define AES_HARDWARE_NEON  2

#ifndef SQLITE3MC_OMIT_AES_HARDWARE_SUPPORT

#if defined __ARM_FEATURE_CRYPTO
#define HAS_AES_HARDWARE AES_HARDWARE_NEON


/* --- CLang --- */
#elif defined(__clang__)

#if __has_attribute(target) && __has_include(<wmmintrin.h>) && (defined(__x86_64__) || defined(__i386))
#define HAS_AES_HARDWARE AES_HARDWARE_NI

#elif __has_attribute(target) && __has_include(<arm_neon.h>) && (defined(__aarch64__))
#define HAS_AES_HARDWARE AES_HARDWARE_NEON

/* Crypto extension in AArch64 can be enabled using __attribute__((target)) */
#define USE_CLANG_ATTR_TARGET_AARCH64

#endif


/* --- GNU C/C++ */
#elif defined(__GNUC__)

#if (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 4)) && (defined(__x86_64__) || defined(__i386))
#define HAS_AES_HARDWARE AES_HARDWARE_NI
#endif


/* --- Visual C/C++ --- */
#elif defined (_MSC_VER)

/* Architecture: x86 or x86_64 */
#if (defined(_M_X64) || defined(_M_IX86)) && _MSC_FULL_VER >= 150030729
#define HAS_AES_HARDWARE AES_HARDWARE_NI

/* Architecture: ARM 64-bit */
#elif defined(_M_ARM64)
#define HAS_AES_HARDWARE AES_HARDWARE_NEON

/* Use header <arm64_neon.h> instead of <arm_neon.h> */
#define USE_ARM64_NEON_H

/* Architecture: ARM 32-bit */
#elif defined _M_ARM
#define HAS_AES_HARDWARE AES_HARDWARE_NEON

/* The following #define is required to enable intrinsic definitions
   that do not omit one of the parameters for vaes[ed]q_u8 */
#define _ARM_USE_NEW_NEON_INTRINSICS

#endif

#else

#define HAS_AES_HARDWARE AES_HARDWARE_NONE

#endif

#else /* SQLITE3MC_OMIT_AES_HARDWARE_SUPPORT defined */

/* Omit AES hardware support */
#define HAS_AES_HARDWARE AES_HARDWARE_NONE

#endif /* SQLITE3MC_OMIT_AES_HARDWARE_SUPPORT */


#if HAS_AES_HARDWARE != AES_HARDWARE_NONE
/* --- Implementation of common data and functions for any AES hardware --- */

/* The first few powers of X in GF(2^8), used during key setup */
static const uint8_t gKeySetupRoundConstants[] =
{
  0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x1b, 0x36,
};

static inline uint32_t
toUint32FromLE(const void* buffer)
{
  const uint8_t* p = (const uint8_t*) buffer;
  return (((uint32_t) p[0])       | ((uint32_t) p[1] << 8) |
          ((uint32_t) p[2] << 16) | ((uint32_t) p[3] << 24));
}

#endif

#if HAS_AES_HARDWARE == AES_HARDWARE_NI
/* --- Implementation for AES-NI --- */

/*
** Define function for detecting hardware AES support at runtime
*/

#if defined(__clang__) || defined(__GNUC__)
/* Compiler CLang or GCC */

#include <cpuid.h>

static int
aesHardwareCheck()
{
  unsigned int cpuInfo[4];
  __cpuid(1, cpuInfo[0], cpuInfo[1], cpuInfo[2], cpuInfo[3]);
  /* Check AES and SSE4.1 */
  return (cpuInfo[2] & (1 << 25)) != 0 && (cpuInfo[2] & (1 << 19)) != 0;
}

#else /* !(defined(__clang__) || defined(__GNUC__)) */
/* Compiler Visual C++ */

#include <intrin.h>

static int
aesHardwareCheck()
{
  unsigned int CPUInfo[4];
  __cpuid((int*) CPUInfo, 1);
  return (CPUInfo[2] & (1 << 25)) != 0 && (CPUInfo[2] & (1 << 19)) != 0; /* Check AES and SSE4.1 */
}

#endif /* defined(__clang__) || defined(__GNUC__) */

#include <wmmintrin.h>
#include <smmintrin.h>

static int
aesGenKeyEncryptInternal(const unsigned char* userKey, const int bits, __m128i* keyData)
{
  int rc = 0;
  int i;
  int j;
  int numberOfRounds = (bits == 128) ? 10 : (bits == 192) ? 12 : (bits == 256) ? 14 : 0;
  int keyWords = bits / 32;
  int schedWords = (numberOfRounds + 1) * 4;

  /*
  ** Key schedule stored as 32-bit integers during expansion.
  ** Final __m128i form produced in the last step.
  */
  uint32_t sched[(_MAX_ROUNDS + 1) * 4];

  unsigned roundConstantPos = 0;

  for (i = 0; i < schedWords; i++)
  {
    if (i < keyWords)
    {
      sched[i] = toUint32FromLE(userKey + 4 * i);
    }
    else
    {
      uint32_t temp = sched[i - 1];

      int rotateAndRoundConstant = (i % keyWords == 0);
      int subOnly = (keyWords == 8 && i % 8 == 4);

      if (rotateAndRoundConstant)
      {
        temp = _mm_extract_epi32(_mm_aeskeygenassist_si128(_mm_setr_epi32(0, temp, 0, 0), 0), 1);
        temp ^= gKeySetupRoundConstants[roundConstantPos++];
      }
      else if (subOnly)
      {
        temp = _mm_extract_epi32(_mm_aeskeygenassist_si128(_mm_setr_epi32(0, temp, 0, 0), 0), 0);
      }

      sched[i] = sched[i - keyWords] ^ temp;
    }
  }

  /* Convert key schedule words into __m128i vectors */
  for (j = 0; j <= numberOfRounds; j++)
  {
    keyData[j] = _mm_setr_epi32(sched[4 * j], sched[4 * j + 1], sched[4 * j + 2], sched[4 * j + 3]);
  }
  return rc;
}

static int
aesGenKeyEncrypt(const unsigned char* userKey, const int bits, unsigned char* keyData)
{
  int numberOfRounds = (bits == 128) ? 10 : (bits == 192) ? 12 : (bits == 256) ? 14 : 0;
  int rc = (!userKey || !keyData) ? -1 : (numberOfRounds > 0) ? 0 : -2;
  
  if (rc == 0)
  {
    __m128i tempKey[_MAX_ROUNDS + 1];
    rc = aesGenKeyEncryptInternal(userKey, bits, tempKey);
    if (rc == 0)
    {
      int j;
      for (j = 0; j <= numberOfRounds; ++j)
      {
        _mm_storeu_si128(&((__m128i*) keyData)[j], tempKey[j]);
      }
    }
  }
  return rc;
}

static int
aesGenKeyDecrypt(const unsigned char* userKey, const int bits, unsigned char* keyData)
{
  int numberOfRounds = (bits == 128) ? 10 : (bits == 192) ? 12 : (bits == 256) ? 14 : 0;
  int rc = (!userKey || !keyData) ? -1 : (numberOfRounds > 0) ? 0 : -2;

  if (rc == 0)
  {
    __m128i tempKeySchedule[_MAX_ROUNDS + 1];
    __m128i keySchedule[_MAX_ROUNDS + 1];
    rc = aesGenKeyEncryptInternal(userKey, bits, tempKeySchedule);
    if (rc == 0)
    {
      int j;
      keySchedule[0] = tempKeySchedule[0];
      for (j = 1; j < numberOfRounds; ++j)
      {
        keySchedule[j] = _mm_aesimc_si128(tempKeySchedule[j]);
      }
      keySchedule[numberOfRounds] = tempKeySchedule[numberOfRounds];

      for (j = 0; j <= numberOfRounds; ++j)
      {
        _mm_storeu_si128(&((__m128i*) keyData)[j], keySchedule[j]);
      }
    }
  }
  return rc;
}

/*
** AES CBC CTS Encryption
*/

static void
aesEncryptCBC(const unsigned char* in,
              unsigned char* out,
              unsigned char ivec[16],
              unsigned long length,
              const unsigned char* keyData,
              int numberOfRounds)
{
  __m128i key[_MAX_ROUNDS + 1];
  __m128i feedback;
  __m128i data;
  unsigned long i;
  int j;
  unsigned long numBlocks = length / 16;
  unsigned long lenFrag = (length % 16);

  /* Load key data into properly aligned local storage */
  for (j = 0; j <= numberOfRounds; ++j)
  {
    key[j] = _mm_loadu_si128(&((__m128i*) keyData)[j]);
  }

  /* Encrypt all complete blocks */
  feedback = _mm_loadu_si128((__m128i*) ivec);
  for (i = 0; i < numBlocks; ++i)
  {
    data = _mm_loadu_si128(&((__m128i*) in)[i]);
    feedback = _mm_xor_si128(data, feedback);

    feedback = _mm_xor_si128(feedback, key[0]);
    for (j = 1; j < numberOfRounds; j++)
    {
      feedback = _mm_aesenc_si128(feedback, key[j]);
    }
    feedback = _mm_aesenclast_si128(feedback, key[j]);
    _mm_storeu_si128(&((__m128i*) out)[i], feedback);
  }

  /* Use Cipher Text Stealing (CTS) for incomplete last block */
  if (lenFrag > 0)
  {
    UINT8 lastblock[16];
    UINT8 partialblock[16];
    /* Adjust the second last plain block. */
    memcpy(lastblock, &out[16*(numBlocks-1)], lenFrag);
    /* Encrypt the last plain block. */
    memset(partialblock, 0, 16);
    memcpy(partialblock, &in[16*numBlocks], lenFrag);

    data = _mm_loadu_si128(&((__m128i*) partialblock)[0]);
    feedback = _mm_xor_si128(data, feedback);

    feedback = _mm_xor_si128(feedback, key[0]);
    for (j = 1; j < numberOfRounds; j++)
    {
      feedback = _mm_aesenc_si128(feedback, key[j]);
    }
    feedback = _mm_aesenclast_si128(feedback, key[j]);
    _mm_storeu_si128(&((__m128i*) out)[numBlocks-1], feedback);

    memcpy(&out[16*numBlocks], lastblock, lenFrag);
  }
}

/*
** AES CBC CTS decryption
*/
static void
aesDecryptCBC(const unsigned char* in,
              unsigned char* out,
              unsigned char ivec[16],
              unsigned long length,
              const unsigned char* keyData,
              int numberOfRounds)
{
  __m128i key[_MAX_ROUNDS + 1];
  __m128i data;
  __m128i feedback;
  __m128i last_in;
  unsigned long i;
  int j;
  unsigned long numBlocks = length / 16;
  unsigned long lenFrag = (length % 16);

  /* Load key data into properly aligned local storage */
  for (j = 0; j <= numberOfRounds; ++j)
  {
    key[j] = _mm_loadu_si128(&((__m128i*) keyData)[j]);
  }

  /* Use Cipher Text Stealing (CTS) for incomplete last block */
  if (lenFrag > 0)
  {
    UINT8 lastblock[16];
    UINT8 partialblock[16];
    int offset;
    --numBlocks;
    offset = numBlocks * 16;
 
    /* Decrypt the last plain block. */
    last_in = _mm_loadu_si128(&((__m128i*) in)[numBlocks]);
    data = _mm_xor_si128(last_in, key[numberOfRounds - 0]);
    for (j = 1; j < numberOfRounds; j++)
    {
      data = _mm_aesdec_si128(data, key[numberOfRounds - j]);
    }
    data = _mm_aesdeclast_si128(data, key[numberOfRounds - j]);
    _mm_storeu_si128(&((__m128i*) partialblock)[0], data);

    memcpy(partialblock, &in[16 * numBlocks + 16], lenFrag);
    last_in = _mm_loadu_si128(&((__m128i*) partialblock)[0]);

    data = _mm_xor_si128(data, last_in);
    _mm_storeu_si128(&((__m128i*) lastblock)[0], data);

    /* Decrypt the second last block. */
    data = _mm_xor_si128(last_in, key[numberOfRounds - 0]);
    for (j = 1; j < numberOfRounds; j++)
    {
      data = _mm_aesdec_si128(data, key[numberOfRounds - j]);
    }
    data = _mm_aesdeclast_si128(data, key[numberOfRounds - j]);

    if (numBlocks > 0)
    {
      feedback = _mm_loadu_si128(&((__m128i*) in)[numBlocks - 1]);
    }
    else
    {
      feedback = _mm_loadu_si128((__m128i*) ivec);
    }
    data = _mm_xor_si128(data, feedback);
    _mm_storeu_si128(&((__m128i*) out)[numBlocks], data);

    memcpy(out + offset + 16, lastblock, lenFrag);
  }

  /* Encrypt all complete blocks */
  feedback = _mm_loadu_si128((__m128i*) ivec);
  for (i = 0; i < numBlocks; i++)
  {
    last_in =_mm_loadu_si128(&((__m128i*) in)[i]);
    data = _mm_xor_si128(last_in, key[numberOfRounds - 0]);
    for (j = 1; j < numberOfRounds; j++)
    {
      data = _mm_aesdec_si128(data, key[numberOfRounds - j]);
    }
    data = _mm_aesdeclast_si128(data, key[numberOfRounds - j]);
    data = _mm_xor_si128(data, feedback);
    _mm_storeu_si128(&((__m128i*) out)[i], data);
    feedback = last_in;
  }
}

#elif HAS_AES_HARDWARE == AES_HARDWARE_NEON
/* --- Implementation for AES-NEON --- */

/* Set target architecture manually, if necessary */
#ifdef USE_CLANG_ATTR_TARGET_AARCH64
#define __ARM_NEON 1
#define __ARM_FEATURE_CRYPTO 1
#define __ARM_FEATURE_AES 1
#define FUNC_ISA __attribute__ ((target("neon,crypto")))
#endif /* USE_CLANG_ATTR_TARGET_AARCH64 */

/* FUNCtion attributes for ISA (Instruction Set Architecture) */
#ifndef FUNC_ISA
#define FUNC_ISA
#endif

#ifdef USE_ARM64_NEON_H
#include <arm64_neon.h>
#else
#include <arm_neon.h>
#endif

#if defined(__linux__) && (defined(__arm__) || defined(__aarch64__))

#include <sys/auxv.h>
#include <asm/hwcap.h>

static int
aesHardwareAvailableOnPlatform()
{
#if defined HWCAP_AES
  return getauxval(AT_HWCAP) & HWCAP_AES;
#elif defined HWCAP2_AES
  return getauxval(AT_HWCAP2) & HWCAP2_AES;
#else
  return 0;
#endif
}

#elif defined _M_ARM || defined _M_ARM64

static int
aesHardwareAvailableOnPlatform()
{
  return (int) IsProcessorFeaturePresent(PF_ARM_V8_CRYPTO_INSTRUCTIONS_AVAILABLE);
}

#else

static int
aesHardwareAvailableOnPlatform()
{
  return 0;
}

#endif

static int
aesHardwareCheck()
{
  return aesHardwareAvailableOnPlatform();
}

/*
** Set up expanded key
*/

static FUNC_ISA int
aesGenKeyEncryptInternal(const unsigned char* userKey, const int bits, uint8x16_t* keyData)
{
  int rc = 0;
  int i;
  int j;
  int numberOfRounds = (bits == 128) ? 10 : (bits == 192) ? 12 : (bits == 256) ? 14 : 0;
  int keyWords = bits / 32;  
  int schedWords = (numberOfRounds + 1) * 4;

  /*
  ** Key schedule stored as 32-bit integers during expansion.
  ** Final uint8x16_t form produced in the last step.
  */
  uint32_t sched[(_MAX_ROUNDS + 1) * 4];

  unsigned roundConstantPos = 0;

  for (i = 0; i < schedWords; i++)
  {
    if (i < keyWords)
    {
      sched[i] = toUint32FromLE(userKey + 4 * i);
    }
    else
    {
      uint32_t temp = sched[i - 1];

      int rotateAndRoundConstant = (i % keyWords == 0);
      int sub = rotateAndRoundConstant || (keyWords == 8 && i % 8 == 4);

      if (rotateAndRoundConstant)
      {
        temp = (temp << 24) | (temp >> 8);
      }

      if (sub)
      {
        uint32x4_t v32 = vdupq_n_u32(temp);
        uint8x16_t v8 = vreinterpretq_u8_u32(v32);
        v8 = vaeseq_u8(v8, vdupq_n_u8(0));
        v32 = vreinterpretq_u32_u8(v8);
        temp = vget_lane_u32(vget_low_u32(v32), 0);
      }

      if (rotateAndRoundConstant)
      {
        temp ^= gKeySetupRoundConstants[roundConstantPos++];
      }

      sched[i] = sched[i - keyWords] ^ temp;
    }
  }

  /* Convert key schedule words into uint8x16_t vectors */
  for (j = 0; j <= numberOfRounds; j++)
  {
    keyData[j] = vreinterpretq_u8_u32(vld1q_u32(sched + 4*j));
  }

  return rc;
}

static FUNC_ISA int
aesGenKeyEncrypt(const unsigned char* userKey, const int bits, unsigned char* keyData)
{
  int numberOfRounds = (bits == 128) ? 10 : (bits == 192) ? 12 : (bits == 256) ? 14 : 0;
  int rc = (!userKey || !keyData) ? -1 : (numberOfRounds > 0) ? 0 : -2;
  
  if (rc == 0)
  {
    uint8x16_t tempKey[_MAX_ROUNDS + 1];
    rc = aesGenKeyEncryptInternal(userKey, bits, tempKey);
    if (rc == 0)
    {
      int j;
      for (j = 0; j <= numberOfRounds; ++j)
      {
        vst1q_u8(&keyData[j*16], tempKey[j]);
      }
    }
  }
  return rc;
}

static FUNC_ISA int
aesGenKeyDecrypt(const unsigned char* userKey, const int bits, unsigned char* keyData)
{
  int numberOfRounds = (bits == 128) ? 10 : (bits == 192) ? 12 : (bits == 256) ? 14 : 0;
  int rc = (!userKey || !keyData) ? -1 : (numberOfRounds > 0) ? 0 : -2;

  if (rc == 0)
  {
    uint8x16_t tempKeySchedule[_MAX_ROUNDS + 1];
    uint8x16_t keySchedule[_MAX_ROUNDS + 1];
    rc = aesGenKeyEncryptInternal(userKey, bits, tempKeySchedule);
    if (rc == 0)
    {
      int j;
      keySchedule[0] = tempKeySchedule[0];

      for (j = 1; j < numberOfRounds; ++j)
      {
        keySchedule[j] = vaesimcq_u8(tempKeySchedule[j]);
      }
      keySchedule[numberOfRounds] = tempKeySchedule[numberOfRounds];

      for (j = 0; j <= numberOfRounds; ++j)
      {
        vst1q_u8(&keyData[j*16], keySchedule[j]);
      }
    }
  }
  return rc;
}

/*
** AES CBC CTS Encryption
*/
static FUNC_ISA void
aesEncryptCBC(const unsigned char* in,
              unsigned char* out,
              unsigned char ivec[16],
              unsigned long length,
              const unsigned char* keyData,
              int numberOfRounds)
{
  uint8x16_t key[_MAX_ROUNDS + 1];
  uint8x16_t feedback;
  uint8x16_t data;
  unsigned long i;
  int j;
  unsigned long numBlocks = length / 16;
  unsigned long lenFrag = (length % 16);

  /* Load key data into properly aligned local storage */
  for (j = 0; j <= numberOfRounds; ++j)
  {
    key[j] = vld1q_u8(&keyData[j*16]);
  }

  /* Encrypt all complete blocks */
  feedback = vld1q_u8(ivec);
  for (i = 0; i < numBlocks; ++i)
  {
    data = vld1q_u8(&in[i*16]);
    feedback = veorq_u8(data, feedback);

    for (j = 0; j < numberOfRounds-1; j++)
    {
      feedback = vaesmcq_u8(vaeseq_u8(feedback, key[j]));
    }
    feedback = vaeseq_u8(feedback, key[numberOfRounds-1]);
    feedback = veorq_u8(feedback, key[numberOfRounds]);                          \

    vst1q_u8(&out[i*16], feedback);
  }

  /* Use Cipher Text Stealing (CTS) for incomplete last block */
  if (lenFrag > 0)
  {
    UINT8 lastblock[16];
    UINT8 partialblock[16];
    /* Adjust the second last plain block. */
    memcpy(lastblock, &out[(numBlocks-1)*16], lenFrag);
    /* Encrypt the last plain block. */
    memset(partialblock, 0, 16);
    memcpy(partialblock, &in[numBlocks*16], lenFrag);

    data = vld1q_u8(partialblock);
    feedback = veorq_u8(data, feedback);

    for (j = 0; j < numberOfRounds-1; j++)
    {
      feedback = vaesmcq_u8(vaeseq_u8(feedback, key[j]));
    }
    feedback = vaeseq_u8(feedback, key[numberOfRounds-1]);
    feedback = veorq_u8(feedback, key[numberOfRounds]);                          \
    
    vst1q_u8(&out[(numBlocks-1)*16], feedback);

    memcpy(&out[numBlocks*16], lastblock, lenFrag);
  }
}

/*
** AES CBC CTS decryption
*/
static FUNC_ISA void
aesDecryptCBC(const unsigned char* in,
              unsigned char* out,
              unsigned char ivec[16],
              unsigned long length,
              const unsigned char* keyData,
              int numberOfRounds)
{
  uint8x16_t key[_MAX_ROUNDS + 1];
  uint8x16_t data;
  uint8x16_t feedback;
  uint8x16_t last_in;
  unsigned long i;
  int j;
  unsigned long numBlocks = length / 16;
  unsigned long lenFrag = (length % 16);

  /* Load key data into properly aligned local storage */
  for (j = 0; j <= numberOfRounds; ++j)
  {
    key[j] = vld1q_u8(&keyData[j*16]);
  }

  /* Use Cipher Text Stealing (CTS) for incomplete last block */
  if (lenFrag > 0)
  {
    UINT8 lastblock[16];
    UINT8 partialblock[16];
    int offset;
    --numBlocks;
    offset = numBlocks * 16;
 
    /* Decrypt the last plain block. */
    last_in = vld1q_u8(&in[numBlocks*16]);

    data = last_in;
    for (j = 0; j < numberOfRounds-1; j++)
    {
      data = vaesimcq_u8(vaesdq_u8(data, key[numberOfRounds-j]));
    }
    data = vaesdq_u8(data, key[1]);
    data = veorq_u8(data, key[0]);

    vst1q_u8(partialblock, data);

    memcpy(partialblock, &in[(numBlocks + 1)*16], lenFrag);
    last_in = vld1q_u8(partialblock);

    data = veorq_u8(data, last_in);
    vst1q_u8(lastblock, data);

    /* Decrypt the second last block. */
    data = last_in;
    for (j = 0; j < numberOfRounds-1; j++)
    {
      data = vaesimcq_u8(vaesdq_u8(data, key[numberOfRounds-j]));
    }
    data = vaesdq_u8(data, key[1]);
    data = veorq_u8(data, key[0]);

    if (numBlocks > 0)
    {
      feedback = vld1q_u8(&in[(numBlocks - 1)*16]);
    }
    else
    {
      feedback = vld1q_u8(ivec);
    }
    data = veorq_u8(data, feedback);
    vst1q_u8(&out[numBlocks*16], data);

    memcpy(out + offset + 16, lastblock, lenFrag);
  }

  /* Decrypt all complete blocks */
  feedback = vld1q_u8(ivec);
  for (i = 0; i < numBlocks; i++)
  {
    last_in = vld1q_u8(&in[i*16]);

    data = last_in;
    for (j = 0; j < numberOfRounds-1; j++)
    {
      data = vaesimcq_u8(vaesdq_u8(data, key[numberOfRounds-j]));
    }
    data = vaesdq_u8(data, key[1]);
    data = veorq_u8(data, key[0]);

    data = veorq_u8(data, feedback);
    vst1q_u8(&out[i*16], data);

    feedback = last_in;
  }
}

#else
/* --- No AES hardware available --- */

static int
aesHardwareCheck()
{
  return 0;
}

#endif

/*
** The top-level selection function, caching the results of
** aesHardwareCheck() so it only has to run once.
*/
static int
aesHardwareAvailable()
{
  static int initialized = 0;
  static int hwAvailable = 0;
  if (!initialized)
  {
    hwAvailable = aesHardwareCheck();
    initialized = 1;
  }
  return hwAvailable;
}
