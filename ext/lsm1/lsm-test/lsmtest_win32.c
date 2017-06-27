
#include "lsmtest.h"

#ifdef _WIN32

#define TICKS_PER_SECOND      (10000000)
#define TICKS_PER_MICROSECOND (10)
#define TICKS_UNIX_EPOCH      (116444736000000000LL)

int win32GetTimeOfDay(
  struct timeval *tp,
  void *tzp
){
  FILETIME fileTime;
  ULARGE_INTEGER largeInteger;
  ULONGLONG temp;

  unused_parameter(tzp);
  memset(&fileTime, 0, sizeof(FILETIME));
  GetSystemTimeAsFileTime(&fileTime);
  memset(&largeInteger, 0, sizeof(ULARGE_INTEGER));
  largeInteger.LowPart = fileTime.dwLowDateTime;
  largeInteger.HighPart = fileTime.dwHighDateTime;
  temp = largeInteger.QuadPart - TICKS_UNIX_EPOCH;
  tp->tv_sec = (long)(temp / TICKS_PER_SECOND);
  temp -= ((ULONGLONG)tp->tv_sec * TICKS_PER_SECOND);
  tp->tv_usec = (long)(temp / TICKS_PER_MICROSECOND);
  return 0;
}
#endif
