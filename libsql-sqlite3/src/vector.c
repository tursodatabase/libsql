#ifdef SQLITE_OMIT_FLOATING_POINT
void sqlite3RegisterVectorFunctions(void) {
}
#else

#include "sqliteInt.h"
#include <sqlite3.h>
#include <stdlib.h>
#include <assert.h>
#include <math.h>
#include "vdbeInt.h"

static inline bool
vector_isspace(char ch)
{
        if (ch == ' ' ||
                ch == '\t' ||
                ch == '\n' ||
                ch == '\r' ||
                ch == '\v' ||
                ch == '\f')
                return true;
        return false;
}

#define MAX_VECTOR_SZ 16000
#define MAX_FLOAT_CHAR_SZ  1024

// FIXME: Write big endian versions.
unsigned serialize_u32(const unsigned char *mem, u32 num) {
  *(u32 *)mem = num;
  return sizeof(u32);
}

unsigned serialize_double(const unsigned char *mem, double num) {
  *(double *)mem = num;
  return sizeof(double);
}

u32 deserialize_u32(const unsigned char *mem) {
  return *(u32 *)mem;
}

double deserialize_double(const unsigned char *mem) {
  return *(double *)mem;
}

void serialize_vector(
  sqlite3_context *context,
  double *vector,
  unsigned vecidx
){
  unsigned char *blob;
  unsigned char *blobPtr;
  unsigned int blobSz;

  blobSz = sizeof(u32) + vecidx * sizeof(double);
  blob = contextMalloc(context, blobSz);

  if( blob ){
    unsigned i;

    blobPtr = blob;
    blobPtr += serialize_u32(blobPtr, vecidx);

    for (i = 0; i < vecidx; i++) {
      blobPtr += serialize_double(blobPtr, vector[i]);
    }
    sqlite3_result_blob(context, (char*)blob, blobSz, sqlite3_free);
  } else {
    sqlite3_result_error_nomem(context);
  }
}

int is_integer(float num) {
    return num == (u64)num;
}

unsigned format_double(double num, char *str) {
  char tmp[32];
  if (is_integer(num)) {
    return snprintf(tmp, 32, "%lld", (u64)num);
  } else {
    return snprintf(tmp, 32, "%.6e", num);
  }
}

void deserialize_vector(
  sqlite3_context *context,
  double *vector,
  unsigned vecSz 
){
  unsigned bufSz;
  unsigned bufIdx = 0;
  char *z;

  bufSz = 2 + vecSz * 33;
  z = contextMalloc(context, bufSz);

  if( z ){
    unsigned i;

    z[bufIdx++]= '[';
    for (i = 0; i < vecSz; i++) { 
      char tmp[12];
      unsigned bytes = format_double(vector[i], tmp);
      memcpy(&z[bufIdx], tmp, bytes);
      bufIdx += strlen(tmp);
      z[bufIdx++] = ',';
    }
    bufIdx--;
    z[bufIdx++] = ']';

    sqlite3_result_text(context, z, bufIdx, sqlite3_free);
  } else {
    sqlite3_result_error_nomem(context);
  }
}

/*
** The to_vector(str) function returns a blob-representation of a string containing a vector
*/
static void tovectorFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  const unsigned char *zStr;
  char zErr[128];

  char elBuf[MAX_FLOAT_CHAR_SZ];
  char badConversion[MAX_FLOAT_CHAR_SZ];
  double vector[MAX_VECTOR_SZ];
  double el;

  int bufidx = 0;
  int vecidx = 0;

  assert( argc==1 );
  UNUSED_PARAMETER(argc);

  zStr = sqlite3_value_text(argv[0]);

  while (zStr && vector_isspace(*zStr))
    zStr++;

  if( zStr==0 ) return;

  if (*zStr != '[') {
    sqlite3_snprintf(sizeof(zErr), zErr, "invalid vector: doesn't start with ']':");
    goto error;
  }
  zStr++;

  memset(elBuf, 0, sizeof(elBuf));
  memset(badConversion, 0, sizeof(badConversion));
  badConversion[1] = '.';

  while (zStr != NULL && *zStr != '\0' && *zStr != ']') {
    char this = *zStr++;

    if (vector_isspace(this)) {
      continue;
    }

    if (this != ',' && this != ']') {
      elBuf[bufidx++] = this;
      if (bufidx > MAX_FLOAT_CHAR_SZ) {
        char zErr[MAX_FLOAT_CHAR_SZ+100];
        sqlite3_snprintf(sizeof(zErr), zErr, "float too big while parsing vector: %s...", elBuf);
        goto error;
      }
    } else {
      if (sqlite3AtoF(elBuf, &el, bufidx, SQLITE_UTF8) <= 0) {
        sqlite3_snprintf(sizeof(zErr), zErr, "invalid number: %s...", elBuf);
        goto error;
      }
      bufidx = 0;
      memset(elBuf, 0, sizeof(elBuf));
      vector[vecidx++] = el;
      if (vecidx >= MAX_VECTOR_SZ) {
        sqlite3_snprintf(sizeof(zErr), zErr, "vector is larger than the maximum: (%d)", MAX_VECTOR_SZ);
        goto error;
      }
    }
  }

  if (bufidx != 0) {
    if (sqlite3AtoF(elBuf, &el, bufidx, SQLITE_UTF8) <= 0) {
      sqlite3_snprintf(sizeof(zErr), zErr, "invalid number: %s...", elBuf);
      goto error;
    }
    vector[vecidx++] = el;
    if (vecidx >= MAX_VECTOR_SZ) {
      sqlite3_snprintf(sizeof(zErr), zErr, "vector is larger than the maximum: (%d)", MAX_VECTOR_SZ);
      goto error;
    }
  }

  if (zStr && *zStr!= ']') {
    sqlite3_snprintf(sizeof(zErr), zErr, "malformed vector, doesn't end with ']'");
    goto error;
  }

  serialize_vector(context, vector, vecidx);
  return;
error:
  sqlite3_result_error(context, zErr, -1);
}

/*
** The to_vector(str) function returns a blob-representation of a string containing a vector
*/
static void fromvectorFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  char zErr[128];
  unsigned vecSz;
  int typeVector;
  const unsigned char *zStr;
  double *vector;
  unsigned i;

  assert( argc==1 );
  UNUSED_PARAMETER(argc);
  typeVector = sqlite3_value_type(argv[0]);
  if (typeVector != SQLITE_BLOB) {
    sqlite3_snprintf(sizeof(zErr), zErr, "invalid vector: not a blob type");
    goto error;
  }

  zStr = sqlite3_value_blob(argv[0]);
  if (!zStr) {
    sqlite3_result_text(context, (char*)"[]", strlen("[]"), SQLITE_TRANSIENT);
    return;
  }

  vecSz = deserialize_u32(zStr);
  zStr += sizeof(u32);

  if (vecSz > MAX_VECTOR_SZ) {
    sqlite3_snprintf(sizeof(zErr), zErr, "invalid vector: too large: %d", vecSz);
    goto error;
  }

  if ( vecSz == 0 ) {
    sqlite3_result_text(context, "[]", 2, 0);
    return;
  }

  vector = contextMalloc(context, vecSz);
  if (!vector) {
    sqlite3_result_error_nomem(context);
    goto error;
  }

  for (i = 0; i < vecSz; i++) {
    if (zStr == NULL) {
      sqlite3_snprintf(sizeof(zErr), zErr, "malformed blob");
      goto errfree;
    }

    vector[i] = deserialize_double(zStr);
    zStr += sizeof(double);
  }

  deserialize_vector(context, vector, vecSz);
  sqlite3_free(vector);
  return;
errfree:
  sqlite3_free(vector);
error:
  sqlite3_result_error(context, zErr, -1);
}

/*
** This function registered all of the above C functions as SQL
** functions.  This should be the only routine in this file with
** external linkage.
*/
void sqlite3RegisterVectorFunctions(void) {
  static FuncDef aVectorFuncs[] = {
    FUNCTION(to_vector,          1, 0, 0, tovectorFunc     ),
    FUNCTION(from_vector,        1, 0, 0, fromvectorFunc     ),
  };
  sqlite3InsertBuiltinFuncs(aVectorFuncs, ArraySize(aVectorFuncs));
}
#endif
