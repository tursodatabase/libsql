/*
** This file contains implementations of the vector_concat and vector_slice functions.
** It is included by vector.c and not compiled separately.
*/

#ifndef VECTOR_FUNC_IMPL_C
#define VECTOR_FUNC_IMPL_C

/*
** Implementation of vector_concat(X, Y) function.
** Concatenates two vectors of same type.
*/
static void vectorConcatFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  char *pzErrMsg = NULL;
  Vector *pVector1 = NULL, *pVector2 = NULL, *pTarget = NULL;
  int type1, dims1, type2, dims2;

  if( argc != 2 ){
    sqlite3_result_error(context, "vector_concat requires exactly two arguments", -1);
    goto out;
  }

  /* Parse first vector */
  if( detectVectorParameters(argv[0], 0, &type1, &dims1, &pzErrMsg) != 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out;
  }
  pVector1 = vectorContextAlloc(context, type1, dims1);
  if( pVector1 == NULL ){
    goto out;
  }
  if( vectorParseWithType(argv[0], pVector1, &pzErrMsg) < 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out;
  }

  /* Parse second vector */
  if( detectVectorParameters(argv[1], 0, &type2, &dims2, &pzErrMsg) != 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out;
  }
  pVector2 = vectorContextAlloc(context, type2, dims2);
  if( pVector2 == NULL ){
    goto out;
  }
  if( vectorParseWithType(argv[1], pVector2, &pzErrMsg) < 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out;
  }

  /* Check if both vectors are of the same type */
  if( type1 != type2 ){
    sqlite3_result_error(context, "vector_concat: vectors must be of the same type", -1);
    goto out;
  }

  /* Allocate target vector */
  pTarget = vectorContextAlloc(context, type1, dims1 + dims2);
  if( pTarget == NULL ){
    goto out;
  }

  /* Copy data from both vectors into the target vector */
  switch( type1 ){
    case VECTOR_TYPE_FLOAT32: {
      float *pDst = (float*)pTarget->data;
      float *pSrc1 = (float*)pVector1->data;
      float *pSrc2 = (float*)pVector2->data;
      memcpy(pDst, pSrc1, dims1 * sizeof(float));
      memcpy(pDst + dims1, pSrc2, dims2 * sizeof(float));
      break;
    }
    case VECTOR_TYPE_FLOAT64: {
      double *pDst = (double*)pTarget->data;
      double *pSrc1 = (double*)pVector1->data;
      double *pSrc2 = (double*)pVector2->data;
      memcpy(pDst, pSrc1, dims1 * sizeof(double));
      memcpy(pDst + dims1, pSrc2, dims2 * sizeof(double));
      break;
    }
    case VECTOR_TYPE_FLOAT1BIT: {
      u8 *pDst = (u8*)pTarget->data;
      u8 *pSrc1 = (u8*)pVector1->data;
      u8 *pSrc2 = (u8*)pVector2->data;
      size_t size1 = (dims1 + 7) / 8;
      size_t size2 = (dims2 + 7) / 8;
      memcpy(pDst, pSrc1, size1);
      memcpy(pDst + size1, pSrc2, size2);
      break;
    }
    case VECTOR_TYPE_FLOAT8: {
      u8 *pDst = (u8*)pTarget->data;
      u8 *pSrc1 = (u8*)pVector1->data;
      u8 *pSrc2 = (u8*)pVector2->data;
      size_t size1 = dims1;
      size_t size2 = dims2;
      memcpy(pDst, pSrc1, size1);
      memcpy(pDst + size1, pSrc2, size2);
      
      /* Copy parameters (alpha and shift) from the first vector */
      float *pParams1 = (float*)(pSrc1 + ALIGN(dims1, sizeof(float)));
      float *pParams = (float*)(pDst + ALIGN(dims1 + dims2, sizeof(float)));
      memcpy(pParams, pParams1, 2 * sizeof(float));
      break;
    }
    case VECTOR_TYPE_FLOAT16: {
      u16 *pDst = (u16*)pTarget->data;
      u16 *pSrc1 = (u16*)pVector1->data;
      u16 *pSrc2 = (u16*)pVector2->data;
      memcpy(pDst, pSrc1, dims1 * sizeof(u16));
      memcpy(pDst + dims1, pSrc2, dims2 * sizeof(u16));
      break;
    }
    case VECTOR_TYPE_FLOATB16: {
      u16 *pDst = (u16*)pTarget->data;
      u16 *pSrc1 = (u16*)pVector1->data;
      u16 *pSrc2 = (u16*)pVector2->data;
      memcpy(pDst, pSrc1, dims1 * sizeof(u16));
      memcpy(pDst + dims1, pSrc2, dims2 * sizeof(u16));
      break;
    }
    default:
      sqlite3_result_error(context, "vector_concat: unsupported vector type", -1);
      goto out;
  }

  vectorSerializeWithMeta(context, pTarget);

out:
  if( pTarget ){
    vectorFree(pTarget);
  }
  if( pVector2 ){
    vectorFree(pVector2);
  }
  if( pVector1 ){
    vectorFree(pVector1);
  }
}

/*
** Implementation of vector_slice(X, start_idx, end_idx) function.
** Extracts a subvector from start_idx (inclusive) to end_idx (exclusive).
*/
static void vectorSliceFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  char *pzErrMsg = NULL;
  Vector *pVector = NULL, *pTarget = NULL;
  int type, dims;
  sqlite3_int64 start_idx, end_idx;
  int new_dims;

  if( argc != 3 ){
    sqlite3_result_error(context, "vector_slice requires exactly three arguments", -1);
    goto out;
  }

  /* Parse the vector */
  if( detectVectorParameters(argv[0], 0, &type, &dims, &pzErrMsg) != 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out;
  }
  pVector = vectorContextAlloc(context, type, dims);
  if( pVector == NULL ){
    goto out;
  }
  if( vectorParseWithType(argv[0], pVector, &pzErrMsg) < 0 ){
    sqlite3_result_error(context, pzErrMsg, -1);
    sqlite3_free(pzErrMsg);
    goto out;
  }

  /* Get start and end indices */
  if( sqlite3_value_type(argv[1]) != SQLITE_INTEGER ){
    sqlite3_result_error(context, "vector_slice: start_idx must be an integer", -1);
    goto out;
  }
  start_idx = sqlite3_value_int64(argv[1]);

  if( sqlite3_value_type(argv[2]) != SQLITE_INTEGER ){
    sqlite3_result_error(context, "vector_slice: end_idx must be an integer", -1);
    goto out;
  }
  end_idx = sqlite3_value_int64(argv[2]);

  /* Validate indices */
  if( start_idx < 0 || end_idx < 0 ){
    sqlite3_result_error(context, "vector_slice: indices must be non-negative", -1);
    goto out;
  }

  if( start_idx > end_idx ){
    sqlite3_result_error(context, "vector_slice: start_idx must not be greater than end_idx", -1);
    goto out;
  }

  if( start_idx >= dims || end_idx > dims ){
    sqlite3_result_error(context, "vector_slice: indices out of bounds", -1);
    goto out;
  }

  new_dims = (int)(end_idx - start_idx);
  pTarget = vectorContextAlloc(context, type, new_dims);
  if( pTarget == NULL ){
    goto out;
  }

  /* Copy the appropriate slice of data */
  switch( type ){
    case VECTOR_TYPE_FLOAT32: {
      float *pDst = (float*)pTarget->data;
      float *pSrc = (float*)pVector->data;
      memcpy(pDst, pSrc + start_idx, new_dims * sizeof(float));
      break;
    }
    case VECTOR_TYPE_FLOAT64: {
      double *pDst = (double*)pTarget->data;
      double *pSrc = (double*)pVector->data;
      memcpy(pDst, pSrc + start_idx, new_dims * sizeof(double));
      break;
    }
    case VECTOR_TYPE_FLOAT1BIT: {
      /* For FLOAT1BIT, we need bit-by-bit extraction, which is more complex */
      sqlite3_result_error(context, "vector_slice: FLOAT1BIT vectors not yet supported", -1);
      goto out;
    }
    case VECTOR_TYPE_FLOAT8: {
      /* For FLOAT8, copy data and parameters */
      u8 *pDst = (u8*)pTarget->data;
      u8 *pSrc = (u8*)pVector->data;
      memcpy(pDst, pSrc + start_idx, new_dims);
      
      /* Copy parameters (alpha and shift) */
      float *pParams = (float*)(pSrc + ALIGN(dims, sizeof(float)));
      float *pNewParams = (float*)(pDst + ALIGN(new_dims, sizeof(float)));
      memcpy(pNewParams, pParams, 2 * sizeof(float));
      break;
    }
    case VECTOR_TYPE_FLOAT16: {
      u16 *pDst = (u16*)pTarget->data;
      u16 *pSrc = (u16*)pVector->data;
      memcpy(pDst, pSrc + start_idx, new_dims * sizeof(u16));
      break;
    }
    case VECTOR_TYPE_FLOATB16: {
      u16 *pDst = (u16*)pTarget->data;
      u16 *pSrc = (u16*)pVector->data;
      memcpy(pDst, pSrc + start_idx, new_dims * sizeof(u16));
      break;
    }
    default:
      sqlite3_result_error(context, "vector_slice: unsupported vector type", -1);
      goto out;
  }

  vectorSerializeWithMeta(context, pTarget);

out:
  if( pTarget ){
    vectorFree(pTarget);
  }
  if( pVector ){
    vectorFree(pVector);
  }
}

#endif /* VECTOR_FUNC_IMPL_C */
