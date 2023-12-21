/*
** Name:        pbkdf2.c
** Purpose:     Implementation of PBKDF2 algoritm with Ascon
** Based on:    Public domain Ascon reference implementation
**              and optimized variants for 32- and 64-bit
**              (see https://github.com/ascon/ascon-c)
**              and the paper "Additional Modes for ASCON Version 1.1"
**              by Rhys Weatherley, Southern Storm Software, Pty Ltd.
** Remarks:     API functions adapted for use in SQLite3 Multiple Ciphers
** Created by:  Ulrich Telle
** Copyright:   (c) 2023-2023 Ulrich Telle
** License:     MIT
*/

#define ASCON_HASH_SIZE 32
#define ASCON_PBKDF2_SIZE 32

void ascon_pbkdf2_init(ascon_state_t* state, const char* functionName,
                       const unsigned char* custom, uint32_t customlen, uint32_t outlen)
{
  /* Format the initial block with the function name and output length */
  uint8_t initial[ASCON_HASH_SIZE];
  size_t fnLength = functionName ? strlen(functionName) : 0;

  if (fnLength == 0)
  {
    /* No function name specified */
    memset(initial, 0, ASCON_HASH_SIZE);
  }
  else if (fnLength <= ASCON_HASH_SIZE)
  {
    /* Pad the function name with zeroes */
    memcpy(initial, functionName, fnLength);
    memset(initial + fnLength, 0, ASCON_HASH_SIZE - fnLength);
  }
  else
  {
    ascon_hash(initial, (const uint8_t*) functionName, fnLength);
  }

  state->x[0] = ASCON_HASH_IV;
  state->x[1] = ASCON_LOAD(initial, 8);
  state->x[2] = ASCON_LOAD(initial + 8, 8);
  state->x[3] = ASCON_LOAD(initial + 16, 8);
  state->x[4] = ASCON_LOAD(initial + 24, 8);
  ASCON_P(state, 12);

  if (customlen > 0)
  {
    ascon_absorb(state, custom, customlen);
    ASCON_P(state, 12);
    /* domain separation */
    state->x[4] ^= 1;
  }
}

/*
 * Implementation of the "F" function from RFC 8018, section 5.2
 *
 * Note: Instead of HMAC like in RFC 8018, use the following PRF:
 * PRF(P, X) = ASCON-cXOF(X, 256, "PBKDF2", P)
 */
static void ascon_pbkdf2_f(ascon_state_t* state,
                           uint8_t* T, /*uint8_t* U,*/
                           const uint8_t* salt, uint32_t saltlen,
                           uint32_t count, uint32_t blocknum)
{
  uint32_t asconSaltLen = (saltlen < ASCON_SALT_LEN) ? saltlen : ASCON_SALT_LEN;
  uint8_t temp[ASCON_SALT_LEN+4];
  ascon_state_t state2;
  int j;

  memset(temp, 0, ASCON_SALT_LEN);
  memcpy(temp, salt, asconSaltLen);
  STORE32_BE(temp+ASCON_SALT_LEN, blocknum);
  
  /* Copy initial state */
  for (j = 0; j < 5; ++j) state2.x[j] = state->x[j];

  ascon_absorb(&state2, temp, ASCON_SALT_LEN+4);
  ascon_squeeze(&state2, T, ASCON_PBKDF2_SIZE);
  sqlite3mcSecureZeroMemory(temp, sizeof(temp));

  if (count > 1)
  {
    uint8_t U[ASCON_PBKDF2_SIZE];
    memcpy(U, T, ASCON_PBKDF2_SIZE);
    while (count > 1)
    {
      uint8_t* dst = T;
      uint8_t* src = U;
      uint32_t len = ASCON_PBKDF2_SIZE;
      /* Copy initial state */
      for (j = 0; j < 5; ++j) state2.x[j] = state->x[j];
      /* Absorb U */
      ascon_absorb(&state2, U, ASCON_PBKDF2_SIZE);
      /* Squeeze next U */
      ascon_squeeze(&state2, U, ASCON_PBKDF2_SIZE);
      /* XOR T with U */
      while (len > 0)
      {
        *dst++ ^= *src++;
        --len;
      }
      --count;
    }
    sqlite3mcSecureZeroMemory(U, sizeof(U));
  }
  sqlite3mcSecureZeroMemory(&state2, sizeof(ascon_state_t));
}

void ascon_pbkdf2(uint8_t* out, uint32_t outlen,
                  const uint8_t* password, uint32_t passwordlen,
                  const uint8_t* salt, uint32_t saltlen, uint32_t count)
{
  ascon_state_t state;
  uint32_t blocknum = 1;
  ascon_pbkdf2_init(&state, "PBKDF2", password, passwordlen, ASCON_PBKDF2_SIZE);
  while (outlen > 0)
  {
    if (outlen >= ASCON_PBKDF2_SIZE)
    {
      ascon_pbkdf2_f(&state, out, /*U,*/ salt, saltlen, count, blocknum);
      out += ASCON_PBKDF2_SIZE;
      outlen -= ASCON_PBKDF2_SIZE;
    }
    else
    {
      uint8_t T[ASCON_PBKDF2_SIZE];
      ascon_pbkdf2_f(&state, T, /*U,*/ salt, saltlen, count, blocknum);
      memcpy(out, T, outlen);
      sqlite3mcSecureZeroMemory(T, sizeof(T));
      break;
    }
    ++blocknum;
  }
}
