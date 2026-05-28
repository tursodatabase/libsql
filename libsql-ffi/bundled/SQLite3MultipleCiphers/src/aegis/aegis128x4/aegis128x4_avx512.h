/*
** Name:        aegis128x4_avx512.h
** Purpose:     Header for implementation structure of AEGIS-128x4 - AES-NI AVX512
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS128X4_AVX512_H
#define AEGIS128X4_AVX512_H

#include "../common/common.h"
#include "implementations.h"

#ifdef HAVE_VAESINTRIN_H
extern struct aegis128x4_implementation aegis128x4_avx512_implementation;
#endif

#endif /* AEGIS128X4_AVX512_H */
