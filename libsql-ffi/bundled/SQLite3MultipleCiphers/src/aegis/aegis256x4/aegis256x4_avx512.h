/*
** Name:        aegis256x4_avx512.h
** Purpose:     Header for implementation structure of AEGIS-256x4 - AES-NI AVX512
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS256X4_AVX512_H
#define AEGIS256X4_AVX512_H

#include "../common/common.h"
#include "implementations.h"

#ifdef HAVE_VAESINTRIN_H
extern struct aegis256x4_implementation aegis256x4_avx512_implementation;
#endif

#endif /* AEGIS256X4_AVX512_H */
