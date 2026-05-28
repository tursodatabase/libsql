/*
** Name:        aegis256x4_avx2.h
** Purpose:     Header for implementation structure of AEGIS-256x4 - AES-NI AVX2
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS256X4_AVX2_H
#define AEGIS256X4_AVX2_H

#include "../common/common.h"
#include "implementations.h"

#ifdef HAVE_VAESINTRIN_H
extern struct aegis256x4_implementation aegis256x4_avx2_implementation;
#endif

#endif /* AEGIS256X4_AVX2_H */
