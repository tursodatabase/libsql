/*
** Name:        cpu.h
** Purpose:     Header for CPU identification and AES hardware support detection
** Copyright:   (c) 2023-2024 Frank Denis
** SPDX-License-Identifier: MIT
*/

#ifndef AEGIS_CPU_H
#define AEGIS_CPU_H

#include "aeshardware.h"

AEGIS_PRIVATE
int aegis_runtime_get_cpu_features(void);

AEGIS_PRIVATE
int aegis_runtime_has_neon(void);

AEGIS_PRIVATE
int aegis_runtime_has_armcrypto(void);

AEGIS_PRIVATE
int aegis_runtime_has_avx(void);

AEGIS_PRIVATE
int aegis_runtime_has_avx2(void);

AEGIS_PRIVATE
int aegis_runtime_has_avx512f(void);

AEGIS_PRIVATE
int aegis_runtime_has_aesni(void);

AEGIS_PRIVATE
int aegis_runtime_has_vaes(void);

AEGIS_PRIVATE
int aegis_runtime_has_altivec(void);

#endif /* AEGIS_CPU_H */
