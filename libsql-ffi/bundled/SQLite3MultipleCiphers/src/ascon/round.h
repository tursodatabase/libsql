/*
** Name:        round.h
** Purpose:     Selector for Ascon implementation variant for 32- resp 64-bit
** Based on:    Public domain Ascon reference implementation
**              and optimized variants for 32- and 64-bit
**              (see https://github.com/ascon/ascon-c)
** Remarks:     API functions adapted for use in SQLite3 Multiple Ciphers
** Modified by: Ulrich Telle
** Copyright:   (c) 2023-2023 Ulrich Telle
** License:     MIT
*/

#ifndef ROUND_H
#define ROUND_H

#include "forceinline.h"

#if defined(__LP64__) || defined(_WIN64)
/* 64-bit machine, Windows or Linux or OS X */
#include "round64.h"
#else
/* 32-bit machine, Windows or Linux or OS X */
#include "round32.h"
#endif

#endif
