/*
** Name:        zlibwrap.h
** Purpose:     Include wrapper for miniz.h
** Author:      Ulrich Telle
** Created:     2022-05-09
** Copyright:   (c) 2022 Ulrich Telle
** License:     MIT
*/

/// \file zlibwrap.h Include wrapper for using miniz.h instead of the original zlib.h

#ifndef SQLITE3MC_ZLIBWRAP_H_
#define SQLITE3MC_ZLIBWRAP_H_

#if SQLITE3MC_USE_MINIZ != 0
#include "miniz.h"
#else
#include <zlib.h>
#endif


#endif /* SQLITE3MC_ZLIBWRAP_H_ */
