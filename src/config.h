/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * this file describes the configuration of hamster - serial number,
 * enabled features etc.
 *
 */

#ifndef __HAM_CONFIG_H__
#define __HAM_CONFIG_H__

#include <ham/types.h>

/*
 * include autoconf header file; otherwise, assume sane default values
 */
#ifdef HAVE_CONFIG_H
#   include "../config.h"
#else
#   ifdef UNDER_CE
#       define HAVE_MMAP        0
#       define HAVE_UNMMAP        0
#   else
#       define HAVE_MMAP        1
#       define HAVE_UNMMAP        1
#   endif
#   define HAVE_PREAD           1
#   define HAVE_PWRITE          1
#endif

/*
 * check for a valid build
 */
#if (!defined(HAM_DEBUG))
#   if (defined(_DEBUG) || defined(DEBUG))
#     define HAM_DEBUG 1
#       if !defined(HAM_LEAN_AND_MEAN_FOR_PROFILING)
//#       define HAM_LEAN_AND_MEAN_FOR_PROFILING 1
#     endif
#   endif
#endif

/*
 * the endian-architecture of the host computer; set this to
 * HAM_LITTLE_ENDIAN or HAM_BIG_ENDIAN
 */
#ifndef HAM_LITTLE_ENDIAN
#   ifndef HAM_BIG_ENDIAN
#     error "neither HAM_LITTLE_ENDIAN nor HAM_BIG_ENDIAN defined"
#   endif
#endif

/*
 * the default cache size is 2 MB
 */
#define HAM_DEFAULT_CACHESIZE    (2*1024*1024)


#endif /* __HAM_CONFIG_H__ */
