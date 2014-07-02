/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

#ifndef HAM_CONFIG_H__
#define HAM_CONFIG_H__

#include <ham/types.h>

// include autoconf header file; otherwise, assume sane default values
#ifdef HAVE_CONFIG_H
#   include "../config.h"
#else
#   ifdef UNDER_CE
#       define HAVE_MMAP          0
#       define HAVE_UNMMAP        0
#   else
#       define HAVE_MMAP          1
#       define HAVE_UNMMAP        1
#   endif
#   define HAVE_PREAD             1
#   define HAVE_PWRITE            1
#endif

// check for a valid build
#if (!defined(HAM_DEBUG))
#   if (defined(_DEBUG) || defined(DEBUG))
#     define HAM_DEBUG 1
#   endif
#endif

// the default cache size is 2 MB
#define HAM_DEFAULT_CACHE_SIZE    (2 * 1024 * 1024)

// the default page size is 16 kb
#define HAM_DEFAULT_PAGE_SIZE     (16 * 1024)

// use tcmalloc?
#if HAVE_GOOGLE_TCMALLOC_H == 1
#  if HAVE_LIBTCMALLOC_MINIMAL == 1
#    define HAM_USE_TCMALLOC 1
#  endif
#endif

#include <stddef.h>
#define OFFSETOF(type, member) offsetof(type, member)

// helper macros to improve CPU branch prediction
#if defined __GNUC__
#   define likely(x) __builtin_expect ((x), 1)
#   define unlikely(x) __builtin_expect ((x), 0)
#else
#   define likely(x) (x)
#   define unlikely(x) (x)
#endif

#endif /* HAM_CONFIG_H__ */
