/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
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

// The endian-architecture of the host computer; set this to
// HAM_LITTLE_ENDIAN or HAM_BIG_ENDIAN. Little endian is the
// default setting
#ifndef HAM_LITTLE_ENDIAN
#   ifndef HAM_BIG_ENDIAN
#       ifdef WORDS_BIGENDIAN
#           define HAM_BIG_ENDIAN           1
#       else // default
#           define HAM_LITTLE_ENDIAN        1
#       endif
#   endif
#endif

// the default cache size is 2 MB
#define HAM_DEFAULT_CACHESIZE    (2 * 1024 * 1024)

// the default page size is 16 kb
#define HAM_DEFAULT_PAGESIZE     (16 * 1024)

// use tcmalloc?
#if HAVE_GOOGLE_TCMALLOC_H == 1
#  if HAVE_LIBTCMALLOC_MINIMAL == 1
#    define HAM_USE_TCMALLOC 1
#  endif
#endif

#include <stddef.h>
#define OFFSETOF(type, member) offsetof(type, member)

#endif /* HAM_CONFIG_H__ */
