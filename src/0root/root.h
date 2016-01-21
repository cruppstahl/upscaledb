/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * The root of all evil. This header file must be included *before all others*!
 *
 * @thread_safe: yes
 * @exception_safe: nothrow
 */

#ifndef UPS_ROOT_H
#define UPS_ROOT_H

//#define UPS_ENABLE_HELGRIND       1

// some feature macros in config.h must be set *before* inclusion
// of any system headers to have the desired effect.
// assume sane default values if there is no config.h.
#ifdef HAVE_CONFIG_H
#   include "../config.h"
#else
#   define HAVE_MMAP              1
#   define HAVE_UNMMAP            1
#   define HAVE_PREAD             1
#   define HAVE_PWRITE            1
#endif

#include "ups/types.h"

// check for a valid build
#if (!defined(UPS_DEBUG))
#   if (defined(_DEBUG) || defined(DEBUG))
#     define UPS_DEBUG 1
#   endif
#endif

// the default cache size is 2 MB
#define UPS_DEFAULT_CACHE_SIZE    (2 * 1024 * 1024)

// the default page size is 16 kb
#define UPS_DEFAULT_PAGE_SIZE     (16 * 1024)

// boost/asio has nasty build dependencies and requires Windows.h,
// therefore it is included here
#ifdef WIN32
#  define WIN32_LEAN_AND_MEAN
#  include <Windows.h>
#  include <boost/asio.hpp>
#  include <boost/thread/thread.hpp>
#endif

// use tcmalloc?
#if HAVE_GOOGLE_TCMALLOC_H == 1
#  if HAVE_LIBTCMALLOC_MINIMAL == 1
#    define UPS_USE_TCMALLOC 1
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

// MSVC: disable warning about use of 'this' in base member initializer list
#ifdef WIN32
#  pragma warning(disable:4355)
#endif

// some compilers define min and max as macros; this leads to errors
// when using std::min and std::max
#ifdef min
#  undef min
#endif

#ifdef max
#  undef max
#endif

// a macro to cast pointers to u64 and vice versa to avoid compiler
// warnings if the sizes of ptr and u64 are not equal
#if defined(UPS_32BIT) && (!defined(_MSC_VER))
#   define U64_TO_PTR(p)  (uint8_t *)(int)p
#   define PTR_TO_U64(p)  (uint64_t)(int)p
#else
#   define U64_TO_PTR(p)  p
#   define PTR_TO_U64(p)  p
#endif

#endif /* UPS_ROOT_H */
