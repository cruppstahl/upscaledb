/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

/*
 * The root of all evil. This header file must be included *before all others*!
 *
 * @thread_safe: yes
 * @exception_safe: nothrow
 */

#ifndef HAM_ROOT_H
#define HAM_ROOT_H

#define HAM_ENABLE_HELGRIND       1

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

#include "ham/types.h"

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

#ifdef WIN32
// MSVC: disable warning about use of 'this' in base member initializer list
#  pragma warning(disable:4355)
#  define WIN32_MEAN_AND_LEAN
#  include <windows.h>
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
#if defined(HAM_32BIT) && (!defined(_MSC_VER))
#   define U64_TO_PTR(p)  (uint8_t *)(int)p
#   define PTR_TO_U64(p)  (uint64_t)(int)p
#else
#   define U64_TO_PTR(p)  p
#   define PTR_TO_U64(p)  p
#endif

/* macros for the evaluation version; will be filled in by a script */
#define EVAL_PREPARE
#define EVAL_CHECK
#define EVAL_HELLO
#define EVAL_RETURN

#endif /* HAM_ROOT_H */
