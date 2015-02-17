/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file types.h
 * @brief Portable typedefs for hamsterdb Embedded Storage.
 * @author Christoph Rupp, chris@crupp.de
 *
 */

#ifndef HAM_TYPES_H
#define HAM_TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Check the operating system and word size
 */
#ifdef WIN32
#  undef  HAM_OS_WIN32
#  define HAM_OS_WIN32 1
#  ifdef WIN64
#    undef  HAM_64BIT
#    define HAM_64BIT 1
#  elif WIN32
#    undef  HAM_32BIT
#    define HAM_32BIT 1
#  else
#    error "Neither WIN32 nor WIN64 defined!"
#  endif
#else /* posix? */
#  undef  HAM_OS_POSIX
#  define HAM_OS_POSIX 1
#  if defined(__LP64__) || defined(__LP64) || __WORDSIZE == 64
#    undef  HAM_64BIT
#    define HAM_64BIT 1
#  else
#    undef  HAM_32BIT
#    define HAM_32BIT 1
#  endif
#endif

#if defined(HAM_OS_POSIX) && defined(HAM_OS_WIN32)
#  error "Unknown arch - neither HAM_OS_POSIX nor HAM_OS_WIN32 defined"
#endif

/*
 * improve memory debugging on WIN32 by using crtdbg.h (only MSVC
 * compiler and debug builds!)
 *
 * make sure crtdbg.h is loaded before malloc.h!
 */
#if defined(_MSC_VER) && defined(HAM_OS_WIN32)
#  if (defined(WIN32) || defined(__WIN32)) && !defined(UNDER_CE)
#    if defined(DEBUG) || defined(_DEBUG)
#      ifndef _CRTDBG_MAP_ALLOC
#        define _CRTDBG_MAP_ALLOC 1
#      endif
#    endif
#  include <crtdbg.h>
#  include <malloc.h>
#  endif
#endif

/*
 * Create the EXPORT macro for Microsoft Visual C++
 */
#ifndef HAM_EXPORT
#  ifdef _MSC_VER
#    define HAM_EXPORT __declspec(dllexport)
#  else
#    define HAM_EXPORT extern
#  endif
#endif

/*
 * The default calling convention is cdecl
 */
#ifndef HAM_CALLCONV
#  define HAM_CALLCONV
#endif

/*
 * Common typedefs. Since stdint.h is not available on older versions of
 * Microsoft Visual Studio, they get declared here.
 * http://msinttypes.googlecode.com/svn/trunk/stdint.h
 */
#if _MSC_VER
#  include <ham/msstdint.h>
#else
#  include <stdint.h>
#endif

/* Deprecated typedefs; used prior to 2.1.9. Please do not use them! */
typedef int64_t     ham_s64_t;
typedef uint64_t    ham_u64_t;
typedef int32_t     ham_s32_t;
typedef uint32_t    ham_u32_t;
typedef int16_t     ham_s16_t;
typedef uint16_t    ham_u16_t;
typedef int8_t      ham_s8_t;
typedef uint8_t     ham_u8_t;

/*
 * Undefine macros to avoid macro redefinitions
 */
#undef HAM_INVALID_FD
#undef HAM_FALSE
#undef HAM_TRUE

/**
 * a boolean type
 */
typedef int                ham_bool_t;
#define HAM_FALSE          0
#define HAM_TRUE           (!HAM_FALSE)

/**
 * typedef for error- and status-code
 */
typedef int                ham_status_t;


#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_TYPES_H */
