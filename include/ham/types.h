/*
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * This file contains portable typedefs.
 *
 */

#ifndef HAM_TYPES_H__
#define HAM_TYPES_H__

#ifdef __cplusplus
extern "C" {
#endif 

/*
 * check the operating system and word size
 */
#ifdef WIN32
#    undef  HAM_OS_WIN32
#    define HAM_OS_WIN32 1
#    ifdef _WIN64
#        undef  HAM_64BIT
#        define HAM_64BIT 1
#    elif _WIN32
#        undef  HAM_32BIT
#        define HAM_32BIT 1
#    elif WIN32
#        undef  HAM_32BIT
#        define HAM_32BIT 1
#    else
#        error "Neither WIN32, _WIN32 nor _WIN64 defined!"
#    endif
#else /* posix? */
#    undef  HAM_OS_POSIX
#    define HAM_OS_POSIX 1
#    if defined(__LP64__) || defined(__LP64) || __WORDSIZE==64
#        undef  HAM_64BIT
#        define HAM_64BIT 1
#    else
#        undef  HAM_32BIT
#        define HAM_32BIT 1
#    endif
#endif

#if defined(HAM_OS_POSIX) && defined(HAM_OS_WIN32)
#    error "Unknown arch - neither HAM_OS_POSIX nor HAM_OS_WIN32 defined"
#endif

/*
 * need windows.h for HANDLE
 */
#ifdef HAM_OS_WIN32
#    define WIN32_MEAN_AND_LEAN
#    include <windows.h>
#endif

/*
 * create the EXPORT-macro for Microsoft Visual C++
 */
#ifdef _MSC_VER
#    define HAM_EXPORT __declspec(dllexport)
#else
#    define HAM_EXPORT extern
#endif

/*
 * typedefs for 32bit operating systems
 */
#ifdef HAM_32BIT
#    ifdef WIN32
typedef signed __int64     ham_s64_t;
typedef unsigned __int64   ham_u64_t;
#else
typedef signed long long   ham_s64_t;
typedef unsigned long long ham_u64_t;
#endif
typedef signed int         ham_s32_t;
typedef unsigned int       ham_u32_t;
typedef signed short       ham_s16_t;
typedef unsigned short     ham_u16_t;
typedef signed char        ham_s8_t;
typedef unsigned char      ham_u8_t;
#endif

/* 
 * typedefs for 64bit operating systems
 */
#ifdef HAM_64BIT
typedef signed long        ham_s64_t;
typedef unsigned long      ham_u64_t;
typedef signed int         ham_s32_t;
typedef unsigned int       ham_u32_t;
typedef signed short       ham_s16_t;
typedef unsigned short     ham_u16_t;
typedef signed char        ham_s8_t;
typedef unsigned char      ham_u8_t;
#endif 

/*
 * undefine macros to avoid macro redefinitions
 */
#undef HAM_INVALID_FD
#undef HAM_FALSE
#undef HAM_TRUE

/* 
 * typedefs for posix
 */
#ifdef HAM_OS_POSIX
typedef int                ham_fd_t;
#   define HAM_INVALID_FD  (-1)
#endif 

/* 
 * typedefs for Windows 32- and 64-bit
 */
#ifdef HAM_OS_WIN32
#   ifdef CYGWIN
typedef int                ham_fd_t; 
#   else
typedef HANDLE             ham_fd_t; 
#   endif
#   define HAM_INVALID_FD  (0)
#endif 

/* 
 * a boolean type
 */
typedef int                ham_bool_t;
#define HAM_FALSE          0
#define HAM_TRUE           (!HAM_FALSE)

/* 
 * typedef for error- and status-code
 */
typedef int                ham_status_t;

/* 
 * typedef for addressing the file; this limits the file size
 * to 64 bit. should be enough, hopefully...
 *
 * @remark if you change this datatype, then you also have to change
 * the endian-macros in src/endian.h (ham_db2h_offset/ham_h2db_offset)
 */
typedef ham_u64_t          ham_offset_t;

/*
 * typedef for sizes; this limits data blobs to 32 bits
 *
 * @remark if you change this datatype, then you also have to change
 * the endian-macros in src/endian.h (ham_db2h_size/ham_h2db_size)
 */
typedef ham_u32_t          ham_size_t;

/*
 * typedef for a prefix-compare-function
 *
 * @remark this function compares two index keys; it returns -1, if lhs
 * ("left-hand side", the paramter on the left side) is smaller than 
 * rhs ("right-hand side"), 0 if both keys are equal, and 1 if lhs 
 * is larger than rhs.
 *
 * @remark if one of the keys is loaded only partially, but the comparison
 * function needs the full key, the return value should be
 * HAM_PREFIX_REQUEST_FULLKEY.
 */
typedef int (*ham_prefix_compare_func_t)
                                 (const ham_u8_t *lhs, ham_size_t lhs_length, 
                                  ham_size_t lhs_real_length,
                                  const ham_u8_t *rhs, ham_size_t rhs_length,
                                  ham_size_t rhs_real_length);

/*
 * typedef for a compare-function
 *
 * @remark this function compares two index keys; it returns -1, if lhs
 * ("left-hand side", the paramter on the left side) is smaller than 
 * rhs ("right-hand side"), 0 if both keys are equal, and 1 if lhs 
 * is larger than rhs.
 */
typedef int (*ham_compare_func_t)(const ham_u8_t *lhs, ham_size_t lhs_length, 
                                  const ham_u8_t *rhs, ham_size_t rhs_length);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_TYPES_H__ */
