/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for licence information
 *
 * this file contains portable typedefs
 *
 */

#ifndef HAM_TYPES_H__
#define HAM_TYPES_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/config.h>

/** 
 * typedefs for 32bit operating systems
 */
#ifdef HAM_32BIT
typedef signed long long   ham_s64_t;
typedef unsigned long long ham_u64_t;
typedef signed int         ham_s32_t;
typedef unsigned int       ham_u32_t;
typedef signed short       ham_s16_t;
typedef unsigned short     ham_u16_t;
typedef signed char        ham_s8_t;
typedef unsigned char      ham_u8_t;
#endif

/** 
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

/** 
 * typedefs for linux/unix
 */
#if (HAM_OS_POSIX)
typedef int                ham_fd_t;
#endif 

/** 
 * typedefs for windows 32- and 64-bit
 */
#if (HAM_OS_WIN)
typedef HANDLE             ham_fd_t; 
#endif 

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

/** 
 * typedef for addressing the file; this limits the file size
 * to 64 bit. should be enough, hopefully...
 *
 * @remark if you change this datatype, then you also have to change
 * the endian-macros in src/endian.h (ham_db2h_offset/ham_h2db_offset)
 */
typedef ham_u64_t          ham_offset_t;

/**
 * typedef for sizes; this limits data blobs to 32 bits
 *
 * @remark if you change this datatype, then you also have to change
 * the endian-macros in src/endian.h (ham_db2h_size/ham_h2db_size)
 */
typedef ham_u32_t          ham_size_t;

/**
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

/**
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
