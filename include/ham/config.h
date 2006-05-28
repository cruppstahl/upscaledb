/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * this file describes the configuration of hamster - serial number, 
 * enabled features etc. 
 *
 */

#ifndef HAM_CONFIG_H__
#define HAM_CONFIG_H__

#ifdef __cplusplus
extern "C" {
#endif 


/**
 * check for a valid build
 */
#if (!defined(HAM_DEBUG) && !defined(HAM_RELEASE))
#   ifdef _DEBUG
#       define HAM_DEBUG 1
#   else
#       define HAM_RELEASE 1
#   endif
#endif

/**
 * is this a 64bit or a 32bit-platform? for 32bit, define HAM_32BIT;
 * for 64bit, define HAM_64BIT
 */
#define HAM_64BIT 1

/**
 * is this a POSIX/Un*x-platform or Microsoft Windows? for POSIX/Un*x, 
 * define HAM_OS_POSIX; for Windows, define HAM_OS_WIN
 */
#define HAM_OS_POSIX 1

/** 
 * the serial number; for non-commercial versions, this is always
 * 0x0; commercial versions get a serial number from the vendor
 */
#define HAM_SERIALNO               0x0

/** 
 * the endian-architecture of the host computer; set this to 
 * HAM_BIG_ENDIAN if necessary
 */
#define HAM_LITTLE_ENDIAN          1

/**
 * feature list; describes the features that are enabled or 
 * disabled
 */
#define HAM_HAS_BTREE              1
#define HAM_HAS_HASHDB             1

/**
 * the default pagesize is 1kb
 */
#define HAM_DEFAULT_PAGESIZE       (1024*1)

/**
 * the default cache size is 1mb
 */
#define HAM_DEFAULT_CACHESIZE      (1024*1024)


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CONFIG_H__ */
