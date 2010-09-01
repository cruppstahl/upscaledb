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
 * this file contains macros for little endian/big endian byte swapping.
 * the database is always in little-endian. 
 *
 */

#ifndef HAM_ENDIANSWAP_H__
#define HAM_ENDIANSWAP_H__

#ifdef __cplusplus
extern "C" {
#endif 


/** 
 * byte swapping macros - we use little endian
 */
#ifdef HAM_BIG_ENDIAN
#   define ham_h2db16(x)      _ham_byteswap16(x)
#   define ham_h2db32(x)      _ham_byteswap32(x)
#   define ham_h2db64(x)      _ham_byteswap64(x)
#   define ham_h2db_offset(x) _ham_byteswap64(x)
#   define ham_h2db_size(x)   _ham_byteswap32(x)
#   define ham_db2h16(x)      _ham_byteswap16(x)
#   define ham_db2h32(x)      _ham_byteswap32(x)
#   define ham_db2h64(x)      _ham_byteswap64(x)
#   define ham_db2h_offset(x) _ham_byteswap64(x)
#   define ham_db2h_size(x)   _ham_byteswap32(x)
#else /* HAM_LITTLE_ENDIAN */
#   define ham_h2db16(x)      (x)
#   define ham_h2db32(x)      (x)
#   define ham_h2db64(x)      (x)
#   define ham_h2db_offset(x) (x)
#   define ham_h2db_size(x)   (x)
#   define ham_db2h16(x)      (x)
#   define ham_db2h32(x)      (x)
#   define ham_db2h64(x)      (x)
#   define ham_db2h_offset(x) (x)
#   define ham_db2h_size(x)   (x)
#endif

#define _ham_byteswap16(x)                       \
        ((((x) >> 8) & 0xff) |                   \
         (((x) & 0xff) << 8))

#define _ham_byteswap32(x)                       \
        ((((x) & 0xff000000) >> 24) |            \
         (((x) & 0x00ff0000) >>  8) |            \
         (((x) & 0x0000ff00) <<  8) |            \
         (((x) & 0x000000ff) << 24))

#define _ham_byteswap64(x)                       \
        ((((x) & 0xff00000000000000ull) >> 56) | \
         (((x) & 0x00ff000000000000ull) >> 40) | \
         (((x) & 0x0000ff0000000000ull) >> 24) | \
         (((x) & 0x000000ff00000000ull) >>  8) | \
         (((x) & 0x00000000ff000000ull) <<  8) | \
         (((x) & 0x0000000000ff0000ull) << 24) | \
         (((x) & 0x000000000000ff00ull) << 40) | \
         (((x) & 0x00000000000000ffull) << 56))


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_ENDIANSWAP_H__ */
