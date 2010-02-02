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
 * @brief utility functions
 *
 */

#ifndef HAM_UTIL_H__
#define HAM_UTIL_H__

#include "internal_fwd_decl.h"

#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * vsnprintf replacement/wrapper
 * 
 * uses sprintf on platforms which do not define snprintf
 */
extern int
util_vsnprintf(char *str, size_t size, const char *format, va_list ap);

/**
 * snprintf replacement/wrapper
 * 
 * uses sprintf on platforms which do not define snprintf
 */
#ifndef HAM_OS_POSIX
#define util_snprintf _snprintf
#else
#define util_snprintf snprintf
#endif

/** 
 * copy a key
 *
 * returns 0 if memory can not be allocated, or a pointer to @a dest.
 * uses ham_mem_malloc() - memory in dest->key has to be freed by the caller
 * 
 * @a dest must have been initialized before calling this function; the 
 * dest->data space will be reused when the specified size is large enough;
 * otherwise the old dest->data will be ham_mem_free()d and a new space 
 * allocated.
 * 
 * This can save superfluous heap free+allocation actions in there.
 * 
 * @note
 * This routine can cope with HAM_KEY_USER_ALLOC-ated 'dest'-inations.
 * 
 * @note
 * When a NULL reference is returned (an error occurred) the 'dest->data' 
 * pointer is either NULL or still pointing at allocated space (when 
 * HAM_KEY_USER_ALLOC was not set).
 */
extern ham_status_t
util_copy_key(ham_db_t *db, const ham_key_t *source, ham_key_t *dest);

/**
 * same as above, but copies a internal int_key_t structure
 */
extern ham_status_t
util_copy_key_int2pub(ham_db_t *db, const int_key_t *source, ham_key_t *dest);

/**
 * read a record 
 *
 * flags: either 0 or HAM_DIRECT_ACCESS
 */
extern ham_status_t
util_read_record(ham_db_t *db, ham_record_t *record, ham_u32_t flags);

/**
 * read a key
 *
 * @a dest must have been initialized before calling this function; the 
 * dest->data space will be reused when the specified size is large enough;
 * otherwise the old dest->data will be ham_mem_free()d and a new 
 * space allocated.
 *
 * This can save superfluous heap free+allocation actions in there.
 *
 * @note
 * This routine can cope with HAM_KEY_USER_ALLOC-ated 'dest'-inations.
 */
extern ham_status_t
util_read_key(ham_db_t *db, int_key_t *source, ham_key_t *dest);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_UTIL_H__ */
