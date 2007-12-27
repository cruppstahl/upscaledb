/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 *
 * utility functions
 *
 */

#ifndef HAM_UTIL_H__
#define HAM_UTIL_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>
#include "db.h"
#include "keys.h"

/** 
 * copy a key
 * returns 0 if memory can not be allocated, or a pointer to @a dest.
 * uses ham_malloc() - memory in dest->key has to be freed by the caller
 */
extern ham_key_t *
util_copy_key(ham_db_t *db, const ham_key_t *source, ham_key_t *dest);

/**
 * same as above, but copies a internal int_key_t structure
 */
extern ham_key_t *
util_copy_key_int2pub(ham_db_t *db, const int_key_t *source, ham_key_t *dest);

/**
 * read a record 
 */
extern ham_status_t
util_read_record(ham_db_t *db, ham_record_t *record, ham_u32_t flags);

/**
 * read a key
 */
extern ham_status_t
util_read_key(ham_db_t *db, int_key_t *source, ham_key_t *dest);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_UTIL_H__ */
