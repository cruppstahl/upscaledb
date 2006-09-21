/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for licence information
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

/** 
 * copy a key
 * returns 0 if memory can not be allocated, or a pointer to @a dest.
 * uses ham_malloc() - memory in dest->key has to be freed by the caller
 */
extern ham_key_t *
util_copy_key(const ham_key_t *source, ham_key_t *dest);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_UTIL_H__ */
